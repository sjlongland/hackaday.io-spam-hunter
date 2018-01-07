import logging
import datetime
import pytz

import re

from tornado.gen import coroutine, Return

from ..hadapi.hadapi import UserSortBy
from ..db.model import User, Group, GroupMember, Session, UserDetail, \
        UserLink, Avatar, Tag, UserTag


# Patterns to look for:
CHECK_PATTERNS = (
        re.compile(r'<a .*href=".*">'),     # Hyperlink
        re.compile(r'\([0-9]+\)[ 0-9\-]+'), # US-style telephone number
        re.compile(r'\+[0-9]+[ 0-9\-]+'),   # International telephone number
        re.compile(r'\+[0-9]+ *\([0-9]+\)[ 0-9\-]+'),  # Hybrid telephone
)

class Crawler(object):
    def __init__(self, db, api, client, log):
        self._log = log
        self._db = db
        self._api = api
        self._client = client

        self._page_last_refresh = {}

    def get_avatar(self, avatar_url):
        avatar = self._db.query(Avatar).filter(
                Avatar.url==avatar_url).first()
        if avatar is None:
            avatar = Avatar(url=avatar_url,
                        avatar_type='',
                        avatar=b'')
            self._db.add(avatar)
            self._db.commit()
        return avatar

    @coroutine
    def fetch_avatar(self, avatar):
        # Do we have their avatar on file?
        if isinstance(avatar, str):
            avatar = self.get_avatar(avatar)

        if not avatar.avatar_type:
            # We don't have the avatar yet
            self._log.debug('Retrieving avatar at %s',
                    avatar.url)
            avatar_res = yield self._client.fetch(
                    avatar.url)
            avatar.avatar_type = avatar_res.headers['Content-Type']
            avatar.avatar=avatar_res.body
            self._db.commit()

        raise Return(avatar)

    @coroutine
    def _inspect_user(self, user_data, user=None):
        """
        Inspect the user, see if they're worth investigating.
        """
        try:
            if user is None:
                user = self._db.query(User).get(user_data['id'])

            if user.last_update is not None:
                age = datetime.datetime.now(tz=pytz.utc) - user.last_update;
                if age.total_seconds() < 300:
                    return

            # Does the user have any hyperlinks or other patterns in their
            # profile?
            self._log.debug('Inspecting user %s [#%d]',
                user_data['screen_name'], user_data['id'])
            match = False
            for pattern in CHECK_PATTERNS:
                if match:
                    break
                for field in ('about_me', 'who_am_i', 'location'):
                    pmatch = pattern.match(user_data[field])
                    if pmatch:
                        self._log.info('Found match for %s (%r) in '\
                                '%s of %s [#%d]',
                                pattern.pattern, pmatch.group(0), field,
                                user_data['screen_name'], user_data['id'])
                        match = True
                        break

            # Does the user have any hyperlinks?  Not an indicator that they're
            # a spammer, just one of the traits.
            pg_idx = 1
            pg_cnt = 1  # Don't know how many right now, but let's start here
            while pg_idx <= pg_cnt:
                link_res = yield self._api.get_user_links(user.user_id,
                        page=pg_idx, per_page=50)

                if link_res['links'] == 0:
                    # No links, yes sometimes it's an integer.
                    break

                try:
                    for link in link_res['links']:
                        # Do we have the link already?
                        l = self._db.query(UserLink).filter(
                                UserLink.user_id==user.user_id,
                                UserLink.url==link['url']).first()
                        if l is None:
                            # Record the link
                            self._log.info('User %s [#%d] has link to %s <%s>',
                                    user_data['screen_name'], user_data['id'],
                                    link['title'], link['url'])

                            l = UserLink(user_id=user.user_id,
                                        title=link['title'],
                                        url=link['url'])
                            self._db.add(l)
                        else:
                            l.title = link['title']

                        match = True
                except:
                    self._log.error('Failed to process link result %r', link_res)
                    raise
                pg_cnt = link_res['last_page']

                # Next page
                pg_idx = link_res['page'] + 1

            if match:
                # Record the user information
                detail = self._db.query(UserDetail).get(user_data['id'])
                if detail is None:
                    detail = UserDetail(
                            user_id=user_data['id'],
                            about_me=user_data['about_me'],
                            who_am_i=user_data['who_am_i'],
                            location=user_data['location'])
                    self._db.add(detail)
                else:
                    detail.about_me = user_data['about_me']
                    detail.who_am_i = user_data['who_am_i']
                    detail.location = user_data['location']
        except:
            self._log.error('Failed to process user data %r', user_data)
            raise

    @coroutine
    def update_user_from_data(self, user_data, inspect_all=True):
        """
        Update a user in the database from data retrieved via the API.
        """
        avatar = self.get_avatar(user_data['image_url'])

        # Look up the user in the database
        user = self._db.query(User).get(user_data['id'])
        if user is None:
            # New user
            user = User(user_id=user_data['id'],
                        screen_name=user_data['screen_name'],
                        url=user_data['url'],
                        avatar_id=avatar.avatar_id,
                        created=datetime.datetime.fromtimestamp(
                            user_data['created'], tz=pytz.utc)
            self._db.add(user)
        else:
            # Existing user, update the user details
            user.screen_name = user_data['screen_name']
            user.avatar_id=avatar.avatar_id
            user.url = user_data['url']

        # Inspect the user
        if inspect_all or (user.last_update is None):
            yield self._inspect_user(user_data, user=user)
            user.last_update = datetime.datetime.now(tz=pytz.utc)
        self._db.commit()

        raise Return(user)

    @coroutine
    def fetch_new_users(self, page=1, inspect_all=False):
        """
        Retrieve new users from the Hackaday.io API and inspect the new arrivals.
        Returns the list of users on the given page and the total number of pages.
        """
        users = []

        while len(users) < 10:
            now = datetime.datetime.now(tz=pytz.utc)
            if page > 10:
                last_refresh = self._page_last_refresh.get(page)
                if (last_refresh is not None) and \
                        ((now - last_refresh).total_seconds() < 3600.0):
                    # Skip this page for now
                    page += 1
                    continue

            new_user_data = yield self._api.get_users(sortby=UserSortBy.newest,
                    page=page, per_page=50)
            self._page_last_refresh[page] = now

            for user_data in new_user_data['users']:
                user = yield self.update_user_from_data(user_data, inspect_all)
                if (now - user.last_update).total_seconds() > 300.0:
                    continue
                users.append(user)
            page += 1

        raise Return((users, page, new_user_data.get('last_page')))
