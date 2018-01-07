import logging
import datetime
import pytz

import re

from tornado.gen import coroutine, Return
from tornado.ioloop import IOLoop

from ..hadapi.hadapi import UserSortBy
from ..db.model import User, Group, Session, UserDetail, \
        UserLink, Avatar, Tag, NewestUserPageRefresh


# Patterns to look for:
CHECK_PATTERNS = (
        re.compile(r'<a .*href=".*">'),     # Hyperlink
        re.compile(r'\([0-9]+\)[ 0-9\-]+'), # US-style telephone number
        re.compile(r'\+[0-9]+[ 0-9\-]+'),   # International telephone number
        re.compile(r'\+[0-9]+ *\([0-9]+\)[ 0-9\-]+'),  # Hybrid telephone
)

class Crawler(object):
    def __init__(self, project_id, db, api, client, log, io_loop=None):
        self._project_id = project_id
        self._log = log
        self._db = db
        self._api = api
        self._client = client

        if io_loop is None:
            io_loop = IOLoop.current()
        self._io_loop = io_loop

        # Some standard groups
        self._admin = self._get_or_make_group('admin')
        self._auto_suspect = self._get_or_make_group('auto_suspect')
        self._auto_legit = self._get_or_make_group('auto_legit')
        self._manual_suspect = self._get_or_make_group('suspect')
        self._manual_legit = self._get_or_make_group('legit')

        self._io_loop.add_callback(self.refresh_admin_group)
        self._refresh_admin_group_timeout = None

    def _get_or_make_group(self, name):
        group = self._db.query(Group).filter(
                Group.name == name).first()
        if group is None:
            group = Group(name=name)
            self._db.add(group)
            self._db.commit()

        return group

    @coroutine
    def refresh_admin_group(self):
        """
        Refresh the membership of the admin group.
        """
        if self._refresh_admin_group_timeout is not None:
            self._io_loop.remove_timeout(self._refresh_admin_group_timeout)

        members_data = []
        page = 1
        page_cnt = 1
        while page <= page_cnt:
            team_data = yield self._api.get_project_team(
                    self._project_id, sortby=UserSortBy.influence,
                    page=page, per_page=50)
            members_data.extend(team_data['team'])
            page += 1
            page_cnt = team_data['last_page']

        members = {}
        for member_data in members_data:
            member = yield self.update_user_from_data(
                    member_data['user'],
                    inspect_all=False)
            members[member.user_id] = member

        # Current members in database
        existing = set([m.user_id for m in self._admin.users])

        # Add any new members
        for user_id in (set(members.keys()) - existing):
            self._log.debug('Adding user ID %d to admin group', user_id)
            self._admin.users.append(members[user_id])
            existing.add(user_id)

        # Remove any old members
        for user_id in (existing - set(members.keys())):
            self._log.debug('Removing user ID %d from admin group', user_id)
            self._admin.users.remove(
                    self._db.query(User).get(user_id))

        self._db.commit()

        # Schedule this to run again tomorrow.
        self._refresh_admin_group_timeout = self._io_loop.add_timeout(
                self._io_loop.time() + 86400.0,
                self.refresh_admin_group)

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

                # Auto-Flag the user as "suspect"
                self._auto_suspect.users.append(user)
            else:
                # Auto-Flag the user as "legit"
                self._auto_legit.users.append(user)
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
                            user_data['created'], tz=pytz.utc))
            self._db.add(user)
        else:
            # Existing user, update the user details
            user.screen_name = user_data['screen_name']
            user.avatar_id=avatar.avatar_id
            user.url = user_data['url']
            if user.created is None:
                user.created = datetime.datetime.fromtimestamp(
                        user_data['created'], tz=pytz.utc)

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
            if page > 1:
                last_refresh = self._db.query(NewestUserPageRefresh).get(page)
                if (last_refresh is not None) and \
                        ((now - last_refresh.refresh_date).total_seconds() \
                            < 86400.0):
                    # Skip this page for now
                    page += 1
                    continue

            new_user_data = yield self._api.get_users(sortby=UserSortBy.newest,
                    page=page, per_page=50)
            if page > 1:
                if last_refresh is None:
                    last_refresh = NewestUserPageRefresh(
                        page_num=page,
                        refresh_date=now)
                    self._db.add(last_refresh)
                else:
                    last_refresh.refresh_date = now
                self._db.commit()

            for user_data in new_user_data['users']:
                user = yield self.update_user_from_data(user_data, inspect_all)

                # See if the user has been manually classified or not
                user_groups = set([g.name for g in user.groups])
                if (self._manual_suspect.name in user_groups) or \
                        (self._manual_legit.name in user_groups):
                    self._log.debug('Skipping user %s due to group membership %s',
                            user.screen_name, user_groups)
                    continue
                users.append(user)
            page += 1

        raise Return((users, page, new_user_data.get('last_page')))
