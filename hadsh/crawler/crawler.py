import datetime
import pytz
from socket import gaierror

from urllib.parse import urlparse

import re

from tornado.httpclient import HTTPError
from tornado.gen import coroutine, Return, sleep
from tornado.ioloop import IOLoop
from tornado.locks import Event

from ..hadapi.hadapi import UserSortBy
from ..db.model import User, Group, Session, UserDetail, \
        UserLink, Avatar, Tag, NewestUserPageRefresh, \
        UserWord, UserWordAdjacent, UserToken, Word, WordAdjacent, \
        DeferredUser, Hostname, UserHostname, NewUser
from ..wordstat import tokenise, frequency, adjacency
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import or_
from .. import extdlog

from .tldcache import TopLevelDomainCache


# Patterns to look for:
CHECK_PATTERNS = (
        # Hyperlink
        re.compile(r'<a .*href=".*">.*</a>'),
        # US-style telephone number
        re.compile(r'\([0-9]+\)[ 0-9\-]+'),
        # International telephone number
        re.compile(r'\+[0-9]+[ 0-9\-]+'),
        # Hybrid telephone (US/International)
        re.compile(r'\+[0-9]+ *\([0-9]+\)[ 0-9\-]+'),
)

# URI whitelist
URI_WHITELIST = (
        # Google Plus
        re.compile(r'^https?://plus.google.com/'),
        # Linked In
        re.compile(r'^https?://([a-z]{2}|www)\.linkedin\.com/in/[^/]+(|/.*)$'),
        # Github
        re.compile(r'^https?://github.com/[^/]+(|/.*)$'),
        # Twitter
        re.compile(r'^https?://(mobile\.|www\.|)twitter.com/[^/]+(|/.*)$'),
        # Youtube
        re.compile(r'^https?://(www.|)youtube.com/channel/'),
        # Hackaday.com
        re.compile(r'^https?://hackaday.com(|/.*)$'),
        # Hackaday.io
        re.compile(r'^https?://hackaday.io(|/.*)$'),
)


class InvalidUser(ValueError):
    pass


class NoUsersReturned(ValueError):
    pass


class Crawler(object):

    DEFAULT_CONFIG = {
            'init_delay': 5.0,
            'new_user_fetch_interval': 900.0,
            'new_check_interval': 5.0,
            'defer_delay': 900.0,
            'deferred_check_interval': 900.0,
            'defer_min_age': 3600.0,
            'defer_max_age': 2419200.0,
            'defer_max_count': 5,
            'old_user_fetch_interval': 300.0,
            'old_user_fetch_interval_lastpage': 604800.0,
            'admin_user_fetch_interval': 86400.0,
            'api_blocked_delay': 86400.0,
            'tld_suffix_uri': TopLevelDomainCache.PUBLICSUFFIX_URI,
            'tld_suffix_cache_duration': TopLevelDomainCache.CACHE_DURATION,
    }

    def __init__(self, project_id, admin_uid, db, api, log,
            config=None, io_loop=None):
        self._project_id = project_id
        self._admin_uid = admin_uid
        self._admin_uid_scanned = False
        self._log = log
        self._db = db
        self._api = api

        log.trace('Given config is %s', config)
        self._config = self.DEFAULT_CONFIG.copy()
        self._config.update(config or {})

        log.trace('Running config is %s',
                self._config)

        # Oldest page refreshed
        oldest_page = \
                self._db.query(NewestUserPageRefresh).order_by(\
                    NewestUserPageRefresh.page_num.desc()).first()
        if oldest_page is None:
            # No pages fetched, start at the first page.
            self._refresh_hist_page = 1
        else:
            # Start at last visited page.
            self._refresh_hist_page = oldest_page.page_num

        if io_loop is None:
            io_loop = IOLoop.current()
        self._io_loop = io_loop

        # Some standard groups
        self._admin = self._get_or_make_group('admin')
        self._auto_suspect = self._get_or_make_group('auto_suspect')
        self._auto_legit = self._get_or_make_group('auto_legit')
        self._manual_suspect = self._get_or_make_group('suspect')
        self._manual_legit = self._get_or_make_group('legit')

        self._refresh_admin_group_timeout = None
        self._io_loop.add_callback(self.refresh_admin_group)
        self._io_loop.add_timeout(
                self._io_loop.time() + self._config['init_delay'],
                self._background_fetch_new_users)
        self._io_loop.add_timeout(
                self._io_loop.time() + self._config['init_delay'],
                self._background_fetch_hist_users)
        self._io_loop.add_timeout(
                self._io_loop.time() + self._config['init_delay'],
                self._background_inspect_deferred)
        self._io_loop.add_timeout(
                self._io_loop.time() + self._config['init_delay'],
                self._background_inspect_new)

        self._tld_cache = TopLevelDomainCache(
                list_uri=self._config['tld_suffix_uri'],
                cache_duration=self._config['tld_suffix_cache_duration'],
                log=log.getChild('tldcache'))

        # Event to indicate when new users have been added
        self.new_user_event = Event()

        # Deleted users, by ID
        self._deleted_users = set()

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

        try:
            members_data = []
            page = 1
            page_cnt = 1
            while page <= page_cnt:
                team_data = yield self._api.get_project_team(
                        self._project_id, sortby=UserSortBy.influence,
                        page=page, per_page=50)
                self._log.trace('Retrieved team member page %d of %d',
                        team_data.get('page',page),
                        team_data.get('last_page',page_cnt))
                members_data.extend(team_data['team'])
                page += 1
                page_cnt = team_data['last_page']

            if not self._admin_uid_scanned:
                extras = list(self._admin_uid)
                while len(extras) > 0:
                    fetch_uids = extras[:50]
                    team_data = yield self._api.get_users(ids=fetch_uids,
                            sortby=UserSortBy.influence, page=page, per_page=50)
                    self._log.trace('Retrieved additional members: %s', fetch_uids)
                    members_data.extend([{'user': u} for u in team_data['users']])
                    extras = extras[50:]
                self._admin_uid_scanned = True

            members = {}
            for member_data in members_data:
                try:
                    member = yield self.update_user_from_data(
                            member_data['user'],
                            inspect_all=False,
                            defer=False)
                    members[member.user_id] = member
                except:
                    self._log.warning('Failed to process admin: %s',
                        member_data, exc_info=1)

            # Current members in database
            existing = set([m.user_id for m in self._admin.users])

            # Add any new members
            for user_id in (set(members.keys()) - existing):
                self._log.trace('Adding user ID %d to admin group', user_id)
                self._admin.users.append(members[user_id])
                existing.add(user_id)

            # Remove any old members
            for user_id in (existing - set(members.keys())):
                if user_id in self._admin_uid:
                    self._log.trace('%d is given via command line, not removing',
                            user_id)
                    continue

                self._log.trace('Removing user ID %d from admin group', user_id)
                self._admin.users.remove(
                        self._db.query(User).get(user_id))

            self._db.commit()
        except:
            self._log.warning('Failed to refresh admin group', exc_info=1)

        # Schedule this to run again tomorrow.
        self._refresh_admin_group_timeout = self._io_loop.add_timeout(
                self._io_loop.time()
                    + self._config['admin_user_fetch_interval'],
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
            self._log.trace('Retrieving avatar at %s',
                    avatar.url)
            avatar_res = yield self._api.api_fetch(
                    avatar.url)
            avatar.avatar_type = avatar_res.headers['Content-Type']
            avatar.avatar=avatar_res.body
            self._db.commit()

        raise Return(avatar)

    @coroutine
    def _inspect_user(self, user_data, user=None, defer=True):
        """
        Inspect the user, see if they're worth investigating.
        """
        if user_data['id'] in self._deleted_users:
            self._log.trace('User %d is deleted', user_data['id'])
            return

        try:
            if user is None:
                user = self._db.query(User).get(user_data['id'])

            # Has the user been classified?
            user_groups = set([g.name for g in user.groups])
            classified = ('legit' in user_groups) or ('suspect' in user_groups)
            self._log.trace('User %s [#%d] is in groups %s (classified %s)',
                    user.screen_name, user.user_id, user_groups, classified)

            # Is the link valid?
            try:
                while True:
                    try:
                        result = yield self._api.api_fetch(
                                user.url, method='HEAD')
                        break
                    except gaierror:
                        continue
            except HTTPError as e:
                if e.code not in (404, 410):
                    raise
                self._log.info('Link to user %s [#%d] no longer valid',
                        user.screen_name, user.user_id)

                new_user = self._db.query(NewUser).get(user.user_id)
                if new_user:
                    try:
                        self._db.delete(new_user)
                    except SQLAlchemyError:
                        self._db.expunge(new_user)

                try:
                    self._db.delete(user)
                    self._db.commit()
                except SQLAlchemyError:
                    # Possible if the object hasn't yet been committed yet.
                    self._db.expunge(user)
                    pass
                self._deleted_users.add(user_data['id'])
                raise InvalidUser('no longer valid')

            if user.last_update is not None:
                age = datetime.datetime.now(tz=pytz.utc) - user.last_update;
                if age.total_seconds() < 300:
                    return

            if classified:
                match = False
            else:
                # Tokenise the users' content.
                user_freq = {}
                user_host_freq = {}
                user_adj_freq = {}
                user_tokens = {}
                def tally(field):
                    wordlist = tokenise(field)
                    frequency(wordlist, user_freq)
                    if len(wordlist) > 2:
                        adjacency(wordlist, user_adj_freq)

                # Does the user have any hyperlinks or other patterns in their
                # profile?
                self._log.debug('Inspecting user %s [#%d]',
                    user_data['screen_name'], user_data['id'])
                match = False
                for field in ('about_me', 'who_am_i', 'location',
                        'what_i_would_like_to_do'):
                    for pattern in CHECK_PATTERNS:
                        pmatch = pattern.search(user_data[field])
                        if pmatch:
                            self._log.info('Found match for %s (%r) in '\
                                    '%s of %s [#%d]',
                                    pattern.pattern, pmatch.group(0), field,
                                    user_data['screen_name'], user_data['id'])
                            try:
                                user_tokens[pmatch.group(0)] += 1
                            except KeyError:
                                user_tokens[pmatch.group(0)] = 1

                            match = True
                            break

                    # Tally up word usage in this field.
                    tally(user_data[field])

                # Does the user have any hyperlinks?  Not an indicator that they're
                # a spammer, just one of the traits.
                pg_idx = 1
                pg_cnt = 1  # Don't know how many right now, but let's start here
                while pg_idx <= pg_cnt:
                    link_res = yield self._api.get_user_links(user.user_id,
                            page=pg_idx, per_page=50)
                    self._log.trace('Retrieved user %s link page %d of %d',
                            user,
                            link_res.get('page',pg_idx),
                            link_res.get('last_page',pg_cnt))
                    if link_res['links'] == 0:
                        # No links, yes sometimes it's an integer.
                        break

                    try:
                        for link in link_res['links']:
                            # Ignore if the link URL or title is empty
                            if (not link['title']) or (not link['url']):
                                continue

                            # Count the link title up
                            tally(link['title'])

                            try:
                                # Count up the hostname/domain frequencies
                                uri_domains = yield self._tld_cache.splitdomain(
                                        urlparse(link['url']).hostname)

                                for hostname in uri_domains:
                                    user_host_freq[hostname] = \
                                            user_host_freq.get(hostname, 0) + 1
                            except:
                                self._log.warning(
                                    'Failed to count up domain frequency for '
                                    'user %s [#%d] link %s <%s>',
                                    user_data['screen_name'],
                                    user_data['id'],
                                    link['title'], link['url'])

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

                            if not match:
                                # Ignore the link if it's in the whitelist
                                uri_matched = False
                                for pattern in URI_WHITELIST:
                                    if pattern.match(link['url']):
                                        uri_matched = True
                                        break

                                match = match or (not uri_matched)
                    except:
                        self._log.error('Failed to process link result %r', link_res)
                        raise
                    pg_cnt = link_res['last_page']

                    # Next page
                    pg_idx = link_res['page'] + 1

                # Does the user have a lot of projects in a short time?
                age = (datetime.datetime.now(tz=pytz.utc) - \
                        user.had_created).total_seconds()

                # How about the content of those projects?
                if user_data.get('projects'):
                    try:
                        pg_idx = 1
                        pg_cnt = 1
                        while pg_idx <= pg_cnt:
                            prj_res = yield self._api.get_user_projects(
                                    user.user_id,
                                    page=pg_idx, per_page=50)
                            self._log.audit('Projects for %s: %s',
                                    user, prj_res)

                            raw_projects = prj_res.get('projects')
                            if isinstance(raw_projects, list):
                                for raw_prj in raw_projects:
                                    # Tokenise the name, summary and description
                                    for field in ('name', 'summary', 'description'):
                                        if field not in raw_prj:
                                            continue
                                        tally(raw_prj[field])
                                    self._db.commit()

                            pg_cnt = prj_res.get('last_page',1)

                            # Next page
                            pg_idx = prj_res.get('page',1) + 1
                    except:
                        self._log.error('Failed to process user %s projects',
                                user, exc_info=1)
                        # Carry on!

                # How about the user's pages?
                try:
                    pg_idx = 1
                    pg_cnt = 1
                    while pg_idx <= pg_cnt:
                        page_res = yield self._api.get_user_pages(user.user_id,
                                page=pg_idx, per_page=50)
                        self._log.audit('Pages for %s: %s',
                                user, page_res)

                        raw_pages = page_res.get('pages')
                        if isinstance(raw_pages, list):
                            for raw_page in raw_pages:
                                # Tokenise the name, summary and description
                                for field in ('title', 'body'):
                                    if field not in raw_page:
                                        continue
                                    tally(raw_page[field])
                                self._db.commit()

                        pg_cnt = page_res.get('last_page',1)

                        # Next page
                        pg_idx = page_res.get('page',1) + 1
                except:
                    self._log.error('Failed to process user %s pages',
                            user, exc_info=1)
                    # Carry on!

                if (age > 300.0) and ((user_data['projects'] / 60.0) > 5):
                    # More than 5 projects a minute on average.
                    self._log.debug('User %s [#%d] has %d projects in %d seconds',
                            user.screen_name, user.user_id, user_data['projects'], age)
                    match = True

                # Commit here so the user ID is valid.
                self._db.commit()

                # Stash any tokens
                for token, count in user_tokens.items():
                    t = self._db.query(UserToken).get((user.user_id, token))
                    if t is None:
                        self._db.add(UserToken(
                            user_id=user.user_id, token=token, count=count))
                    else:
                        t.count = count

                # Retrieve all the hostnames
                hostnames = {}
                for hostname in user_host_freq.keys():
                    h = self._db.query(Hostname).filter(
                            Hostname.hostname==hostname).one_or_none()
                    if h is None:
                        self._log.audit('New hostname: %s', hostname)
                        h = Hostname(hostname=hostname, score=0, count=0)
                        self._db.add(h)
                    hostnames[hostname] = h

                # Retrieve all the words
                words = {}
                for word in user_freq.keys():
                    w = self._db.query(Word).filter(
                            Word.word==word).one_or_none()
                    if w is None:
                        self._log.audit('New word: %s', word)
                        w = Word(word=word, score=0, count=0)
                        self._db.add(w)
                    words[word] = w

                # Stash the new words, if any
                self._db.commit()

                # Add the user words, compute user's score
                score = []
                for word, count in user_freq.items():
                    w = words[word]
                    uw = self._db.query(UserWord).get((user.user_id,
                            w.word_id))

                    if uw is None:
                        uw = UserWord(
                            user_id=user.user_id, word_id=w.word_id,
                            count=count)
                        self._db.add(uw)
                    else:
                        uw.count = count

                    if w.count > 0:
                        score.append(float(w.score) / float(w.count))

                # Add the user host names
                for hostname, count in user_host_freq.items():
                    h = hostnames[hostname]
                    uh = self._db.query(UserHostname).get((user.user_id,
                            h.hostname_id))

                    if uh is None:
                        uh = UserHostname(
                            user_id=user.user_id,
                            hostname_id=h.hostname_id,
                            count=count)
                        self._db.add(uh)
                    else:
                        uh.count = count

                    if h.count > 0:
                        score.append(float(h.score) / float(h.count))

                # Add the user word adjcancies
                for (proc_word, follow_word), count in user_adj_freq.items():
                    proc_w = words[proc_word]
                    follow_w = words[follow_word]

                    uwa = self._db.query(UserWordAdjacent).get((
                        user.user_id, proc_w.word_id, follow_w.word_id))
                    if uwa is None:
                        uwa = UserWordAdjacent(
                            user_id=user.user_id,
                            proceeding_id=proc_w.word_id,
                            following_id=follow_w.word_id,
                            count=count)
                        self._db.add(uwa)
                    else:
                        uwa.count = count

                    wa = self._db.query(WordAdjacent).get((
                        proc_w.word_id, follow_w.word_id
                    ))
                    if wa is None:
                        continue
                    if wa.count > 0:
                        score.append(float(wa.score) / float(wa.count))

                # Compute score
                score.sort()
                score = sum(score[:10])

                defuser = self._db.query(DeferredUser).get(user_data['id'])

                if (defer and (abs(score < 0.5) \
                        or (age < self._config['defer_min_age']))) \
                        and (age < self._config['defer_max_age']):
                    # There's nothing to score.  Inspect again later.
                    if defuser is None:
                        defuser = DeferredUser(user_id=user_data['id'],
                                inspect_time=datetime.datetime.now(tz=pytz.utc) \
                                        + datetime.timedelta(
                                            seconds=self._config['defer_delay']),
                                inspections=1)
                        self._db.add(defuser)
                    else:
                        defuser.inspections += 1
                        defuser.inspect_time=datetime.datetime.now(tz=pytz.utc) \
                                        + datetime.timedelta(
                                                seconds=self._config['defer_delay'] \
                                                        * defuser.inspections)
                    self._log.info('User %s has score %f and age %f, '\
                            'inspect again after %s (inspections %s)',
                            user, score, age, defuser.inspect_time,
                            defuser.inspections)
                elif defuser is not None:
                    self._log.info('Cancelling deferred inspection for %s',
                            user)
                    self._db.delete(defuser)

                self._log.debug('User %s [#%d] has score %f',
                        user.screen_name, user.user_id, score)
                if score < -0.5:
                    match = True

                # Record the user information
                detail = self._db.query(UserDetail).get(user_data['id'])
                if detail is None:
                    detail = UserDetail(
                            user_id=user_data['id'],
                            about_me=user_data['about_me'],
                            who_am_i=user_data['who_am_i'],
                            location=user_data['location'],
                            projects=user_data['projects'],
                            what_i_would_like_to_do=\
                                    user_data['what_i_would_like_to_do'])
                    self._db.add(detail)
                else:
                    detail.about_me = user_data['about_me']
                    detail.who_am_i = user_data['who_am_i']
                    detail.projects = user_data['projects']
                    detail.location = user_data['location']
                    detail.what_i_would_like_to_do = \
                            user_data['what_i_would_like_to_do']

            if match:
                # Auto-Flag the user as "suspect"
                if not classified:
                    self._log.debug('Auto-classifying %s [#%d] as suspect',
                            user.screen_name, user.user_id)
                    self._auto_suspect.users.append(user)
            elif not classified:
                # Auto-Flag the user as "legit"
                self._log.debug('Auto-classifying %s [#%d] as legitmate',
                        user.screen_name, user.user_id)
                self._auto_legit.users.append(user)

            self._db.commit()
            self._log.audit('Finished inspecting %s', user_data)
        except:
            self._log.error('Failed to process user data %r',
                    user_data, exc_info=1)
            raise

    @coroutine
    def update_user_from_data(self, user_data, inspect_all=True,
            defer=True, return_new=False):
        """
        Update a user in the database from data retrieved via the API.
        """
        self._log.audit('Inspecting user data: %s', user_data)
        avatar = self.get_avatar(user_data['image_url'])

        # Look up the user in the database
        user = self._db.query(User).get(user_data['id'])
        user_created = datetime.datetime.fromtimestamp(
                        user_data['created'], tz=pytz.utc)

        new = user is None
        if new:
            # New user
            user = User(user_id=user_data['id'],
                        screen_name=user_data['screen_name'],
                        url=user_data['url'],
                        avatar_id=avatar.avatar_id,
                        created=datetime.datetime.now(pytz.utc),
                        had_created=user_created)
            self._log.info('New user: %s [#%d]',
                    user.screen_name, user.user_id)
            self._db.add(user)
        else:
            # Existing user, update the user details
            self._log.debug('Updating existing user %s', user)
            user.screen_name = user_data['screen_name']
            user.avatar_id=avatar.avatar_id
            user.url = user_data['url']
            if user.created is None:
                user.created = datetime.datetime.now(pytz.utc)
            user.had_created = user_created

        # Inspect the user
        if inspect_all or (user.last_update is None):
            yield self._inspect_user(user_data, user=user, defer=defer)
            user.last_update = datetime.datetime.now(tz=pytz.utc)
        self._db.commit()

        if new:
            self.new_user_event.set()
        self._log.debug('User %s up-to-date', user)
        if return_new:
            raise Return((user, new))
        raise Return(user)

    @coroutine
    def _background_fetch_new_users(self):
        """
        Try to retrieve users newer than our current set.
        """
        page = 1
        page_count = 0
        try:
            while (page < max([self._refresh_hist_page,2])) \
                    and (page_count < 10):
                self._log.info('Scanning for new users page %d', page)
                page = yield self.fetch_new_user_ids(
                        page=page, inspect_all=True,
                        defer=True)
                page_count += 1
        except NoUsersReturned:
            # Okay, so we've got nothing, move along.
            pass
        except SQLAlchemyError:
            # SQL cock up, roll back.
            self._db.rollback()
            self._log.exception('Failed to retrieve newer users'\
                    ': database rolled back')
        except:
            self._log.exception('Failed to retrieve newer users')

        delay = self._config['new_user_fetch_interval']
        next_time = self._io_loop.time()
        next_time += (delay - (next_time % delay))
        delay = next_time - self._io_loop.time()
        self._log.info('Next new user scan in %.3f sec', delay)
        self._io_loop.add_timeout(
                self._io_loop.time() + delay,
                self._background_fetch_new_users)

    @coroutine
    def _background_inspect_deferred(self):
        """
        Inspect previously deferred users
        """
        if not self._api.is_forbidden:
            delay = self._config['deferred_check_interval']
            self._log.info('Scanning deferred users')
            try:
                # Grab a handful of deferred users
                ids = [u.user_id for u in self._db.query(DeferredUser).filter(
                        DeferredUser.inspections \
                                < self._config['defer_max_count'],
                        DeferredUser.inspect_time <
                            datetime.datetime.now(tz=pytz.utc)).order_by(
                                    DeferredUser.inspect_time).limit(50).all()]

                if ids:
                    self._log.trace('Scanning %s', ids)

                    user_data = yield self._api.get_users(ids=ids, per_page=50)
                    self._log.audit('Received deferred users: %s', user_data)
                    if isinstance(user_data['users'], list):
                        for this_user_data in user_data['users']:
                            while True:
                                try:
                                    yield self.update_user_from_data(
                                            this_user_data, inspect_all=True)
                                    break
                                except InvalidUser:
                                    pass
                                except SQLAlchemyError:
                                    self._db.rollback()
                    else:
                        # Mark as checked
                        for user_id in ids:
                            self._log.debug('Deferring user ID %d', user_id)
                            du = self._db.query(DeferredUser).get(user_id)
                            du.inspections += 1
                            du.inspect_time=datetime.datetime.now(tz=pytz.utc) \
                                            + datetime.timedelta(
                                                seconds=self._config['defer_delay']
                                                * du.inspections)
                        self._db.commit()

                self._log.debug('Successfully fetched deferred users')
            except:
                self._log.exception('Failed to retrieve deferred users')
        else:
            self._log.warning('API blocked, cannot inspect deferred users')
            delay = self._config['api_blocked_delay']

        self._log.info('Next deferred user scan in %.3f sec', delay)
        self._io_loop.add_timeout(
                self._io_loop.time() + delay,
                self._background_inspect_deferred)

    @coroutine
    def _background_inspect_new(self):
        """
        Inspect new users
        """
        if not self._api.is_forbidden:
            delay = self._config['new_check_interval']
            self._log.info('Scanning new users')
            try:
                # Grab a handful of new users
                ids = dict([
                    (u.user_id, u) for u in
                    self._db.query(NewUser).order_by(
                        NewUser.user_id.desc()).limit(50).all()])

                if ids:
                    self._log.debug('Scanning %s', list(ids.keys()))

                    user_data = yield self._api.get_users(
                            ids=list(ids.keys()), per_page=50)
                    self._log.audit('Received new users: %s', user_data)
                    if isinstance(user_data['users'], list):
                        user_data['users'].sort(
                                key=lambda u : u.get('id') or 0,
                                reverse=True)
                        for this_user_data in user_data['users']:
                            while True:
                                try:
                                    user = yield self.update_user_from_data(
                                            this_user_data, inspect_all=True)
                                    new_user = ids.get(user.user_id)
                                    if new_user:
                                        self._db.delete(new_user)
                                    break
                                except InvalidUser:
                                    pass
                                except SQLAlchemyError:
                                    self._db.rollback()
                            self._db.commit()

                self._log.debug('Successfully fetched new users')
            except:
                self._log.exception('Failed to retrieve new users')
        else:
            delay = self._config['api_blocked_delay']
            self._log.warning('API blocked, cannot inspect new users')

        self._log.info('Next new user scan in %.3f sec', delay)
        self._io_loop.add_timeout(
                self._io_loop.time() + delay,
                self._background_inspect_new)

    @coroutine
    def _background_fetch_hist_users(self):
        """
        Try to retrieve users registered earlier.
        """
        self._log.info('Beginning historical user retrieval')
        delay = self._config['old_user_fetch_interval']
        try:
            self._refresh_hist_page = \
                    yield self.fetch_new_user_ids(
                        page=self._refresh_hist_page,
                        defer=False)
        except SQLAlchemyError:
            # SQL cock up, roll back.
            self._db.rollback()
            self._log.exception('Failed to retrieve older users'\
                    ': database rolled back')
        except NoUsersReturned:
            self._log.info('Last user page reached')
            delay = self._config['old_user_fetch_interval_lastpage']
        except:
            self._log.exception('Failed to retrieve older users')

        self._log.info('Next historical user fetch in %.3f sec', delay)
        self._io_loop.add_timeout(
                self._io_loop.time() + delay,
                self._background_fetch_hist_users)

    @coroutine
    def fetch_new_user_ids(self, page=1, inspect_all=False, defer=True):
        """
        Retrieve new users IDs not currently known from the Hackaday.io API.
        """
        last_refresh = None
        num_uids = 0
        pages = 0

        now = datetime.datetime.now(tz=pytz.utc)
        while (num_uids < 10) and (pages < 10):
            if page > 1:
                last_refresh = self._db.query(NewestUserPageRefresh).get(page)
                if last_refresh is not None:
                    self._log.audit('Page %s last refreshed on %s',
                            last_refresh.page_num, last_refresh.refresh_date)
                    if (now - last_refresh.refresh_date).total_seconds() \
                            < 2592000.0:    # 30 days
                        # Skip this page for now
                        self._log.audit('Skipping page %d', page)
                        page += 1
                        yield sleep(0.001)
                        continue

            self._log.trace('Retrieving newest user page %d', page)
            ids = yield self._api.get_user_ids(sortby=UserSortBy.newest,
                    page=page)
            self._log.trace('Retrieved newest user page %d', page)

            if not ids:
                # Nothing returned, so stop here
                raise NoUsersReturned()

            if page > 1:
                if last_refresh is None:
                    self._log.debug('Adding page %s refresh time %s',
                            page, now)
                    last_refresh = NewestUserPageRefresh(
                            page_num=page, refresh_date=now)
                    self._db.add(last_refresh)
                else:
                    self._log.debug('Updating page %s refresh time %s',
                            page, now)
                    last_refresh.refresh_date = now
                self._db.commit()

            # Filter out the users we already have.  Create new user objects
            # for the ones we don't have.
            for uid in ids:
                if (self._db.query(User).get(uid) is None) \
                        and (self._db.query(NewUser).get(uid) is None):
                    new_user = NewUser(user_id=uid)
                    self._db.add(new_user)
                    num_uids += 1
            self._db.commit()
            page += 1
            pages += 1

            if not ids:
                # No more IDs to fetch
                break

        raise Return(page)
