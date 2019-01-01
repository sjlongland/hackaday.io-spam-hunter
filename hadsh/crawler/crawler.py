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
        DeferredUser, Hostname, UserHostname, NewUser, AvatarHash
from ..wordstat import tokenise, frequency, adjacency
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import or_
from .. import extdlog

from .tldcache import TopLevelDomainCache

from ..traits.trait import Trait


# Patterns to look for:
CHECK_PATTERNS = (
        # Hyperlink
        re.compile(r'<a .*?href=".*?">.*?</a>'),
        # US-style telephone number
        re.compile(r'\([0-9]+?\)[ 0-9\-]+?'),
        # International telephone number
        re.compile(r'\+[0-9]+?[ 0-9\-]+?'),
        # Hybrid telephone (US/International)
        re.compile(r'\+[0-9]+? *\([0-9]+?\)[ 0-9\-]+?'),
)

# URI whitelist
URI_WHITELIST = (
        # Google Plus
        re.compile(r'^https?://plus.google.com/'),
        # Linked In
        re.compile(r'^https?://([a-z]{2}|www)\.linkedin\.com/in/[^/]+(|/.*)$'),
        # Github
        re.compile(r'^https?://github.com/[^/]+(|/.*)$'),
        re.compile(r'^https?://github.com/?$'),
        # Twitter
        re.compile(r'^https?://(mobile\.|www\.|)twitter.com/[^/]+(|/.*)$'),
        re.compile(r'^https?://twitter.com/?$'),
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

    def __init__(self, project_id, admin_uid, db, api, hasher, log,
            config=None, io_loop=None):
        self._hasher = hasher
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

        self._refresh_hist_page = None

        if io_loop is None:
            io_loop = IOLoop.current()
        self._io_loop = io_loop

        self._refresh_admin_group_timeout = None
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

    @coroutine
    def get_avatar(self, avatar_url):
        # Ensure it exists, do nothing if already present
        yield self._db.query(
                '''
                INSERT INTO "avatar"
                    (url)
                VALUES
                    (%s)
                ON CONFLICT DO NOTHING
                ''', avatar_url, commit=True)

        # Fetch the avatar
        avatars = yield Avatar.fetch(self._db,
                'url=%s LIMIT 1', avatar_url)

        raise Return(avatars[0])

    @coroutine
    def fetch_avatar(self, avatar):
        # Do we have their avatar on file?
        if isinstance(avatar, str):
            avatar = yield self.get_avatar(avatar)

        if not avatar.avatar_type:
            # We don't have the avatar yet
            self._log.debug('Retrieving avatar at %s',
                    avatar.url)
            avatar_res = yield self._api.api_fetch(
                    avatar.url)
            avatar.avatar_type = avatar_res.headers['Content-Type']
            avatar.avatar=avatar_res.body
            yield avatar.commit()

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
                users = yield User.fetch(self._db,
                        'user_id=%s', user_data['id'])
                user = users[0]

            # Has the user been classified?
            user_groups = yield user.get_groups()
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

                yield self._db.query('''
                    DELETE FROM "new_user"
                    WHERE user_id=%s
                    ''', user.user_id, commit=True)
                yield self._db.query('''
                    DELETE FROM "user"
                    WHERE user_id=%s
                    CASCADE
                    ''', user.user_id, commit=True)

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
                                    link['title'], link['url'], exc_info=1)

                            # Insert the link if not already present.
                            yield self._db.query(
                                    '''
                                    INSERT INTO "user_link"
                                        (user_id, url, title)
                                    VALUES
                                        (%s, %s, title)
                                    ON CONFLICT DO UPDATE
                                    SET
                                        title=%s
                                    WHERE
                                        user_id=%s
                                    AND
                                        url=%s
                            ''', user.user_id, link['url'],
                                link['title'], link['title'],
                                user.user_id, link['url'],
                                commit=True)

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

                # Stash any tokens
                for token, count in user_tokens.items():
                    if count > 0:
                        yield self._db.query('''
                            INSERT INTO "user_token"
                                (user_id, token, count)
                            VALUES
                                (%s, %s, %s)
                            ON CONFLICT DO UPDATE
                            SET
                                count=%s
                            WHERE
                                user_id=%s
                            AND
                                token=%s
                            ''', user.user_id, token, count,
                            count, user.user_id, token,
                            commit=True)
                    else:
                        yield self._db.query('''
                            DELETE FROM "user_token"
                            WHERE
                                user_id=%s
                            AND
                                token=%s
                        ''', user.user_id, token, commit=True)

                # Retrieve all the hostnames
                yield self._db.query('''
                    INSERT INTO "hostname"
                        (hostname, score, count)
                    VALUES
                        %(insert_template)s
                    ON CONFLICT DO NOTHING
                ''' % {
                    'insert_template': ', '.join([
                        '(%s, 0, 0)' for x
                        in range(0, len(user_host_freq))
                    ])
                }, *tuple(user_host_freq.keys()), commit=True)
                hostnames = dict([
                    (h.hostname, h) for h in
                    (yield Hostname.fetch(self._db,
                        'hostname IN %s',
                        tuple(user_host_freq.keys())))
                ])

                # Retrieve all the words
                yield self._db.query('''
                    INSERT INTO "word"
                        (word, score, count)
                    VALUES
                        %(insert_template)s
                    ON CONFLICT DO NOTHING
                ''' % {
                    'insert_template': ', '.join([
                        '(%s, 0, 0)' for x
                        in range(0, len(user_freq))
                    ])
                }, *tuple(user_freq.keys()), commit=True)

                words = dict([
                    (w.word, w) for w in
                    (yield Word.fetch(self._db,
                        'word IN %s',
                        tuple(user_freq.keys())))
                ])

                # Retrieve all the word adjacencies
                yield self._db.query('''
                    INSERT INTO "word_adjacenct"
                        (proceeding_id, following_id, score, count)
                    VALUES
                        %(insert_template)s
                    ON CONFLICT DO NOTHING
                ''' % {
                    'insert_template': ', '.join([
                        '(%s, %s, 0, 0)' for x
                        in range(0, len(user_freq))
                    ])
                }, *tuple([
                        word[w].word_id
                        for w in sum(user_adj_freq.keys(), ())
                    ]),
                    commit=True)
                # There's no clean way I know of to retrieve
                # composite keys in an IN query.
                word_adj = {}
                for (proc_w, follow_w) in user_adj_freq.keys():
                    word_adjs = yield WordAdjacent.fetch(self._db,
                            'proceeding_id=%s AND following_id=%s',
                            word[proc_w], word[follow_w])
                    word_adj[(proc_w, follow_w)] = word_adjs[0]

                # Add the user words, compute user's score
                score = []
                for word, count in user_freq.items():
                    w = words[word]
                    if count > 0:
                        yield self._db.query('''
                            INSERT INTO "user_word"
                                (user_id, word_id, count)
                            VALUES
                                (%s, %s, %s)
                            ON CONFLICT DO UPDATE
                            SET
                                count=%s
                            WHERE
                                user_id=%s
                            AND
                                word_id=%s
                        ''', user.user_id, w.word_id, count,
                            count, user.user_id,
                            w.word_id, commit=True)
                    else:
                        yield self._db.query('''
                            DELETE FROM "user_word"
                            WHERE
                                user_id=%s
                            AND
                                word_id=%s''',
                                user.user_id, w.word_id,
                                commit=True)

                    if w.count > 0:
                        score.append(float(w.score) / float(w.count))

                # Add the user host names
                for hostname, count in user_host_freq.items():
                    h = hostnames[hostname]
                    if count > 0:
                        yield self._db.query('''
                            INSERT INTO "user_hostname"
                                (user_id, hostname_id, count)
                            VALUES
                                (%s, %s, %s)
                            ON CONFLICT DO UPDATE
                            SET
                                count=%s
                            WHERE
                                user_id=%s
                            AND
                                hostname_id=%s
                        ''', user.user_id, h.hostname_id, count,
                            count, user.user_id,
                            h.hostname_id, commit=True)
                    else:
                        yield self._db.query('''
                            DELETE FROM "user_hostname"
                            WHERE
                                user_id=%s
                            AND
                                hostname_id=%s''',
                                user.user_id, h.hostname_id,
                                commit=True)

                    if h.count > 0:
                        score.append(float(h.score) / float(h.count))

                # Add the user word adjcancies
                for (proc_word, follow_word), count in user_adj_freq.items():
                    wa = word_adj[(proc_word, follow_word)]
                    proc_w = words[proc_word]
                    follow_w = words[follow_word]

                    if count > 0:
                        yield self._db.query('''
                            INSERT INTO "user_word_adjacent"
                                (user_id, proceeding_id, following_id, count)
                            VALUES
                                (%s, %s, %s, %s)
                            ON CONFLICT DO UPDATE
                            SET
                                count=%s
                            WHERE
                                user_id=%s
                            AND
                                proceeding_id=%s
                            AND
                                following_id=%s
                        ''', user.user_id, proc_w.word_id,
                            follow_w.word_id, count,
                            count, user.user_id, proc_w.word_id,
                            follow_w.word_id, commit=True)
                    else:
                        yield self._db.query('''
                            DELETE FROM "user_word_adjacent"
                            WHERE
                                user_id=%s
                            AND
                                proceeding_id=%s
                            AND
                                following_id=%s''',
                                user.user_id, proc_w.word_id,
                                follow_w.word_id, commit=True)

                    if wa.count > 0:
                        score.append(float(wa.score) / float(wa.count))

                # Append each traits' weighted score
                for trait in Trait.assess(user,
                        self._log.getChild('user%d' % user.user_id)):
                    score.append(trait.weighted_score)
                    trait.persist()

                # Compute score
                score.sort()
                score = sum(score[:10])

                if (defer and (abs(score < 0.5) \
                        or (age < self._config['defer_min_age']))) \
                        and (age < self._config['defer_max_age']):
                    # There's nothing to score.  Inspect again later.

                    yield self._db.query('''
                        INSERT INTO "deferred_user"
                            (user_id, inspect_time, inspections)
                        VALUES
                            (%s, CURRENT_TIMESTAMP + make_interval(secs => %s), 1)
                        ON CONFLICT DO UPDATE
                        SET
                            inspect_time=CURRENT_TIMESTAMP + make_interval(secs => (%s * (inspections+1))),
                            inspections=inspections+1
                        WHERE
                            user_id=%s''', user_data['id'],
                            config['defer_delay'],
                            config['defer_delay'],
                            user_data['id'], commit=True)
                else:
                    yield self._db.query('''
                        DELETE FROM "deferred_user"
                        WHERE
                            user_id=%s
                        ''', user_data['id'], commit=True)

                self._log.debug('User %s [#%d] has score %f',
                        user.screen_name, user.user_id, score)
                if score < -0.5:
                    match = True

                # Record the user information
                yield self._db.query('''
                    INSERT INTO "user_detail"
                        (user_id, about_me, who_am_i, location,
                         projects, what_i_would_like_to_do)
                    VALUES
                        (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO UPDATE
                    SET
                        about_me=%s,
                        who_am_i=%s,
                        location=%s,
                        projects=%s,
                        what_i_would_like_to_do=%s
                    WHERE
                        user_id=%s
                ''',
                    user_data['id'],
                    user_data['about_me'],
                    user_data['who_am_i'],
                    user_data['location'],
                    user_data['projects'],
                    user_data['what_i_would_like_to_do'],
                    user_data['about_me'],
                    user_data['who_am_i'],
                    user_data['location'],
                    user_data['projects'],
                    user_data['what_i_would_like_to_do'],
                    user_data['id'], commit=True)

            if match:
                # Auto-Flag the user as "suspect"
                if not classified:
                    self._log.debug('Auto-classifying %s [#%d] as suspect',
                            user.screen_name, user.user_id)
                    yield user.add_groups('auto_suspect')
                    yield user.rm_groups('auto_legit')
            elif not classified:
                # Auto-Flag the user as "legit"
                self._log.debug('Auto-classifying %s [#%d] as legitmate',
                        user.screen_name, user.user_id)
                yield user.add_groups('auto_legit')
                yield user.rm_groups('auto_suspect')
            self._log.audit('Finished inspecting %s', user_data)
        except:
            self._log.error('Failed to process user data %r',
                    user_data, exc_info=1)
            raise

    @coroutine
    def update_user_from_data(self, user_data, inspect_all=True,
            defer=True):
        """
        Update a user in the database from data retrieved via the API.
        """
        self._log.audit('Inspecting user data: %s', user_data)
        avatar = yield self.get_avatar(user_data['image_url'])
        user_created = datetime.datetime.fromtimestamp(
                        user_data['created'], tz=pytz.utc)

        # See if the user exists:
        user = yield User.fetch(self._db, 'user_id=%s', user_data['id'], single=True)
        if user is None:
            # Nope, create the user.
            yield self._db.query('''
                INSERT INTO "user"
                    (user_id, screen_name, url, avatar_id, created,
                     had_created)
                VALUES
                    (%s, %s, %s, %s, CURRENT_TIMESTAMP, %s)
                ''',
                    user_data['id'], user_data['screen_name'],
                    user_data['url'], avatar.avatar_id,
                    user_created, commit=True)
            # Try again
            user = yield User.fetch(self._db, 'user_id=%s', user_data['id'], single=True)
        else:
            # Update the user
            user.screen_name = user_data['screen_name']
            user.url = user_data['url']
            user.avatar_id = avatar.avatar_id
            user.last_update = datetime.datetime.now(tz=pytz.utc)
            yield user.commit()

        # Inspect the user
        if inspect_all or (user.last_update is None):
            yield self._inspect_user(user_data, user=user, defer=defer)
            user.last_update = datetime.datetime.now(tz=pytz.utc)
            yield user.commit()

        if new:
            self.new_user_event.set()
        self._log.debug('User %s up-to-date', user)
        raise Return(user)

    @coroutine
    def _fetch_last_refresh_page(self):
        page_num = yield self._db.query('''
            SELECT
                max(page_num)
            FROM
                "newest_user_page_refresh"
        ''')
        assert len(page_num) == 1
        raise Return(page_num[0][0])


    @coroutine
    def _background_fetch_new_users(self):
        """
        Try to retrieve users newer than our current set.
        """
        page = 1
        page_count = 0
        if self._refresh_hist_page is None:
            self._refresh_hist_page = yield self._fetch_last_refresh_page()

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
                ids = yield self._db.query('''
                    SELECT
                        user_id
                    FROM
                        "deferred_user"
                    WHERE
                        inspections < %s
                    AND
                        inspect_time < CURRENT_TIMESTAMP
                    ORDER BY
                        inspect_time ASC,
                        inspections DESC
                    LIMIT 50''',
                    self._config['defer_max_count'])
                ids = [r[0] for r in ids]

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
                        yield self._db.query('''
                            UPDATE
                                "deferred_user"
                            SET
                                inspections=inspections+1,
                                inspect_time=CURRENT_TIMESTAMP
                                    + make_interval(secs => (%s * (inspections + 1)))
                            WHERE
                                user_id IN %s
                        ''', self._config['defer_delay'],
                            tuple(ids), commit=True)

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
                ids = yield self._db.query('''
                    SELECT
                        user_id
                    FROM
                        "new_user"
                    ORDER BY
                        user_id DESC
                    LIMIT 50''')
                ids = [r[0] for r in ids]

                if ids:
                    self._log.debug('Scanning %s', ids)

                    user_data = yield self._api.get_users(
                            ids=ids, per_page=50)
                    self._log.audit('Received new users: %s', user_data)
                    if isinstance(user_data['users'], list):
                        user_data['users'].sort(
                                key=lambda u : u.get('id') or 0,
                                reverse=True)
                        for this_user_data in user_data['users']:
                            user = yield self.update_user_from_data(
                                    this_user_data, inspect_all=True)

                # Clean up the new user list
                yield self._db.query('''
                    DELETE FROM
                        "new_user"
                    WHERE
                        user_id IN (
                            SELECT
                                user_id
                            FROM
                                "user"
                            WHERE
                                user_id IN %s
                        )
                ''', tuple(ids), commit=True)
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

        if self._refresh_hist_page is None:
            self._refresh_hist_page = yield self._fetch_last_refresh_page()

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
                last_refresh = yield NewestUserPageRefresh.fetch(self._db, 'page_num=%s', page)
                if len(last_refresh) == 1:
                    last_refresh = last_refresh[0]
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
            existing_ids = yield self._db.query('''
                SELECT
                    user_id
                FROM
                    "users"
                WHERE
                    user_id IN %s
                ''', tuple(ids))
            existing_ids = set([r[0] for r in existing_ids])

            ids = list(filter(
                lambda id : id not in existing_ids,
                ids))

            yield self._db.query('''
                INSERT INTO "new_user"
                    (user_id)
                VALUES
                    %(value_template)s
                ON CONFLICT DO NOTHING
            ''' % {
                'value_template': ', '.join([
                    '(%s)' for x in ids
                ])
            }, tuple(ids), commit=True)

            page += 1
            pages += 1

            if not ids:
                # No more IDs to fetch
                break

        raise Return(page)

    @coroutine
    def _get_avatar_hashes(self, avatar_id):
        hashes = {}
        for algo in ('sha512','average_hash','dhash','phash','whash'):
            hashes[algo] = yield self.get_avatar_hash(algo, avatar_id)
        raise Return(hashes)

    @coroutine
    def get_avatar_hash(self, algorithm, avatar_id):
        avatars = yield Avatar.fetch(self._db,
                'avatar_id=%s', avatar_id)
        if len(avatars) != 1:
            return

        avatar = avatars[0]
        del avatars

        if not avatar.avatar_type:
            yield self.fetch_avatar(avatar)

        # Do we have the hash on file already?
        hashes = yield avatar.get_hashes()
        avatar_hash = hashes.get(algorithm)

        if avatar_hash is None:
            # We need to retreive it
            hash_data = yield self._hasher.hash(avatar, algorithm)

            # Create the hash instance if not already present
            yield self._db.query('''
                INSERT INTO "avatar_hash"
                    (hashalgo, hashdata)
                VALUES
                    (%s, %s)
                ON CONFLICT DO NOTHING
            ''', algorithm, hash_data, commit=True)

            # Fetch the hash instance.
            avatar_hashes = yield AvatarHash.fetch(self._db,
                    'hashalgo=%s AND hashdata=%s',
                    algorithm, hash_data)
            avatar_hash = avatar_hashes[0]

            # Associate the hash with the avatar
            yield self._db.query('''
                INSERT INTO "avatar_hash_assoc"
                    (avatar_id, hash_id)
                VALUES
                    (%s, %s)
                ON CONFLICT DO NOTHING''',
                avatar.avatar_id, avatar_hash.hash_id,
                commit=True)

            self._log.debug('Generated new hash for avatar %d algorithm %s',
                    avatar_id, algorithm)

        raise Return(avatar_hash)
