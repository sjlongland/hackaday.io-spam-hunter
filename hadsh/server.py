#!/usr/bin/env python

import base64
import argparse
import uuid
import datetime
import pytz
import json
import functools
import os

from passlib.context import CryptContext

from tornado.web import Application, RequestHandler, \
        RedirectHandler, MissingArgumentError
from tornado.httpclient import AsyncHTTPClient, HTTPError
from tornado.httpserver import HTTPServer
from tornado.locks import Semaphore
from tornado.gen import coroutine, TimeoutError, Return
from tornado.ioloop import IOLoop

from .pool import WorkerPool
from .hadapi.hadapi import HackadayAPI
from .crawler.crawler import Crawler
from .resizer import ImageResizer
from .hasher import ImageHasher
from .wordstat import tokenise, frequency, adjacency
from .db.db import Database
from .db.model import UserToken, Avatar, User
from .util import decode_body
from .traits.trait import Trait
from .traits import init_traits
from sqlalchemy import or_
from sqlalchemy.exc import InvalidRequestError
from . import extdlog


class AuthRequestHandler(RequestHandler):
    @coroutine
    def _get_session_or_redirect(self):
        db = self.application._db

        # Are we logged in?
        session_id = self.get_cookie('hadsh')
        if session_id is None:
            # Not yet logged in
            self.set_header('Cache-Control', 'private, no-cache')
            self.redirect('/login')
            return

        # Fetch the user details from the session
        session = yield db.query('''
            SELECT
                user_id, expiry_date
            FROM
                session
            WHERE
                session_id=%s
            LIMIT 1
        ''', session_id)

        if len(session) != 1:
            # Session is invalid
            self.set_header('Cache-Control', 'private, no-cache')
            self.redirect('/login')
            return

        (user_id, expiry_date) = session[0]

        # Is the session within a day of expiry?
        now = datetime.datetime.now(tz=pytz.utc)
        expiry_secs = (expiry_date - now).total_seconds()
        if expiry_secs < 0:
            # Session is defunct.
            self.set_header('Cache-Control', 'private, no-cache')
            self.redirect('/login')
            return

        if expiry_secs < 86400:
            # Extend the session another week.
            yield db.query('''
                UPDATE
                    session
                SET
                    expiry_date=%s
                WHERE
                    session_id=%s
            ''',
                now + datetime.timedelta(days=7),
                session_id, commit=True)
            self.set_cookie(name='hadsh',
                    value=str(session_id),
                    domain=self.application._domain,
                    secure=self.application._secure,
                    expires_days=7)

        raise Return((session_id, user_id, expiry_date))


class AuthAdminRequestHandler(AuthRequestHandler):
    @coroutine
    def _is_admin(self, session):
        # Is the user an admin?
        db = self.application._db
        (_, user_id, _) = session
        is_admin = yield db.query(
                '''
                SELECT
                    count(*)
                FROM
                    user_group_assoc
                WHERE
                    user_id=%s
                AND
                    group_id in (
                        SELECT
                            group_id
                        FROM
                            "group"
                        WHERE
                            name IN ('admin')
                    )
                ''', user_id)
        return bool(is_admin[0])


class LoginHandler(RequestHandler):
    def get(self):
        self.set_status(200)
        self.render('login.html',
                api_forbidden=self.application._api.is_forbidden)

    def post(self):
        db = self.application._db
        username = self.get_body_argument('username')
        password = self.get_body_argument('password')
        log = self.application._log.getChild('login[%s]' % username)
        account = db.query(Account).filter(Account.name == username).first()

        if account is None:
            log.info('No user account found matching user name')
            self.set_status(401)
            self.render('login.html',
                    api_forbidden=self.application._api.is_forbidden,
                    error="Invalid log-in credentials")
            return

        # Check the password
        match, newhash = self.application._crypt_context.verify_and_update(
                password, account.hashedpassword)
        if not match:
            log.info('Incorrect password given')
            self.set_status(401)
            self.render('login.html',
                    api_forbidden=self.application._api.is_forbidden,
                    error="Invalid log-in credentials")
            return

        if newhash:
            log.info('Password hash needs updating')
            account.hashedpassword = newhash

        # We have the user account, create the session
        expiry = datetime.datetime.now(tz=pytz.utc) \
                + datetime.timedelta(days=7)
        session = Session(
                session_id=uuid.uuid4(),
                user_id=account.user_id,
                expiry_date=expiry)
        db.add(session)
        db.commit()
        log.info('Session %s created.', session.session_id)

        # Grab the session ID and set that in a cookie.
        self.set_cookie(name='hadsh',
                value=str(session.session_id),
                domain=self.application._domain,
                secure=self.application._secure,
                expires_days=7)
        self.redirect('/', permanent=False)


class RootHandler(AuthRequestHandler):
    @coroutine
    def get(self):
        # Are we logged in?
        session = yield self._get_session_or_redirect()
        if session is None:
            return

        (session_id, user_id, expiry_date) = session
        db = self.application._db

        user = yield db.query('''
            SELECT
                screen_name,
                avatar_id,
                url
            FROM
                "user"
            WHERE
                user_id=%s
            LIMIT 1
            ''', user_id)
        assert len(user) == 1
        (screen_name, avatar_id, url) = user[0]

        self.set_status(200)
        self.render('index.html',
                user_name=screen_name,
                user_avatar_id=avatar_id,
                user_profile=url)


class AvatarHandler(AuthRequestHandler):
    @coroutine
    def get(self, avatar_id):
        # Are we logged in?
        db = self.application._db
        session = yield self._get_session_or_redirect()
        if session is None:
            return

        try:
            width = int(self.get_query_argument('width'))
        except MissingArgumentError:
            width = None
        try:
            height = int(self.get_query_argument('height'))
        except MissingArgumentError:
            height = None

        avatar_id = int(avatar_id)
        log = self.application._log.getChild('avatar[%d]' % avatar_id)
        log.audit('Retrieving from database')
        avatar = yield Avatar.fetch(db, 'avatar_id=%s',
                avatar_id, single=True)
        if avatar is None:
            self.set_status(404)
            self.finish()
            return

        if not avatar.avatar_type:
            yield self.application._crawler.fetch_avatar(avatar)

        if (width is not None) or (height is not None):
            image_data = yield self.application._resizer.resize(
                    avatar, width, height)
        else:
            image_data = avatar.avatar

        self.set_status(200)
        self.set_header('Content-Type', avatar.avatar_type)
        self.write(image_data)
        self.finish()


class AvatarHashHandler(AuthRequestHandler):
    @coroutine
    def get(self, algorithm, avatar_id):
        # Are we logged in?
        session = self._get_session_or_redirect()
        if session is None:
            return

        avatar_id = int(avatar_id)
        log = self.application._log.getChild('avatar[%d]' % avatar_id)
        avatar_hash = yield self.application._crawler.get_avatar_hash(
                algorithm, avatar_id)

        self.set_status(200)
        self.set_header('Content-Type', 'application/json')
        self.write(json.dumps({
            'id': avatar_hash.hash_id,
            'algo': avatar_hash.hashalgo,
            'hash': avatar_hash.hashstr.decode(),
            'instances': len(avatar_hash.avatars)
        }))
        self.finish()


class UserHandler(AuthRequestHandler):

    # User base query
    _USER_SQL = '''
        SELECT
            u.user_id,
            u.screen_name,
            u.url,
            u.avatar_id,
            u.created,
            u.had_created,
            u.last_update,
            ud.about_me,
            ud.who_am_i,
            ud.what_i_would_like_to_do,
            ud.location,
            ud.projects,
            du.inspect_time,
            du.inspections
        FROM
            "user" u
                LEFT OUTER JOIN user_detail ud
                    ON u.user_id=ud.user_id
                LEFT OUTER JOIN deferred_user du
                    ON u.user_id=du.user_id
    '''

    @coroutine
    def get(self, user_id):
        db = self.application._db

        log = self.application._log.getChild(\
            '%s[%s]' % (self.__class__.__name__, user_id))

        try:
            # Are we logged in?
            session = yield self._get_session_or_redirect()
            if session is None:
                return

            self.set_header('Content-Type', 'application/json')
            self.set_header('Cache-Control',
                    'no-cache, no-store, must-revalidate')

            user = yield db.query(self._USER_SQL + '''
            WHERE
                u.user_id=%s
            LIMIT 1
            ''', user_id)
            if len(user) != 1:
                self.set_status(404)
                self.write(json.dumps({
                    'id': user_id
                }))
            else:
                self.set_status(200)
                self.write(json.dumps((
                    yield self._dump_user(db, log, user[0])
                )))
        finally:
            db.close()

    @staticmethod
    @coroutine
    def _dump_user(db, log, user):
        # Extract the row information
        (user_id, screen_name, url, avatar_id, created,
                had_created, last_update, about_me, who_am_i,
                what_i_would_like_to_do, location, projects,
                inspect_time, inspections) = user

        # Return JSON data
        def _dump_link(link):
            (title, url) = link
            return {
                    'title':        title,
                    'url':          url
            }

        def _dump_trait(trait):
            data = {
                    'class': trait.trait._TRAIT_CLASS,
                    'weighted_score': trait.weighted_score,
                    'site_score': trait.trait_score,
                    'site_count': trait.trait_count,
                    'user_count': trait.count
            }
            if trait.instance:
                data['instance'] = trait.instance
            return data

        user_words = {}
        user_hostnames = {}
        user_adj = []

        if (inspections is None) or (inspections >= 5):
            pending = False
            inspections = None
            next_inspection = None
        else:
            pending = True
            next_inspection = inspect_time.isoformat()

        # Retrieve the user's traits.
        user_rec = yield User.fetch(db, 'user_id=%s',
                user_id, single=True)
        traits = yield Trait.assess(user_rec, log)

        # Retrieve the user's groups
        groups = yield db.query('''
            SELECT
                g.name
            FROM
                "group" g,
                "user_group_assoc" uga
            WHERE
                g.group_id=uga.group_id
            AND
                uga.user_id=%s
            ''', user_id)
        groups = [g[0] for g in groups]
        groups.sort()

        # Retrieve the user's tags
        tags = yield db.query('''
            SELECT
                t.tag
            FROM
                "tag" t,
                "user_tag" ut
            WHERE
                t.tag_id=ut.tag_id
            AND
                ut.user_id=%s
            ''', user_id)
        tags = [t[0] for t in tags]
        tags.sort()

        # Retrieve the user's links
        links = yield db.query('''
            SELECT
                title,
                url
            FROM
                user_link
            WHERE
                user_id=%s
        ''', user_id)

        # Retrieve the user's tokens
        tokens = yield UserToken.fetch(db,
                'user_id=%s', user_id)

        data = {
                'id':           user_id,
                'screen_name':  screen_name,
                'url':          url,
                'avatar_id':    avatar_id,
                'created':      (created or last_update).isoformat(),
                'had_created':  had_created.isoformat() \
                                if had_created is not None else None,
                'last_update':  last_update.isoformat() \
                                if last_update is not None else None,
                'links':        list(map(_dump_link, links)),
                'hostnames':    user_hostnames,
                'traits':       list(map(_dump_trait, traits)),
                'groups':       groups,
                'tags':         tags,
                'tokens':       dict([
                    (t.token, t.count) for t in tokens
                ]),
                'words':        user_words,
                'word_adj':     user_adj,
                'pending':      pending,
                'inspections':  inspections,
                'next_inspection': next_inspection,
                'about_me': about_me,
                'who_am_i': who_am_i,
                'location': location,
                'projects': projects,
                'what_i_would_like_to_do': what_i_would_like_to_do,
        }

        for (
                hostname, hostname_id, user_count,
                site_count, site_score
        ) in (yield db.query('''
            SELECT
                h.hostname,
                h.hostname_id,
                uh.count,
                h.score,
                h.count
            FROM
                user_hostname uh,
                hostname h
            WHERE
                uh.user_id=%s
            AND
                h.hostname_id=uh.hostname_id
        ''', user_id)):
            user_hostnames[hostname] = {
                    'id': hostname_id,
                    'user_count': user_count,
                    'site_count': site_count,
                    'site_score': site_score,
            }

        for (
            word, word_id, user_count, site_count, site_score
        ) in (yield db.query('''
            SELECT
                w.word,
                w.word_id,
                uw.count,
                w.count,
                w.score
            FROM
                word w,
                user_word uw
            WHERE
                uw.user_id=%s
            AND
                w.word_id=uw.word_id
        ''', user_id)):
            user_words[word] = {
                    'id': word_id,
                    'user_count': user_count,
                    'site_count': site_count,
                    'site_score': site_score,
            }

        for (
            proceeding_id, proceeding_word,
            following_id, following_word,
            user_count, site_count, site_score
        ) in (yield db.query('''
            SELECT
                uwa.proceeding_id,
                (SELECT
                    word
                FROM
                    word
                WHERE
                    word_id=uwa.proceeding_id),
                uwa.following_id,
                (SELECT
                    word
                FROM
                    word
                WHERE
                    word_id=uwa.following_id),
                uwa.count,
                wa.count,
                wa.score
            FROM
                user_word_adjacent uwa
                    LEFT OUTER JOIN
                        word_adjacent wa
                    ON uwa.proceeding_id=wa.proceeding_id
                    AND uwa.following_id=wa.following_id
            WHERE
                uwa.user_id=%s
        ''', user_id)):
            user_adj.append({
                'proceeding': proceeding_word,
                'proceeding_id': proceeding_id,
                'following': following_word,
                'following_id': following_id,
                'user_count': user_count,
                'site_count': site_count,
                'site_score': site_score,
            })
        return data


class UserBrowserHandler(AuthRequestHandler):
    @coroutine
    def get(self):
        db = self.application._db

        # Are we logged in?
        session = yield self._get_session_or_redirect()
        if session is None:
            return

        try:
            page = int(self.get_query_argument('page'))
        except MissingArgumentError:
            page = 0

        try:
            count = int(self.get_query_argument('count'))
        except MissingArgumentError:
            count = 10

        try:
            before_user_id = int(self.get_query_argument('before_user_id'))
        except MissingArgumentError:
            before_user_id = None

        try:
            after_user_id = int(self.get_query_argument('after_user_id'))
        except MissingArgumentError:
            after_user_id = None

        try:
            order = self.get_query_argument('order')
        except MissingArgumentError:
            if (after_user_id is not None) and (before_user_id is None):
                order = 'asc'
            else:
                order = 'desc'

        try:
            groups = tuple(self.get_query_argument('groups').split(' '))
        except MissingArgumentError:
            groups = self.DEFAULT_GROUPS

        log = self.application._log.getChild(\
                '%s[%s < user_id < %s]' \
                % (self.__class__.__name__, before_user_id, after_user_id))

        new_users = []
        while len(new_users) == 0:
            # Retrieve users from the database
            query_str = UserHandler._USER_SQL + '''
                    WHERE
                        u.user_id IN (
                            SELECT
                                user_id
                            FROM
                                user_group_assoc
                            WHERE
                                group_id IN (
                                    SELECT
                                        group_id
                                    FROM
                                        "group"
                                    WHERE
                                        name in %s
                                )
                        )
                    '''
            query_args = [tuple(groups)]

            if before_user_id is not None:
                query_str += '''
                    AND u.user_id < %s
                '''
                query_args.append(before_user_id)

            if after_user_id is not None:
                query_str += '''
                    AND u.user_id > %s
                '''
                query_args.append(after_user_id)

            assert order in ('asc', 'desc'), 'Unknown order %s' % order
            query_str += '''
                ORDER BY u.user_id %s
                LIMIT %d
                OFFSET %d
            ''' % (order, count, page*count)

            new_users = yield db.query(query_str, *query_args)

            if len(new_users) == 0:
                # There are no more new users, wait for crawl to happen
                log.debug('No users found, waiting for more from crawler')
                self.application._crawler.new_user_event.clear()
                try:
                    yield self.application._crawler.new_user_event.wait(
                            timeout=60.0)
                except TimeoutError:
                    break
        user_data = yield list(map(functools.partial(
                UserHandler._dump_user, db, log), new_users))

        self.set_status(200)
        self.set_header('Content-Type', 'application/json')
        self.set_header('Cache-Control',
                'no-cache, no-store, must-revalidate')
        self.write(json.dumps({
                'page': page,
                'users': user_data
        }))


class NewcomerDataHandler(UserBrowserHandler):
    DEFAULT_GROUPS = ('auto_legit', 'auto_suspect')


class LegitUserDataHandler(UserBrowserHandler):
    DEFAULT_GROUPS = ('legit',)


class SuspectUserDataHandler(UserBrowserHandler):
    DEFAULT_GROUPS = ('suspect',)


class AdminUserDataHandler(UserBrowserHandler):
    DEFAULT_GROUPS = ('admin',)


class ClassifyHandler(AuthAdminRequestHandler):
    @coroutine
    def post(self, user_id):
        # Are we logged in?
        session = yield self._get_session_or_redirect()
        if session is None:
            return

        self.set_header('Content-Type', 'application/json')
        if not (yield self._is_admin(session)):
            self.set_status(401)
            self.write(json.dumps({
                'error': 'not an admin'
            }))
            return

        db = self.application._db
        user_id = int(user_id)
        log = self.application._log.getChild('classify[%d]' % user_id)

        user = yield User.fetch(db, 'user_id=%s', user_id,
                single=True)

        (content_type, _, body_data) = decode_body(
                self.request.headers['Content-Type'],
                self.request.body)

        if content_type != 'application/json':
            self.set_status(400)
            self.write(json.dumps({
                'error': 'unrecognised payload type',
                'type': content_type,
            }))
            return

        classification = json.loads(body_data)
        if not isinstance(classification, str):
            self.set_status(400)
            self.write(json.dumps({
                'error': 'payload is not a string'
            }))
            return

        # Remove from all 'auto_%' groups, 'legit' and
        # 'suspect' group
        yield db.query('''
            DELETE FROM
                "user_group_assoc"
            WHERE
                user_id=%s
            AND
                group_id IN (
                    SELECT
                        group_id
                    FROM
                        "group"
                    WHERE
                        name='legit'
                    OR
                        name='suspect'
                    OR
                        name LIKE 'auto_%%'
                )
        ''', user_id, commit=True)

        # Grab the groups for classification
        if classification == 'legit':
            yield db.query('''
                INSERT INTO user_group_assoc (
                    user_id,
                    group_id
                ) VALUES (
                    %s, (SELECT
                            group_id
                        FROM
                            "group"
                        WHERE
                            name='legit')
                )''', user_id, commit=True)

            score_inc = 1
            keep_detail = False
        elif classification == 'suspect':
            yield db.query('''
                INSERT INTO user_group_assoc (
                    user_id,
                    group_id
                ) VALUES (
                    %s, (SELECT
                            group_id
                        FROM
                            "group"
                        WHERE
                            name='suspect')
                )''', user_id, commit=True)
            score_inc = -1
            keep_detail = True
        else:
            self.set_status(400)
            self.write(json.dumps({
                'error': 'unrecognised classification',
                'classification': classification
            }))
            return

        # Count up the hostname, word and word adjacencies
        yield db.query('''
            UPDATE
                hostname h
            SET
                score=h.score + (uh.count * %s),
                count=h.count + uh.count
            FROM
                user_hostname uh
            WHERE
                h.hostname_id=uh.hostname_id
            AND
                uh.user_id=%s
        ''', score_inc, user_id, commit=True)

        yield db.query('''
            UPDATE
                word w
            SET
                score=w.score + (uw.count * %s),
                count=w.count + uw.count
            FROM
                user_word uw
            WHERE
                w.word_id=uw.word_id
            AND
                uw.user_id=%s
        ''', score_inc, user_id, commit=True)

        # Update *existing* word adjacencies.
        yield db.query('''
            UPDATE
                word_adjacent wa
            SET
                score=wa.score + (uwa.count * %s),
                count=wa.count + uwa.count
            FROM
                user_word_adjacent uwa
            WHERE
                wa.proceeding_id=uwa.proceeding_id
            AND
                wa.following_id=uwa.following_id
            AND
                uwa.user_id=%s
        ''', score_inc, user_id, commit=True)

        # Initialise any new word adjacencies
        # Ignore conflicts, as these have been taken
        # care of above.
        yield db.query('''
            INSERT INTO word_adjacent (
                proceeding_id, following_id,
                score, count
            ) SELECT
                proceeding_id, following_id,
                count * %s, count
            FROM
                user_word_adjacent
            WHERE
                user_id=%s
            ON CONFLICT DO NOTHING
        ''', score_inc, user_id, commit=True)

        # Update the user's traits.
        for trait in (yield Trait.assess(user, log)):
            trait.increment_trait(score_inc)
            trait.discard()

        # Drop the user detail unless we're keeping it
        if not keep_detail:
            for table in ('user_detail', 'user_link',
                    'user_word', 'user_word_adjacent',
                    'user_hostname', 'user_trait',
                    'user_trait_instance'):
                yield db.query('''
                    DELETE FROM
                        %s
                    WHERE
                        user_id=%%s
                    ''' % (table,),
                    user_id, commit=True)

        # Remove user from deferred list
        yield db.query('''
            DELETE FROM deferred_user
            WHERE user_id=%s
        ''', user_id)

        log.info('User %d marked as %s', user_id, classification)

        # Get a list of all groups
        groups = yield db.query('''
            SELECT
                g.name
            FROM
                user_group_assoc uga,
                "group" g
            WHERE
                uga.group_id=g.group_id
            AND
                uga.user_id=%s
        ''', user_id)
        res = {
                'user_id': user_id,
                'groups': [g[0] for g in groups]
        }

        self.set_status(200)
        self.write(json.dumps(res))


class CallbackHandler(RequestHandler):
    @coroutine
    def get(self):
        db = self.application._db
        log = self.application._log.getChild('callback')

        # Retrieve the code
        try:
            code = self.get_query_argument('code', strip=False)
            log.audit('Code is %s, retrieving token', code)
            oauth_data = yield self.application._api.get_token(code)
            log.audit('OAuth response %s', oauth_data)

            try:
                token = oauth_data['access_token']
            except KeyError:
                # Not a successful response.
                self.set_status(403)
                self.set_header('Content-Type', 'application/json')
                self.write(json.dumps(oauth_data))
                return

            user_data = yield self.application._api.get_current_user(token)
        except HTTPError as e:
            if e.code in (400, 403):
                # We've been blocked.
                self.set_header('Content-Type', e.response.headers['Content-Type'])
                self.write(e.response.body)
                return
            raise

        # Retrieve and update the user from the website data.
        user = yield self.application._crawler.update_user_from_data(
                user_data)

        # We have the user account, create the session
        expiry = datetime.datetime.now(tz=pytz.utc) \
                + datetime.timedelta(days=7)
        session_id = uuid.uuid4()

        yield db.query('''
            INSERT INTO session (
                session_id, user_id, expiry_date
            ) VALUES (
                %s, %s, %s
            )''', session_id, user_id, expiry, commit=True)

        # Grab the session ID and set that in a cookie.
        self.set_cookie(name='hadsh',
                value=str(session_id),
                domain=self.application._domain,
                secure=self.application._secure,
                expires_days=7)
        self.redirect('/', permanent=False)


class HADSHApp(Application):
    """
    Hackaday.io Spambot Hunter application.
    """
    def __init__(self, db_uri, project_id, admin_uid,
            client_id, client_secret, api_key, api_rq_interval,
            domain, secure, static_uri, static_path,
            thread_count, crawler_config, **kwargs):
        # Database connection
        self._db = Database(db_uri)
        self._log = extdlog.getLogger(self.__class__.__name__)
        # Session management connection
        self._pool = WorkerPool(thread_count)
        self._hasher = ImageHasher(self._log.getChild('hasher'), self._pool)
        AsyncHTTPClient.configure(
                None, defaults=dict(
                    user_agent="HADSH/0.0.1 (https://hackaday.io/project/29161-hackadayio-spambot-hunter-project)"))
        self._api = HackadayAPI(client_id=client_id,
                client_secret=client_secret, api_key=api_key,
                rqlim_time=api_rq_interval,
                client=AsyncHTTPClient(), log=self._log.getChild('api'))
        self._crawler = Crawler(project_id, admin_uid, self._db,
                self._api, self._hasher, self._log.getChild('crawler'),
                config=crawler_config)
        self._resizer = ImageResizer(self._log.getChild('resizer'),
                self._pool)
        self._domain = domain
        self._secure = secure
        self._classify_sem = Semaphore(1)

        self._crypt_context = CryptContext([
            'argon2', 'scrypt', 'bcrypt'
        ])

        # Initialise traits
        init_traits(self, self._log.getChild('trait'))

        super(HADSHApp, self).__init__([
            (r"/", RootHandler),
            (r"/login", LoginHandler),
            (r"/avatar/([0-9]+)", AvatarHandler),
            (r"/avatar/(average_hash|dhash|phash|whash|sha512)/([0-9]+)", \
                    AvatarHashHandler),
            (r"/user/([0-9]+)", UserHandler),
            (r"/callback", CallbackHandler),
            (r"/classify/([0-9]+)", ClassifyHandler),
            (r"/data/newcomers.json", NewcomerDataHandler),
            (r"/data/legit.json", LegitUserDataHandler),
            (r"/data/suspect.json", SuspectUserDataHandler),
            (r"/data/admin.json", AdminUserDataHandler),
            (r"/authorize", RedirectHandler, {
                "url": self._api.auth_uri
            }),
        ],
        static_url_prefix=static_uri,
        static_path=static_path,
        **kwargs)


def main(*args, **kwargs):
    """
    Console entry point.
    """
    parser = argparse.ArgumentParser(
            description='HAD Spambot Hunter Project')
    parser.add_argument('--project-id', dest='project_id', type=int,
            help='Owner project ID; for determining who gets admin rights')
    parser.add_argument('--admin-uid', dest='admin_uid', type=int,
            action='append', help='Additional user IDs to consider admins')
    parser.add_argument('--domain', dest='domain',
            help='Domain to use for cookies')
    parser.add_argument('--cleartext', action='store_const',
            default=True, const=False, dest='secure',
            help='Use cleartext HTTP not HTTPS')
    parser.add_argument('--db-uri', dest='db_uri',
            help='Back-end database URI')
    parser.add_argument('--client-id', dest='client_id',
            help='Hackaday.io client ID')
    parser.add_argument('--client-secret', dest='client_secret',
            help='Hackaday.io client secret')
    parser.add_argument('--api-key', dest='api_key',
            help='Hackaday.io user key')
    parser.add_argument('--api-rq-interval', dest='api_rq_interval',
            type=float, default=HackadayAPI.RQLIM_TIME,
            help='Minimum period between consecutive requests')
    parser.add_argument('--listen-address', dest='listen_address',
            default='', help='Interface address to listen on.')
    parser.add_argument('--listen-port', dest='listen_port', type=int,
            default=3000, help='Port number (TCP) to listen on.')
    parser.add_argument('--log-level', dest='log_level',
            default='INFO', help='Logging level')
    parser.add_argument('--thread-count', dest='thread_count', type=int,
            default=8, help='Number of concurrent threads.')
    parser.add_argument('--static-uri', dest='static_uri', type=str,
            help='Static resource URI', default='/static/')
    parser.add_argument('--static-path', dest='static_path', type=str,
            help='Static resource path', default=os.path.realpath(
                os.path.join(os.path.dirname(__file__), 'static')))
    parser.add_argument('--template-path', dest='template_path', type=str,
            help='Directory containing template files', default=os.path.realpath(
                os.path.join(os.path.dirname(__file__), 'static')))

    # Add arguments from the crawler config
    for key, default_value in Crawler.DEFAULT_CONFIG.items():
        parser.add_argument(
                '--crawler-%s' % key.replace('_','-'),
                dest='crawler_%s' % key,
                type=type(default_value),
                default=default_value)

    args = parser.parse_args(*args, **kwargs)

    # Start logging
    extdlog.basicConfig(level=args.log_level,
            format='%(asctime)s %(levelname)10s '\
                    '%(name)16s %(process)d/%(threadName)s: %(message)s')

    # Validate arguments
    if (args.client_id is None) or \
            (args.client_secret is None) or \
            (args.api_key is None):
        raise ValueError('--client-id, --client-secret and '\
                '--user-key are mandatory.  Retrieve those '\
                'when you register at '\
                'https://dev.hackaday.io/applications')

    # Grab crawler settings
    crawler_config = dict([
        (key, getattr(args, 'crawler_%s' % key))
        for key in Crawler.DEFAULT_CONFIG.keys()
    ])

    application = HADSHApp(
            project_id=args.project_id,
            admin_uid=set(args.admin_uid or []),
            db_uri=args.db_uri,
            client_id=args.client_id,
            client_secret=args.client_secret,
            api_key=args.api_key,
            api_rq_interval=args.api_rq_interval,
            domain=args.domain,
            secure=args.secure,
            static_path=args.static_path,
            static_uri=args.static_uri,
            template_path=args.template_path,
            thread_count=args.thread_count,
            crawler_config=crawler_config
    )
    http_server = HTTPServer(application)
    http_server.listen(port=args.listen_port, address=args.listen_address)
    IOLoop.current().start()

if __name__ == '__main__':
    main()
