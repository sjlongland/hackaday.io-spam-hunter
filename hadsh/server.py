#!/usr/bin/env python

import argparse
import uuid
import datetime
import pytz
import json
import functools

from passlib.context import CryptContext

from tornado.web import Application, RequestHandler, \
        RedirectHandler, MissingArgumentError
from tornado.httpclient import AsyncHTTPClient, HTTPError
from tornado.httpserver import HTTPServer
from tornado.locks import Semaphore
from tornado.gen import coroutine, TimeoutError
from tornado.ioloop import IOLoop

from .pool import WorkerPool
from .hadapi.hadapi import HackadayAPI
from .crawler.crawler import Crawler
from .resizer import ImageResizer
from .wordstat import tokenise, frequency, adjacency
from .db.db import get_db, User, Group, Session, UserDetail, \
        UserLink, Avatar, Tag, Word, WordAdjacent, DeferredUser, \
        Hostname, Account
from .util import decode_body
from sqlalchemy import or_
from sqlalchemy.exc import InvalidRequestError
from . import extdlog


class AuthRequestHandler(RequestHandler):
    def _get_session_or_redirect(self):
        db = self.application._session_db

        # Are we logged in?
        session_id = self.get_cookie('hadsh')
        if session_id is None:
            # Not yet logged in
            self.redirect('/login')
            return

        # Fetch the user details from the session
        session = db.query(Session).get(session_id)
        if session is None:
            # Session is invalid
            self.redirect('/login')
            return

        # Is the session within a day of expiry?
        now = datetime.datetime.now(tz=pytz.utc)
        expiry_secs = (session.expiry_date - now).total_seconds()
        if expiry_secs < 0:
            # Session is defunct.
            self.redirect('/login')
            return

        if expiry_secs < 86400:
            # Extend the session another week.
            session.expiry_date = now + datetime.timedelta(days=7)
            db.commit()
            self.set_cookie(name='hadsh',
                    value=str(session.session_id),
                    domain=self.application._domain,
                    secure=self.application._secure,
                    expires_days=7)

        return session


class AuthAdminRequestHandler(AuthRequestHandler):
    def _is_admin(self, session):
        # Is the user an admin?
        return 'admin' in set([
            g.name for g in session.user.groups
        ])


class LoginHandler(RequestHandler):
    def get(self):
        self.set_status(200)
        self.render('login.html',
                api_forbidden=self.application._api.is_forbidden)

    def post(self):
        db = self.application._db
        username = self.get_body_argument('username')
        password = self.get_body_argument('password')
        account = db.query(Account).filter(Account.name == username).first()

        if account is None:
            self.set_status(401)
            self.render('login.html',
                    api_forbidden=self.application._api.is_forbidden,
                    error="Invalid log-in credentials")
            return

        # Check the password
        match, newhash = self.application._crypt_context.verify_and_update(
                password, account.hashedpassword)
        if not match:
            self.set_status(401)
            self.render('login.html',
                    api_forbidden=self.application._api.is_forbidden,
                    error="Invalid log-in credentials")
            return

        if newhash:
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

        # Grab the session ID and set that in a cookie.
        self.set_cookie(name='hadsh',
                value=str(session.session_id),
                domain=self.application._domain,
                secure=self.application._secure,
                expires_days=7)
        self.redirect('/', permanent=False)


class RootHandler(AuthRequestHandler):
    def get(self):
        # Are we logged in?
        session = self._get_session_or_redirect()
        if session is None:
            return

        user = session.user

        self.set_status(200)
        self.render('index.html',
                user_name=user.screen_name,
                user_avatar_id=user.avatar_id,
                user_profile=user.url)


class AvatarHandler(AuthRequestHandler):
    @coroutine
    def get(self, avatar_id):
        db = self.application._db
        try:
            # Are we logged in?
            session = self._get_session_or_redirect()
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
            avatar = db.query(Avatar).get(avatar_id)
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
        finally:
            db.close()


class WordHandler(AuthRequestHandler):
    @coroutine
    def get(self, word_id):
        db = self.application._db
        try:
            # Are we logged in?
            session = self._get_session_or_redirect()
            if session is None:
                return

            w = db.query(Word).get(word_id)
            self.set_status(200)
            self.set_header('Content-Type', 'application/json')
            self.write(json.dumps({
                'id': w.word_id,
                'word': w.word,
                'score': w.score,
                'count': w.count
            }))
        finally:
            db.close()


class WordAdjacencyHandler(AuthRequestHandler):
    @coroutine
    def get(self, wa_id):
        db = self.application._db
        try:
            # Are we logged in?
            session = self._get_session_or_redirect()
            if session is None:
                return

            wa = db.query(WordAdjacent).get(wa_id)
            self.set_status(200)
            self.set_header('Content-Type', 'application/json')
            self.write(json.dumps({
                'proceeding_id': wa.proceeding_id,
                'proceeding': wa.proceeding.word,
                'following_id': wa.following_id,
                'following': wa.following.word,
                'score': wa.score,
                'count': wa.count
            }))
        finally:
            db.close()


class HostnameHandler(AuthRequestHandler):
    @coroutine
    def get(self, hostname_id):
        db = self.application._db
        try:
            # Are we logged in?
            session = self._get_session_or_redirect()
            if session is None:
                return

            h = db.query(Hostname).get(hostname_id)
            self.set_status(200)
            self.set_header('Content-Type', 'application/json')
            self.write(json.dumps({
                'hostname_id': h.hostname_id,
                'hostname': h.hostname,
                'score': h.score,
                'count': h.count
            }))
        finally:
            db.close()


class UserHandler(AuthRequestHandler):
    @coroutine
    def get(self, user_id):
        db = self.application._db

        try:
            # Are we logged in?
            session = self._get_session_or_redirect()
            if session is None:
                return

            self.set_header('Content-Type', 'application/json')
            self.set_header('Cache-Control',
                    'no-cache, no-store, must-revalidate')

            user = db.query(User).get(user_id)
            if user is None:
                self.set_status(404)
                self.write(json.dumps({
                    'id': user_id
                }))
            else:
                self.set_status(200)
                self.write(json.dumps(self._dump_user(db, user)))
        finally:
            db.close()

    @staticmethod
    def _dump_user(db, user):
        # Return JSON data
        def _dump_link(link):
            return {
                    'title':        link.title,
                    'url':          link.url
            }

        user_words = {}
        user_hostnames = {}
        user_adj = []

        du = db.query(DeferredUser).get(user.user_id)
        if (du is None) or (du.inspections >= 5):
            pending = False
            inspections = None
            next_inspection = None
        else:
            pending = True
            inspections = du.inspections
            next_inspection = du.inspect_time.isoformat()

        data = {
                'id':           user.user_id,
                'screen_name':  user.screen_name,
                'url':          user.url,
                'avatar_id':    user.avatar_id,
                'created':      (user.created or user.last_update).isoformat(),
                'had_created':  user.had_created.isoformat() \
                                if user.had_created is not None else None,
                'last_update':  user.last_update.isoformat() \
                                if user.last_update is not None else None,
                'links':        list(map(_dump_link, user.links)),
                'hostnames':    user_hostnames,
                'groups':       [
                    g.name for g in user.groups
                ],
                'tags':         [
                    t.tag for t in user.tags
                ],
                'tokens':       dict([
                    (t.token, t.count) for t in user.tokens
                ]),
                'words':        user_words,
                'word_adj':     user_adj,
                'pending':      pending,
                'inspections':  inspections,
                'next_inspection': next_inspection
        }

        for uh in user.hostnames:
            h = db.query(Hostname).get(uh.hostname_id)
            user_hostnames[h.hostname] = {
                    'id': h.hostname_id,
                    'user_count': uh.count,
                    'site_count': h.count,
                    'site_score': h.score,
            }

        for uw in user.words:
            w = db.query(Word).get(uw.word_id)
            user_words[w.word] = {
                    'id': w.word_id,
                    'user_count': uw.count,
                    'site_count': w.count,
                    'site_score': w.score,
            }

        for uwa in user.adj_words:
            pw = db.query(Word).get(uwa.proceeding_id)
            fw = db.query(Word).get(uwa.following_id)
            wa = db.query(WordAdjacent).get(
                    (uwa.proceeding_id, uwa.following_id))

            if wa is not None:
                wa_count = wa.count
                wa_score = wa.score
            else:
                wa_count = 0
                wa_score = 0

            user_adj.append({
                'proceeding': pw.word,
                'proceeding_id': uwa.proceeding_id,
                'following': fw.word,
                'following_id': uwa.following_id,
                'user_count': uwa.count,
                'site_count': wa_count,
                'site_score': wa_score,
            })

        detail = user.detail
        if detail is not None:
            data.update({
                'about_me': detail.about_me,
                'who_am_i': detail.who_am_i,
                'location': detail.location,
                'projects': detail.projects,
                'what_i_would_like_to_do': detail.what_i_would_like_to_do,
            })
        return data


class UserBrowserHandler(AuthRequestHandler):
    @coroutine
    def get(self):
        db = self.application._db

        try:
            # Are we logged in?
            session = self._get_session_or_redirect()
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
                query = db.query(User).join(User.groups).filter(\
                        Group.name.in_(groups))
                if before_user_id is not None:
                    query = query.filter(User.user_id < before_user_id)
                if after_user_id is not None:
                    query = query.filter(User.user_id > after_user_id)
                new_users = query.order_by(\
                        User.user_id.asc() \
                            if order == 'asc' \
                            else User.user_id.desc()
                ).offset(page*count).limit(count).all()

                if len(new_users) == 0:
                    # There are no more new users, wait for crawl to happen
                    log.debug('No users found, waiting for more from crawler')
                    self.application._crawler.new_user_event.clear()
                    try:
                        yield self.application._crawler.new_user_event.wait(
                                timeout=60.0)
                    except TimeoutError:
                        break
            user_data = list(map(functools.partial(
                    UserHandler._dump_user, db), new_users))

            self.set_status(200)
            self.set_header('Content-Type', 'application/json')
            self.set_header('Cache-Control',
                    'no-cache, no-store, must-revalidate')
            self.write(json.dumps({
                    'page': page,
                    'users': user_data
            }))
        finally:
            db.close()


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
        session = self._get_session_or_redirect()
        if session is None:
            return

        self.set_header('Content-Type', 'application/json')
        if not self._is_admin(session):
            self.set_status(401)
            self.write(json.dumps({
                'error': 'not an admin'
            }))
            return

        def _exec(user_id):
            db = self.application._db
            try:
                user_id = int(user_id)
                log = self.application._log.getChild('classify[%d]' % user_id)

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

                user = db.query(User).get(user_id)
                if user is None:
                    self.set_status(404)
                    self.write(json.dumps({
                        'error': 'no such user',
                        'user_id': user_id,
                    }))

                # Grab the groups for classification
                groups = dict([
                    (g.name, g) for g in db.query(Group).all()
                ])

                if classification == 'legit':
                    try:
                        user.groups.remove(groups['auto_suspect'])
                    except ValueError:
                        pass

                    try:
                        user.groups.remove(groups['auto_legit'])
                    except ValueError:
                        pass

                    try:
                        user.groups.remove(groups['suspect'])
                    except ValueError:
                        pass

                    user.groups.append(groups['legit'])
                    score_inc = 1
                    keep_detail = False
                elif classification == 'suspect':
                    try:
                        user.groups.remove(groups['auto_suspect'])
                    except ValueError:
                        pass

                    try:
                        user.groups.remove(groups['auto_legit'])
                    except ValueError:
                        pass

                    try:
                        user.groups.remove(groups['legit'])
                    except ValueError:
                        pass

                    user.groups.append(groups['suspect'])
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
                for uh in user.hostnames:
                    h = db.query(Hostname).get(uh.hostname_id)
                    h.score += uh.count * score_inc
                    h.count += uh.count

                for uw in user.words:
                    w = db.query(Word).get(uw.word_id)
                    w.score += uw.count * score_inc
                    w.count += uw.count

                for uwa in user.adj_words:
                    wa = db.query(WordAdjacent).get((
                        uwa.proceeding_id, uwa.following_id))
                    if wa is None:
                        proc_word = db.query(Word).get(
                                uwa.proceeding_id)
                        follow_word = db.query(Word).get(
                                uwa.following_id)

                        log.audit('New word adjacency: %s %s',
                                proc_word, follow_word)
                        wa = WordAdjacent(proceeding_id=proc_word.word_id,
                                following_id=follow_word.word_id,
                                score=0, count=0)
                        db.add(wa)
                    wa.score += uwa.count * score_inc
                    wa.count += uwa.count

                # Drop the user detail unless we're keeping it
                if not keep_detail:
                    if user.detail is not None:
                        db.delete(user.detail)
                    for link in user.links:
                        db.delete(link)

                # Remove user from deferred list
                du = db.query(DeferredUser).get(user_id)
                if du is not None:
                    db.delete(du)

                db.commit()
                log.info('User %d marked as %s', user_id, classification)
                res = {
                        'user_id': user_id,
                        'groups': [g.name for g in user.groups]
                }
                return res
            finally:
                db.close()

        # Wait for semaphore before proceeding
        yield self.application._classify_sem.acquire()
        try:
            # Execute the above in a worker thread
            res = yield self.application._pool.apply(_exec, (user_id,))
            self.set_status(200)
            self.write(json.dumps(res))
        finally:
            self.application._classify_sem.release()


class CallbackHandler(RequestHandler):
    @coroutine
    def get(self):
        db = self.application._db
        try:
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
            session = Session(
                    session_id=uuid.uuid4(),
                    user_id=user.user_id,
                    expiry_date=expiry)
            db.add(session)
            db.commit()

            # Grab the session ID and set that in a cookie.
            self.set_cookie(name='hadsh',
                    value=str(session.session_id),
                    domain=self.application._domain,
                    secure=self.application._secure,
                    expires_days=7)
            self.redirect('/', permanent=False)
        finally:
            db.close()


class HADSHApp(Application):
    """
    Hackaday.io Spambot Hunter application.
    """
    def __init__(self, db_uri, project_id, admin_uid,
            client_id, client_secret, api_key, api_rq_interval,
            domain, secure, thread_count, crawler_config):
        self._log = extdlog.getLogger(self.__class__.__name__)
        self._db_uri = db_uri
        # Session management connection
        self._session_db = get_db(db_uri)
        AsyncHTTPClient.configure(
                None, defaults=dict(
                    user_agent="HADSH/0.0.1 (https://hackaday.io/project/29161-hackadayio-spambot-hunter-project)"))
        self._api = HackadayAPI(client_id=client_id,
                client_secret=client_secret, api_key=api_key,
                rqlim_time=api_rq_interval,
                client=AsyncHTTPClient(), log=self._log.getChild('api'))
        self._crawler = Crawler(project_id, admin_uid, get_db(db_uri),
                self._api, self._log.getChild('crawler'),
                config=crawler_config)
        self._pool = WorkerPool(thread_count)
        self._resizer = ImageResizer(self._log.getChild('resizer'),
                self._pool)
        self._domain = domain
        self._secure = secure
        self._classify_sem = Semaphore(1)

        self._crypt_context = CryptContext([
            'argon2', 'scrypt', 'bcrypt'
        ])

        super(HADSHApp, self).__init__([
            (r"/", RootHandler),
            (r"/login", LoginHandler),
            (r"/avatar/([0-9]+)", AvatarHandler),
            (r"/user/([0-9]+)", UserHandler),
            (r"/word/([0-9]+)", WordHandler),
            (r"/wordadj/([0-9]+)", WordAdjacencyHandler),
            (r"/hostname/([0-9]+)", HostnameHandler),
            (r"/callback", CallbackHandler),
            (r"/classify/([0-9]+)", ClassifyHandler),
            (r"/data/newcomers.json", NewcomerDataHandler),
            (r"/data/legit.json", LegitUserDataHandler),
            (r"/data/suspect.json", SuspectUserDataHandler),
            (r"/data/admin.json", AdminUserDataHandler),
            (r"/authorize", RedirectHandler, {
                "url": self._api.auth_uri
            }),
        ])

    @property
    def _db(self):
        return get_db(self._db_uri)


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
            admin_uid=set(args.admin_uid),
            db_uri=args.db_uri,
            client_id=args.client_id,
            client_secret=args.client_secret,
            api_key=args.api_key,
            api_rq_interval=args.api_rq_interval,
            domain=args.domain,
            secure=args.secure,
            thread_count=args.thread_count,
            crawler_config=crawler_config
    )
    http_server = HTTPServer(application)
    http_server.listen(port=args.listen_port, address=args.listen_address)
    IOLoop.current().start()

if __name__ == '__main__':
    main()
