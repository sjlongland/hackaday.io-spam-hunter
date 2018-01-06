#!/usr/bin/env python

import argparse
import logging
import uuid
import datetime
import pytz

from tornado.web import Application, RequestHandler, \
        RedirectHandler
from tornado.httpclient import AsyncHTTPClient
from tornado.httpserver import HTTPServer
from tornado.gen import coroutine
from tornado.ioloop import IOLoop

from .hadapi.hadapi import HackadayAPI
from .db.db import get_db, User, Group, GroupMember, Session, UserDetail, \
        UserLink, Avatar, Tag, UserTag


class AuthRequestHandler(RequestHandler):
    def _get_session_or_redirect(self):
        # Are we logged in?
        session_id = self.get_cookie('hadsh')
        if session_id is None:
            # Not yet logged in
            self.redirect('/authorize')
            return

        # Fetch the user details from the session
        session = self.application._db.query(Session).get(session_id)
        if session is None:
            # Session is invalid
            self.redirect('/authorize')
            return

        return session


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


class CallbackHandler(RequestHandler):
    @coroutine
    def get(self):
        log = self.application._log.getChild('callback')

        # Retrieve the code
        code = self.get_query_argument('code', strip=False)
        log.debug('Code is %s, retrieving token', code)
        oauth_data = yield self.application._api.get_token(code)
        log.debug('OAuth response %s', oauth_data)
        token = oauth_data['access_token']
        user_data = yield self.application._api.get_current_user(token)

        # Do we have their avatar on file?
        avatar = self.application._db.query(Avatar).filter(
                Avatar.url==user_data['image_url']).first()
        if avatar is None:
            # We don't have the avatar yet
            log.debug('Retrieving avatar at %s',
                    user_data['image_url'])
            avatar_res = yield self.application._client.fetch(
                    user_data['image_url'])
            avatar = Avatar(url=user_data['image_url'],
                        avatar_type=avatar_res.headers['Content-Type'],
                        avatar=avatar_res.body)
            self.application._db.add(avatar)
            self.application._db.commit()

        # Look up the user in the database
        user = self.application._db.query(User).get(user_data['id'])
        if user is None:
            # New user
            user = User(user_id=user_data['id'],
                        screen_name=user_data['screen_name'],
                        url=user_data['url'],
                        avatar_id=avatar.avatar_id,
                        last_update=datetime.datetime.now(tz=pytz.utc))
            self.application._db.add(user)
        else:
            # Existing user, update the user details
            user.screen_name = user_data['screen_name']
            user.avatar_id=avatar.avatar_id
            user.url = user_data['url']
            user.last_update = datetime.datetime.now(tz=pytz.utc)

        self.application._db.commit()

        # We have the user account, create the session
        session = Session(
                session_id=uuid.uuid4(),
                user_id=user.user_id,
                token=token)
        self.application._db.add(session)
        self.application._db.commit()

        # Grab the session ID and set that in a cookie.
        self.set_cookie(name='hadsh',
                value=str(session.session_id),
                domain=self.application._domain,
                secure=self.application._secure,
                expires_days=7)
        self.redirect('/', permanent=False)


class HADSHApp(Application):
    """
    Hackaday.io Spambot Hunter application.
    """
    def __init__(self, db_uri, client_id, client_secret, api_key,
            domain, secure):
        self._log = logging.getLogger(self.__class__.__name__)
        self._db = get_db(db_uri)
        self._client = AsyncHTTPClient()
        self._api = HackadayAPI(client_id=client_id,
                client_secret=client_secret, api_key=api_key,
                client=self._client, log=self._log.getChild('api'))
        self._domain = domain
        self._secure = secure
        super(HADSHApp, self).__init__([
            (r"/", RootHandler),
            (r"/callback", CallbackHandler),
            (r"/authorize", RedirectHandler, {
                "url": self._api.auth_uri
            }),
        ])


def main(*args, **kwargs):
    """
    Console entry point.
    """
    parser = argparse.ArgumentParser(
            description='HAD Spambot Hunter Project')
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
    parser.add_argument('--listen-address', dest='listen_address',
            default='', help='Interface address to listen on.')
    parser.add_argument('--listen-port', dest='listen_port', type=int,
            default=3000, help='Port number (TCP) to listen on.')
    parser.add_argument('--log-level', dest='log_level',
            default='INFO', help='Logging level')

    args = parser.parse_args(*args, **kwargs)

    # Start logging
    logging.basicConfig(level=args.log_level)

    # Validate arguments
    if (args.client_id is None) or \
            (args.client_secret is None) or \
            (args.api_key is None):
        raise ValueError('--client-id, --client-secret and '\
                '--user-key are mandatory.  Retrieve those '\
                'when you register at '\
                'https://dev.hackaday.io/applications')

    application = HADSHApp(
            db_uri=args.db_uri,
            client_id=args.client_id,
            client_secret=args.client_secret,
            api_key=args.api_key,
            domain=args.domain,
            secure=args.secure
    )
    http_server = HTTPServer(application)
    http_server.listen(port=args.listen_port, address=args.listen_address)
    IOLoop.current().start()

if __name__ == '__main__':
    main()
