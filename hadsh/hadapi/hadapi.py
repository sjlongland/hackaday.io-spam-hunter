#!/usr/bin/env python

import re
import json
import logging

from tornado.httpclient import AsyncHTTPClient, HTTPError
from tornado.ioloop import IOLoop
from tornado.gen import coroutine, Return, sleep
from tornado.locks import Semaphore
from enum import Enum
from socket import gaierror
from errno import EAGAIN

from ..util import decode_body

try:
    from urllib import parse as urlparse
except ImportError:
    import urlparse


class UserSortBy(Enum):
    influence='influence'
    newest='newest'
    followers='followers'
    projects='projects'
    skulls='skulls'


class ProjectSortBy(Enum):
    skulls='skulls'
    newest='newest'
    views='views'
    comments='comments'
    followers='followers'
    updated='updated'


class HackadayAPI(object):
    """
    Core Hackaday.io API handler.
    """

    HAD_API_URI='https://api.hackaday.io/v1'
    HAD_AUTH_URI='https://hackaday.io/authorize'\
            '?client_id=%(CLIENT_ID)s'\
            '&response_type=code'
    HAD_TOKEN_URI='https://auth.hackaday.io/access_token'\
            '?client_id=%(CLIENT_ID)s'\
            '&client_secret=%(CLIENT_SECRET)s'\
            '&code=%(CODE)s'\
            '&grant_type=authorization_code'

    # Rate limiting
    RQLIM_NUM=1    # requests
    RQLIM_TIME=10  # seconds
    RQLIM_CONCURRENT=1

    def __init__(self, client_id, client_secret, api_key,
            api_uri=HAD_API_URI, auth_uri=HAD_AUTH_URI,
            token_uri=HAD_TOKEN_URI, rqlim_num=RQLIM_NUM,
            rqlim_time=RQLIM_TIME, rqlim_concurrent=RQLIM_CONCURRENT,
            client=None, log=None, io_loop=None):

        if log is None:
            log = logging.getLogger(self.__class__.__module__)

        if io_loop is None:
            io_loop = IOLoop.current()

        if client is None:
            client = AsyncHTTPClient()

        self._client = client
        self._io_loop = io_loop
        self._log = log
        self._client_id = client_id
        self._client_secret = client_secret
        self._api_key = api_key
        self._api_uri = api_uri
        self._auth_uri = auth_uri
        self._token_uri = token_uri

        # Timestamps of last rqlim_num requests
        self._last_rq = []
        self._rqlim_num = rqlim_num
        self._rqlim_time = rqlim_time

        # Semaphore to limit concurrent access
        self._rq_sem = Semaphore(rqlim_concurrent)

        # If None, then no "forbidden" status is current.
        # Otherwise, this stores when the "forbidden" flag expires.
        self._forbidden_expiry = None

    @property
    def is_forbidden(self):
        """
        Return true if the last request returned a "forbidden" response
        code and was made within the last hour.
        """
        if self._forbidden_expiry is None:
            return False

        return self._forbidden_expiry > self._io_loop.time()

    @coroutine
    def _ratelimit_sleep(self):
        """
        Ensure we don't exceed the rate limit by tracking the request
        timestamps and adding a sleep if required.
        """
        now = self._io_loop.time()

        # Push the current request expiry time to the end.
        self._last_rq.append(now + self._rqlim_time)

        # Drop any that are more than rqlim_time seconds ago.
        self._last_rq = list(filter(lambda t : t < now, self._last_rq))

        # Are there rqlim_num or more requests?
        if len(self._last_rq) < self._rqlim_num:
            # There aren't, we can go.
            return

        # When does the next one expire?
        expiry = self._last_rq[0]

        # Wait until then
        delay = expiry - now

        self._log.debug('Waiting %f sec for rate limit', delay)
        yield sleep(delay)
        self._log.debug('Resuming operations')

    def _decode(self, response, default_encoding='UTF-8'):
        """
        Decode a given reponse body.
        """
        return decode_body(response.headers['Content-Type'], response.body,
                default_encoding)

    @coroutine
    def _api_call(self, uri, query=None, token=None, api_key=True, **kwargs):
        headers = kwargs.setdefault('headers', {})
        headers.setdefault('Accept', 'application/json')
        if token is not None:
            headers['Authorization'] = 'token %s' % token

        if query is None:
            query = {}

        if api_key:
            query.setdefault('api_key', self._api_key)

        self._log.debug('Query arguments: %r', query)
        encode_kv = lambda k, v : '%s=%s' % (k, urlparse.quote_plus(str(v)))
        def encode_item(item):
            (key, value) = item
            if isinstance(value, list):
                return '&'.join(map(lambda v : encode_kv(key, v), value))
            else:
                return encode_kv(key, value)

        if len(query) > 0:
            uri += '?%s' % '&'.join(map(encode_item, query.items()))

        if not uri.startswith('http'):
            uri = self._api_uri + uri

        if 'connect_timeout' not in kwargs:
            kwargs['connect_timeout'] = 120.0
        if 'request_timeout' not in kwargs:
            kwargs['request_timeout'] = 120.0

        try:
            yield self._rq_sem.acquire()
            yield self._ratelimit_sleep()
            self._log.debug('%s %r', kwargs.get('method','GET'), uri)
            while True:
                try:
                    response = yield self._client.fetch(uri, **kwargs)
                    self._log.debug('Request:\n'
                        '%s %s\n'
                        'Headers: %s\n'
                        'Response: %s\n'
                        'Headers: %s\n'
                        'Body:\n%s',
                        response.request.method,
                        response.request.url,
                        response.request.headers,
                        response.code,
                        response.headers,
                        response.body)
                    break
                except gaierror as e:
                    if e.errno != EAGAIN:
                        raise
                except HTTPError as e:
                    self._log.debug('Request:\n'
                        '%s %s\n'
                        'Headers: %s\n'
                        'Response: %s\n'
                        'Headers: %s\n'
                        'Body:\n%s',
                        e.response.request.method,
                        e.response.request.url,
                        e.response.request.headers,
                        e.response.code,
                        e.response.headers,
                        e.response.body)
                    if e.code == 403:
                        # Back-end is rate limiting us.  Back off an hour.
                        self._forbidden_expiry = self._io_loop.time() \
                                + 3600.0
                    raise
                except ConnectionResetError:
                    # Back-end is blocking us.  Back off a minute.
                    self._forbidden_expiry = self._io_loop.time() \
                            + 60.0
                    raise
        finally:
            self._rq_sem.release()

        # If we get here, then our service is back.
        self._forbidden_expiry = None
        (ct, ctopts, body) = self._decode(response)
        if ct.lower() != 'application/json':
            raise ValueError('Server returned unrecognised type %s' % ct)
        raise Return(json.loads(body))

    # oAuth endpoints

    @property
    def auth_uri(self):
        """
        Return the auth URI that we need to send the user to if they're not
        logged in.
        """
        return self._auth_uri % dict(CLIENT_ID=self._client_id)

    def get_token(self, code):
        """
        Fetch the token for API queries from the authorization code given.
        """
        # Determine where to retrieve the access token from
        post_uri = self._token_uri % dict(
                CLIENT_ID=urlparse.quote_plus(self._client_id),
                CLIENT_SECRET=urlparse.quote_plus(self._client_secret),
                CODE=urlparse.quote_plus(code)
        )

        return self._api_call(
            post_uri, method='POST', body=b'', api_key=False)

    # Pagination options

    def _page_query_opts(self, page, per_page):
        query = {}
        if page is not None:
            query['page'] = int(page)
        if per_page is not None:
            query['per_page'] = int(per_page)

        return query

    # User API endpoints

    def get_current_user(self, token):
        """
        Fetch the current user's profile information.
        """
        return self._api_call('/me', token=token)

    def _user_query_opts(self, sortby, page, per_page):
        query = self._page_query_opts(page, per_page)
        sortby = UserSortBy(sortby)
        query['sortby'] = sortby.value
        return query

    _GET_USERS_WORKAROUND_RE = re.compile(
            '    <a href="/hacker/(\d+)" class="hacker-image">')
    @coroutine
    def _get_users_workaround(self, sortby=UserSortBy.influence, page=None,
            per_page=None):
        if page is None:
            page = 1
        if per_page is None:
            per_page = 50

        sortby = UserSortBy(sortby)
        response = yield self._client.fetch(
                'https://hackaday.io/hackers?sort=%s&page=%d' \
                        % (sortby.value, page))
        self._log.debug('Request:\n'
            '%s %s\n'
            'Headers: %s\n'
            'Response: %s\n'
            'Headers: %s\n'
            'Body:\n%s',
            response.request.method,
            response.request.url,
            response.request.headers,
            response.code,
            response.headers,
            response.body)
        (ct, ctopts, body) = self._decode(response)

        # Body is in HTML
        ids = []
        for line in body.split('\n'):
            match = self._GET_USERS_WORKAROUND_RE.match(line)
            if match:
                ids.append(int(match.group(1)))

        users = yield self.get_users(ids=ids, per_page=per_page)
        raise Return(users)


    @coroutine
    def get_users(self, sortby=UserSortBy.influence,
            ids=None, page=None, per_page=None):
        """
        Retrieve a list of all users
        """
        query = self._user_query_opts(sortby, page, per_page)

        if ids is None:
            # This is currently broken for sortby=newest,
            # see if it's fixed though.
            try:
                result = yield self._api_call('/users', query=query)
            except HTTPError:
                # Nope, it isn't work-around time!
                result = yield self._get_users_workaround(sortby,
                        query.get('page'), query.get('per_page'))
        elif isinstance(ids, slice):
            query['ids'] = '%d,%d' % (ids.start, ids.stop)
            result = yield self._api_call('/users/range', query=query)
        else:
            ids = set(ids)
            if len(ids) > 50:
                raise ValueError('Too many IDs')
            query['ids'] = ','.join(['%d' % uid for uid in ids])
            result = yield self._api_call('/users/batch', query=query)
        raise Return(result)

    def search_users(self, screen_name=None, location=None, tag=None,
            sortby=UserSortBy.influence, page=None, per_page=None):
        query = self._user_query_opts(sortby, page, per_page)

        for (arg, val) in   (   ('screen_name', screen_name),
                                ('location', location),
                                ('tag', tag)    ):
            if val is not None:
                query[arg] = str(val)
        return self._api_call('/users/search', query=query)

    def get_user(self, user_id):
        return self._api_call('/users/%d' % user_id)

    def get_user_followers(self, user_id,
            sortby=UserSortBy.influence, page=None, per_page=None):
        query = self._user_query_opts(sortby, page, per_page)
        return self._api_call('/users/%d/followers' % user_id, query=query)

    def get_user_following(self, user_id,
            sortby=UserSortBy.influence, page=None, per_page=None):
        query = self._user_query_opts(sortby, page, per_page)
        return self._api_call('/users/%d/following' % user_id, query=query)

    def get_user_projects(self, user_id,
            sortby=ProjectSortBy.skulls, page=None, per_page=None):
        query = self._project_query_opts(sortby, page, per_page)
        return self._api_call('/users/%d/projects' % user_id, query=query)

    def get_user_skulls(self, user_id,
            sortby=UserSortBy.influence, page=None, per_page=None):
        query = self._user_query_opts(sortby, page, per_page)
        return self._api_call('/users/%d/skulls' % user_id, query=query)

    def get_user_links(self, user_id, page=None, per_page=None):
        query = self._page_query_opts(page, per_page)
        return self._api_call('/users/%d/links' % user_id, query=query)

    def get_user_tags(self, user_id, page=None, per_page=None):
        query = self._page_query_opts(page, per_page)
        return self._api_call('/users/%d/tags' % user_id, query=query)

    def get_user_pages(self, user_id, page=None, per_page=None):
        query = self._page_query_opts(page, per_page)
        return self._api_call('/users/%d/pages' % user_id, query=query)

    # Projects API

    def _project_query_opts(self, sortby, page, per_page):
        query = self._page_query_opts(page, per_page)
        sortby = ProjectSortBy(sortby)
        query['sortby'] = sortby.value
        return query

    def get_projects(self, sortby=ProjectSortBy.skulls,
            ids=None, page=None, per_page=None):
        """
        Retrieve a list of all projects
        """
        query = self._project_query_opts(sortby, page, per_page)

        if ids is None:
            return self._api_call('/projects', query=query)
        elif isinstance(ids, slice):
            query['ids'] = '%d,%d' % (slice.start, slice.stop)
            return self._api_call('/projects/range', query=query)
        else:
            ids = set(ids)
            if len(ids) > 50:
                raise ValueError('Too many IDs')
            query['ids'] = ','.join(['%d' % pid for pid in ids])
            return self._api_call('/projects/batch', query=query)

    def search_projects(self, term,
            sortby=ProjectSortBy.skulls, page=None, per_page=None):
        query = self._project_query_opts(sortby, page, per_page)
        query['search_term'] = str(term)
        return self._api_call('/projects/search', query=query)

    def get_project(self, project_id):
        return self._api_call('/projects/%d' % project_id)

    def get_project_team(self, project_id,
            sortby=UserSortBy.influence, page=None, per_page=None):
        query = self._user_query_opts(sortby, page, per_page)
        return self._api_call('/projects/%d/team' % project_id, query=query)

    def get_project_followers(self, project_id,
            sortby=UserSortBy.influence, page=None, per_page=None):
        query = self._user_query_opts(sortby, page, per_page)
        return self._api_call('/projects/%d/followers' % project_id,
                query=query)

    def get_project_skulls(self, project_id,
            sortby=UserSortBy.influence, page=None, per_page=None):
        query = self._user_query_opts(sortby, page, per_page)
        return self._api_call('/projects/%d/skulls' % project_id,
                query=query)

    def get_project_comments(self, project_id,
            sortby=UserSortBy.influence, page=None, per_page=None):
        query = self._user_query_opts(sortby, page, per_page)
        return self._api_call('/projects/%d/comments' % project_id,
                query=query)

    def get_project_links(self, project_id, page=None, per_page=None):
        query = self._page_query_opts(page, per_page)
        return self._api_call('/projects/%d/links' % project_id,
                query=query)

    def get_project_images(self, project_id, page=None, per_page=None):
        query = self._page_query_opts(page, per_page)
        return self._api_call('/projects/%d/images' % project_id,
                query=query)

    def get_project_components(self, project_id, page=None, per_page=None):
        query = self._page_query_opts(page, per_page)
        return self._api_call('/projects/%d/components' % project_id,
                query=query)

    def get_project_tags(self, project_id, page=None, per_page=None):
        query = self._page_query_opts(page, per_page)
        return self._api_call('/projects/%d/tags' % project_id, query=query)

    def get_project_logs(self, project_id, page=None, per_page=None):
        query = self._page_query_opts(page, per_page)
        return self._api_call('/projects/%d/logs' % project_id, query=query)

    def get_project_instructions(self, project_id, page=None, per_page=None):
        query = self._page_query_opts(page, per_page)
        return self._api_call('/projects/%d/instructions' % project_id,
                query=query)

    def get_project_details(self, project_id, page=None, per_page=None):
        query = self._page_query_opts(page, per_page)
        return self._api_call('/projects/%d/details' % project_id,
                query=query)
