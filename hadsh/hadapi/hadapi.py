#!/usr/bin/env python

import json
import logging

from tornado.httpclient import AsyncHTTPClient
from tornado.ioloop import IOLoop
from tornado.gen import coroutine, Return, sleep
from tornado.locks import Semaphore
from enum import Enum

try:
    from urllib import parse as urlparse
except ImportError:
    import urlparse

from cgi import parse_header


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
    RQLIM_NUM=5    # requests
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
        # Ideally, encoding should be in the content type
        (ct, ctopts) = parse_header(response.headers['Content-Type'])
        encoding = ctopts.get('charset', default_encoding)

        # Return the decoded payload along with the content-type.
        return (ct, ctopts, response.body.decode(encoding))

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

        encode_kv = lambda k, v : '%s=%s' % (k, quote_plus(str(v)))
        def encode_item(item):
            (key, value) = item
            if isinstance(value, list):
                return '&'.join(map(lambda v : encode_kv(key, v), value))
            else:
                return encode_kv(key, value)

        uri += '?%s' % '&'.join(map(encode_item, query.items()))

        if not uri.startswith('http'):
            uri = self._api_uri + uri

        try:
            yield self._rq_sem.acquire()
            yield self._ratelimit_sleep()
            response = yield self._client.fetch(uri, **kwargs)
        finally:
            self._rq_sem.release()

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
                CLIENT_ID=quote_plus(self._client_id),
                CLIENT_SECRET=quote_plus(self._client_secret),
                CODE=quote_plus(code)
        )

        return self._api_call(
            post_uri, method='POST', body=b'', api_key=False)

    # Pagination options

    def _page_query_opts(self, page, perpage):
        query = {}
        if page is not None:
            query['page'] = int(page)
        if perpage is not None:
            query['perpage'] = int(perpage)

        return query

    # User API endpoints

    def get_current_user(self, token):
        """
        Fetch the current user's profile information.
        """
        return self._api_call('/me', token=token)

    def _user_query_opts(self, sortby, page, perpage):
        query = _page_query_opts(page, perpage)
        sortby = UserSortBy(sortby)
        query['sortby'] = sortby.value
        return query

    def get_users(self, sortby=UserSortBy.influence,
            ids=None, page=None, perpage=None):
        """
        Retrieve a list of all users
        """
        query = self._user_query_opts(sortby, page, perpage)

        if ids is None:
            return self._api_call('/users', query=query)
        elif isinstance(ids, slice):
            query['ids'] = '%d,%d' % (slice.start, slice.stop)
            return self._api_call('/users/range', query=query)
        else:
            ids = list(ids)
            if len(ids) > 50:
                raise ValueError('Too many IDs')
            query['ids'] = ','.join(ids)
            return self._api_call('/users/batch', query=query)

    def search_users(self, screen_name=None, location=None, tag=None,
            sortby=UserSortBy.influence, page=None, perpage=None):
        query = self._user_query_opts(sortby, page, perpage)

        for (arg, val) in   (   ('screen_name', screen_name),
                                ('location', location),
                                ('tag', tag)    ):
            if val is not None:
                query[arg] = str(val)
        return self._api_call('/users/search', query=query)

    def get_user(self, user_id):
        return self._api_call('/users/%d' % user_id)

    def get_user_followers(self, user_id,
            sortby=UserSortBy.influence, page=None, perpage=None):
        query = self._user_query_opts(sortby, page, perpage)
        return self._api_call('/users/%d/followers' % user_id, query=query)

    def get_user_following(self, user_id,
            sortby=UserSortBy.influence, page=None, perpage=None):
        query = self._user_query_opts(sortby, page, perpage)
        return self._api_call('/users/%d/following' % user_id, query=query)

    def get_user_projects(self, user_id,
            sortby=ProjectSortBy.skulls, page=None, perpage=None):
        query = self._project_query_opts(sortby, page, perpage)
        return self._api_call('/users/%d/projects' % user_id, query=query)

    def get_user_skulls(self, user_id,
            sortby=UserSortBy.influence, page=None, perpage=None):
        query = self._user_query_opts(sortby, page, perpage)
        return self._api_call('/users/%d/skulls' % user_id, query=query)

    def get_user_links(self, user_id, page=None, perpage=None):
        query = self._page_query_opts(page, perpage)
        return self._api_call('/users/%d/links' % user_id, query=query)

    def get_user_tags(self, user_id, page=None, perpage=None):
        query = self._page_query_opts(page, perpage)
        return self._api_call('/users/%d/tags' % user_id, query=query)

    def get_user_pages(self, user_id, page=None, perpage=None):
        query = self._page_query_opts(page, perpage)
        return self._api_call('/users/%d/pages' % user_id, query=query)

    # Projects API

    def _project_query_opts(self, sortby, page, perpage):
        query = _page_query_opts(page, perpage)
        sortby = ProjectSortBy(sortby)
        query['sortby'] = sortby.value
        return query

    def get_projects(self, sortby=ProjectSortBy.influence,
            ids=None, page=None, perpage=None):
        """
        Retrieve a list of all projects
        """
        query = self._project_query_opts(sortby, page, perpage)

        if ids is None:
            return self._api_call('/projects', query=query)
        elif isinstance(ids, slice):
            query['ids'] = '%d,%d' % (slice.start, slice.stop)
            return self._api_call('/projects/range', query=query)
        else:
            ids = list(ids)
            if len(ids) > 50:
                raise ValueError('Too many IDs')
            query['ids'] = ','.join(ids)
            return self._api_call('/projects/batch', query=query)

    def search_projects(self, term,
            sortby=ProjectSortBy.influence, page=None, perpage=None):
        query = self._project_query_opts(sortby, page, perpage)
        query['search_term'] = str(term)
        return self._api_call('/projects/search', query=query)

    def get_project(self, project_id):
        return self._api_call('/projects/%d' % project_id)

    def get_project_team(self, project_id,
            sortby=UserSortBy.influence, page=None, perpage=None):
        query = self._user_query_opts(sortby, page, perpage)
        return self._api_call('/projects/%d/team' % project_id, query=query)

    def get_project_followers(self, project_id,
            sortby=UserSortBy.influence, page=None, perpage=None):
        query = self._user_query_opts(sortby, page, perpage)
        return self._api_call('/projects/%d/followers' % project_id,
                query=query)

    def get_project_skulls(self, project_id,
            sortby=UserSortBy.influence, page=None, perpage=None):
        query = self._user_query_opts(sortby, page, perpage)
        return self._api_call('/projects/%d/skulls' % project_id,
                query=query)

    def get_project_comments(self, project_id,
            sortby=UserSortBy.influence, page=None, perpage=None):
        query = self._user_query_opts(sortby, page, perpage)
        return self._api_call('/projects/%d/comments' % project_id,
                query=query)

    def get_project_links(self, project_id, page=None, perpage=None):
        query = self._page_query_opts(page, perpage)
        return self._api_call('/projects/%d/links' % project_id,
                query=query)

    def get_project_images(self, project_id, page=None, perpage=None):
        query = self._page_query_opts(page, perpage)
        return self._api_call('/projects/%d/images' % project_id,
                query=query)

    def get_project_components(self, project_id, page=None, perpage=None):
        query = self._page_query_opts(page, perpage)
        return self._api_call('/projects/%d/components' % project_id,
                query=query)

    def get_project_tags(self, project_id, page=None, perpage=None):
        query = self._page_query_opts(page, perpage)
        return self._api_call('/projects/%d/tags' % project_id, query=query)

    def get_project_logs(self, project_id, page=None, perpage=None):
        query = self._page_query_opts(page, perpage)
        return self._api_call('/projects/%d/logs' % project_id, query=query)

    def get_project_instructions(self, project_id, page=None, perpage=None):
        query = self._page_query_opts(page, perpage)
        return self._api_call('/projects/%d/instructions' % project_id,
                query=query)

    def get_project_details(self, project_id, page=None, perpage=None):
        query = self._page_query_opts(page, perpage)
        return self._api_call('/projects/%d/details' % project_id,
                query=query)
