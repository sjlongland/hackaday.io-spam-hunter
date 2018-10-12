#!/usr/bin/env python
from socket import gaierror

from urllib.parse import urlparse

from time import time
import re

from tornado.httpclient import AsyncHTTPClient, HTTPError
from tornado.gen import coroutine, Return
from tornado.ioloop import IOLoop
from tornado.locks import Event


class TopLevelDomainCache(object):

    PUBLICSUFFIX_URI = 'https://publicsuffix.org/list/public_suffix_list.dat'
    CACHE_DURATION = 604800.0   # 1 week

    def __init__(self, list_uri=PUBLICSUFFIX_URI, cache_duration=CACHE_DURATION,
            client=None, log=None):

        if client is None:
            client = AsyncHTTPClient()
        if log is None:
            log = logging.getLogger(self.__class__.__module__)

        self._list_uri = list_uri
        self._cache_duration = int(cache_duration)
        self._cache_expiry = 0
        self._client = client
        self._log = log

        self._list = None

    @coroutine
    def refresh(self):
        if self._cache_expiry > time():
            return

        self._log.debug('Retrieving TLD listing')
        response = yield self._client.fetch(self._list_uri)

        # Strip out the wildcards, comments and blank lines.
        self._list = set(filter(
            lambda line : (len(line) > 0) \
                    and (not line.startswith('//')) \
                    and ('*' not in line),
                        response.body.decode('utf-8').split('\n')))

        self._cache_expiry = int(time()) + self._cache_duration
        self._log.debug('Cached %d entries', len(self._list))

    @coroutine
    def splitdomain(self, domain):
        """
        Take a full domain name, split it up and return the hostname
        along with the sub-domains it belongs to.  e.g.

        "foo.bar.example.com" returns [
            "example.com",          # the parent domain
            "bar.example.com",      # the sub-domain
            "foo.bar.example.com"   # the original fully-qualified hostname
        ]
        """
        # First ensure our cache is fresh
        try:
            yield self.refresh()
        except:
            # Just log if we have something to work from
            if self._list is None:
                raise
            self._log.warning('Failed to refresh cache', exc_info=1)

        # Strip out any idna encoded bits.  This might fail if we're
        # given a domain with the IDNA stuff worked out already or if
        # we're given a byte string (we shouldn't).
        try:
            domain = domain.encode('us-ascii').decode('idna')
        except:
            pass

        result = []
        suffix_parts = []
        for part in reversed(domain.split('.')):
            suffix_parts.insert(0, part)
            suffix = '.'.join(suffix_parts)
            if suffix not in self._list:
                result.append(suffix)

        raise Return(result)
