#!/usr/bin/env python

import code
import codecs
import contextlib
import csv
import gc
import hashlib
import json
import logging
import os
import re
import sys
import time
import urlparse
import zlib

from gexf import Gexf, GexfImport
from datetime import datetime
from optparse import OptionParser
from ConfigParser import SafeConfigParser
from collections import defaultdict, namedtuple

from xml.etree import cElementTree as ElementTree

import networkx as nx

import oauth2

from twisted.internet import reactor, stdio
from twisted.internet.defer import (
    Deferred, DeferredList, DeferredSemaphore, maybeDeferred, gatherResults,
    inlineCallbacks, returnValue,
)

from twisted.python.failure import Failure
from twisted.web.client import getPage
from twisted.web.error import Error as HTTPError

from sqlalchemy import (
    Table, Column, MetaData, ForeignKey, PrimaryKeyConstraint,
    Integer, Float, Boolean, LargeBinary, String, Unicode, UnicodeText, DateTime,
    create_engine, select, bindparam, func,
    and_, or_, not_,
)

try:
    import matplotlib.pyplot as plt
except ImportError:
    pass
else:
    def draw(g, *args, **kwargs):
        """Draw a graph using matplotlib.  This is only suitable for very
        small graphs (100 nodes or less)."""
        nx.draw(g, *args, **kwargs)
        plt.show()


def chunkize(iterable, chunk_size=200):
    l = list(iterable)
    for i in range(0, len(l), chunk_size):
        yield l[i : i + chunk_size]

@contextlib.contextmanager
def timed(logfunc, message, *args, **kargs):
    t1 = time.time()
    yield
    dt = time.time() - t1
    logfunc("spent %.1f s. " + message, dt, *args, **kargs)



class ThrottlingError(RuntimeError):
    pass


class RateLimitedClient(object):
    """A Web client with per-second request limit.
    """

    # Max number of requests per second (can be < 1.0)
    rate_limit = None
    # Grace delay (seconds) when the server throttles us
    grace_delay = 30
    # Max number of parallel requests
    max_concurrency = 5

    def __init__(self, time=None):
        self.sem = DeferredSemaphore(self.max_concurrency)
        self.grace_deferred = None
        self.logger = logging.getLogger("webclient")
        self.time = time or reactor
        self.last_request = 0.0

    def _enable_grace_delay(self, delay):
        if self.grace_deferred:
            # Already enabled by an earlier concurrent request
            return
        self.grace_deferred = Deferred()
        def expire():
            g = self.grace_deferred
            self.grace_deferred = None
            g.callback(None)
        reactor.callLater(self.grace_delay, expire)

    def _delay_if_necessary(self, func, *args, **kwargs):
        d = Deferred()
        d.addCallback(lambda _: func(*args, **kwargs))
        trigger = None
        if self.grace_deferred:
            trigger = self.grace_deferred
        elif self.rate_limit:
            delay = (self.last_request + 1.0 / self.rate_limit) - self.time.seconds()
            if delay > 0:
                self.logger.debug("inserting rate limit delay of %.1f", delay)
                trigger = Deferred()
                self.time.callLater(delay, trigger.callback, None)
        (trigger or maybeDeferred(lambda: None)).chainDeferred(d)
        return d

    def get_page(self, url, *args, **kwargs):
        if isinstance(url, unicode):
            url = url.encode('utf8')

        def schedule_request(_):
            return self._delay_if_necessary(issue_request, None)

        def issue_request(_):
            self.last_request = self.time.seconds()
            self.logger.debug("fetching %r", url)
            return getPage(url, *args, **kwargs)

        def handle_success(value):
            self.sem.release()
            self.logger.debug("got %d bytes for %r", len(value), url)
            return value

        def handle_error(failure):
            self.sem.release()
            failure.trap(HTTPError)
            self.logger.debug("got HTTP error %s", failure.value)
            self.trap_throttling(failure)
            delay = self.grace_delay
            self.logger.warning("we are throttled, delaying by %.1f seconds",
                                delay)
            self._enable_grace_delay(delay)
            # auto-retry
            return do_get_page()

        def do_get_page():
            # We acquire the semaphore *before* seeing if we should delay
            # the request, so that we avoid pounding on the server when
            # the grace period is entered.
            d = self.sem.acquire()
            d.addCallback(schedule_request)
            d.addCallbacks(handle_success, handle_error)
            return d

        return do_get_page()

    def trap_throttling(self, failure):
        """Trap HTTP failures and return if we are
        throttled by the distant site, else re-raise.
        """
        e = failure.value
        if e.status in ("400", "420", "500", "503"):
            return
        failure.raiseException()


class TwitterClient(RateLimitedClient):

    rate_limit = 10
    grace_delay = 30

    base_api_url = "http://api.twitter.com/1/"

    def __init__(self, consumer_key, consumer_secret,
                       access_token, access_token_secret):
        RateLimitedClient.__init__(self)
        self.consumer = oauth2.Consumer(key=consumer_key,
                                        secret=consumer_secret)
        self.token = oauth2.Token(key=access_token,
                                  secret=access_token_secret)
        self.signature_method = oauth2.SignatureMethod_HMAC_SHA1()

    def _make_authenticated_url(self, url, params):
        req = oauth2.Request.from_consumer_and_token(
            self.consumer, self.token, 'GET', url, params)
        req.sign_request(self.signature_method, self.consumer, self.token)
        return req.to_url()

    def get_results(self, api_url, **params):
        url = urlparse.urljoin(self.base_api_url, api_url)
        url = self._make_authenticated_url(url, params)
        return self.get_page(url).addCallback(json.loads)

    def lookup_users(self, user_ids):
        chunk_size = 100
        l = []
        def _group_users(user_list):
            return dict(
                (u['screen_name'], u)
                for u in user_list
            )
        def _merge_dicts(dict_list):
            merged = {}
            for d in dict_list:
                merged.update(d)
            return merged

        for chunk in chunkize(user_ids, chunk_size):
            d = self.get_results('http://api.twitter.com/1/users/lookup.json',
                screen_name=','.join(chunk))
            d.addCallback(_group_users)
            l.append(d)

        return gatherResults(l).addCallback(_merge_dicts)


class RetweetRankClient(RateLimitedClient):

    rate_limit = 0.8
    max_concurrency = 5
    grace_delay = 10

    url_pat = "http://api.retweetrank.com/rank/%(twitter_id)s.xml?appid=%(app_id)s"

    def __init__(self, app_id):
        RateLimitedClient.__init__(self)
        self.app_id = app_id

    @classmethod
    def _parse_xml(cls, xml_string, twitter_id):
        """Parse XML payload of the web service response.
        Return a dictionnary containing 'rank' (an int) and 'percentile'
        (a float)."""
        try:
            xml = ElementTree.fromstring(xml_string)
        except ElementTree.ParseError:
            # invalid XML happens when a "&" in an URL is unescaped
            # (example with the "2012villepin" user)
            logging.getLogger("retweetrank").warn(
                "got invalid XML for user %r", twitter_id)
            return None
        return {
            'rank': int(xml.find('rank').text),
            # abs() will remove negative zeroes
            'percentile': abs(float(xml.find('percentile').text)),
        }

    def _trap_failure(self, failure):
        """When a 404 is received, return None (user is unknown)."""
        failure.trap(HTTPError)
        if failure.value.status == "404":
            return None
        failure.raiseException()

    def get_rank(self, twitter_id):
        url = self.url_pat % {
            'twitter_id': twitter_id,
            'app_id': self.app_id }
        d = self.get_page(url)
        d.addCallback(self._parse_xml, twitter_id)
        d.addErrback(self._trap_failure)
        return d


class DeliciousClient(RateLimitedClient):

    rate_limit = 1.0
    max_concurrency = 5
    grace_delay = 10

    url_pat = "http://feeds.delicious.com/v2/json/urlinfo/%(md5)s"

    def __init__(self):
        RateLimitedClient.__init__(self)

    def _possible_urls(self, site):
        yield site
        # yield "http://" + site + "/"
        # yield "http://www." + site + "/"

    # Sample output from the urlinfo API:
    # [{"hash":"e234eb4c528169e3471d81823074965e",
    #   "title":"Le Monde",
    #   "url":"http:\/\/www.lemonde.fr\/",
    #   "total_posts":6639,
    #   "top_tags":{"news":2319,"france":1308,"presse":1118,"french":1056,
    #               "journal":923,"newspaper":854,"media":548,"quotidien":317,
    #               "actualit\u00e9":255,"information":223}}]

    @inlineCallbacks
    def get_urlinfo(self, site):
        for site_url in self._possible_urls(site):
            h = hashlib.md5(site_url)
            hexmd5 = h.hexdigest()
            binmd5 = h.digest()
            url = self.url_pat % { 'md5': hexmd5 }
            data = yield self.get_page(url)
            data = json.loads(data)
            assert isinstance(data, list)
            if data:
                break
        data = data[0] if data else None
        returnValue((site, binmd5, data))


# https://dev.twitter.com/docs/rate-limiting
# https://dev.twitter.com/docs/rate-limiting/faq
# https://dev.twitter.com/docs/auth/oauth
# https://dev.twitter.com/docs/auth/oauth/single-user-with-examples
# http://www.delicious.com/help/api
# http://www.retweetrank.com/view/api


def unicode_reader(tuples, encoding='utf8'):
    for t in tuples:
        yield [s.decode(encoding) for s in t]

def enable_readline_completion():
    try:
        # this add completion to python interpreter
        import readline
        import rlcompleter
        # see readline man page for this
        readline.parse_and_bind("set show-all-if-ambiguous on")
        readline.parse_and_bind("tab: complete")
    except:
        pass

class IteratingConsole(code.InteractiveConsole):
    def resetbuffer(self):
        reactor.iterate(0.05)
        code.InteractiveConsole.resetbuffer(self)


def interact(banner=None, readfunc=None, local=None):
    # Straight from the standard `code` module, except InteractiveConsole
    # replaced with IteratingConsole
    """Closely emulate the interactive Python interpreter.

    This is a backwards compatible interface to the InteractiveConsole
    class.  When readfunc is not specified, it attempts to import the
    readline module to enable GNU readline if it is available.

    Arguments (all optional, all default to None):

    banner -- passed to InteractiveConsole.interact()
    readfunc -- if not None, replaces InteractiveConsole.raw_input()
    local -- passed to InteractiveInterpreter.__init__()

    """
    console = IteratingConsole(local)
    if readfunc is not None:
        console.raw_input = readfunc
    else:
        try:
            import readline
        except ImportError:
            pass
    console.interact(banner)


#
# Code for Web corpus
#

class WebCorpus(object):

    metadata = MetaData()
    t_delicious = Table('delicious', metadata,
        # as fetched from the GEXF file (e.g. "acrimed.org")
        Column('site', Unicode(255), primary_key=True, nullable=False),
        # md5 of site used for the delicious API
        Column('md5', LargeBinary(16), unique=True, index=True, nullable=False),
        Column('title', UnicodeText),
        Column('url', Unicode(255)),
        Column('total_posts', Integer),
    )
    t_delicious_tags = Table('delicious_tags', metadata,
        Column('tag', Unicode(255), primary_key=True, nullable=False),
        Column('site', Unicode(255), ForeignKey(t_delicious.c.site),
               primary_key=True, index=True,nullable=False),
        Column('count', Integer),
    )

    def __init__(self, config):
        self.config = config
        self.delclient = DeliciousClient()

    def create_or_upgrade_db(self, engine):
        self.engine = engine
        self.metadata.create_all(engine)

    def load_file(self, fn):
        """Load full corpus data from a GEXF file"""
        logger = logging.getLogger("web.load")

        #Loading gexf file
        with open(fn, 'r') as gf:
            gexf = Gexf.importXML(gf)

        # Parsing Graph
        graph = gexf.graphs[0]

        # breakpoint --> unicode
        self.sites = set(str(node) for node_id,node in graph.nodes.iteritems())
        return len(self.sites)

    def preprocess_data(self):
        pass

    @inlineCallbacks
    def _fetch_delicious_data(self):
        logger = logging.getLogger("delicious.api")
        sites = self.sites

        t_del = self.t_delicious
        t_tags = self.t_delicious_tags
        conn = self.engine.connect()

        q = select([t_del.c.site])
        known_sites = set(row[0] for row in conn.execute(q))
        unknown_sites = sorted(sites - known_sites)

        if unknown_sites:
            logger.info("fetching Delicious info for %d sites (out of %d)",
                        len(unknown_sites), len(sites))
        chunk_size = 50

        for chunk in chunkize(unknown_sites, chunk_size):
            results = yield gatherResults(
                [self.delclient.get_urlinfo(s) for s in chunk])
            logger.info("got partial Delicious info for %d sites", len(results))

            with conn.begin():
                del_insert = []
                tags_insert = []
                for site, binmd5, data in sorted(results):
                    if data is None:
                        del_insert.append({
                            'site': site,
                            'md5': binmd5,
                            'title': None,
                            'url': None,
                            'total_posts': None,
                        })
                    else:
                        del_insert.append({
                            'site': site,
                            'md5': binmd5,
                            'title': data['title'] or u'',
                            'url': data['url'] or u'',
                            'total_posts': data['total_posts'],
                        })
                        top_tags = data['top_tags']
                        # Sometimes top_tags is an empty list
                        if top_tags:
                            for tag, count in top_tags.iteritems():
                                tags_insert.append({
                                    'tag': tag,
                                    'site': site,
                                    'count': count,
                                })
                if del_insert:
                    conn.execute(t_del.insert(), del_insert)
                if tags_insert:
                    conn.execute(t_tags.insert(), tags_insert)

        returnValue(None)

    def fetch_external_data(self):
        """Fetch external data from Web services.
        Returns a Deferred."""
        return gatherResults([
            self._fetch_delicious_data(),
        ])

    def recompute_data(self):
        logger = logging.getLogger("web.compute")
        t_del = self.t_delicious
        conn = self.engine.connect()

        # Populate node attributes with Delicious info
        q = select([t_del.c.site, t_del.c.title, t_del.c.url, t_del.c.total_posts])
        for site, title, url, total_posts in conn.execute(q):
            try:
                node = self.sites.node[site]
            except KeyError:
                logger.error("No node found for %r", site)
                continue
            if title is not None:
                node['title'] = title
                node['url'] = url
                node['delicious_posts'] = total_posts

        with timed(logger.info, "computing pagerank"):
            self.pagerank = nx.pagerank_scipy(self.sites)
            for site, value in self.pagerank.iteritems():
                self.sites.node[site]['pagerank'] = float(value)

        conn.close()

    def save_data(self, filename):
        """Output sites data to a CSV file."""
        logger = logging.getLogger("web.save")
        sites = self.sites
        conn = self.engine.connect()

        tags = set()
        sites_tags = defaultdict(dict)
        t_tags = self.t_delicious_tags
        q = select([t_tags.c.site, t_tags.c.tag, t_tags.c.count])
        for site, tag, count in conn.execute(q):
            tags.add(tag)
            sites_tags[site][tag] = count
        tags = sorted(tags)

        with timed(logger.info, "saving sites to %r", filename):
            with open(filename, "wb") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "site", "url", "delicious_posts",
                    "in_degree", "out_degree", "pagerank",
                    "TAGS -->"] + [tag.encode('utf8') for tag in tags]
                )
                def write_row(row):
                    writer.writerow([str(x) if x is not None else "" for x in row])

                for site in sorted(sites,
                                   key=lambda x: self.pagerank[x], reverse=True):
                    a = sites.node[site]
                    pr = a.get('pagerank')
                    if pr is not None:
                        pr = "%f" % pr
                    row = [site, a.get('url'), a.get('delicious_posts'),
                           sites.in_degree(site), sites.out_degree(site),
                           pr, '']
                    row += [sites_tags[site].get(tag) for tag in tags]
                    write_row(row)

        conn.close()

    def graphes(self):
        return {
            "": self.sites,
            #"-normweighted": self.g_normweighted_authors,
        }


#
# Code for Twitter corpus
#

_BaseTweet = namedtuple('_BaseTweet', 'author date text target')

class Tweet(_BaseTweet):
    # This exploits the fast built-in __hash__ / __eq__ of tuples:
    # custom attributes added to Tweet instances won't change equality
    # relationships and hashing.

    def __init__(self, *args, **kwargs):
        # Refs to other authors (in the "@someone" form)
        self.author_refs = set()
        # Retweets of other authors (in the "RT @someone" form)
        self.rts = set()
        self.md5 = hashlib.md5(
             self.author.id.encode('utf8') + '\0' + self.text.encode('utf8')).digest()


_BaseTweetAuthor = namedtuple('_BaseTweetAuthor', 'id')

class TweetAuthor(_BaseTweetAuthor):
    # Exists by default, this is overriden if users/lookup doesn't return
    # it amongs the results.
    suspended = False
    followers = None
    listed = None
    friends = None
    statuses = None
    # RetweetRank data
    rtrank = None
    rtpercentile = None
    # Number of tweets in the corpus
    ntweets = None
    # Number of incoming/outgoing refs
    ninrefs = 0
    noutrefs = 0
    ninrts = 0
    noutrts = 0


class TwitterCorpus(object):
    # For input files
    csv_dialect = csv.excel_tab
    # Looks for retweets and author references in a tweet
    author_pat = re.compile(
        r"(?:\A|\W)"
        r"(RT )?"
        r"@([\w]+)"
        r"(?:\Z|\W)"
    )

    metadata = MetaData()
    t_users_lookup = Table('users_lookup', metadata,
        Column('screen_name', Unicode(255), primary_key=True, nullable=False),
        Column('suspended', Boolean, nullable=False, index=True),
        # zlib-compressed json (saves ~50% on disk space consumption)
        Column('zjson', LargeBinary),
    )
    t_retweetrank = Table('retweetrank', metadata,
        Column('screen_name', Unicode(255), primary_key=True, nullable=False),
        Column('rtrank', Integer),
        Column('rtpercentile', Float),
    )
    t_authors = Table('authors', metadata,
        Column('screen_name', Unicode(255), ForeignKey(t_users_lookup.c.screen_name),
               primary_key=True, nullable=False),
        # Parsed from Twitter API results, None if suspended
        Column('name', UnicodeText, default=None),
        Column('description', UnicodeText, default=None),
        Column('followers', Integer, default=None),
        Column('listed', Integer, default=None),
        Column('friends', Integer, default=None),
        Column('statuses', Integer, default=None),
    )
    t_tweets = Table('tweets', metadata,
        # md5 hash of screen_name + '\0' + text
        Column('md5', LargeBinary(16), primary_key=True, nullable=False),
        Column('screen_name', Unicode(255), ForeignKey(t_users_lookup.c.screen_name),
               nullable=False),
        Column('posted_at', DateTime, nullable=False),
        Column('text', Unicode(255), nullable=False),
    )

    def __init__(self, config):
        self.config = config
        self.new_tweets = set()
        self.new_authors = {}
        self.authors = {}
        self.twclient = TwitterClient(**dict(config.items('twitter.api')))
        self.rrclient = RetweetRankClient(**dict(config.items('retweetrank.api')))

    def create_or_upgrade_db(self, engine):
        self.engine = engine
        self.metadata.create_all(engine)

    def _normalize_author_id(self, author_id):
        return author_id.lower()

    def load_file(self, fn):
        """Load incremental corpus data from a CSV file"""
        logger = logging.getLogger("twitter.parse")
        conn = self.engine.connect()
        # md5 -> tweet
        new_tweets = {}
        with open(fn, "r") as f:
            reader = csv.reader(f, dialect=self.csv_dialect)
            reader = unicode_reader(reader)
            for author, date, text, target in reader:
                # Parse author id and name
                author_id, sep, author_name = author.partition(' ')
                author_id = self._normalize_author_id(author_id)
                if (not sep
                    or not author_name.startswith('(')
                    or not author_name.endswith(')')):
                    logger.debug("invalid author %r" % author)
                    continue
                # Parse date
                date = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
                try:
                    author = self.new_authors[author_id]
                except KeyError:
                    author = TweetAuthor(id=author_id)
                    author.name = author_name
                    self.new_authors[author_id] = author
                tweet = Tweet(author, date, text, target)
                if tweet.md5 not in new_tweets:
                    new_tweets[tweet.md5] = tweet
                else:
                    t = new_tweets[tweet.md5]
                    logger.debug("duplicate tweet (%r, %r, %r, %r, ...)",
                                 author_id, date, t.author.id, t.date)

        n = len(new_tweets)

        with conn.begin():
            # We have to chunk the request to avoid an OperationalError with
            # too many parameters
            for batch in chunkize(new_tweets):
                q = select([self.t_tweets.c.md5],
                           self.t_tweets.c.md5.in_(batch))
                for md5, in conn.execute(q):
                    new_tweets.pop(md5)
            self.new_tweets.update(new_tweets.values())

            tweets_insert = []
            for tweet in new_tweets.itervalues():
                tweets_insert.append({
                    'screen_name': tweet.author.id,
                    'md5': tweet.md5,
                    'posted_at': tweet.date,
                    'text': tweet.text,
                })
            if tweets_insert:
                logger.info("adding %d new tweets to the database",
                            len(tweets_insert))
                conn.execute(self.t_tweets.insert(), tweets_insert)

        return n

    @inlineCallbacks
    def _fetch_twitter_data(self):
        logger = logging.getLogger("twitter.api")
        users = set(self.new_authors)

        conn = self.engine.connect()
        q = select([self.t_users_lookup.c.screen_name])
        known_users = set(row[0] for row in conn.execute(q))
        unknown_users = sorted(users - known_users)

        if unknown_users:
            logger.info("fetching Twitter info for %d authors (out of %d)",
                        len(unknown_users), len(users))
        chunk_size = 3000

        for chunk in chunkize(unknown_users, chunk_size):
            results = yield self.twclient.lookup_users(chunk)
            logger.info("got partial Twitter info for %d authors", len(results))

            with conn.begin():
                cache_insert = []
                suspended = set(chunk)
                for author_id, data in sorted(results.iteritems()):
                    author_id = self._normalize_author_id(author_id)
                    suspended.remove(author_id)
                    cache_insert.append({
                        'screen_name': author_id,
                        'zjson': zlib.compress(json.dumps(data)),
                        'suspended': False,
                    })
                if suspended:
                    logger.info("marking %d authors as suspended", len(suspended))
                    for author_id in suspended:
                        self.new_authors[author_id].suspended = True
                        cache_insert.append({
                            'screen_name': author_id,
                            'zjson': None,
                            'suspended': True,
                        })
                if cache_insert:
                    conn.execute(self.t_users_lookup.insert(), cache_insert)

        returnValue(None)

    @inlineCallbacks
    def _fetch_retweetrank_data(self):
        logger = logging.getLogger("retweetrank")
        # we have to pay per-request, better to minimize their number
        users = set(self.new_authors) & self.keep_authors

        conn = self.engine.connect()
        q = select([self.t_retweetrank.c.screen_name])
        known_users = set(x for x, in conn.execute(q))
        unknown_users = sorted(users - known_users)

        if unknown_users:
            logger.info("fetching RetweetRank info for %d authors (out of %d)",
                        len(unknown_users), len(users))
        chunk_size = 100

        for chunk in chunkize(unknown_users, chunk_size):
            results = yield gatherResults(
                [self.rrclient.get_rank(u) for u in chunk])
            logger.info("got partial RetweetRank info for %d authors", len(results))

            with conn.begin():
                cache_insert = []
                for u, data in zip(chunk, results):
                    author_id = self._normalize_author_id(u)
                    cache_insert.append({
                        'screen_name': author_id,
                        'rtrank': data['rank'] if data else None,
                        'rtpercentile': data['percentile'] if data else None,
                    })
                if cache_insert:
                    conn.execute(self.t_retweetrank.insert(), cache_insert)

        returnValue(None)

    def _got_twitter_data(self, _):
        """Update authors table after Twitter API data has been fetched
        and stored."""
        logger = logging.getLogger("twitter.api")
        conn = self.engine.connect()
        with conn.begin():
            q_update = (self.t_users_lookup.update()
                        .where(self.t_users_lookup.c.zjson == None)
                        .values(suspended=True))
            conn.execute(q_update)

            q_missing = select(
                [self.t_users_lookup.c.screen_name,
                 self.t_users_lookup.c.suspended,
                 self.t_users_lookup.c.zjson],
                self.t_authors.c.screen_name == None,
                from_obj=[self.t_users_lookup.outerjoin(self.t_authors)]
            )
            authors_insert = []
            for screen_name, suspended, zjson in conn.execute(q_missing):
                if suspended:
                    authors_insert.append({'screen_name': screen_name})
                else:
                    d = json.loads(zlib.decompress(zjson))
                    authors_insert.append({
                        'screen_name': screen_name,
                        # `or u''` avoids SQLAlchemy warnings when we get a (empty) str
                        'name': d['name'] or u'',
                        'description': d['description'] or u'',
                        'followers': d['followers_count'],
                        'listed': d['listed_count'],
                        'friends': d['friends_count'],
                        'statuses': d['statuses_count'],
                    })
            if authors_insert:
                logger.info("adding %d authors to the master table",
                            len(authors_insert))
                conn.execute(self.t_authors.insert(), authors_insert)
        return None

    def fetch_external_data(self):
        """Fetch external data from Web services.
        Returns a Deferred."""
        return gatherResults([
            self._fetch_twitter_data().addCallback(self._got_twitter_data),
            self._fetch_retweetrank_data(),
        ])

    def preprocess_data(self):
        """Preprocess data, filtering the first connected component."""
        conn = self.engine.connect()
        logger = logging.getLogger("twitter.compute")

        self.keep_authors = set()
        edges = self.edges = defaultdict(int)
        rt_edges = self.rt_edges = defaultdict(int)

        # Get authors data from the master table
        authors = self.authors = {}
        q = self.t_authors.select()
        for row in conn.execute(q):
            a = TweetAuthor(id=row['screen_name'])
            for attr in ('followers', 'listed', 'friends', 'statuses'):
                setattr(a, attr, row[attr])
            authors[a.id] = a
        q = select([self.t_tweets.c.screen_name, func.count()]
                  ).group_by(self.t_tweets.c.screen_name)
        for author_id, count in conn.execute(q):
            try:
                authors[author_id].ntweets = count
            except KeyError:
                # Suspended author
                pass

        with timed(logger.info, "extracting edges"):
            c = self.t_tweets.c
            q = select([c.screen_name, c.text])
            nb_ignored = 0
            nb_tweets = 0
            for author_id, text in conn.execute(q):
                nb_tweets += 1
                for is_rt, ref_id in self.author_pat.findall(text):
                    ref_id = self._normalize_author_id(ref_id)
                    if ref_id == author_id:
                        # Self references are ignored
                        continue
                    if (author_id not in authors or
                        ref_id not in authors):
                        nb_ignored += 1
                        continue
                    edges[author_id, ref_id] += 1
                    authors[author_id].noutrefs += 1
                    authors[ref_id].ninrefs += 1
                    if is_rt:
                        rt_edges[author_id, ref_id] += 1
                        authors[author_id].noutrts += 1
                        authors[ref_id].ninrts += 1
        logger.info("extracted %d edges from %d tweets "
                    "(%d references ignored)",
                    len(edges), nb_tweets, nb_ignored)

        # First an undirected graph to extract connected components
        g = nx.Graph()
        g.add_edges_from(edges)
        cc = nx.connected_components(g)
        self.keep_authors = set(cc[0])
        logger.info("keeping only the largest connected component "
                    "(%d authors out of %d)",
                    len(self.keep_authors), len(self.authors))
        if len(self.keep_authors) < 0.1 * len(authors):
            logger.error("the largest connected component is less "
                         "than 10% of all authors")
        elif len(self.keep_authors) < 10 * len(cc[1]):
            logger.error("the largest connected component is less "
                         "than 10x the second largest")

    def recompute_data(self):
        """Recompute internal data"""
        logger = logging.getLogger("twitter.compute")
        authors = self.authors
        conn = self.engine.connect()

        t = self.t_retweetrank
        q = t.select(t.c.rtrank != None)
        for row in conn.execute(q):
            a = self.authors[row['screen_name']]
            a.rtrank = row['rtrank']
            a.rtpercentile = row['rtpercentile']

        with timed(logger.info, "building graphes"):
            self.g_authors = nx.DiGraph()
            self.g_weighted_authors = nx.DiGraph()
            self.g_rt_authors = nx.DiGraph()
            self.g_rt_weighted_authors = nx.DiGraph()

            graphs = (self.g_authors,
                      self.g_weighted_authors,
                      self.g_rt_authors,
                      self.g_rt_weighted_authors,
                      )
            attr_map = {
                'followers': 'followers',
                'listed': 'listed',
                'friends': 'friends',
                'statuses': 'total_tweets',
                'ntweets': 'tweets',
                'rtrank': 'rtrank',
                'rtpercentile': 'rtpercentile',
                'ninrefs': 'in_degree',
                'noutrefs': 'out_degree',
                'ninrts': 'rt_in_degree',
                'noutrts': 'rt_out_degree',
            }.items()
            for author_id, author in self.authors.iteritems():
                if author_id not in self.keep_authors:
                    continue
                d = dict((k, v) for k, v in
                            ((mapped, getattr(author, k)) for k, mapped in attr_map)
                         if v is not None)
                for g in graphs:
                    g.add_node(author_id, **d)
            for (u, v), weight in self.edges.iteritems():
                if u not in self.keep_authors or v not in self.keep_authors:
                    continue
                self.g_authors.add_edge(u, v)
                self.g_weighted_authors.add_edge(u, v, weight=weight)
            for (u, v), weight in self.rt_edges.iteritems():
                if u not in self.keep_authors or v not in self.keep_authors:
                    continue
                self.g_rt_authors.add_edge(u, v)
                self.g_rt_weighted_authors.add_edge(u, v, weight=weight)

    def save_data(self, filename):
        """Output authors data to a CSV file."""
        logger = logging.getLogger("twitter.compute")
        with timed(logger.info, "computing pageranks"):
            npr = nx.pagerank_scipy(self.g_authors)
            wpr = nx.pagerank_scipy(self.g_weighted_authors)
            rt_npr = nx.pagerank_scipy(self.g_rt_authors)
            rt_wpr = nx.pagerank_scipy(self.g_rt_weighted_authors)

        logger = logging.getLogger("twitter.save")
        conn = self.engine.connect()
        with timed(logger.info, "saving authors to %r", filename):
            with open(filename, "wb") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "screen_name", "followers", "listed", "friends",
                    "total_tweets", "tweets",
                    "rtrank", "rtpercentile",
                    "in_degree", "out_degree",
                    "rt_in_degree", "rt_out_degree",
                    "unweighted_pr", "weighted_pr",
                    "rt_unweighted_pr", "rt_weighted_pr",
                ])
                def write_row(row):
                    writer.writerow([str(x) if x is not None else "" for x in row])

                format_pr = lambda pr: "%f" % pr
                for author_id in sorted(self.g_authors,
                                        key=lambda x: wpr[x], reverse=True):
                    a = self.authors[author_id]
                    write_row([
                        author_id, a.followers, a.listed, a.friends,
                        a.statuses, a.ntweets, a.rtrank, a.rtpercentile,
                        a.ninrefs, a.noutrefs, a.ninrts, a.noutrts,
                        format_pr(npr[author_id]), format_pr(wpr[author_id]),
                        format_pr(rt_npr[author_id]), format_pr(rt_wpr[author_id]),
                    ])
        conn.close()

    def graphes(self):
        return {
            "": self.g_authors,
            "-weighted": self.g_weighted_authors,
            "-rt": self.g_rt_authors,
            "-rt-weighted": self.g_rt_weighted_authors,
        }


def main():
    usage = "usage: %prog (-T|-W) [.. options ..] <corpus file>"
    parser = OptionParser(usage=usage)
    parser.add_option("-T", "--twitter", action="store_true",
                      default=False,
                      help="operate on Twitter corpus")
    parser.add_option("-W", "--web", action="store_true",
                      default=False,
                      help="operate on Web corpus")

    parser.add_option("-o", "--outfile", action="store", type="string",
                      help="output data to file (CSV)")
    parser.add_option("-G", "--save-graphs", action="store", type="string",
                      help="output graphs to GEXF files")

    parser.add_option("-q", "--quiet", action="store_true",
                      default=False, help="display only errors")
    parser.add_option("-v", "--verbose", action="store_true",
                      default=False,
                      help="display informative messages (default)")
    parser.add_option("-d", "--debug", action="store_true",
                      default=False, help="display debug messages")
    parser.add_option("-S", "--sql-debug", action="store_true",
                      default=False, help="display SQL requests")

    parser.add_option("-n", "--offline", action="store_true",
                      default=False,
                      help="do not fetch external data")
    parser.add_option("-i", "--interactive", action="store_true",
                      default=False,
                      help="launch interactive prompt after processing data")

    (options, args) = parser.parse_args()

    t, w = options.twitter, options.web
    if (t and w) or (not t and not w):
        parser.error("need to specify exactly one of -T and -W")

    verbosity = (2 if options.debug else (0 if options.quiet else 1))
    level = {
        0: logging.WARNING,
        1: logging.INFO,
        2: logging.DEBUG,
    }[verbosity]
    sqla_level = logging.INFO if options.sql_debug else logging.WARNING
    logging.basicConfig(
        stream=sys.stderr, level=level)
    logging.getLogger('sqlalchemy.engine').setLevel(sqla_level)

    config = SafeConfigParser()
    with open('ipinion.ini') as f:
        config.readfp(f)
    if w:
        corpus = WebCorpus(config)
        dbname = "store.web.db"
    else:
        corpus = TwitterCorpus(config)
        dbname = "store.twitter.db"

    u, v, w = gc.get_threshold()
    gc.set_threshold(u, v * 4, w * 10)

    engine = create_engine("sqlite:///" + dbname)
    corpus.create_or_upgrade_db(engine)

    logger = logging.getLogger('load')

    for fn in args:
        logger.info("loading incremental dataset %r...", fn)
        n = corpus.load_file(fn)
        logger.info("%d items read", n)

    # Initialize reactor
    reactor.startRunning(installSignalHandlers=False)
    must_stop = []

    def reactor_loop():
        # This is very crude but avoids reliance on private reactor data
        while reactor.running and not must_stop:
            reactor.iterate(0.1)
        must_stop[:] = []

    def after_fetch(_):
        corpus.recompute_data()

        if options.save_graphs:
            logger = logging.getLogger('save')
            base, ext = os.path.splitext(options.save_graphs)
            if not ext:
                ext = ".gexf"
            for suffix, g in corpus.graphes().items():
                gfn = base + suffix + ext
                logger.info("saving %d nodes and %d edges to %r..."
                            % (len(g), g.number_of_edges(), gfn))
                nx.write_gexf(g, gfn)

        if options.outfile:
            corpus.save_data(options.outfile)

        must_stop.append(None)

    def on_failure(failure):
        must_stop.append(None)
        failure.printTraceback(elideFrameworkCode=True)
        reactor.crash()
        os._exit(1)

    corpus.preprocess_data()
    if not options.offline:
        d = corpus.fetch_external_data()
    else:
        d = maybeDeferred(lambda: None)
    d.addCallback(after_fetch)
    d.addErrback(on_failure)

    try:
        reactor_loop()

        if options.interactive:
            enable_readline_completion()
            gvars = ('config', 'corpus', 'engine', 'options')
            for var in gvars:
                globals()[var] = locals()[var]
            interact("Available variables: %r" % (gvars,), None, globals())
    finally:
        reactor.stop()
        # Needed after stop to properly terminate the reactor
        reactor_loop()


if __name__ == '__main__':
    main()
