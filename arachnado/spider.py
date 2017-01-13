# -*- coding: utf-8 -*-
from __future__ import absolute_import
import contextlib
import logging

import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.http.response.html import HtmlResponse
from scrapy_splash.response import SplashResponse, SplashTextResponse, SplashJsonResponse
from autologin_middleware import link_looks_like_logout

from arachnado.utils.misc import add_scheme_if_missing, get_netloc
import pkgutil
import json
import re
# from urllib.parse import urlsplit
from scrapy_redis.spiders import RedisMixin
from arachnado.scheduler.scheduler import Scheduler
from w3lib.http import basic_auth_header
from six.moves.urllib.parse import urlparse, urlsplit
from scrapy.exceptions import DontCloseSpider
import scrapy
import os
import logging
from scrapy_splash.request import SplashRequest
from io import StringIO
import base64
import autopager
from pprint import pprint
from arachnado.utils.mongo import motor_from_uri
from scrapy import signals
import uuid
import pymongo


class ArachnadoSpider(scrapy.Spider):
    """
    A base spider that contains common attributes and utilities for all
    Arachnado spiders
    """
    crawl_id = None       # unique crawl ID, assigned by DomainCrawlers
    motor_job_id = None   # MongoDB record ID, assigned by MongoExportPipeline
    domain = None         # seed url, set by caller code

    def __init__(self, *args, **kwargs):
        super(ArachnadoSpider, self).__init__(*args, **kwargs)
        # don't log scraped items
        logging.getLogger("scrapy.core.scraper").setLevel(logging.INFO)

    @classmethod
    def inherit_from_me(cls, spider_cls):
        """
        Ensure that spider is inherited from ArachnadoSpider
        to receive its features. HackHackHack.

        >>> class Foo(scrapy.Spider):
        ...     name = 'foo'
        >>> issubclass(Foo, ArachnadoSpider)
        False
        >>> Foo2 = ArachnadoSpider.inherit_from_me(Foo)
        >>> Foo2.name
        'foo'
        >>> issubclass(Foo2, ArachnadoSpider)
        True
        """
        if not isinstance(spider_cls, cls):
            return type(spider_cls.__name__, (spider_cls, cls), {})
        return spider_cls


class CrawlWebsiteSpider(ArachnadoSpider):
    """
    A spider which crawls all the website.
    To run it, set its ``crawl_id`` and ``domain`` arguments.
    """
    name = 'generic'
    custom_settings = {
        'DEPTH_LIMIT': 10,
    }

    def __init__(self, *args, **kwargs):
        super(CrawlWebsiteSpider, self).__init__(*args, **kwargs)
        self.start_url = add_scheme_if_missing(self.domain)

    def start_requests(self):
        self.logger.info("Started job %s (mongo id=%s) for domain %s",
                         self.crawl_id, self.motor_job_id, self.domain)
        yield scrapy.Request(self.start_url, self.parse_first,
                             dont_filter=True)

    def parse_first(self, response):
        # If there is a redirect in the first request, use the target domain
        # to restrict crawl instead of the original.
        self.domain = get_netloc(response.url)
        self.crawler.stats.set_value('arachnado/start_url', self.start_url)
        self.crawler.stats.set_value('arachnado/domain', self.domain)

        allow_domain = self.domain
        if self.domain.startswith("www."):
            allow_domain = allow_domain[len("www."):]

        self.state['allow_domain'] = allow_domain

        for elem in self.parse(response):
            yield elem

    @property
    def link_extractor(self):
        return LinkExtractor(
            allow_domains=[self.state['allow_domain']],
            canonicalize=False,
        )

    @property
    def get_links(self):
        return self.link_extractor.extract_links

    def parse(self, response):
        if not isinstance(response, HtmlResponse):
            self.logger.info("non-HTML response is skipped: %s" % response.url)
            return
        if self.settings.getbool('PREFER_PAGINATION'):
            # Follow pagination links; pagination is not a subject of
            # a max depth limit. This also prioritizes pagination links because
            # depth is not increased for them.
            with _dont_increase_depth(response):
                for url in self._pagination_urls(response):
                    yield scrapy.Request(url, meta={'is_page': True})
        for link in self.get_links(response):
            if link_looks_like_logout(link):
                continue
            yield scrapy.Request(link.url, self.parse)

    def _pagination_urls(self, response):
        return [url for url in autopager.urls(response)
                if self.link_extractor.matches(url)]

    def should_drop_request(self, request):
        if 'allow_domain' not in self.state:  # first request
            return
        if not self.link_extractor.matches(request.url):
            return True


class WideOnionCrawlSpider(CrawlWebsiteSpider):
    """
    """
    name = 'onionwide'
    download_maxsize = 1024 * 1024 * 1
    start_urls = None
    file_feed = None
    link_ext_allow = None
    link_ext_allow_domains = None
    use_splash = False
    splash_script = None
    # images storage
    fs_export_path = None
    s3_export_path = None
    s3_key = None
    s3_secret_key = None
    processed_netloc = None
    only_landing_screens = True
    splash_in_parallel = True
    out_file_dir = None
    handle_httpstatus_list = [400, 404, 401, 403, 404, 429, 500, 520, 504, 503]
    start_priority = 1000
    settings = None
    stats = None
    validate_html = True
    allowed_statuses = [200, 301, 302, 303, 304, 307]

    def __init__(self, *args, **kwargs):
        if not self.settings:
            self.settings = {}
        super(WideOnionCrawlSpider, self).__init__(*args, **kwargs)
        self.start_urls = kwargs.get("start_urls", [])
        self.file_feed = kwargs.get("file_feed", None)
        self.link_ext_allow = kwargs.get("link_ext_allow", "https?:\/\/[^\/]*\.onion")
        self.link_ext_allow_domains = kwargs.get("link_ext_allow_domains", ())
        self.use_splash = kwargs.get("use_splash", False)
        self.only_landing_screens = kwargs.get("only_landing_screens", True)
        self.splash_in_parallel = kwargs.get("splash_in_parallel", True)
        if self.use_splash:
            self.splash_script = pkgutil.get_data("arachnado", "lua/info.lua").decode("utf-8")
            # self.processed_urls = set([])
            self.processed_netloc = set([])
            self.out_file_num = 0

    def post_init(self):
        self.download_maxsize = self.settings.get("DOWNLOAD_MAXSIZE", self.download_maxsize)
        self.fs_export_path = self.settings.get('PNG_STORAGE_FS', None)
        self.s3_export_path = self.settings.get('PNG_STORAGE_AWS_S3', None)
        if self.s3_export_path:
            self.s3_key = self.settings.get('AWS_STORAGE_KEY', None)
            self.s3_secret_key = self.settings.get('AWS_STORAGE_SECRET_KEY', None)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        obj = super(WideOnionCrawlSpider, cls).from_crawler(crawler, *args, **kwargs)
        obj.settings = crawler.settings
        obj.stats = crawler.stats
        obj.post_init()
        return obj

    def start_requests(self):
        self.logger.info("Started job %s (mongo id=%s)",    self.crawl_id, self.motor_job_id)
        req_urls = []
        if self.start_urls:
            req_urls.extend(self.start_urls)
        if self.file_feed:
            with open(self.file_feed, "r") as urls_file:
                for url in urls_file:
                    req_urls.append(url)
        for url in req_urls:
            for req in self.create_request(url, self.parse):
                yield req

    def create_request(self,
                           url,
                           callback,
                           cookies={},
                           add_args={},
                           add_meta={},
                           priority=0
                           ):
        site_passwords = self.settings.get("SITE_PASSWORDS", {})
        fixed_url = add_scheme_if_missing(url)
        meta = {}
        parsed_url = urlparse(url)
        # dots are replaced for Mongo storage
        url_domain = parsed_url.netloc.replace(".", "_")
        if url_domain in site_passwords:
            meta['autologin_username'] = site_passwords[url_domain].get("username", "")
            meta['autologin_password'] = site_passwords[url_domain].get("password", "")
        meta.update(add_meta)
        if not self.use_splash:
            # print("1")
            yield scrapy.Request(fixed_url, callback,  meta=meta, priority=priority)
        else:
            netloc = get_domain(fixed_url)
            if netloc in self.processed_netloc and self.only_landing_screens:
                # print("2")
                yield scrapy.Request(fixed_url, callback,  meta=meta, priority=priority)
            else:
                if self.splash_in_parallel:
                    # print("3")
                    yield scrapy.Request(fixed_url, callback,  meta=meta, priority=priority)
                meta.update({"url": fixed_url})
                endpoint = "execute"
                args = {'lua_source': self.splash_script, "cookies": cookies}
                args.update(add_args)
                # print("4")
                yield SplashRequest(url=fixed_url,
                                    callback=callback,
                                    args=args,
                                    endpoint=endpoint,
                                    dont_filter=True,
                                    meta=meta,
                    )
            self.processed_netloc.add(netloc)

    def parse(self, response):
        is_splash_resp = isinstance(response, SplashResponse) or isinstance(response, SplashTextResponse)

        if not isinstance(response, HtmlResponse) and not is_splash_resp and not response.meta.get("unusable", False):
            response.meta["unusable"] = True
            self.logger.warning("not usable response type skipped: {} from {}".format(type(response), response.url))
            return

        if self.validate_html:
            validation_result = len(response.xpath('.//*')) > 0
            if not validation_result:
                response.meta["no_item"] = True

        if self.allowed_statuses:
            if response.status not in self.allowed_statuses:
                response.meta["no_item"] = True

        req_priority = self.start_priority - response.meta["depth"]
        if self.settings.getbool('PREFER_PAGINATION'):
            with _dont_increase_depth(response):
                for url in self._pagination_urls(response):
                    for req in self.create_request(url, self.parse, add_meta={'is_page': True}):
                        yield req

        for link in self.get_links(response):
            if link_looks_like_logout(link):
                continue
            for req in self.create_request(link.url.replace("\n", ""), self.parse, priority=req_priority):
                yield req

        if is_splash_resp:
            splash_res = extract_splash_response(response)
            if splash_res:
                picfilename = "{}.png".format(str(uuid.uuid4()))
                if self.fs_export_path:
                    store_img(picfilename, self.fs_export_path, splash_res["png"])
                if self.s3_export_path:
                    s3_store_img(picfilename, self.s3_export_path,
                                 splash_res["png"],
                                 self.s3_key, self.s3_secret_key)
                if self.s3_export_path or self.fs_export_path:
                    response.meta["pagepicurl"] = picfilename
                    if self.stats:
                        self.stats.inc_value('screenshots/taken', spider=self)

    @property
    def link_extractor(self):
        return LinkExtractor(
            allow=self.link_ext_allow,
            allow_domains=self.link_ext_allow_domains,
            canonicalize=False,
        )

    def should_drop_request(self, request):
        return False


class RedisWideOnionCrawlSpider(RedisMixin, WideOnionCrawlSpider):
    """
    """
    name = 'onionqueue'

    @classmethod
    def from_crawler(self, crawler, *args, **kwargs):
        obj = super(RedisWideOnionCrawlSpider, self).from_crawler(crawler, *args, **kwargs)
        obj.setup_redis(crawler)
        obj.stats = crawler.stats
        return obj

    def next_requests(self):
        # print("--01")
        use_set = self.settings.getbool('REDIS_START_URLS_AS_SET')
        fetch_one = self.server.spop if use_set else self.server.lpop
        found = 0
        # print("--02")
        # print(self.redis_key)
        while found < self.redis_batch_size:
            data = fetch_one(self.redis_key)
            # print("--03")
            # print(data)
            if not data:
                break
            # print("--04")
            url = data.decode("utf-8")
            # print(url)
            reqs = self.create_request(url, self.parse, priority=(self.start_priority + 1))
            if reqs:
                # print("--05")
                for req in reqs:
                    # print("--06")
                    yield req
                    found += 1
            else:
                self.logger.debug("Request not made from data: %r", data)
        if found:
            self.logger.debug("Read %s requests from '%s'", found, self.redis_key)

    @property
    def link_extractor(self):
        return LinkExtractor(
            allow=self.link_ext_allow,
            allow_domains=self.link_ext_allow_domains,
            canonicalize=False,
        )


class RedisCheatOnionCrawlSpider(RedisWideOnionCrawlSpider):
    name = 'onioncheat'

    def start_requests(self):
        # print(self.settings["SCHEDULER"])
        scheduler = Scheduler.from_settings(self.settings)
        scheduler.open(self)
        scheduler.stats = self.stats
        # print("scheduler created")
        # print(scheduler.server)
        first_req = None
        for req in self.next_requests():
            # print(req.url)
            if not first_req:
                first_req = req
            else:
                scheduler.enqueue_request(req)
        if first_req:
            yield first_req



@contextlib.contextmanager
def _dont_increase_depth(response):
    # XXX: a hack to keep the same depth for outgoing requests
    response.meta['depth'] -= 1
    try:
        yield
    finally:
        response.meta['depth'] += 1


def extract_splash_response(response):
    # print(response.body)
    # print(dir(response))
    if response.status != 200:
        logging.error(response.body)
    else:
        splash_res = response.data
        # splash_res = response.body
        # return json.loads(splash_res)
        return splash_res
    return None

def store_img(file_name, storage_dir, img_content):
    image_bytes = base64.b64decode(img_content)
    full_path = os.path.join(storage_dir, file_name)
    with open(full_path, "wb") as fout:
        fout.write(image_bytes)
    return full_path


from boto.s3.connection import OrdinaryCallingFormat, S3Connection
from boto.s3.key import Key


def s3_store_img(file_name, bucket_name, img_content, aws_key, aws_secret_key):
    conn = S3Connection(aws_access_key_id=aws_key,
                        aws_secret_access_key=aws_secret_key)
    image_bytes = base64.b64decode(img_content)
    bucket = conn.get_bucket(bucket_name)
    k = Key(bucket)
    k.key = '/img/{}'.format(file_name)
    k.set_contents_from_string(image_bytes)
    k.set_acl('public-read')
    k.close()


def store_file(file_name, storage_dir, file_content):
    full_path = os.path.join(storage_dir, file_name)
    with open(full_path, "w") as fout:
        fout.write(file_content)
    return full_path


def img_convert(image_splash):
    image_bytes = base64.b64decode(image_splash)
    return image_bytes


def get_domain(url):
    domain = urlsplit(url).netloc
    return re.sub(r'^www\.', '', domain)





class ScreenshotSpider(scrapy.Spider):
    name = 'screenshots'
    pages_collection = None
    pages_client = None
    pages_uri = None
    fs_export_path = None
    s3_export_path = None
    s3_key = None
    s3_secret_key = None
    last_id = None
    splash_script = None
    stats = None
    handle_httpstatus_list = [400, 404, 401, 403, 404, 429, 500, 520, 504, 503]

    def start_requests(self):
        return self.next_requests()

    def setup_collection(self, crawler=None):
        if crawler is None:
            crawler = getattr(self, 'crawler', None)
        if crawler is None:
            raise ValueError("crawler is required")
        settings = crawler.settings
        self.pages_uri = settings.get('MONGO_PAGES_URI')
        self.db_uri = settings.get('MOTOR_PIPELINE_URI')
        # self.pages_client, _, _, _, self.pages_collection = \
        #     motor_from_uri(self.pages_uri)
        self.db = pymongo.MongoClient(self.db_uri)
        self.db_name = 'arachnado'
        self.pages_collection = self.db[self.db_name]['items']
        self.mongo_batch_size = settings.getint('MONGO_BATCH_SIZE', 20)
        try:
            self.mongo_batch_size = int(self.mongo_batch_size)
        except (TypeError, ValueError):
            raise ValueError("mongo_batch_size must be an integer")
        self.fs_export_path = settings.get('PNG_STORAGE_FS', None)
        self.s3_export_path = settings.get('PNG_STORAGE_AWS_S3', None)
        if self.s3_export_path:
            self.s3_key = settings.get('AWS_STORAGE_KEY', None)
            self.s3_secret_key = settings.get('AWS_STORAGE_SECRET_KEY', None)
        crawler.signals.connect(self.spider_idle, signal=signals.spider_idle)
        #TODO: client close on spider close

    def next_requests(self):
        items_query = {"pagepicurl": { '$exists': False}}
        if self.last_id:
            items_query = {'$and':[
                {"_id": {"$gt": self.last_id}},
                items_query]
            }
        for cdr in self.pages_collection.find(items_query,
                                             limit=self.mongo_batch_size,
                                             fields=['url',],
                                             sort=[('_id', 1), ]):
            self.last_id = cdr["_id"]
            req = self.create_request(url=cdr["url"],
                                      callback=self.parse,
                                      add_meta={"mongo_id":cdr["_id"]})
            if req:
                yield req

    def schedule_next_requests(self):
        for req in self.next_requests():
            self.crawler.engine.crawl(req, spider=self)

    def spider_idle(self):
        self.schedule_next_requests()
        raise DontCloseSpider

    @classmethod
    def from_crawler(self, crawler, *args, **kwargs):
        obj = super(ScreenshotSpider, self).from_crawler(crawler, *args, **kwargs)
        obj.setup_collection(crawler)
        obj.splash_script = pkgutil.get_data("arachnado", "lua/info.lua").decode("utf-8")
        obj.stats = crawler.stats
        return obj

    def parse(self, response):
        if response.status != 200:
            logging.debug(response.body)
        response.meta["no_item"] = True
        # print(type(response))
        is_splash_resp = isinstance(response, SplashJsonResponse)
        if is_splash_resp:
            splash_res = extract_splash_response(response)
            if splash_res:
                picfilename = "{}.png".format(str(uuid.uuid4()))
                if self.fs_export_path:
                    store_img(picfilename, self.fs_export_path, splash_res["png"])
                if self.s3_export_path:
                    s3_store_img(picfilename, self.s3_export_path,
                                 splash_res["png"],
                                 self.s3_key,
                                 self.s3_secret_key)
                if self.fs_export_path or self.s3_export_path:
                    update_res = self.pages_collection.update(
                            {"_id": response.meta["mongo_id"]},
                            {'$set': {"pagepicurl": picfilename}})

                    if self.stats:
                        self.stats.inc_value('screenshots/taken', spider=self)

    def create_request(self,
                           url,
                           callback,
                           cookies={},
                           add_args={},
                           add_meta={},
                           ):
        site_passwords = self.settings.get("SITE_PASSWORDS", {})
        fixed_url = url
        # fixed_url = add_scheme_if_missing(url)
        meta = {}
        parsed_url = urlparse(url)
        # dots are replaced for Mongo storage
        url_domain = parsed_url.netloc.replace(".", "_")
        if url_domain in site_passwords:
            meta['autologin_username'] = site_passwords[url_domain].get("username", "")
            meta['autologin_password'] = site_passwords[url_domain].get("password", "")
        meta.update(add_meta)
        meta.update({"url": fixed_url})
        endpoint = "execute"
        args = {'lua_source': self.splash_script,
                "cookies": cookies,
                "timeout":60}
        args.update(add_args)
        return SplashRequest(url=fixed_url,
                            callback=callback,
                            args=args,
                            endpoint=endpoint,
                            dont_filter=True,
                            meta=meta,
            )
