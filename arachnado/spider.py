# -*- coding: utf-8 -*-
from __future__ import absolute_import
import contextlib
import logging

import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.http.response.html import HtmlResponse
from scrapy_splash.response import SplashResponse, SplashTextResponse
from autologin_middleware import link_looks_like_logout

from arachnado.utils.misc import add_scheme_if_missing, get_netloc
import pkgutil
import json
import re
from urllib.parse import urlsplit
from scrapy_redis.spiders import RedisMixin
from scrapy_redis.scheduler import Scheduler
from w3lib.http import basic_auth_header


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
        import autopager
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
    download_maxsize = 1024 * 1024 * 5
    start_urls = None
    file_feed = None
    link_ext_allow = None
    use_splash = False
    splash_script = None
    # processed_urls = None
    processed_netloc = None
    only_landing_screens = True
    splash_in_parallel = True
    out_file_dir = None
    handle_httpstatus_list = [400, 404, 401, 403, 500, 520, 504]
    start_priority = 1000

    def __init__(self, *args, **kwargs):
        super(WideOnionCrawlSpider, self).__init__(*args, **kwargs)
        self.start_urls = kwargs.get("start_urls", [])
        self.file_feed = kwargs.get("file_feed", None)
        self.link_ext_allow = kwargs.get("link_ext_allow", "https?:\/\/[^\/]*\.onion")
        self.use_splash = kwargs.get("use_splash", False)
        self.only_landing_screens = kwargs.get("only_landing_screens", True)
        self.splash_in_parallel = kwargs.get("splash_in_parallel", True)
        if self.use_splash:
            self.splash_script = pkgutil.get_data("arachnado", "lua/info.lua").decode("utf-8")
            # self.processed_urls = set([])
            self.processed_netloc = set([])
            self.out_file_num = 0

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
        fixed_url = add_scheme_if_missing(url)
        meta = {}
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
        # print("-- 2")
        # print(dir(response))
        # print("{} : {}".format(response.meta["depth"], response.url))
        req_priority = self.start_priority - response.meta["depth"]
        if self.settings.getbool('PREFER_PAGINATION'):
            # Follow pagination links; pagination is not a subject of
            # a max depth limit. This also prioritizes pagination links because
            # depth is not increased for them.
            with _dont_increase_depth(response):
                for url in self._pagination_urls(response):
                    # print("---------------" + url)
                    for req in self.create_request(url, self.parse, add_meta={'is_page': True}):
                        yield req
                    # yield scrapy.Request(url, meta={'is_page': True})
        for link in self.get_links(response):
            if link_looks_like_logout(link):
                continue
            # print("-----" + link.url)
            # yield scrapy.Request(link.url, self.parse)
            for req in self.create_request(link.url.replace("\n", ""), self.parse, priority=req_priority):
                yield req
        if self.use_splash and is_splash_resp:
            # print("--- 0.2")
            splash_res = extract_splash_response(response)
            if splash_res:
                self.out_file_num += 1
                if self.out_file_dir:
                    store_file("{}.html".format(self.out_file_num),
                                             self.out_file_dir, splash_res["html"])
                    store_img("{}.png".format(self.out_file_num),
                                             self.out_file_dir, splash_res["png"])
                item = SiteScreenshotItem()
                item["url"] = splash_res["url"]
                item["png_image"] = splash_res["png"]
                yield item

    @property
    def link_extractor(self):
        return LinkExtractor(
            allow=self.link_ext_allow,
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


import os
import logging
from scrapy_splash.request import SplashRequest
from io import StringIO
import base64


def extract_splash_response(response):
    if response.status != 200:
        logging.error(response.body)
    else:
        splash_res = response.data
        return splash_res
    return None

def store_img(file_name, storage_dir, img_content):
    image_bytes = base64.b64decode(img_content)
    full_path = os.path.join(storage_dir, file_name)
    with open(full_path, "wb") as fout:
        fout.write(image_bytes)
    return full_path


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


import scrapy


class SiteScreenshotItem(scrapy.Item):
    url = scrapy.Field()
    png_image = scrapy.Field()


