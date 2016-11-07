import pymongo
from scrapy.http import Headers
from scrapy.responsetypes import responsetypes
from scrapy_splash import SplashResponse


class MongoCacheStorage(object):
    """A read-only cache storage that uses Arachnado MongoDB for retrieving
    responses"""

    def __init__(self, settings):
        self.db_name = settings.get('MOTOR_PIPELINE_DB_NAME', 'arachnado')
        self.db_uri = settings.get('MOTOR_PIPELINE_URI')
        usable_codes_key = "USABLE_CACHED_RESPONSE_CODES"
        if usable_codes_key in settings:
            self.status_codes = settings[usable_codes_key]
        else:
            self.status_codes = ['200', '203', '301', '302', '303', '307']

    def open_spider(self, spider):
        self.db = pymongo.MongoClient(self.db_uri)
        self.col = self.db[self.db_name]['items']
        self.col.ensure_index('url')

    def close_spider(self, spider):
        self.db.close()

    def retrieve_response(self, spider, request):
        print("--------------------------------------------------")
        if 'splash' in request.meta:
            doc_url = request.meta.get("url", None)
        else:
            doc_url = request.url
        print("URL: " + doc_url)
        if not doc_url:
            return
        doc = self.col.find_one({'url': request.url})
        if doc is None:
            print("doc not found")
            return
        status = str(doc.get("status", -1))
        if status not in self.status_codes:
            print("status")
            return
        url = doc['url']
        headers = Headers(doc['headers'])
        body = doc['body'].encode('utf-8')
        if 'splash' in request.meta:
            respcls = SplashResponse
        else:
            respcls = responsetypes.from_args(headers=headers, url=url)
        response = respcls(url=url, headers=headers, status=status, body=body, request=request)
        response.meta["mongo_id"] = doc["_id"]
        print("response ready")
        return response

    def store_response(self, spider, request, response):
        pass
