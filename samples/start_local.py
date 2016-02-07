import json
import logging
import requests


if __name__ == '__main__':
    url = 'http://127.0.0.1:8888/crawler/start'
    logging.basicConfig(level=logging.INFO)
    data = {"domain":"spider://tortest","options": {"args":{}}}
    r = requests.post(url, data=data)
    print r
    print r.text

