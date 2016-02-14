# -*- coding: utf-8 -*-
import json
import logging
import requests


if __name__ == '__main__':
    url = 'http://127.0.0.1:8888/crawler/start'
    logging.basicConfig(level=logging.INFO)
    headers = {"Content-Type": "application/json"}
    data = {"domain":"spider://ar_phpbb","options":{"args":{"username":"sammy",
                                                             "password":"badpassword",
                                                             "start_urls":["http://127.0.0.1"],
                                                             "post_days":-1},
                                                     "settings":{}}}
    r = requests.post(url, data=json.dumps(data), headers=headers)
    print r
    print r.text

