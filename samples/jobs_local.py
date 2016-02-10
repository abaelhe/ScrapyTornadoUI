# -*- coding: utf-8 -*-
import asyncio
import json
import logging
import ssl
import websockets


def ssl_ctx_no_validate():
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


@asyncio.coroutine
def subscribe(url, headers, name, command):
    logger = logging.getLogger(name)
    ssl_ctx = ssl_ctx_no_validate()
    job_socket = yield from websockets.connect(url)


    logger.info('sending request')
    yield from job_socket.send(json.dumps(command))

    # throw away first response -- it doesn't contain data, just a confirmation
    # of the command
    # json_response = yield from job_socket.recv()

    while True:
        json_response = yield from job_socket.recv()

        if json_response is None:
            logger.info('Websocket dropped unexpectedly')
            break

        response = json.loads(json_response)
        logger.info(response)
        # event = response['event']
        # doc_id = response['data']['_id']
        # logger.info('Received event={} doc={}'.format(event, doc_id))

    yield from job_socket.close()
    logger.info('Websocket closed')


if __name__ == '__main__':
    # url = 'wss://54.213.99.77/ws-rpc'
    # url = 'wss://52.26.94.28/ws-rpc'
    url = 'ws://127.0.0.1:8888/ws-rpc'
    # headers = {'Authorization': 'Basic YWRtaW46bWVtZXhwYXNz'}
    headers = {}
    logging.basicConfig(level=logging.INFO)

    # items_command = {
    #     'event': 'rpc:request',
    #     'data': {
    #         'id':0,
    #         'jsonrpc': '2.0',
    #         'method': 'pages.subscribe',
    #         'params': {},
    #     },
    # }
    #
    jobs_command = {
        'event': 'rpc:request',
        'data': {
            'id':0,
            'jsonrpc': '2.0',
            'method': 'jobs.subscribe',
            'params': {},
        },
    }

    # sites_command = {
    #     'event': 'rpc:request',
    #     'data': {
    #         'id':0,
    #         'jsonrpc': '2.0',
    #         'method': 'sites.list',
    #         'params': {},
    #     },
    # }
    # add_site_command = {
    #     'event': 'rpc:request',
    #     'data': {
    #         'id':0,
    #         'jsonrpc': '2.0',
    #         'method': 'sites.post',
    #         'params': {'site': {'url':'http://127.0.0.1'}},
    #         # 'params': {'site': {'url':'http://crdclub4wraumez4.onion'}},
    #     },
    # }

    # asyncio.Task(subscribe(url, headers, 'items', items_command))
    asyncio.Task(subscribe(url, headers, 'jobs', jobs_command))
    # asyncio.Task(subscribe(url, headers, 'sites', sites_command))
    # asyncio.Task(subscribe(url, headers, 'sites', add_site_command))
    asyncio.get_event_loop().run_forever()

