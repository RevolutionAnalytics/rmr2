from gevent import monkey
monkey.patch_all()
import bottle
import os
import argparse
import random
import base64
from auth import verify
from flickr_crawl import setup, scanner
import itertools
import time

START_ROW = ''

@bottle.route('/:auth_key#[a-zA-Z0-9\_\-]+#/')
@verify
def main(auth_key):
    global START_ROW
    st = time.time()
    x = ''
    images = ['<img src="%s"></img>' % y['metadata:url_s'] for x, y in itertools.islice(scanner(client, 'flickr', ['metadata:url_s'], per_call=100, start_row=START_ROW), 100)]
    START_ROW = x
    run_time = time.time() - st
    return ('%d-%f<br>' % (len(images), run_time)) + '<br>'.join(images)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Serve a directory")

    # Server port
    parser.add_argument('--port', type=str, help='bottle.run webpy on this port',
                        default='8080')
    ARGS = parser.parse_args()
    client = setup()
    bottle.run(host='0.0.0.0', port=ARGS.port, server='gevent')
