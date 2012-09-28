from flickr_crawl import scanner, setup
import time

c = setup()
cnt = 0
st = time.time()
N = 5000
for x in scanner(c, 'flickr', per_call=N, columns=['metadata:license']):
    cnt += 1
    if cnt % N == 0:
        print(((time.time() - st) / N, cnt))
        st = time.time()
