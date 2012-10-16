import hadoopy_hbase
import time

c = hadoopy_hbase.connect('localhost')
cnt = 0
st = time.time()
N = 5000
for x in hadoopy_hbase.scanner(c, 'flickr', per_call=N, columns=['metadata:license']):
    cnt += 1
    if cnt % N == 0:
        print(((time.time() - st) / N, cnt))
        st = time.time()
