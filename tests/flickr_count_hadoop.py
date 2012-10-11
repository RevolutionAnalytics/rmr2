import hadoopy
import hadoopy_hbase
import time
import logging
logging.basicConfig(level=logging.DEBUG)

st = time.time()

# NOTE(brandyn): If launch fails, you may need to use launch_frozen see hadoopy.com for details

out = 'out-%f/0' % st
hadoopy_hbase.launch('flickr', out, 'flickr_count_job.py', libjars=['hadoopy_hbase.jar', '/usr/lib/hbase/hbase.jar'],
                     num_mappers=8, columns=['metadata:'])
results = dict(hadoopy.readtb(out))
print(results)
