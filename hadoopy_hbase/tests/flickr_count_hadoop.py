import hadoopy
import hadoopy_hbase
import time
import logging
logging.basicConfig(level=logging.DEBUG)

st = time.time()

# NOTE(brandyn): If launch fails, you may need to use launch_frozen see hadoopy.com for details

out = 'out-%f/0' % st
jobconfs = ['mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec',
            'mapred.compress.map.output=true',
            'mapred.output.compression.type=BLOCK']
hadoopy_hbase.launch('flickr', out, 'identity_hbase_job.py', libjars=['hadoopy_hbase.jar'],
                     num_mappers=8, columns=['metadata:'], jobconfs=jobconfs)
#results = dict(hadoopy.readtb(out))
#print(results)
