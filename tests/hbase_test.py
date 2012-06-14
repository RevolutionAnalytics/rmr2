import hadoopy
import hadoopy_hbase
import time
import logging
logging.basicConfig(level=logging.DEBUG)

out = 'out-%f' % time.time()
hadoopy_hbase.launch_frozen('testtable', out, 'hbase_test_job.py', columns=['colfam1:'])

print list(hadoopy.readtb(out))
