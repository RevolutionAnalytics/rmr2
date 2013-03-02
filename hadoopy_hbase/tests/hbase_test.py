import hadoopy
import hadoopy_hbase
import time
import logging
logging.basicConfig(level=logging.DEBUG)

st = time.time()

# NOTE(brandyn): If launch fails, you may need to use launch_frozen see hadoopy.com for details
#,
#
out = 'out-%f/3' % st
hadoopy_hbase.launch('testtable', out, 'hbase_test_job.py', columns=['colfam1:'], libjars=['hadoopy_hbase.jar'], start_row='5', stop_row='52')
results = hadoopy.readtb(out)
print list(results)[:10]

out = 'out-%f/1' % st
hadoopy_hbase.launch('testtable', out, 'hbase_test_job.py', columns=['colfam1:'], libjars=['hadoopy_hbase.jar'], jobconfs={'hbase.mapred.rowfilter': '.*3'})
results = hadoopy.readtb(out)
print list(results)[:10]

out = 'out-%f/0' % st
hadoopy_hbase.launch('testtable', out, 'hbase_test_job.py', columns=['colfam1:'], libjars=['hadoopy_hbase.jar'])
results = hadoopy.readtb(out)
print list(results)[:10]

out = 'out-%f/2' % st
hadoopy_hbase.launch('testtable', out, 'hbase_test_job2.py', columns=['colfam1:'], libjars=['hadoopy_hbase.jar'])
results = hadoopy.readtb(out)
print list(results)[:10]
