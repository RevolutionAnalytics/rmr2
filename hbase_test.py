import hadoopy
import time
import logging
logging.basicConfig(level=logging.DEBUG)

out = 'out-%f' % time.time()
hadoopy.launch_frozen('testtable', out, 'hbase_test_job.py', use_autoinput='tb', jobconfs=['hbase.mapred.tablecolumns=colfam1:'], use_typedbytes=True)

print list(hadoopy.readtb(out))
