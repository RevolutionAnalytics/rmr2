#!/usr/bin/env python
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
from thrift.protocol import TBinaryProtocol
from thrift_bench import random_string, remove_table
import hadoopy_hbase

client = hadoopy_hbase.connect('localhost')
remove_table(client, 'testtable')
client.createTable('testtable', [hadoopy_hbase.ColumnDescriptor('colfam1:')])

for x in xrange(100):
    client.mutateRow('testtable', str(x), [hadoopy_hbase.Mutation(column='colfam1:col%d' % y, value=random_string(5)) for y in range(10)])
print(client.getRow('testtable', '0'))
