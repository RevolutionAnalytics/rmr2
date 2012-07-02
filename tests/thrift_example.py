#!/usr/bin/env python
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from thrift_bench import random_string, setup, ColumnDescriptor, Mutation, remove_table

client = setup()
remove_table(client, 'testtable')
client.createTable('testtable', [ColumnDescriptor('colfam1:')])

for x in xrange(100):
    client.mutateRow('testtable', str(x), [Mutation(column='colfam1:col%d' % y, value=random_string(5)) for y in range(10)])
