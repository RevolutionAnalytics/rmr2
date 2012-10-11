#!/usr/bin/env python
# Useful link: https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/thrift/doc-files/Hbase.html
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
from thrift.protocol import TBinaryProtocol
import hadoopy_hbase
from hadoopy_hbase import Hbase, ColumnDescriptor, Mutation
import time
import contextlib
import random

@contextlib.contextmanager
def timer(name):
    st = time.time()
    yield
    print('[%s]: %s' % (name, time.time() - st))


def random_string(l):
    s = hex(random.getrandbits(8 * l))[2:]
    if s[-1] == 'L':
        s = s[:-1]
    # Pad with zeros
    if len(s) != l * 2:
        s = '0' * (2 * l - len(s)) + s
    return s.decode('hex')

def remove_table(client, table):
    if table in client.getTableNames():
        client.disableTable(table)
        client.deleteTable(table)

def scanner(client, table, column, num_rows, max_rows):
    with timer('scanner:rows%d-%s' % (num_rows, column)):
        sc = client.scannerOpen('benchtable', '', [column] if column else [])
        for x in xrange(max_rows / num_rows):
            out = client.scannerGetList(sc, num_rows)
            if not out:
                break
        client.scannerClose(sc)

def delete_rows(client, table, max_rows):
    with timer('deleteAllRow:Delete'):
        for x in xrange(max_rows):
            client.deleteAllRow(table, str(x))


def simple(client, max_rows):
    print('\nsimple')
    remove_table(client, 'benchtable')
    with timer('createTable'):
        client.createTable('benchtable', [ColumnDescriptor('cf0:')])

    with timer('mutateRow:Create'):
        for x in xrange(max_rows):
            client.mutateRow('benchtable', str(x), [Mutation(column='cf0:small', value=random_string(2 ** 10))])

    scanner(client, 'benchtable', '', 10, max_rows)
    scanner(client, 'benchtable', '', 1, max_rows)
    delete_rows(client, 'benchtable', max_rows)
    scanner(client, 'benchtable', '', 1, max_rows)
    remove_table(client, 'benchtable')

def small_large_1cf(client, max_rows):
    print('\nsmall_large_1cf')
    remove_table(client, 'benchtable')
    with timer('createTable'):
        client.createTable('benchtable', [ColumnDescriptor('cf0:')])
    
    with timer('mutateRow:Create-small'):
        for x in xrange(max_rows):
            client.mutateRow('benchtable', str(x), [Mutation(column='cf0:small', value=random_string(2 ** 10))])

    for x in ['cf0:small']:
        scanner(client, 'benchtable', x, 10, max_rows)
        scanner(client, 'benchtable', x, 1, max_rows)
    
    with timer('mutateRow:Create-large'):
        for x in xrange(max_rows):
            client.mutateRow('benchtable', str(x), [Mutation(column='cf0:large', value=random_string(2 ** 20))])

    for x in ['cf0:small', 'cf0:large', '']:
        scanner(client, 'benchtable', x, 10, max_rows)
        scanner(client, 'benchtable', x, 1, max_rows)

    delete_rows(client, 'benchtable', max_rows)
    scanner(client, 'benchtable', '', 1, max_rows)
    remove_table(client, 'benchtable')


def small_large_2cf(client, max_rows):
    print('\nsmall_large_2cf')
    remove_table(client, 'benchtable')
    with timer('createTable'):
        client.createTable('benchtable', [ColumnDescriptor('cf0:'), ColumnDescriptor('cf1:')])
    
    with timer('mutateRow:Create-small'):
        for x in xrange(max_rows):
            client.mutateRow('benchtable', str(x), [Mutation(column='cf0:small', value=random_string(2 ** 10))])

    for x in ['cf0:small']:
        scanner(client, 'benchtable', x, 10, max_rows)
        scanner(client, 'benchtable', x, 1, max_rows)
    
    with timer('mutateRow:Create-large'):
        for x in xrange(max_rows):
            client.mutateRow('benchtable', str(x), [Mutation(column='cf1:large', value=random_string(2 ** 20))])

    for x in ['cf0:small', 'cf1:large', '']:
        scanner(client, 'benchtable', x, 10, max_rows)
        scanner(client, 'benchtable', x, 1, max_rows)

    delete_rows(client, 'benchtable', max_rows)
    scanner(client, 'benchtable', '', 1, max_rows)
    remove_table(client, 'benchtable')


def few_many_1cf(client, max_rows):
    print('\nfew_many_1cf')
    remove_table(client, 'benchtable')
    with timer('createTable'):
        client.createTable('benchtable', [ColumnDescriptor('cf0:')])
    
    with timer('mutateRow:Create-few'):
        for x in xrange(max_rows):
            if x % 100 == 0:
                client.mutateRow('benchtable', str(x), [Mutation(column='cf0:few', value=random_string(2 ** 10))])

    for x in ['cf0:few']:
        scanner(client, 'benchtable', x, 100, max_rows)
        scanner(client, 'benchtable', x, 10, max_rows)
        scanner(client, 'benchtable', x, 1, max_rows)
    
    with timer('mutateRow:Create-many'):
        for x in xrange(max_rows):
            client.mutateRow('benchtable', str(x), [Mutation(column='cf0:many', value=random_string(2 ** 10))])

    for x in ['cf0:few', 'cf0:many', '']:
        scanner(client, 'benchtable', x, 100, max_rows)
        scanner(client, 'benchtable', x, 10, max_rows)
        scanner(client, 'benchtable', x, 1, max_rows)

    delete_rows(client, 'benchtable', max_rows)
    scanner(client, 'benchtable', '', 1, max_rows)
    remove_table(client, 'benchtable')


def few_many_2cf(client, max_rows):
    print('\nfew_many_2cf')
    remove_table(client, 'benchtable')
    with timer('createTable'):
        client.createTable('benchtable', [ColumnDescriptor('cf0:'), ColumnDescriptor('cf1:')])
    
    with timer('mutateRow:Create-few'):
        for x in xrange(max_rows):
            if x % 100 == 0:
                client.mutateRow('benchtable', str(x), [Mutation(column='cf0:few', value=random_string(2 ** 10))])

    for x in ['cf0:few']:
        scanner(client, 'benchtable', x, 100, max_rows)
        scanner(client, 'benchtable', x, 10, max_rows)
        scanner(client, 'benchtable', x, 1, max_rows)
    
    with timer('mutateRow:Create-many'):
        for x in xrange(max_rows):
            client.mutateRow('benchtable', str(x), [Mutation(column='cf1:many', value=random_string(2 ** 10))])

    for x in ['cf0:few', 'cf1:many', '']:
        scanner(client, 'benchtable', x, 100, max_rows)
        scanner(client, 'benchtable', x, 10, max_rows)
        scanner(client, 'benchtable', x, 1, max_rows)

    delete_rows(client, 'benchtable', max_rows)
    scanner(client, 'benchtable', '', 1, max_rows)
    remove_table(client, 'benchtable')


def manycols_1cf(client, max_rows):
    print('\nmanycols_1cf')
    num_cols = 100
    remove_table(client, 'benchtable')
    with timer('createTable'):
        client.createTable('benchtable', [ColumnDescriptor('cf0:')])
    
    with timer('mutateRow:Create'):
        for x in xrange(max_rows):
            client.mutateRow('benchtable', str(x), [Mutation(column='cf0:%d' % y, value=random_string(2 ** 5)) for y in xrange(num_cols)])

    for x in ['cf0:0', '']:
        scanner(client, 'benchtable', x, 10, max_rows)
        scanner(client, 'benchtable', x, 1, max_rows)

    delete_rows(client, 'benchtable', max_rows)
    scanner(client, 'benchtable', '', 1, max_rows)
    remove_table(client, 'benchtable')


def manycols_manycf(client, max_rows):
    print('\nmanycols_manycf')
    num_cols = 100
    remove_table(client, 'benchtable')
    with timer('createTable'):
        client.createTable('benchtable', [ColumnDescriptor('cf%d:' % x) for x in xrange(num_cols)])
    
    with timer('mutateRow:Create'):
        for x in xrange(max_rows):
            client.mutateRow('benchtable', str(x), [Mutation(column='cf%d:%d' % (y, y), value=random_string(2 ** 5)) for y in xrange(num_cols)])

    for x in ['cf0:0', '']:
        scanner(client, 'benchtable', x, 10, max_rows)
        scanner(client, 'benchtable', x, 1, max_rows)

    delete_rows(client, 'benchtable', max_rows)
    scanner(client, 'benchtable', '', 1, max_rows)
    remove_table(client, 'benchtable')



if __name__ == '__main__':
    client = hadoopy_hbase.connect('localhost')
    simple(client, 1000)
    small_large_1cf(client, 100)
    small_large_2cf(client, 100)
    few_many_1cf(client, 10000)
    few_many_2cf(client, 10000)
    manycols_1cf(client, 100)
    manycols_manycf(client, 100)
