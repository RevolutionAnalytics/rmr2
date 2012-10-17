import hadoopy
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.ttypes import ColumnDescriptor, Mutation, BatchMutation
import hadoopy_hbase


def connect(server='localhost', port=9090):
    transport = TBufferedTransport(TSocket(server, int(port)))
    transport.open()
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = Hbase.Client(protocol)
    return client


def scanner(client, table, columns=None, per_call=1, start_row=''):
    try:
        sc = client.scannerOpen(table, start_row, columns if columns else [])
        if per_call == 1:
            scanner = lambda : client.scannerGet(sc)
        else:
            scanner = lambda : client.scannerGetList(sc, per_call)
        while True:
            outs = scanner()
            if outs:
                for out in outs:
                    yield (out.row, dict((x, y.value)
                                         for x, y in out.columns.items()))
            else:
                break
    finally:
        client.scannerClose(sc)


def launch_frozen(in_name, out_name, script_path, hbase_in=True, hbase_out=False, columns=(), **kw):
    if hbase_in:
        kw['input_format'] = 'com.dappervision.hbase.mapred.TypedBytesTableInputFormat'
    if hbase_out:
        kw['output_format'] = 'com.dappervision.hbase.mapred.TypedBytesTableOutputFormat'
    jobconfs = kw.get('jobconfs', [])
    jobconfs.append('hbase.mapred.tablecolumns="%s"' % ' '.join(columns))
    kw['jobconfs'] = jobconfs
    hadoopy.launch_frozen(in_name, out_name, script_path, **kw)


def launch(in_name, out_name, script_path, hbase_in=True, hbase_out=False, columns=(), **kw):
    if hbase_in:
        kw['input_format'] = 'com.dappervision.hbase.mapred.TypedBytesTableInputFormat'
    if hbase_out:
        kw['output_format'] = 'com.dappervision.hbase.mapred.TypedBytesTableOutputFormat'
    jobconfs = kw.get('jobconfs', [])
    jobconfs.append('hbase.mapred.tablecolumns="%s"' % ' '.join(columns))
    kw['jobconfs'] = jobconfs
    hadoopy.launch(in_name, out_name, script_path, **kw)


class HBaseColumnDict(object):

    def __init__(self, table, row, cf, **kw):
        self._db = hadoopy_hbase.connect(**kw)
        self._table = table
        self._row = row
        self._cf = cf + ':'

    def __setitem__(self, key, value):
        assert isinstance(key, str)
        assert isinstance(value, str)
        self._db.mutateRow(self._table, self._row, [hadoopy_hbase.Mutation(column=self._cf + key, value=value)])

    def __getitem__(self, key):
        assert isinstance(key, str)
        result = self._db.get(self._table, self._row, self._cf + key)
        if not result:
            raise KeyError
        return result[0].value

    def __delitem__(self, key):
        assert isinstance(key, str)
        self._db.mutateRow(self._table, self._row, [hadoopy_hbase.Mutation(column=self._cf + key, isDelete=True)])

    def items(self):
        result = self._db.getRow(self._table, self._row)
        if not result:
            return []
        return [(x, y.value) for x, y in result[0].columns.items()]


class HBaseRowDict(object):

    def __init__(self, table, col, **kw):
        self._db = hadoopy_hbase.connect(**kw)
        self._table = table
        self._col = col

    def __setitem__(self, key, value):
        assert isinstance(key, str)
        assert isinstance(value, str)
        self._db.mutateRow(self._table, key, [hadoopy_hbase.Mutation(column=self._col, value=value)])

    def __getitem__(self, key):
        assert isinstance(key, str)
        result = self._db.get(self._table, key, self._col)
        if not result:
            raise KeyError
        return result[0].value

    def __delitem__(self, key):
        assert isinstance(key, str)
        self._db.mutateRow(self._table, key, [hadoopy_hbase.Mutation(column=self._col, isDelete=True)])

