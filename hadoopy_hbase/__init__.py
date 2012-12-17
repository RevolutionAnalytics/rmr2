import hadoopy
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.ttypes import ColumnDescriptor, Mutation, BatchMutation
import hadoopy_hbase
import hashlib
import base64


def connect(server='localhost', port=9090):
    transport = TBufferedTransport(TSocket(server, int(port)))
    transport.open()
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = Hbase.Client(protocol)
    return client


def scanner(client, table, columns=None, per_call=1, start_row='', stop_row=None, max_rows=None):
    num_rows = 0
    sc = None
    try:
        if stop_row is None:
            sc = client.scannerOpen(table, start_row, columns if columns else [])
        else:
            sc = client.scannerOpenWithStop(table, start_row, stop_row, columns if columns else [])
        if per_call == 1:
            scanner = lambda : client.scannerGet(sc)
        else:
            scanner = lambda : client.scannerGetList(sc, per_call)
        while True:
            if max_rows is not None and num_rows >= max_rows:
                break
            outs = scanner()
            if outs:
                for out in outs:
                    if max_rows is not None and num_rows >= max_rows:
                        break
                    yield (out.row, dict((x, y.value)
                                         for x, y in out.columns.items()))
                    num_rows += 1
            else:
                break
    finally:
        if sc is not None:
            client.scannerClose(sc)


def scanner_row_column(client, table, column, **kw):
    scanner = hadoopy_hbase.scanner(client, table, columns=[column], **kw)
    for row, cols in scanner:
        yield row, cols[column]


def scanner_column(*args, **kw):
    return (y for x, y in scanner_row_column(*args, **kw))


def _launch_args(hbase_in, hbase_out, columns, start_row, stop_row, kw):
    if hbase_in:
        kw['input_format'] = 'com.dappervision.hbase.mapred.TypedBytesTableInputFormat'
    if hbase_out:
        kw['output_format'] = 'com.dappervision.hbase.mapred.TypedBytesTableOutputFormat'
    jobconfs = hadoopy._runner._listeq_to_dict(kw.get('jobconfs', []))
    jobconfs['hbase.mapred.tablecolumnsb64'] = ' '.join(map(base64.b64encode, columns))
    if start_row is not None:
        jobconfs['hbase.mapred.startrowb64'] = base64.b64encode(start_row)
    if stop_row is not None:
        jobconfs['hbase.mapred.stoprowb64'] = base64.b64encode(stop_row)
    kw['jobconfs'] = jobconfs


def launch_frozen(in_name, out_name, script_path, hbase_in=True, hbase_out=False, columns=(), start_row=None, stop_row=None, **kw):
    _launch_args(hbase_in, hbase_out, columns, start_row, stop_row, kw)
    hadoopy.launch_frozen(in_name, out_name, script_path, **kw)


def launch(in_name, out_name, script_path, hbase_in=True, hbase_out=False, columns=(), start_row=None, stop_row=None, **kw):
    _launch_args(hbase_in, hbase_out, columns, start_row, stop_row, kw)
    hadoopy.launch(in_name, out_name, script_path, **kw)


class HBaseColumnDict(object):

    def __init__(self, table, row, cf, db=None, **kw):
        if db is None:
            self._db = hadoopy_hbase.connect(**kw)
        else:
            self._db = db
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

    def __init__(self, table, col, db=None, **kw):
        if db is None:
            self._db = hadoopy_hbase.connect(**kw)
        else:
            self._db = db
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


def hash_key(*args, **kw):
    """Convenient key engineering function

    Allows for raw prefix/suffix, with other arguments md5 hashed and truncated.
    The key is only guaranteed to be unique if its prefix+suffix is unique.  If
    being used to create a start key, you can leave off args/suffix but they must
    be done in order (e.g., if you leave off an arg you must also leave off suffix).

    Args:
        *args: List of arguments to hash in order using hash_bytes of md5
        prefix: Raw prefix of the string (default '')
        suffix: Raw suffix of the string (default '')
        delimiter: Raw delimiter of each field (default '')
        hash_bytes: Number of md5 bytes (binary not hex) for each of *args

    Returns:
        Combined key (binary)
    """
    prefix = kw.get('prefix', '')
    suffix = kw.get('suffix', '')
    delimiter = kw.get('delimiter', '')
    if args:
        try:
            hash_bytes = kw['hash_bytes']
        except KeyError:
            raise ValueError('hash_bytes keyword argument must be specified')
        return delimiter.join([prefix] + [hashlib.md5(x).digest()[:hash_bytes] for x in args] + [suffix])
    else:
        return delimiter.join([prefix, suffix])

