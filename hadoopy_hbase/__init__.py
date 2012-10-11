import hadoopy
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.ttypes import ColumnDescriptor, Mutation, BatchMutation


def connect(server, port=9090):
    transport = TBufferedTransport(TSocket(server, 9090))
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
