import hadoopy


def launch_frozen(in_name, out_name, script_path, hbase_in=True, hbase_out=False, columns=(,), **kw):
    if hbase_in:
        kw['input_format'] = 'com.dappervision.hbase.mapred.TypedBytesTableInputFormat'
    if hbase_out:
        kw['output_format'] = 'com.dappervision.hbase.mapred.TypedBytesTableOutputFormat'
    jobconfs = kw.get('jobconfs', [])
    jobconfs.append('hbase.mapred.tablecolumns="%s"' % ' '.join(columns))
    kw['jobconfs'] = jobconfs
    hadoopy.launch_frozen(in_name, out_name, script_path, **kw)
