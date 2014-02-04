hdfs.ls = 
  function(fname) 
    read.table(
      textConnection(hdfs("ls", fname, intern = TRUE)), 
      skip=1, 
      col.names=c("permissions", "links", "owner", "group", "size", "date", "time", "path"),
      stringsAsFactors = FALSE)
hdfs.exists = 
  function(fname)
    hdfs("test -e", fname, test = TRUE)
hdfs.rmr = 
  function(fname)
    hdfs("rm -r", fname)
hdfs.isdir = 
  function(fname)
    hdfs("test -d", fname, test = TRUE)
hdfs.mv = 
  function(src, dst)
    hdfs("mv", src, dst)
hdfs.mkdir = 
  function(fname)
    hdfs("mkdir", fname)
hdfs.put = 
  function(src, dst)
    hdfs("put", src, dst)
hdfs.get = 
  function(src, dst)
    hdfs("get", src, dst)

hdfs = 
  function(cmd, ..., intern = FALSE, test = FALSE) {
    retval = 
      system(
        paste(
          hdfs.cmd(), 
          "dfs", 
          paste("-", cmd, sep = ""), 
          paste(..., collapse=" ")), 
        intern = intern)
    if(intern)
      retval
    else{
      if(test)
        retval == 0
      else {
        stopifnot(retval == 0)
        NULL }}}