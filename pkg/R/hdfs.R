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
test.rmr =
  function() {
    length(
      suppressWarnings(
        rmr2:::hdfs("- 2>&1 | grep rmr", intern=T))) > 0}

hdfs.rmr = 
  (function() {
    rmr = NULL
    function(fname) {
      if(is.null(rmr))
        rmr <<- test.rmr()
      if(rmr)
        hdfs("rmr", fname)
      else 
        hdfs("rm -r", fname)}})()
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