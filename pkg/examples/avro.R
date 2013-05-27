# known limitations: these format work only with mapreduce, not with from.dfs or to.dfs, nor they work in on the local backend
# as a workaround, use a simple conversion job 
# from.dfs(mapreduce(some.input, input.format = avroIF)) or mapreduce(to.dfs(some.data), output.format = avroOF)
# avroOF uses a fixed schema "bytes" containing the JSON representation of the data.


paste.fromJSON = 
  function(...)
    fromJSON(paste("[", paste(..., sep = ","), "]"))

mapply.fromJSON = 
  function(...)
    mapply(paste.fromJSON, ..., SIMPLIFY = FALSE)

avro.input.format = 
  function(con, keyval.length) {
    lines = readLines(con = con, n = keyval.length)
    if  (length(lines) == 0) NULL
    else
      do.call(
        keyval,
        unname(
          do.call(
            mapply.fromJSON,
            strsplit(
              lines,
              "\t"))))}

avroIF = 
  make.input.format(
    format = avro.input.format,
    mode = "text",
    streaming.format = "org.apache.avro.mapred.AvroAsTextInputFormat")

avro.input.format2 = 
  function(con, keyval.length) {
    lines = readLines(con = con, n = keyval.length)
    if  (length(lines) == 0) NULL
    else
      do.call(
        keyval,
        unname(
          do.call(
            mapply.fromJSON,
            strsplit(
              lines,
              "\t"))))}

avroIF2 = 
  make.input.format(
    format = rmr2:::make.native.input.format(),
    mode = "binary",
    streaming.format = "org.apache.avro.mapred.AvroAsTextInputFormat")

avro.output.format =
  function(kv, con)
    writeLines(
      unlist(
        rmr2:::reduce.keyval(
          kv,
          function(k, v)
            paste(
              toJSON(k, .escapeEscapes = TRUE),
              toJSON(v, .escapeEscapes = TRUE),
              sep = "\t"))),
      con = con)

avroOF =
  make.output.format(
    format = avro.output.format,
    mode = "text",
    streaming.format = "org.apache.avro.mapred.AvroTextOutputFormat")


avro.test = 
  mapreduce(
    to.dfs(keyval(1:2, 1:10)), 
    output.format = avroOF, 
    backend.parameters =
      list(
        hadoop =
          list(
            libjars = "/Users/antonio/Downloads/avro-mapred-1.7.4-hadoop1.jar")))

from.dfs(
  mapreduce(
    avro.test,
    input.format = avroIF,
    backend.parameters = 
      list(
        hadoop = 
          list(
            libjars = "/Users/antonio/Downloads/avro-mapred-1.7.4-hadoop1.jar"))))