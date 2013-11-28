# Copyright 2011 Revolution Analytics
#    
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, 
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


make.json.input.format =
  function(key.class = rmr2:::qw(list, vector, data.frame, matrix),
           value.class = rmr2:::qw(list, vector, data.frame, matrix)) { #leave the pkg qualifier in here
    key.class = match.arg(key.class)
    value.class = match.arg(value.class)
    cast =
      function(class)
        switch(
          class,
          list = identity,
          vector = as.vector,
          data.frame = function(x) do.call(data.frame, x),
          matrix = function(x) do.call(rbind, x))
    process.field = 
      function(field, class)
        cast(class)(fromJSON(field, asText = TRUE))
    function(con, keyval.length) {
      lines = readLines(con, keyval.length)
      if (length(lines) == 0) NULL
      else {
        splits =  strsplit(lines, "\t")
        c.keyval(
          lapply(splits, 
                 function(x) 
                   if(length(x) == 1) 
                     keyval(NULL, process.field(x[1], value.class)) 
                 else 
                   keyval(process.field(x[1], key.class), process.field(x[2], value.class))))}}}

json.output.format = function(kv, con) {
  ser = function(k, v) paste(gsub("\n", "", toJSON(k, .escapeEscapes=TRUE, collapse = "")),
                             gsub("\n", "", toJSON(v, .escapeEscapes=TRUE, collapse = "")),
                             sep = "\t")
  out = reduce.keyval(kv, ser, rmr.options('keyval.length'))
  writeLines(paste(out, collapse = "\n"), sep = "", con = con)}

text.input.format = function(con, keyval.length) {
  lines = readLines(con, keyval.length)
  if (length(lines) == 0) NULL
  else keyval(NULL, lines)}

text.output.format = function(kv, con) {
  ser = function(k, v) paste(k, v, collapse = "", sep = "\t")
  out = reduce.keyval(kv, ser, length.keyval(kv))
  writeLines(paste(out, "\n", collapse="", sep = ""), sep = "", con = con)}

make.csv.input.format = function(...) function(con, keyval.length) {
  df = 
    tryCatch(
      read.table(file = con, nrows = keyval.length, header = FALSE, ...),
      error = 
        function(e) {
          if(e$message != "no lines available in input")
            stop(e$message)
          NULL})  
  if(is.null(df) || dim(df)[[1]] == 0) NULL
  else keyval(NULL, df)}

make.csv.output.format = function(...) function(kv, con) {
  kv = recycle.keyval(kv)
  k = keys(kv)
  v = values(kv)
  write.table(file = con, 
              x = if(is.null(k)) v else cbind(k, v), 
              ..., 
              row.names = FALSE, 
              col.names = FALSE)}

typedbytes.reader = function(data, nobjs) {
  if(is.null(data)) NULL
  else
    .Call("typedbytes_reader", data, nobjs, PACKAGE = "rmr2")}

typedbytes.writer = function(objects, con, native) {
  writeBin(
    .Call("typedbytes_writer", objects, native, PACKAGE = "rmr2"),
    con)}

make.typedbytes.input.format = function(recycle = TRUE) {
  obj.buffer = list()
  obj.buffer.rmr.length = 0
  raw.buffer = raw()
  read.size = 100
  function(con, keyval.length) {
    while(length(obj.buffer) < 2 || 
      obj.buffer.rmr.length < keyval.length) {
      raw.buffer <<- c(raw.buffer, readBin(con, raw(), read.size))
      if(length(raw.buffer) == 0) break;
      parsed = typedbytes.reader(raw.buffer, as.integer(read.size/2))
      obj.buffer <<- c(obj.buffer, parsed$objects)
      approx.read.records = {
        if(length(parsed$objects) == 0) 0 
        else 
          sum(
            sapply(sample(parsed$objects, 10, replace = T), rmr.length)) * 
          length(parsed$objects)/10.0 }
      obj.buffer.rmr.length <<- obj.buffer.rmr.length + approx.read.records 
      read.size <<- ceiling(1.1^sign(keyval.length - obj.buffer.rmr.length) * read.size)
      if(parsed$length != 0) raw.buffer <<- raw.buffer[-(1:parsed$length)]}
    straddler = list()
    retval = 
      if(length(obj.buffer) == 0) NULL 
      else { 
        if(length(obj.buffer)%%2 ==1) {
           straddler = obj.buffer[length(obj.buffer)]
           obj.buffer <<- obj.buffer[-length(obj.buffer)]}
        kk = odd(obj.buffer)
        vv = even(obj.buffer)
        if(recycle) {
          keyval(
            c.or.rbind.rep(kk, sapply.rmr.length(vv)), 
            c.or.rbind(vv))}
        else {
          keyval(kk, vv)}}
    obj.buffer <<- straddler
    obj.buffer.rmr.length <<- 0
    retval}}
  
make.native.input.format = make.typedbytes.input.format

make.native.or.typedbytes.output.format = 
  function(keyval.length, native)
    function(kv, con){
      kvs = split.keyval(kv, keyval.length)
      typedbytes.writer(interleave(keys(kvs), values(kvs)), con, native)}

make.native.output.format = Curry(make.native.or.typedbytes.output.format, native = TRUE)
make.typedbytes.output.format = Curry(make.native.or.typedbytes.output.format, native = FALSE)

pRawToChar = 
  function(rl)
    .Call("raw_list_to_character", rl, PACKAGE="rmr2")

hbase.rec.to.data.frame = 
  function(
    source, 
    atomic, 
    dense, 
    key.deserialize = pRawToChar, 
    cell.deserialize =
      function(x, column, family) pRawToChar(x)) {
    filler = replicate(length(unlist(source))/2, NULL)
    dest = 
      list(
        key = filler,
        family = filler,
        column = filler,
        cell = filler)
    tmp = 
      .Call(
        "hbase_to_df", 
        source, 
        dest, 
        PACKAGE="rmr2")
    retval = data.frame(
      key = 
        I(
          key.deserialize(
            tmp$data.frame$key[1:tmp$nrows])), 
      family = 
        pRawToChar(
          tmp$data.frame$family[1:tmp$nrows]), 
      column = 
        pRawToChar(
          tmp$data.frame$column[1:tmp$nrows]), 
      cell = 
        I(
          cell.deserialize(
            tmp$data.frame$cell[1:tmp$nrows],
            tmp$data.frame$family[1:tmp$nrows],
            tmp$data.frame$column[1:tmp$nrows])))
    if(atomic) 
      retval = 
      as.data.frame(
        lapply(
          retval, 
          function(x) if(is.factor(x)) x else unclass(x)))
    if(dense) retval = dcast(retval,  key ~ family + column)
    retval}

make.hbase.input.format = 
  function(dense, atomic, key.deserialize, cell.deserialize) {
    deserialize.opt = 
      function(deser) {
        if(is.null(deser)) deser = "raw"
        if(is.character(deser))
          deser =
          switch(
            deser,
            native = 
              function(x, family = NULL, column = NULL) lapply(x, unserialize),
            typedbytes = 
              function(x, family = NULL, column = NULL) 
                typedbytes.reader(
                  do.call(c, x),  
                  nobjs = length(x)),
            raw = function(x, family = NULL, column = NULL) pRawToChar(x))
        deser}
    key.deserialize = deserialize.opt(key.deserialize)
    cell.deserialize = deserialize.opt(cell.deserialize)
    tif = make.typedbytes.input.format(recycle = FALSE)
    if(is.null(dense)) dense = FALSE
    function(con, keyval.length) {
      rec = tif(con, keyval.length)
      if(is.null(rec)) NULL
      else {
        df = hbase.rec.to.data.frame(rec, atomic, dense, key.deserialize, cell.deserialize)
        keyval(NULL, df)}}}

data.frame.to.nested.map = function(x,ind) {
  if(length(ind)>0 && nrow(x) > 0) {
    spl = split(x, x[,ind[1]])
    lapply(x[,ind[1]], function(y) keyval(as.character(y), data.frame.to.nested.map(spl[[y]], ind[-1])))}
  else x$value}

hbdf.to.m3 = Curry(data.frame.to.nested.map, ind = c("key", "family", "column"))
# I/O 
make.keyval.readwriter = 
  function(mode, format, keyval.length, con = NULL, read) {
    if(is.null(con)) 
      con = {
        if(mode == "text") { 
          if(read)  file("stdin", "r") #not stdin() which is parsed by the interpreter
          else stdout()}
        else {
          cat  = {
            if(.Platform$OS.type == "windows")
              paste(
                "\"", 
                system.file(
                  package="rmr2", 
                  "bin", 
                  .Platform$r_arch, 
                  "catwin.exe"), 
                "\"", 
                sep="")
            else
              "cat"}
          pipe(cat, ifelse(read, "rb", "wb"))}}
    if (read) {
      function() 
        format(con, keyval.length)}
    else {
      function(kv)
        format(kv, con)}}

make.keyval.reader = Curry(make.keyval.readwriter, read = TRUE)
make.keyval.writer = Curry(make.keyval.readwriter, keyval.length = NULL, read = FALSE)

IO.formats = c("text", "json", "csv", "native",
               "sequence.typedbytes", "hbase", 
               "pig.hive")

make.input.format = 
  function(
    format = make.native.input.format(), 
    mode = c("binary", "text"),
    streaming.format = NULL, 
    ...) {
    mode = match.arg(mode)
    backend.parameters = NULL
    if(is.character(format)) {
      format = match.arg(format, IO.formats)
      switch(
        format, 
        text = {
          format = text.input.format 
          mode = "text"}, 
        json = {
          format = make.json.input.format(...) 
          mode = "text"}, 
        csv = {
          format = make.csv.input.format(...) 
          mode = "text"}, 
        native = {
          format = make.native.input.format() 
          mode = "binary"}, 
        sequence.typedbytes = {
          format = make.typedbytes.input.format() 
          mode = "binary"},
        pig.hive = {
          format = 
            make.csv.input.format(
              sep = "\001",
              comment.char = "",
              fill = TRUE,
              flush = TRUE,
              quote = "")
          mode = "text"},
        hbase = {
          optlist = list(...)
          format = 
            make.hbase.input.format(
              default(optlist$dense, F),
              default(optlist$atomic, F),
              default(optlist$key.deserialize, "raw"),
              default(optlist$cell.deserialize, "raw"))
          mode = "binary"
          streaming.format = 
            "com.dappervision.hbase.mapred.TypedBytesTableInputFormat"
          family.columns = optlist$family.columns
	  start.row = optlist$start.row
	  stop.row = optlist$stop.row
	  regxrow.filter = optlist$regxrow.filter
          if (length(start.row)==0) {start.row=""} 
          if (length(stop.row)==0) {stop.row=""}
          if (length(regxrow.filter)==0) {regxrow.filter=""}
          backend.parameters = 
            list(
              hadoop = 
                list(
                  D = 
                    paste(
                      "hbase.mapred.tablecolumnsb64=",
                      paste(
                        sapply(
                          names(family.columns), 
                          function(fam) 
                            paste(
                              sapply(
                                1:length(family.columns[[fam]]),
                                function(i) 
                                  base64encode(
                                      paste(
                                        fam,
                                        ":",
                                        family.columns[[fam]][i],
                                        sep = "",
                                        collapse = ""))),
                              sep = "",
                              collapse = " ")),
                        collapse = " "),
                      sep = ""),
		  D = 
                    paste(
                      "hbase.mapred.startrowb64=",
                      base64encode(start.row),
                      sep = ""),
		  D = 
                    paste(
                      "hbase.mapred.stoprowb64=",
                      base64encode(stop.row),
                      sep = ""),
		  D = 
                    paste(
                      "hbase.mapred.rowfilter=",
                      regxrow.filter,
                      sep = ""),
                  libjars = system.file(package = "rmr2", "hadoopy_hbase.jar")))})}
    if(is.null(streaming.format) && mode == "binary") 
      streaming.format = "org.apache.hadoop.streaming.AutoInputFormat"
    list(mode = mode, 
         format = format, 
         streaming.format = streaming.format, 
         backend.parameters = backend.parameters)}

set.separator.options =
  function(sep) {
    if(!is.null(sep))
      list(
        hadoop = 
          list(
            D = 
              paste(
                "mapred.textoutputformat.separator=",
                sep,
                sep = ""),
            D =
              paste(
                "stream.map.output.field.separator=",
                sep,
                sep = ""),
            D = 
              paste(
                "stream.reduce.output.field.separator=",
                sep,
                sep = "")))}

make.output.format = 
  function(
    format = make.native.output.format(keyval.length = rmr.options('keyval.length')),
    mode = c("binary", "text"),
    streaming.format = "org.apache.hadoop.mapred.SequenceFileOutputFormat", 
    ...) {
    mode = match.arg(mode)
    backend.parameters = NULL
    if(is.character(format)) {
      format = match.arg(format, IO.formats)
      switch(
        format, 
        text = {
          format = text.output.format
          mode = "text"
          streaming.format = NULL},
        json = {
          format = json.output.format
          mode = "text"
          streaming.format = NULL}, 
        csv = {
          format = make.csv.output.format(...)
          mode = "text"
          streaming.format = NULL
          sep = list(...)$sep
          backend.parameters = set.separator.options(sep)}, 
        pig.hive = {
          format = 
            make.csv.output.format(  
              sep = "\001",
              quote = FALSE)
          mode = "text"
          streaming.format = NULL}, 
        native = {
          format = make.native.output.format(
            keyval.length = rmr.options('keyval.length'))
          mode = "binary"
          streaming.format = "org.apache.hadoop.mapred.SequenceFileOutputFormat"}, 
        sequence.typedbytes = {
          format = make.typedbytes.output.format(keyval.length = rmr.options('keyval.length'))
          mode = "binary"
          streaming.format = "org.apache.hadoop.mapred.SequenceFileOutputFormat"},
        hbase = {
          stop("hbase output format not implemented yet")
          format = make.typedbytes.output.format(recycle = FALSE)
          mode = "binary"
          streaming.format = "com.dappervision.mapreduce.TypedBytesTableOutputFormat"
          backend.parameters = 
            list(
              hadoop = 
                list(
                  D = paste(
                    "hbase.mapred.tablecolumnsb64=", 
                    list(...)$family, 
                    ":", 
                    list(...)$column, 
                    sep = ""),
                libjars = system.file(package = "rmr2", "java/hadoopy_hbase.jar")))})}
    mode = match.arg(mode)
    list(mode = mode, format = format, streaming.format = streaming.format, backend.parameters = backend.parameters)}
