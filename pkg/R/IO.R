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
           value.class = rmr2:::qw(list, vector, data.frame, matrix),#leave the pkg qualifier in here
           read.size = 1000) { 
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
    function(con) {
      lines = readLines(con, read.size)
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


make.json.output.format = 
  function(write.size = 1000)
    function(kv, con) {
      ser =
        function(k, v) 
          paste(
            gsub(
              "\n",
              "", 
              toJSON(k, .escapeEscapes=TRUE, collapse = "")),
            gsub("\n", "", toJSON(v, .escapeEscapes=TRUE, collapse = "")),
            sep = "\t", 
            write.size)
      out = reduce.keyval(kv, ser, 1000)
      writeLines(paste(out, collapse = "\n"), sep = "", con = con)}

make.text.input.format = 
  function(read.size = 1000)
    function(con) {
      lines = readLines(con, read.size)
      if (length(lines) == 0) NULL
      else keyval(NULL, lines)}

text.output.format = 
  function(kv, con) {
    ser = function(k, v) paste(k, v, collapse = "", sep = "\t")
    out = reduce.keyval(kv, ser, length.keyval(kv))
    writeLines(paste(out, "\n", collapse="", sep = ""), sep = "", con = con)}

make.csv.input.format =
  function(...) function(con) {
    df = 
      tryCatch(
        read.table(file = con, header = FALSE, ...),
        error = 
          function(e) {
            if(e$message != "no lines available in input")
              stop(e$message)
            NULL})  
    if(is.null(df) || dim(df)[[1]] == 0) NULL
    else keyval(NULL, df)}

make.csv.output.format =
  function(...) function(kv, con) {
    k = keys(kv)
    v = values(kv)
    write.table(file = con, 
                x = if(is.null(k)) v else cbind(k, v), 
                ..., 
                row.names = FALSE, 
                col.names = FALSE)}

typedbytes.reader =
  function(data, nobjs) {
    if(is.null(data)) NULL
    else
      .Call("typedbytes_reader", data, nobjs, PACKAGE = "rmr2")}

typedbytes.writer =
  function(objects, con, native) {
    writeBin(
      .Call("typedbytes_writer", objects, native, PACKAGE = "rmr2"),
      con)}

to.data.frame = 
  function(x, template){
    x = t.list(x)
    y = 
      lapply(
        seq_along(x), 
        function(i)
          if(is.atomic(template[[i]])) unlist(x[[i]]) else x[[i]])
    names(y) = names(template)
    data.frame(y)}

from.list = 
  function (x, template) {
    switch(
      class(template),
      NULL = NULL,
      list = splat(c)(x),
      matrix = splat(rbind)(x), 
      data.frame = to.data.frame(x, template),
      unlist(x))}

make.typedbytes.input.format =
  function(read.size = 10^7) {
    obj.buffer = list()
    obj.buffer.rmr.length = 0
    raw.buffer = raw()
    template.pe = NULL
    function(con) {
      is.native = length(con) > 1
      while(length(obj.buffer) < 2) {
        raw.buffer <<- c(raw.buffer, readBin(con[[1]], raw(), read.size))
        if(length(raw.buffer) == 0) break;
        parsed = typedbytes.reader(raw.buffer, as.integer(read.size/20)) #this is a ridiculous upper bound
        obj.buffer <<- c(obj.buffer, parsed$objects)
        if(parsed$length != 0) raw.buffer <<- raw.buffer[-(1:parsed$length)]}
      straddler = list()
      retval = {
        if(length(obj.buffer) == 0) NULL 
        else { 
          if(length(obj.buffer)%%2 ==1) {
            straddler = obj.buffer[length(obj.buffer)]
            obj.buffer <<- obj.buffer[-length(obj.buffer)]}
          kk = odd(obj.buffer)
          vv = even(obj.buffer)
          if(is.native)
            kk = rep(kk,sapply.rmr.length(vv))
          if(is.null(template.pe) && is.native) {
            load(con[[2]])
            template.pe <<- template}
          if(is.native) {
            keyval(
              from.list(kk, template.pe[[1]]),
              from.list(vv, template.pe[[2]]))}
          else {
            keyval(kk,vv)}}}
      obj.buffer <<- straddler
      retval}}

make.native.input.format = make.typedbytes.input.format

to.list = 
  function(x) {
    if (is.null(x))
      list(NULL)
    else {
      if (is.matrix(x)) x = as.data.frame(x)
      if (is.data.frame(x)) 
        unname(
          t.list(lapply(x, as.list)))
      else
        as.list(x)}}

make.native.or.typedbytes.output.format = 
  function(native, write.size = 1000) {
    template = NULL
    function(kv, con){
      k = keys(kv)
      v = values(kv)
      if(is.null(template) && native)  {
        template <<- 
          list(key = rmr.slice(k, 0), val = rmr.slice(v, 0))
        save(template, file = con[[2]])}
      kvs = {
        if(native)
          split.keyval(kv, write.size, TRUE)
        else 
          keyval(to.list(k), to.list(v))}
      if(is.null(k)) {
        if(!native) stop("Can't handle NULL in typedbytes")
        k =  rep_len(list(NULL), length.keyval(kvs)) }
      else 
        k = keys(kvs)
      v = values(kvs)
      typedbytes.writer(
        interleave(k, v), 
        con[[1]], 
        native)}}

make.native.output.format = 
  Curry(make.native.or.typedbytes.output.format, native = TRUE)
make.typedbytes.output.format = 
  Curry(make.native.or.typedbytes.output.format, native = FALSE)

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
  function(dense, atomic, key.deserialize, cell.deserialize, read.size) {
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
    tif = make.typedbytes.input.format(read.size)
    if(is.null(dense)) dense = FALSE
    function(con) {
      rec = tif(con)
      if(is.null(rec)) NULL
      else {
        df = hbase.rec.to.data.frame(rec, atomic, dense, key.deserialize, cell.deserialize)
        keyval(NULL, df)}}}

data.frame.to.nested.map =
  function(x,ind) {
    if(length(ind)>0 && nrow(x) > 0) {
      spl = split(x, x[,ind[1]])
      lapply(x[,ind[1]], function(y) keyval(as.character(y), data.frame.to.nested.map(spl[[y]], ind[-1])))}
    else x$value}

hbdf.to.m3 = Curry(data.frame.to.nested.map, ind = c("key", "family", "column"))
# I/O 

open.stdinout = 
  function(mode, is.read) {
    if(mode == "text") { 
      if(is.read)  
        file("stdin", "r") #not stdin() which is parsed by the interpreter
      else 
        stdout()}
    else { # binary
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
      pipe(cat, ifelse(is.read, "rb", "wb"))}}

#opens all required connections for a format. If in a task, replace first connection with stdin or out and
#make all the other names unique before opening

get.section = 
  function(fname)
    arrange(dfs.du(dirname(fname), basename(fname)), -size)$path[1]

make.section =
  function(fname)        
    paste(c(fname, if(in.a.task()) current.task()), collapse = "-")

make.keyval.readwriter = 
  function(fname, format, is.read) {
    if(length(fname) > 1)
      fname = current.input()
    if(!is.null(format$sections)) {
      if(!is.read) dfs.mkdir(fname) 
      fname = file.path(fname, format$sections)}
    con = list()
    if(in.a.task()){
      con[[1]] = open.stdinout(format$mode, is.read)
      fname = fname[-1]}
    con = 
      c(
        con,
        lapply(
          fname,
          function(fn) 
            file(
              if(is.read)
                get.section(fn)
              else
                make.section(fn), 
              paste(
                if(is.read) "r" else "w", 
                if(format$mode == "text") "" else "b",
                sep = ""))))
    if (is.null(format$sections))
      con = con[[1]]
    if (is.read) {
      function() 
        format$format(con)}
    else {
      function(kv)
        format$format(kv, con)}}

make.keyval.reader = Curry(make.keyval.readwriter, is.read = TRUE)
make.keyval.writer = Curry(make.keyval.readwriter, is.read = FALSE)

IO.formats = c("text", "json", "csv", "native",
               "sequence.typedbytes", "hbase", 
               "pig.hive")

make.input.format = 
  function(
    format = make.native.input.format(), 
    mode = c("binary", "text"),
    streaming.format = NULL, 
    backend.parameters = NULL,
    sections = NULL,
    ...) {
    mode = match.arg(mode)
    args = list(...)
    if(is.character(format)) {
      format = match.arg(format, IO.formats)
      switch(
        format, 
        text = {
          format = make.text.input.format(...)
          mode = "text"}, 
        json = {
          format = make.json.input.format(...) 
          mode = "text"}, 
        csv = {
          format = make.csv.input.format(...) 
          mode = "text"}, 
        native = {
          format = make.native.input.format(...) 
          mode = "binary"
          sections = c("part-00000", "_rmr2_template")}, 
        sequence.typedbytes = {
          format = make.typedbytes.input.format(...) 
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
          format = 
            make.hbase.input.format(
              default(args$dense, F),
              default(args$atomic, F),
              default(args$key.deserialize, "raw"),
              default(args$cell.deserialize, "raw"))
          mode = "binary"
          streaming.format = 
            "com.dappervision.hbase.mapred.TypedBytesTableInputFormat"
          family.columns = args$family.columns
          start.row = args$start.row
          stop.row = args$stop.row
          backend.parameters = 
            list(
              hadoop = 
                c(
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
                        sep = "")),
                  if(!is.null(start.row))
                    list(
                      D = 
                        paste(
                          "hbase.mapred.startrowb64=",
                          base64encode(start.row),
                          sep = "")),
                  if(!is.null(stop.row))
                    list(
                      D = 
                        paste(
                          "hbase.mapred.stoprowb64=",
                          base64encode(stop.row),
                          sep = "")),
                  list(
                    libjars = system.file(package = "rmr2", "hadoopy_hbase.jar"))))})}
    if(is.null(streaming.format) && mode == "binary") 
      streaming.format = "org.apache.hadoop.streaming.AutoInputFormat"
    list(mode = mode, 
         format = format, 
         streaming.format = streaming.format, 
         backend.parameters = backend.parameters,
         sections = sections)}

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
    format = make.native.output.format(),
    mode = c("binary", "text"),
    streaming.format = "org.apache.hadoop.mapred.SequenceFileOutputFormat", 
    backend.parameters = NULL,
    sections = NULL,
    ...) {
    mode = match.arg(mode)
    args = list(...)
    if(is.character(format)) {
      format = match.arg(format, IO.formats)
      switch(
        format, 
        text = {
          format = text.output.format
          mode = "text"
          streaming.format = NULL},
        json = {
          format = make.json.output.format(...)
          mode = "text"
          streaming.format = NULL}, 
        csv = {
          format = make.csv.output.format(...)
          mode = "text"
          streaming.format = NULL
          backend.parameters = set.separator.options(args$sep)}, 
        pig.hive = {
          format = 
            make.csv.output.format(  
              sep = "\001",
              quote = FALSE)
          mode = "text"
          streaming.format = NULL}, 
        native = {
          format = make.native.output.format(...)
          mode = "binary"
          streaming.format = "org.apache.hadoop.mapred.SequenceFileOutputFormat"
          sections = c("part-00000", "_rmr2_template")}, 
        sequence.typedbytes = {
          format = make.typedbytes.output.format(...)
          mode = "binary"
          streaming.format = "org.apache.hadoop.mapred.SequenceFileOutputFormat"},
        hbase = {
          stop("hbase output format not implemented yet")
          format = make.typedbytes.output.format(...)
          mode = "binary"
          streaming.format = "com.dappervision.mapreduce.TypedBytesTableOutputFormat"
          backend.parameters = 
            list(
              hadoop = 
                list(
                  D = paste(
                    "hbase.mapred.tablecolumnsb64=", 
                    args$family, 
                    ":", 
                    args$column, 
                    sep = ""),
                  libjars = system.file(package = "rmr2", "java/hadoopy_hbase.jar")))})}
    mode = match.arg(mode)
    list(
      mode = mode, 
      format = format, 
      streaming.format = streaming.format, 
      backend.parameters = backend.parameters, 
      sections = sections)}
