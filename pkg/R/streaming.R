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

#some option formatting utils

paste.options = 
  function(...) {
    quote.char = 
      if(.Platform$OS.type == "windows") "\""
    else "'"
    
    optlist = 
      unlist(
        sapply(
          list(...), 
          function(x) { 
            if (is.null(x)) {NULL}
            else {
              if (is.logical(x)) {
                if(x) "" else NULL} 
              else paste(quote.char, x, quote.char, sep = "")}}))
    if(is.null(optlist)) "" 
    else 
      paste(
        " ",
        unlist(
          rbind(
            paste(
              "-", 
              names(optlist), 
              sep = ""), 
            optlist)), 
        " ",
        collapse = " ")}

make.input.files = 
  function(infiles) {
    if(length(infiles) == 0) return(" ")
    paste(sapply(infiles, 
                 function(r) {
                   paste.options(input = r)}), 
          collapse = " ")}

## loops section, or what runs on the nodes

activate.profiling = 
  function(profile) {
    dir = file.path("/tmp/Rprof", current.job(), Sys.getenv('mapred_tip_id'))
    dir.create(dir, recursive = TRUE)
    if(is.element(profile, c("calls", "both"))) {
      prof.file = file.path(dir, paste(current.task(), Sys.time(), sep = "-")) 
      warning("Profiling data in ", prof.file)
      Rprof(prof.file)}
    if(is.element(profile, c("memory", "both"))) {
      mem.prof.file = file.path(dir, paste(current.task(), Sys.time(), "mem", sep = "-")) 
      warning("Memory profiling data in ", mem.prof.file)
      Rprofmem(mem.prof.file)}}

close.profiling = 
  function(profile) {
    if(is.element(profile, c("calls", "both")))
      Rprof(NULL)
    if(is.element(profile, c("memory", "both")))
      Rprofmem(NULL)}

reduce.as.keyval = 
  function(k, vv, reduce) 
    as.keyval(reduce(k, vv))

apply.reduce = 
  function(kv, reduce)
    c.keyval(
      reduce.keyval(
        kv, 
        reduce))

map.loop = 
  function(
    map, 
    keyval.reader, 
    keyval.writer, 
    profile, 
    combine, 
    vectorized,
    map.only) {
    if(profile != "off") activate.profiling(profile)
    combine.as.kv = 
      Curry(
        reduce.as.keyval,
        reduce = combine)
    kv = keyval.reader()
    force(keyval.writer)
    while(!is.null(kv)) { 
      out = as.keyval(map(keys(kv), values(kv)))
      if(length.keyval(out) > 0) {
        if(is.function(combine)) {
          if(!vectorized) {
            increment.counter("rmr", "reduce calls", rmr.length(unique(keys(kv))))
            out = apply.reduce(out, combine.as.kv)}
          else {
            increment.counter("rmr", "reduce calls", 1)
            out = combine.as.kv(keys(out), values(out))}}
        out = as.keyval(out)
        if(!map.only && is.null(keys(out)) && !is.null(values(out)))
          stop("Must specify key when using reduce or combine functions")
        keyval.writer(out)}
      kv = keyval.reader()}
    if(profile != "off") close.profiling(profile)
    invisible()}

reduce.loop = 
  function(reduce, vectorized, keyval.reader, keyval.writer, profile) {
    if(profile != "off") activate.profiling(profile)
    kv = keyval.reader()
    force(keyval.writer)
    straddler = NULL
    red.as.kv = 
      Curry(
        reduce.as.keyval, 
        reduce = reduce)
    while(!is.null(kv)){
      if(!is.null(straddler))
        kv = c.keyval(straddler, kv)
      last.key = rmr.slice(keys(kv), rmr.length(keys(kv)))
      last.key.mask = rmr.equal(keys(kv), last.key)
      straddler = slice.keyval(kv, last.key.mask)
      complete = slice.keyval(kv, !last.key.mask)
      if(length.keyval(complete) > 0) {
        if(!vectorized) {
          increment.counter("rmr", "reduce calls", rmr.length(unique(keys(complete))))
          out = apply.reduce(complete, red.as.kv)}
        else {
          increment.counter("rmr", "reduce calls", 1)
          out = as.keyval(reduce(keys(complete), values(complete)))}
        if(length.keyval(out) > 0)
          keyval.writer(out)}
      kv = keyval.reader()}
    if(!is.null(straddler)){
      if(!vectorized) {
        increment.counter("rmr", "reduce calls",  rmr.length(unique(keys((straddler)))))
        out = apply.reduce(straddler, red.as.kv)}
      else{
        increment.counter("rmr", "reduce calls", 1)
        out = as.keyval(reduce(keys(straddler), values(straddler)))}
      if(length.keyval(out) > 0)
        keyval.writer(out)}    
    if(profile != "off") close.profiling(profile)
    invisible()}

# the main function for the hadoop backend

rmr.stream = 
  function(
    in.folder, 
    out.folder, 
    map, 
    reduce, 
    vectorized.reduce,
    combine, 
    in.memory.combine,
    input.format, 
    output.format, 
    backend.parameters, 
    verbose, 
    debug) {
    pkg.opts = as.list(rmr.options.env)
    profile.nodes = pkg.opts$profile.nodes
    
    backend.parameters = 
      c(
        rmr.options("backend.parameters")$hadoop,
        input.format$backend.parameters$hadoop, 
        output.format$backend.parameters$hadoop,
        backend.parameters)
    ## prepare map and reduce executables
    work.dir = 
      if(.Platform$OS.type == "windows") "../../jars"
    else "."
    rmr.local.env = tempfile(pattern = "rmr-local-env")
    rmr.global.env = tempfile(pattern = "rmr-global-env")
        
    preamble = paste(sep = "", '
  sink(file = stderr())
  options(warn = 1) 
  options(error = quote({sink(stderr()); traceback(); stop()}))
  library(functional)
  invisible(
    if(is.null(formals(load)$verbose)) #recent R change
      load("',file.path(work.dir, basename(rmr.global.env)),'")
    else 
      load("',file.path(work.dir, basename(rmr.global.env)),'", verbose = TRUE))
(function(){  
  invisible(
    if(is.null(formals(load)$verbose)) #recent R change
      load("',file.path(work.dir, basename(rmr.local.env)),'")
    else 
      load("',file.path(work.dir, basename(rmr.local.env)),'", verbose = TRUE))
    lapply(
      libs, 
        function(l)
          if (!require(l, character.only = TRUE)) 
            warning(paste("can\'t load", l)))
    sink(NULL)
    input.reader = 
      function()
        rmr2:::make.keyval.reader(
          NULL,
          input.format)
    output.writer = 
      function()
        rmr2:::make.keyval.writer(
          NULL,
          output.format)
    default.reader = 
      function(input) 
        rmr2:::make.keyval.reader(
          NULL,
          default.input.format)
    default.writer = 
      function(output) 
        rmr2:::make.keyval.writer(
          NULL,
          default.output.format)
    has.combine = !(is.null(combine) || identical(combine, FALSE))
    has.reduce = !(is.null(reduce))

  ')  
    map.line = '  
  rmr2:::map.loop(
    map = map, 
    keyval.reader = input.reader(), 
    keyval.writer = 
      if(!has.reduce) 
        output.writer()
      else 
        default.writer(),
    profile = profile.nodes,
    combine = in.memory.combine,
    vectorized = vectorized.reduce, 
    map.only = !has.reduce)
'
  reduce.line = '  
  rmr2:::reduce.loop(
    reduce = reduce, 
    vectorized = vectorized.reduce,
    keyval.reader = 
      default.reader(), 
    keyval.writer = output.writer(),
    profile = profile.nodes)
'
    
  combine.line = '  
  rmr2:::reduce.loop(
    reduce = combine, 
    vectorized = vectorized.reduce,
    keyval.reader = default.reader(),
    keyval.writer = default.writer(), 
    profile = profile.nodes)    
'
    
    postamble = '
    })()'
    
    map.file = tempfile(pattern = "rmr-streaming-map")
    writeLines(c(preamble, map.line, postamble), con = map.file)
    reduce.file = tempfile(pattern = "rmr-streaming-reduce")
    writeLines(c(preamble, reduce.line, postamble), con = reduce.file)
    combine.file = tempfile(pattern = "rmr-streaming-combine")
    writeLines(c(preamble, combine.line, postamble), con = combine.file)
    
    ## set up the execution environment for map and reduce
    if (is.logical(combine) && combine) {
      combine = reduce}
    if (in.memory.combine) {
      in.memory.combine = {
        if(is.function(combine))
          combine 
        else 
          reduce}}
    save.env = 
      function(fun, fname, exclude) {
        envir = {
          if (is.function(fun)) environment(fun)
          else fun}
        all.names = ls(all.names = TRUE, envir = envir)
        obj.names = {
          if(is.null(exclude))
            all.names
          else
            setdiff(all.names, exclude)}
        save(list = obj.names, file = fname, envir = envir)
        fname}
    
    default.input.format = make.input.format("native")
    default.output.format = make.output.format("native")
    
    libs = sub("package:", "", grep("package", search(), value = TRUE))
    image.cmd.line = 
      paste(
        "-file",
        c(
          save.env(
            environment(), 
            rmr.local.env, 
            NULL),
          save.env(
            .GlobalEnv, 
            rmr.global.env, 
            pkg.opts$exclude.objects)),
        collapse = " ")
    ## prepare hadoop streaming command
    hadoop.command = hadoop.streaming()
    input = make.input.files(in.folder)
    output = paste.options(output = out.folder)
    input.format.opt = paste.options(inputformat = input.format$streaming.format)
    output.format.opt = paste.options(outputformat = output.format$streaming.format)
    stream.map.input = 
      if(input.format$mode == "binary") {
        paste.options(D = "stream.map.input=typedbytes")}
    else {''}
    stream.map.output = 
      if(is.null(reduce) && output.format$mode == "text") "" 
    else   paste.options(D = "stream.map.output=typedbytes")
    stream.reduce.input = paste.options(D = "stream.reduce.input=typedbytes")
    stream.reduce.output = 
      if(output.format$mode == "binary") paste.options(D = "stream.reduce.output=typedbytes")
    else ''
    stream.mapred.io = paste(stream.map.input,
                             stream.map.output,
                             stream.reduce.input,
                             stream.reduce.output)
    rscript = 'Rscript --vanilla'
    mapper = paste.options(
      mapper = 
        paste(
          rscript, 
          file.path(work.dir, basename(map.file))))
    m.fl = paste.options(file = map.file)
    if(!is.null(reduce) ) {
      reducer = 
        paste.options(
          reducer = 
            paste(
              rscript, 
              file.path(work.dir, basename(reduce.file))))
      r.fl = paste.options(file = reduce.file)}
    else {
      reducer = ""
      r.fl = "" }
    if(is.function(combine)) {
      combiner = 
        paste.options(
          combiner = 
            paste(
              rscript, 
              file.path(work.dir, basename(combine.file))))  
      c.fl = paste.options(file = combine.file)}
    else {
      combiner = ""
      c.fl = "" }
    if(is.null(reduce) && 
         !is.element("mapred.reduce.tasks",
                     sapply(strsplit(as.character(named.slice(backend.parameters, 'D')), '='), 
                            function(x)x[[1]])))
      backend.parameters = c(list(D = 'mapred.reduce.tasks=0'), backend.parameters)
    #debug.opts = "-mapdebug kdfkdfld -reducexdebug jfkdlfkja"
    
    final.command = 
      paste(
        hadoop.command, 
        stream.mapred.io,  
        if(is.null(backend.parameters)) ""
        else
          do.call(paste.options, backend.parameters), 
        input, 
        output, 
        mapper, 
        combiner,
        reducer, 
        image.cmd.line, 
        m.fl, 
        r.fl, 
        c.fl,
        input.format.opt, 
        output.format.opt, 
        "2>&1")
    if(verbose) {
      retval = system(final.command)
      if (retval != 0) stop("hadoop streaming failed with error code ", retval, "\n")}
    else {
      console.output = tryCatch(system(final.command, intern = TRUE), 
                                warning = function(e) stop(e)) 
      retval = 0}
    retval}


#mapreduce env
hadoop.cmd = 
  function() {
    hadoop_cmd = Sys.getenv("HADOOP_CMD")
    if( hadoop_cmd == "") {
      hadoop_home = Sys.getenv("HADOOP_HOME")
      if(hadoop_home == "") stop("Please make sure that the env. variable HADOOP_CMD is set")
      file.path(hadoop_home, "bin", "hadoop")}
    else hadoop_cmd}

hdfs.cmd = 
  function() {
    alternatives = 
      c(
        Sys.getenv("HDFS_CMD"),
        file.path(dirname(hadoop.cmd()), "hdfs"),
        file.path(Sys.getenv("HADOOP_HOME"), "bin", "hdfs"),
        hadoop.cmd())
    alternatives[min(which(sapply(alternatives, file.exists)))]}

hadoop.streaming = 
  function() {
    hadoop_streaming = Sys.getenv("HADOOP_STREAMING")
    if(hadoop_streaming == ""){
      hadoop_home = Sys.getenv("HADOOP_HOME")
      if(hadoop_home == "") stop("Please make sure that the env. variable HADOOP_STREAMING is set")
      stream.jar = list.files(path = file.path(hadoop_home, "contrib", "streaming"), pattern = "jar$", full.names = TRUE)
      paste(hadoop.cmd(), "jar", stream.jar)}
    else paste(hadoop.cmd(), "jar", hadoop_streaming)}

in.a.task = 
  function()
    !is.null(current.task())

nonempty.or.null =
  function(var) 
    function() {
      x = Sys.getenv(var)
      if(x == "") NULL else x} 
  
current.task = nonempty.or.null("mapred_task_id")
    
current.job = nonempty.or.null("mapred_job_id")

