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

#options

rmr.options.env = new.env(parent=emptyenv())

rmr.options.env$backend = "hadoop"
rmr.options.env$profile.nodes = "off"
rmr.options.env$dfs.tempdir = NULL # tempdir() here doesn't work!
rmr.options.env$exclude.objects = NULL
rmr.options.env$backend.parameters = list()

add.last =
  function(action) {
    old.Last = {
      if (exists(".Last")) 
        .Last
      else
        function() NULL}
    .Last <<-
      function() {
        action()
        .Last <<- old.Last
        .Last()}}

rmr.options = 
  function(
    backend = c("hadoop", "local"), 
    profile.nodes = c("off", "calls", "memory", "both"),
    dfs.tempdir = NULL,
    exclude.objects = NULL,
    backend.parameters = list()) {
    opt.assign = Curry(assign, envir = rmr.options.env)
    args = as.list(sys.call())[-1]
    is.named.arg = function(x) is.element(x, names(args))
    if(is.named.arg("backend"))
      opt.assign("backend", match.arg(backend))  
    if(is.named.arg("profile.nodes")) {
      if (is.logical(profile.nodes)) {
        profile.nodes = {
          if(profile.nodes)
            "calls"
          else
            "off"}}
      else
        opt.assign("profile.nodes", match.arg(profile.nodes))}
    if(is.named.arg("dfs.tempdir")) {
      if(!is.null(dfs.tempdir)) {
        if(!dfs.exists(dfs.tempdir)) {
          stopifnot(dfs.mkdir(dfs.tempdir))
          add.last(function() if(!in.a.task()) dfs.rmr(dfs.tempdir))}
        opt.assign("dfs.tempdir", dfs.tempdir)}}
    if(is.named.arg("backend.parameters"))
      opt.assign("backend.parameters", backend.parameters)
    if(is.named.arg("exclude.objects"))
      opt.assign("exclude.objects", exclude.objects)
    read.args = {
      if(is.null(names(args)))
        args
      else 
        named.slice(args, "")}
    if(length(read.args) > 0) {
      read.args = simplify2array(read.args)
      retval = as.list(rmr.options.env)[read.args]
      if (length(retval) == 1) retval[[1]] else retval}
    else NULL }

## map and reduce function generation

to.map = 
  function(fun1, fun2 = identity) {
    if (missing(fun2)) {
      function(k, v) fun1(keyval(k, v))}
    else {
      function(k, v) keyval(fun1(k), fun2(v))}}

to.reduce = to.map

## mapred combinators

compose.mapred = 
  function(mapred, map) 
    function(k, v) {
      out = mapred(k, v)
      if (is.null(out)) NULL
      else map(keys(out), values(out))}

union.mapred = 
  function(mr1, mr2) function(k, v) {
    c.keyval(mr1(k, v), mr2(k, v))}

#output cmp
cmp = 
  function(x, y) {
    kx  = keys(x)
    ky = keys(y)
    vx = values(x)
    vy = values(y)
    ox = order(sapply(kx, digest), sapply(vx, function(z){attr(z, "rmr.input") = NULL; digest(z)}))
    oy = order(sapply(ky, digest), sapply(vy, function(z){attr(z, "rmr.input") = NULL; digest(z)}))
    isTRUE(all.equal(kx[ox], ky[oy], check.attributes = FALSE)) &&
      isTRUE(all.equal(vx[ox], vy[oy], check.attributes = FALSE))}

# backend independent dfs section

is.hidden.file = 
  function(fname)
    regexpr("[\\._]", basename(fname)) == 1

part.list = 
  function(fname) {
    fname = to.dfs.path(fname)
    if(rmr.options('backend') == "local") fname
    else {
      if(dfs.is.dir(fname)) {
        du = hdfs.du(fname)
        du[!is.hidden.file(du[,2]),2]}
      else fname}}

dfs.exists = 
  function(fname) {
    fname = to.dfs.path(fname)
    if (rmr.options('backend') == 'hadoop') 
      hdfs.test(e = fname) 
    else file.exists(fname)}

dfs.rmr = 
  function(fname) {
    fname = to.dfs.path(fname)
    if(rmr.options('backend') == 'hadoop')
      hdfs.rmr(fname)
    else unlink(fname, recursive = TRUE)}

dfs.is.dir = 
  function(fname) { 
    fname = to.dfs.path(fname)
    if (rmr.options('backend') == 'hadoop') 
      hdfs.test(d = fname)
    else file.info(fname)['isdir']}

dfs.empty = 
  function(fname) 
    dfs.size(fname) == 0

dfs.size = 
  function(fname) {
    fname = to.dfs.path(fname)
    if(rmr.options('backend') == 'hadoop') {
      du = hdfs.du(fname)
      if(is.null(du)) 0 
      else
        sum(as.numeric(du[!is.hidden.file(du[,2]), 1]))}
    else file.info(fname)[1, 'size'] }

dfs.mv = 
  function(from, to) { 
    fname = to.dfs.path(from)
    if(rmr.options('backend') == 'hadoop') 
      hdfs.mv(fname, to)
    else 
      file.rename(fname, to)}

dfs.mkdir = 
  function(fname) { 
    fname = to.dfs.path(fname)
    if (rmr.options('backend') == 'hadoop') 
      hdfs.mkdir(fname)
    else
      dir.create(fname)}

dfs.du = 
  function(fname, pattern = NULL) {
    fname = to.dfs.path(fname)
    lst = 
      if (rmr.options('backend') == 'hadoop') {
        fi = as.data.frame(hdfs.ls(fname))
        data.frame(
          path = as.character(fi[, 8]), 
          size = as.numeric(levels(fi[, 5]))[as.integer(fi[, 5])], 
          stringsAsFactors = FALSE)} 
    else {
      fi = file.info(list.files(fname, full.names=TRUE))
      data.frame(
        path = rownames(fi), 
        size = fi$size,
        stringsAsFactors = FALSE)}
    if(is.null(pattern))
      lst
    else
      lst[grep(lst$path, pattern = pattern),]}

# dfs bridge

to.dfs.path = 
  function(input) {
    if (is.character(input)) {
      input}
    else {
      if(is.function(input)) {
        input()}}}

loadtb = 
  function(inf, outf)
    system(paste(hadoop.streaming(),  "loadtb", outf, "<", inf))


to.dfs = 
  function(
    kv, 
    output = dfs.tempfile(), 
    format = "native") {
    if(!is.keyval(kv))
      warning("Converting to.dfs argument to keyval with a NULL key")
    kv = as.keyval(kv)
    tmp = tempfile()
    dfs.output = to.dfs.path(output)
    if(is.character(format)) 
      format = make.output.format(format)
    keyval.writer = make.keyval.writer(tmp, format)
    keyval.writer(kv)
    eval(
      quote(
        if(length(con) == 1)
          close(con) 
        else lapply(con, close)), 
      envir=environment(keyval.writer))
    move.results = 
      function(action, action2 = action) {
        a = as.character(substitute(action))
        if(is.null(format$sections))
          eval(call(a, tmp, dfs.output))
        else {
          s = format$sections[[1]]
          eval(call(a, file.path(tmp, s), file.path(dfs.output, s)))
          a2 = as.character(substitute(action2))
          lapply(
            format$sections[-1], 
            function(s) 
              eval(call(a2, file.path(tmp, s), file.path(dfs.output, s))))}}
    if(rmr.options('backend') == 'hadoop') {
      if(format$mode == "binary") 
        move.results(loadtb, hdfs.put)
      else   #text
        move.results(hdfs.put) }
    else { #local
      if(file.exists(dfs.output))
        stop("Can't overwrite ", dfs.output)
      if(!is.null(format$sections))
        dfs.mkdir(dfs.output)
      move.results(file.copy) }
    file.remove(tmp)
    output}

from.dfs = function(input, format = "native") {
  read.file = function(fname) {
    keyval.reader = 
      make.keyval.reader(fname, format)
    retval = make.fast.list()
    kv = keyval.reader()
    while(!is.null(kv)) {
      retval(list(kv))
      kv = keyval.reader()}
    eval(
      quote(
        if(length(con) == 1)
          close(con) 
        else lapply(con, close)), 
      envir=environment(keyval.reader))
    c.keyval(retval())}
  
  dumptb = function(src, dest){
    lapply(src, function(x) system(paste(hadoop.streaming(), "dumptb", x, ">>", dest)))}
  
  getmerge = function(src, dest) {
    on.exit(unlink(tmp))
    tmp = tempfile()
    lapply(src, function(x) {
      hdfs.get(as.character(x), tmp)
      if(.Platform$OS.type == "windows") {
        cmd = paste('type', tmp, '>>' , dest)
        system(paste(Sys.getenv("COMSPEC"),"/c",cmd))}
      else {
        system(paste('cat', tmp, '>>' , dest))}
      unlink(tmp)})
    dest}
  
  fname = to.dfs.path(input)
  if(is.character(format)) format = make.input.format(format)
  if(rmr.options("backend") == "hadoop") {
    tmp = tmp.data = tempfile()
    if(!is.null(format$sections)){
      dir.create(tmp)
      tmp.data = file.path(tmp, format$sections[[1]])}
    if(format$mode == "binary") 
      dumptb(part.list(fname), tmp.data)
    else getmerge(part.list(fname), tmp.data)
    if(!is.null(format$sections))
      lapply(
        file.path(fname, format$sections[-1]), 
        function(fn) rmr2:::hdfs.get(get.section(fn), tmp))}
  else
    tmp = fname
  retval = read.file(tmp)
  if(rmr.options("backend") == "hadoop") unlink(tmp)
  retval}

# mapreduce

in.a.task = 
  function()
    !is.null(current.task())

current.task = 
  function() {
    id = Sys.getenv("mapred_task_id")
    if (id == "") NULL else id }

current.job = 
  function() {
    id = Sys.getenv("mapred_job_id")
    if (id == "") NULL else id }

rmr.normalize.path = 
  function(url.or.path) {
    if(.Platform$OS.type == "windows")
      url.or.path = gsub("\\\\","/", url.or.path)
    gsub(
      "/+", 
      "/", 
      paste(
        "/", 
        gsub(
          "part-[0-9]+$", 
          "", 
          parse_url(url.or.path)$path), 
        "/", 
        sep = ""))}

current.input = 
  function() {
    fname = Sys.getenv("map_input_file")
    if (fname == "") NULL 
    else rmr.normalize.path(fname)}

dfs.tempfile = function(pattern = "file", tmpdir = rmr.options("dfs.tempdir")) {
  if(is.null(tmpdir)) { 
    tmpdir = tempdir()
    rmr.options(dfs.tempdir = tmpdir)}
  fname  = tempfile(pattern, tmpdir)
  subfname = strsplit(fname, ":")
  if(length(subfname[[1]]) > 1) fname = subfname[[1]][2]
  namefun = function() {fname}
  reg.finalizer(environment(namefun), 
                function(e) {
                  fname = eval(expression(fname), envir = e)
                  if(!in.a.task() && dfs.exists(fname)) dfs.rmr(fname)
                },
                onexit = TRUE)
  namefun}

dfs.managed.file = function(call, managed.dir = rmr.options('managed.dir')) {
  file.path(managed.dir, digest(lapply(call, eval)))}

mapreduce = function(
  input, 
  output = NULL, 
  map = to.map(identity), 
  reduce = NULL, 
  vectorized.reduce = FALSE,
  combine = NULL, 
  in.memory.combine = FALSE,  
  input.format = "native", 
  output.format = "native", 
  backend.parameters = list(), 
  verbose = TRUE) {
  
  on.exit(expr = gc(), add = TRUE) #this is here to trigger cleanup of tempfiles
  if (is.null(output)) 
    output = dfs.tempfile()
  if(is.character(input.format)) input.format = make.input.format(input.format)
  if(is.character(output.format)) output.format = make.output.format(output.format)
  if(!missing(backend.parameters)) warning("backend.parameters is deprecated.")
  
  backend  =  rmr.options('backend')
  
  mr = switch(backend, 
              hadoop = rmr.stream, 
              local = mr.local, 
              stop("Unsupported backend: ", backend))
  
  mr(map = map, 
     reduce = reduce, 
     combine = combine, 
     vectorized.reduce,
     in.folder = if(is.list(input)) {lapply(input, to.dfs.path)} else to.dfs.path(input), 
     out.folder = to.dfs.path(output), 
     input.format = input.format, 
     output.format = output.format, 
     in.memory.combine = in.memory.combine,
     backend.parameters = backend.parameters[[backend]], 
     verbose = verbose)
  output
}


##special jobs

## a sort of relational join very useful in a variety of map reduce algorithms

## to.dfs(lapply(1:10, function(i) keyval(i, i^2)), "/tmp/reljoin.left")
## to.dfs(lapply(1:10, function(i) keyval(i, i^3)), "/tmp/reljoin.right")
## equijoin(left.input="/tmp/reljoin.left", right.input="/tmp/reljoin.right", output = "/tmp/reljoin.out")
## from.dfs("/tmp/reljoin.out")

reduce.default = 
  function(k, vl, vr) {
    if((is.list(vl) && !is.data.frame(vl)) || 
         (is.list(vr) && !is.data.frame(vr)))
      list(left = vl, right = vr)
    else{
      vl = as.data.frame(vl)
      vr = as.data.frame(vr)
      names(vl) = paste(names(vl), "l", sep = ".")
      names(vr) = paste(names(vr), "r", sep = ".")
      if(all(is.na(vl))) vr
      else {
        if(all(is.na(vr))) vl
        else
          merge(vl, vr, by = NULL)}}}

equijoin = 
  function(
    left.input = NULL, 
    right.input = NULL, 
    input = NULL, 
    output = NULL, 
    input.format = "native",
    output.format = "native",
    outer = c("", "left", "right", "full"), 
    map.left = to.map(identity), 
    map.right = to.map(identity), 
    reduce  = reduce.default) { 
    stopifnot(
      xor(
        !is.null(left.input), !is.null(input) &&
          (is.null(left.input) == is.null(right.input))))
    outer = match.arg(outer)
    left.outer = outer == "left"
    right.outer = outer == "right"
    full.outer = outer == "full"
    if (is.null(left.input)) {
      left.input = input}
    mark.side =
      function(kv, is.left) {
        kv = split.keyval(kv)
        keyval(keys(kv),
               lapply(values(kv),
                      function(v) {
                        list(val = v, is.left = is.left)}))}
    is.left.side = 
      function(left.input) {
        rmr.normalize.path(to.dfs.path(left.input)) ==
          current.input()}
    reduce.split =
      function(vv) {
        tapply(
          vv, 
          sapply(vv, function(v) v$is.left), 
          function(v) lapply(v, function(x)x$val), 
          simplify = FALSE)}
    pad.side =
      function(vv, outer) 
        if (length(vv) == 0 && (outer)) c(NA) else c.or.rbind(vv)
    map = 
      if (is.null(input)) {
        function(k, v) {
          ils = is.left.side(left.input)
          mark.side(if(ils) map.left(k, v) else map.right(k, v), ils)}}
    else {
      function(k, v) {
        c.keyval(mark.side(map.left(k, v), TRUE), 
                 mark.side(map.right(k, v), FALSE))}}
    eqj.reduce = 
      function(k, vv) {
        rs = reduce.split(vv)
        left.side = pad.side(rs$`TRUE`, right.outer || full.outer)
        right.side = pad.side(rs$`FALSE`, left.outer || full.outer)
        if(!is.null(left.side) && !is.null(right.side))
          reduce(k[[1]], left.side, right.side)}
    mapreduce(
      map = map, 
      reduce = eqj.reduce,
      input = c(left.input, right.input), 
      output = output,
      input.format = input.format,
      output.format = output.format,)}

status = function(value)
  cat(
    sprintf(
      "reporter:status:%s\n", 
      value), 
    file = stderr())

increment.counter =
  function(group, counter, increment = 1)
    cat(
      sprintf(
        "reporter:counter:%s\n", 
        paste(group, counter, increment, sep=",")), 
      file = stderr())
