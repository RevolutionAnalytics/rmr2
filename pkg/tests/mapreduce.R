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

library(quickcheck)
library(rmr2)
library(rhdfs)
hdfs.init()

kv.cmp = rmr2:::kv.cmp


for (be in c("local", "hadoop")) {
  rmr.options(backend = be)
  
  ##from.dfs to.dfs
  
  ##native
  test(
    function(kv) 
      kv.cmp(
        kv, 
        from.dfs(to.dfs(kv))),
    generators = list(rmr2:::rkeyval),
    sample.size = 10)
  
  ## csv
  cg = quickcheck:::column.generators()
  cg = cg[-which(names(cg) == "rraw" | names(cg) == "rDate")]
  rdata.frame.simple = fun(rdata.frame(element = cg, ncol = 10))
  test(
    function(df) 
      kv.cmp(
        keyval(NULL, df),
        from.dfs(
          to.dfs(
            keyval(NULL, df), 
            format = "csv"), 
          format = "csv")),
    generators = list(rdata.frame.simple),
    sample.size = 10)
  
  #json
  fmt = "json"
  test(
    function(df) 
      kv.cmp(
        keyval(1, df), 
        from.dfs(
          to.dfs(
            keyval(1, df), 
            format = fmt), 
          format = make.input.format("json", key.class = "list", value.class = "data.frame"))), 
    generators = list(rdata.frame.simple),
    sample.size = 10)
  
  #sequence.typedbytes
  seq.tb.data.loss = 
    function(l)
      rapply(
        l,
        function(x) if(class(x) == "raw" || length(x) == 1) x else as.list(x),
        how = "replace")    
  
  fmt = "sequence.typedbytes"
  test(
    function(l) {
      l = c(0, l)
      kv.cmp(
        keyval(seq.tb.data.loss(list(1)), seq.tb.data.loss(l)),
        from.dfs(
          to.dfs(
            keyval(1, l), 
            format = fmt), 
          format = fmt))}, 
    generators = list(rlist),
    sample.size = 10)
  
  ##mapreduce
  
  ##simplest mapreduce, all default
  test(
    function(kv) {
      if(rmr2:::length.keyval(kv) == 0) TRUE
      else {
        kv1 = from.dfs(mapreduce(input = to.dfs(kv)))
        kv.cmp(kv, kv1)}},
    generators = list(rmr2:::rkeyval),
    sample.size = 10)
  
  ##put in a reduce for good measure
  test(
    function(kv) {
      if(rmr2:::length.keyval(kv) == 0) TRUE
      else {
        kv1 = 
          from.dfs(
            mapreduce(
              input = to.dfs(kv),
              reduce = to.reduce(identity)))
        kv.cmp(kv, kv1)}},
    generators = list(rmr2:::rkeyval),
    sample.size = 10)
  
  ## csv
  test(
    function(df)
      kv.cmp(
        keyval(NULL, df),
        from.dfs(
          mapreduce(
            to.dfs(
              keyval(NULL, df), 
              format = "csv"),
            input.format = "csv",
            output.format = "csv"),
          format = "csv")),
    generators = list(rdata.frame),
    sample.size = 10, stop = FALSE)
  
  #json
  # a more general test would be better for json but the subtleties of mapping R to to JSON are many
  fmt = "json"
  test(
    function(df) 
      kv.cmp(
        keyval(1, df),
        from.dfs(
          mapreduce(
            to.dfs(
              keyval(1, df), 
              format = fmt),
            input.format = make.input.format("json", key.class = "list", value.class = "data.frame"),
            output.format = fmt),
          format = make.input.format("json", key.class = "list", value.class = "data.frame"))),
    generators = list(rdata.frame),
    sample.size = 10)
  
  #sequence.typedbytes
  fmt = "sequence.typedbytes"
  test(
    function(l) {
      l = c(0, l)
      kv.cmp(
        keyval(seq.tb.data.loss(list(1)), seq.tb.data.loss(l)),
        from.dfs(
          mapreduce(
            to.dfs(
              keyval(1, l), 
              format = fmt), 
            input.format = fmt,
            output.format = fmt),
          format = fmt))}, 
    generators = list(rlist),
    sample.size = 10)
  
  #avro
  pathname = ravro::AVRO_TOOLS
  if(.Platform$OS.type == "windows") {
    subfname = strsplit(pathname, ":")
    if(length(subfname[[1]]) > 1)
    {
      pathname = subfname[[1]][2]
    }
    pathname = gsub("\"","",pathname)
    pathname = shortPathName(pathname)
    pathname = gsub("\\\\","/",pathname)}
  Sys.setenv(AVRO_LIBS = pathname)
  
  test(
    function(df) {
      if(rmr.options("backend") == "local") TRUE 
      else {
        names(df) = sub("\\.", "_", names(df))
        tf1 = tempfile()
        ravro:::write.avro(df, tf1)
        tf2 = "/tmp/rmr2.test.avro"
        on.exit(hdfs.rm(tf2))
        hdfs.put(tf1, tf2)
        kv.cmp(
          keyval(NULL, df),
          from.dfs(
            mapreduce(
              tf2, 
              input.format = 
                make.input.format(
                  format = "avro",
                  schema.file = tf1))))}},
    generators = list(rdata.frame),
    sample.size = 10)
  
  #equijoin
  stopifnot(
    all(
      apply(
        values(
          from.dfs(
            equijoin(
              left.input = to.dfs(keyval(1:10, (1:10)^2)), 
              right.input = to.dfs(keyval(1:10, (1:10)^3))))),
        1, 
        function(x) x[[1]]^(3/2) == x[[2]])))
}