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

library(rmr2)
library(testthat)
library(ravro)

rmr.options(backend = "hadoop")
test_avro_rmr <-   function(df,test,...) {
  if(rmr.options("backend") == "local") TRUE 
  else {
    tf1 = tempfile(fileext=".avro")
    expect_true(ravro:::write.avro(df, tf1,...))
    tf2 = rmr2:::dfs.tempfile()
    tf3 = paste(tf2(), "data.avro", sep = "/")
    rmr2:::hdfs.mkdir(tf2())
    rmr2:::hdfs.put(tf1, tf3)
    
    retdf <- values(
      from.dfs(
        mapreduce(
          tf2(), 
          map = function(k,v) rmr.str(v),
          input.format = 
            make.input.format(
              format = "avro",
              schema.file = tf1))))
    attributes(retdf) <- attributes(retdf)[names(attributes(df))]
    test(retdf)
  }}

expect_equal_avro_rmr <- function(df,...)
  test_avro_rmr(df,function(x)expect_equal(x,df),...)
expect_equivalent_avro_rmr <- function(df,...)
  test_avro_rmr(df,function(x)expect_equivalent(x,df),...)

expect_equivalent_avro_rmr(
  read.avro(system.file("data/yield1k.avro",package="ravro")),
  unflatten=T)

d <- data.frame(x = 1, 
                y = as.factor(1:10), 
                fac = as.factor(sample(letters[1:3], 10, replace = TRUE)))
expect_equivalent_avro_rmr(d)
