# Copyright 2013 Revolution Analytics
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

## @knitr hbase-blogposts
from.dfs(
  mapreduce(
    input="blogposts", 
    input.format = 
      make.input.format(
        "hbase", 
        family.columns = 
          list(
            image= list("bodyimage"), 
            post = list("author", "body")), 
        key.deserialize = "raw", 
        cell.deserialize = "raw", 
        dense = TRUE, 
        atomic = TRUE)))

## @knitr hbase-freebase.input.format
freebase.input.format = 
  make.input.format(
    "hbase", 
    family.columns = 
      list(
        name = "", 
        freebase = "types"), 
    key.deserialize = "raw", 
    cell.deserialize = "raw", 
    dense = FALSE, 
    atomic = FALSE)

## @knitr hbase-freebase-mapreduce
from.dfs(
  mapreduce(
    input = "freebase",
    input.format = freebase.input.format,
    map = function(k,v) keyval(k[1,], v[1,])))
## @knitr end