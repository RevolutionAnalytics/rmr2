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

#has.rows
test(
  function(x = rmr2:::rrmr.data()) {
    is.null(nrow(x)) == !rmr2:::has.rows(x)})

#all.have rows TODO
#rmr.length TODO

#keyval, keys.values
test(
  function(k = rmr2:::rrmr.data(size = c(min = 1)), v = rmr2:::rrmr.data(size = ~rmr2:::rmr.length(k))){
    kv = keyval(k, v)
    identical(keys(kv), k) &&
      identical(values(kv), v)})

#NULL key case
test(
  function(v = rmr2:::rrmr.data(size = c(min = 1))){
    k = NULL
    kv = keyval(k, v)
    identical(keys(kv), k) &&
      identical(values(kv), v)})