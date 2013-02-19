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

input.1000 = mapreduce (input = to.dfs(1:1000), 
                        map = function(k, v) keyval(rnorm(1), v), 
                        reduce = to.reduce(identity))

input.10e6 = mapreduce (input = input.1000, 
                        map = function(k, v) lapply(1:1000, function(i) keyval(rnorm(1), v)), 
                        reduce = to.reduce(identity))

kmeans.input.10e6 = mapreduce(input.1000, 
                              map = function(k, v) keyval(rnorm(1), cbind(sample(0:2, recsize, replace = T) + 
                                                                           rnorm(recsize, sd = .1), 
                                                                         sample(0:3, recsize, replace = T) + 
                                                                           rnorm(recsize, sd = .1))))

kmeans.input.10e9 = mapreduce(input.10e6, 
                              map = function(k, v) keyval(rnorm(1), cbind(sample(0:2, recsize, replace = T) + 
                                                                           rnorm(recsize, sd = .1), 
                                                                         sample(0:3, recsize, replace = T) + 
                                                                           rnorm(recsize, sd = .1))))
