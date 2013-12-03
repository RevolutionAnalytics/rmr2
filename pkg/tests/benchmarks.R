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
#timings from macbook pro i7 2011, standalone CDH4, one core, SDD

report = list()
for (be in c("local", "hadoop")) {
  rmr.options(backend = be)
## @knitr input
  input.size = {  
    if(rmr.options('backend') == "local") 
      10^4   
    else 
      10^6} 
## @knitr end
  report[[be]] =
    rbind(
      report[[be]], 
      write = 
        system.time({
          out = 
## @knitr write
  input = to.dfs(1:input.size)
## @knitr end  
        }))
  
  report[[be]] =
    rbind(
      report[[be]],
      read = 
        system.time({
          out = 
## @knitr read
  from.dfs(input)
## @knitr end        
        }))
  stopifnot(
    all(
      1:input.size == sort(values(out))))
  
  report[[be]] =
    rbind(
      report[[be]],
      pass.through = system.time({
        out = 
## @knitr pass-through
  mapreduce(
    input, 
    map = function(k, v) keyval(k, v))
## @knitr end        
      }))
  stopifnot(
    all(
      1:input.size == 
        sort(values(from.dfs(out)))))  
  
## @knitr predicate            
  predicate = 
    function(., v) v%%2 == 0
## @knitr end            
  report[[be]] =
    rbind(
      report[[be]],
      filter = system.time({
        out = 
## @knitr filter              
  mapreduce(
    input, 
    map = 
      function(k, v) {
        filter = predicate(k, v)
        keyval(k[filter], v[filter])})
## @knitr end                               
          }))
  stopifnot(
    all(
      2*(1:(input.size/2)) == 
        sort(values(from.dfs(out)))))
  
## @knitr select-input           
  input.select = 
    to.dfs(
      data.frame(
        a = rnorm(input.size),
        b = 1:input.size,
        c = sample(as.character(1:10),
                   input.size, 
                   replace=TRUE)))
## @knitr end             
  report[[be]] =
    rbind(
      report[[be]],
      select = system.time({
        out = 
## @knitr select                 
  mapreduce(input.select,
            map = function(., v) v$b)
## @knitr end                                   
      }))
  stopifnot(
    all(
      1:input.size == 
        sort(values(from.dfs(out)))))
  
## @knitr bigsum-input
  set.seed(0)
  big.sample = rnorm(input.size)
  input.bigsum = to.dfs(big.sample)
## @knitr end 
  report[[be]] =
    rbind(
      report[[be]],
      bigsum = system.time({
        out = 
## @knitr bigsum                
  mapreduce(
    input.bigsum, 
    map  = 
      function(., v) keyval(1, sum(v)), 
    reduce = 
      function(., v) keyval(1, sum(v)),
    combine = TRUE)
## @knitr end                                   
      }))
  stopifnot(
    isTRUE(
      all.equal(
        sum(values(from.dfs(out))), 
        sum(big.sample), 
        tolerance=.000001)))
## @knitr group-aggregate-input
  input.ga = 
    to.dfs(
      cbind(
        1:input.size,
        rnorm(input.size)))
## @knitr group-aggregate-functions
  group = function(x) x%%10
  aggregate = function(x) sum(x)
## @knitr end  
  report[[be]] =
    rbind(
      report[[be]],
      group.aggregate = system.time({
        out = 
## @knitr group-aggregate
  mapreduce(
    input.ga, 
      map = 
        function(k, v) 
          keyval(group(v[,1]), v[,2]),
      reduce = 
        function(k, vv) 
          keyval(k, aggregate(vv)),
      combine = TRUE)
## @knitr end        
      }))
  report[[be]] =
    rbind(
      report[[be]],
      df1 = system.time(from.dfs(to.dfs(keyval(data.frame(x = 1), data.frame(x =1:10^5))))))
  report[[be]] =
    rbind(
      report[[be]],
      df10 = system.time(from.dfs(to.dfs(keyval(data.frame(x = 1:10), data.frame(x =1:10^5))))))
  report[[be]] =
    rbind(
      report[[be]],
      df100 = system.time(from.dfs(to.dfs(keyval(data.frame(x = 1:100), data.frame(x =1:10^5))))))
  report[[be]] =
    rbind(
      report[[be]],
      df1000 = system.time(from.dfs(to.dfs(keyval(data.frame(x = 1:1000), data.frame(x =1:10^5))))))
  report[[be]] =
    rbind(
      report[[be]],
      df10E4 = system.time(from.dfs(to.dfs(keyval(data.frame(x = 1:10000), data.frame(x =1:10^5))))))
  report[[be]] =
    rbind(
      report[[be]],
      df10E5 = system.time(from.dfs(to.dfs(keyval(data.frame(x = 1:100000), data.frame(x =1:10^5))))))
}
print(report)

# $local
# user.self sys.self elapsed user.child sys.child
# write               0.011    0.002   0.013          0         0
# read                0.008    0.001   0.009          0         0
# pass.through        0.091    0.004   0.097          0         0
# filter              0.076    0.004   0.081          0         0
# select              0.458    0.037   0.495          0         0
# bigsum              0.061    0.008   0.069          0         0
# group.aggregate     0.180    0.012   0.192          0         0
# df1                 1.095    0.068   1.163          0         0
# df10                0.830    0.059   0.894          0         0
# df100               0.834    0.068   0.907          0         0
# df1000              0.946    0.056   1.005          0         0
# df10E4              0.982    0.061   1.046          0         0
# df10E5              0.739    0.052   0.797          0         0
# 
# $hadoop
# user.self sys.self elapsed user.child sys.child
# write               2.050    0.122  11.818     12.461     0.581
# read                1.426    0.160  15.561     13.955     4.538
# pass.through        1.023    0.035  14.553     17.662     1.921
# filter              0.249    0.027  12.548     16.816     1.763
# select              0.279    0.041  57.553     62.465     4.567
# bigsum              1.155    0.068  21.621     27.599     3.138
# group.aggregate     1.304    0.141  34.679     40.627     4.318
# df1                 0.754    0.150  10.359     16.151     1.520
# df10                0.606    0.068  10.209     16.126     1.521
# df100               0.702    0.080  10.346     16.138     1.503
# df1000              0.882    0.065  10.471     16.080     1.518
# df10E4              0.923    0.073  10.518     16.119     1.508
# df10E5              0.758    0.054  10.417     16.274     1.533