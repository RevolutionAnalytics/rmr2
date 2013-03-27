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

normalize = 
  function(x, to) round(to*x/sum(x))

cluster.sizes = 
  function(clusters)
    do.call(
      c, 
      lapply(clusters, function(x) x$size))

n.clusters =
  function(clusters)
    sapply(clusters, function(x) nrow(x$centers))

#create a data set based on a set of clusters with a given size
clusters.to.dataset =
  function(clusters) {
    centers = 
      do.call(
        rbind, 
        lapply(clusters, function(x) x$centers))
    cl.sz = cluster.sizes(clusters)
    centers[
      rep(
        1:nrow(centers), 
        normalize(cl.sz, min(sum(cl.sz, 10^5)))),] +
        rnorm(nrow(centers), sd = sum(apply(centers,2,sd))/10^4)}

combine.clusters = 
  function(clusters){
    points = 
      clusters.to.dataset(clusters)
    rmr.str(clusters)
    km = 
      kmeans(
        x = points, 
        centers = max(n.clusters(clusters)),
        nstart = 10)
    km$size = 
      normalize(
        km$size, sum(cluster.sizes(clusters)))
    km}

single.pass.kmeans = 
  function(
    data, 
    centers)
    mapreduce(
      data,
      map = 
        function(., data) {
          keyval(
            1, 
            list(
              kmeans(
                x = data, 
                centers = centers,
                nstart = 10)))},
      combine = 
        function(., clusters)
          keyval(
            1, 
            list(
              combine.clusters(clusters))),
      reduce = 
        function(., clusters) 
          keyval(
            1, 
            list(
              combine.clusters(clusters))))