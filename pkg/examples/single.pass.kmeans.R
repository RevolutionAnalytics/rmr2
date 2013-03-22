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
  function(x, s) round(s*x/sum(x))

clusters.to.dataset =
  function(clusters, cluster.sizes, dataset.size) {
    centers = do.call(rbind, lapply(clusters, function(x) x$centers))
    centers[rep(1:nrow(centers), normalize(cluster.sizes, dataset.size)),]}


combine.clusters = 
  function(clusters, centers, dataset.size){
    cluster.sizes = do.call(c, lapply(clusters, function(x) x$size))
    points = clusters.to.dataset(clusters, cluster.sizes, dataset.size)
    km = kmeans(x=points, centers = min(centers, nrow(points) - 1))
    km$size = normalize(km$size, sum(cluster.sizes))
    km}

single.pass.kmeans = 
  function(data, centers, intermediate.data, intermediate.centers, splits)
    mapreduce(
      data,
      map = 
        function(k,v) 
          keyval(sample(1:splits, nrow(v), replace = T), v),
      combine = 
        function(k, vv) {
          if (is.matrix(vv)){
            keyval(1, list(kmeans(vv, min(intermediate.centers, nrow(vv) - 1))))}
          else
            keyval(1, list(combine.clusters(vv, intermediate.centers, intermediate.data)))},
      reduce = 
        function(k, vv) 
          keyval(1, list(combine.clusters(vv, centers, intermediate.data))))