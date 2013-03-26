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

#create a data set based on a set of clusters with a given size
clusters.to.dataset =
  function(clusters, dataset.size) {
    centers = 
      do.call(
        rbind, 
        lapply(clusters, function(x) x$centers))
    centers[
      rep(
        1:nrow(centers), 
        normalize(cluster.sizes(clusters), dataset.size)),
      ]}

combine.clusters = 
  function(clusters, centers, dataset.size){
    points = 
      clusters.to.dataset(clusters, dataset.size)
    km = 
      kmeans(
        x = points, 
        centers = min(centers, nrow(points) - 1))
    km$size = 
      normalize(
        km$size, sum(cluster.sizes(clusters)))
    km}

single.pass.kmeans = 
  function(
    data, 
    centers, 
    intermediate.dataset.size = 100 * centers, 
    intermediate.centers = centers)
    mapreduce(
      data,
      map = 
        function(., data) {
          keyval(
            1, 
            list(
              kmeans(
                x = data, 
                centers = min(
                  intermediate.centers, 
                  nrow(data) - 1))))},
      combine = 
        function(., clusters)
            keyval(
              1, 
              list(
                combine.clusters(
                  clusters, 
                  intermediate.centers, 
                  intermediate.dataset.size))),
      reduce = 
        function(., clusters) 
          keyval(
            1, 
            list(
              combine.clusters(
                clusters, 
                centers, 
                intermediate.dataset.size))))