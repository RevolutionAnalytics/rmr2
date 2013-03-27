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

library(cluster)


cluster.mr = 
  function(data, subcluster, merge) 
    mapreduce(
      data,
      map = 
        function(., data.chunk) {
          rmr.str(data.chunk)
          keyval(1, list(subcluster(data.chunk)))},
      combine = T,
      reduce = 
        function(., clusters)
          keyval(1, list(merge(clusters))))


subclara = 
  function(n.clusters)
    function(data) {
      clust = clara(rmr.str(data), n.clusters, keep.data=F)
      rmr.str(clust)
      clust$sampled.data = data[clust$sample,]
      clust}

merge.clara =
  function(n.clusters)
    function(clusters)
      subclara(n.clusters)(
        do.call(rbind, lapply(clusters, function(cl)cl$sampled.data)))
        

clara.mr = function(data, n.clusters)
  values(
    from.dfs(
      cluster.mr(
        data, 
        subclara(n.clusters), 
        merge.clara(n.clusters))))[[1]]