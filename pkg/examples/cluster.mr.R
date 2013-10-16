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



## @knitr cluster-napply
library(cluster)
napply = function(ll, a.name) lapply(ll, function(l) l[[a.name]])

## @knitr cluster-mr
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

## @knitr cluster-subclara
subclara = 
	function(n.clusters)
		function(data) {
			clust = 
				clara(
					rmr.str(data), 
					n.clusters, 
					keep.data=F)
			list(
				size = nrow(data),
				sample = data[clust$sample,],
				medoids = clust$medoids)}

## @knitr cluster-merge-clara
merge.clara =
	function(n.clusters)
		function(clusters){
			sizes = unlist(napply(clusters, 'size'))
			total.size = sum(sizes)
			size.range = range(sizes)
			size.ratio = max(size.range)/min(size.range)
			clust = 
				subclara(n.clusters)(
					do.call(
						rbind, 
						lapply(
							clusters, 
							function(x) 
								x$sample[
									sample(
										1:nrow(x$sample), 
										round(nrow(x$sample) * size.ratio),
										replace = TRUE),
									])))
			clust$size = total.size
			clust}        

## @knitr cluster-clara
clara.mr = 
	function(data, n.clusters)
		values(
			from.dfs(
				cluster.mr(
					data, 
					subclara(n.clusters), 
					merge.clara(n.clusters))))[[1]]