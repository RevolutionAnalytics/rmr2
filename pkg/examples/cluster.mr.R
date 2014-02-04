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
				function(., data.chunk) 
					keyval(1, list(subcluster(data.chunk))),
			combine = TRUE,
			reduce = 
				function(., clusterings)
					keyval(1, list(merge(clusterings))))

## @knitr cluster-subclara
subclara = 
	function(data, n.centers) {
		clust = 
			clara(
				data, 
				n.centers, 
				keep.data = FALSE)
		list(
			size = nrow(data),
			sample = data[clust$sample,],
			medoids = clust$medoids)}

## @knitr cluster-merge-clara
merge.clara =
	function(clusterings, n.centers){
		sizes = unlist(napply(clusterings, 'size'))
		total.size = sum(sizes)
		size.range = range(sizes)
		size.ratio = max(size.range)/min(size.range)
		resample = 
			function(x) 
				x$sample[
					sample(
						1:nrow(x$sample), 
						round(nrow(x$sample) * size.ratio),
						replace = TRUE)]
		clust = 
			subclara(
				do.call(
					rbind, 
					lapply(
						clusterings, 
						resample)),
				n.centers)
		clust$size = total.size
		clust}        

## @knitr cluster-clara
clara.mr = 
	function(data, n.centers)
		values(
			from.dfs(
				cluster.mr(
					data, 
					Curry(subclara, n.centers = n.centers), 
					Curry(merge.clara, n.centers = n.centers))))[[1]]