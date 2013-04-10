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

library(mclust)

fast.mclust = 
  function(data)
    Mclust(
      data, 
      initialization = 
        list(
          subset = 
            sample(
              1:nrow(data), 
              size = min(100, nrow(data)))))
      

mclust.mr = 
  function(data, merge.dataset.size = 10000)
    mapreduce(
      data,
      map = 
        function(.,data)        
          keyval(1, list(fast.mclust(data)[c('n', 'modelName', 'parameters')])),
      reduce = 
        function(., models) {
          shrink = 
            merge.dataset.size/
            sum(sapply(models, function(m) m$n))
          model = 
            fast.mclust(
              do.call(
                rbind,
                lapply(
                  models, 
                  function(m) 
                    sim(
                      modelName = m$modelName,
                      parameters = m$parameters,
                      n = round(m$n/shrink))[,-1])))
          keyval(
            1, 
            list(
              list(
                n = round(model$n*shrink), 
                modelName = model$modelName, 
                parameters = model$parameters)))})