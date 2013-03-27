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