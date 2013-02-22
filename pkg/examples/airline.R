library(rmr2)
from.dfs(
  mapreduce(
    input = '../RHadoop.data/airline.1000',
    input.format = make.input.format("csv", sep = ","),
    map = function(., data) {
      # filter out non-numeric values (header and NA)
      filter = !is.na(data[,16])
      data = data[filter,]
      # emit composite key (airline|year|month) and delay
      keyval(
        data[,c(9,1,2)],
        data[,16, drop = FALSE])},
    reduce = function(k,delays) {
      keyval(k, mean(delays[,1]))}))