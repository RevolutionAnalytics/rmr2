from.dfs(
  mapreduce(
    input="blogposts", 
    input.format = 
      make.input.format(
        "hbase", 
        family.columns = 
          list(
            image= list("bodyimage"), 
            post = list("author", "body")), 
        key.deserialize = "raw", 
        cell.deserialize = "raw", 
        dense = T, 
        atomic = T), 
    backend.parameters = 
      list(
        hadoop = list(libjars = "../hadoopy_hbase.jar"))))

freebase.input.format = 
  make.input.format(
    "hbase", 
    family.columns = 
      list(
        name = "", 
        freebase = "types"), 
    key.deserialize = "raw", 
    cell.deserialize = "raw", 
    dense = F, 
    atomic = F)

from.dfs(
  mapreduce(
    input = "freebase",
    input.format = freebase.input.format,
    backend.parameters = 
      list(
        hadoop = list(libjars = hadoopy.hbase.jar)),
    map = function(k,v) keyval(k[1,], v[1,])))