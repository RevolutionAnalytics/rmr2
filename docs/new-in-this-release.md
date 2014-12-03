# What's new in `rmr2` 3.3.0

## Features

* `dfs.ls`: backend-independent listing of directories
* [`avro`](avro.apache.org/) input format

## Enhancements

* `equijoin` preserves keys in default reducer 
* default `backend.parameters` less intrusive, hopefully work for more people out of the box
* added startup message about important Hadoop settings pointing to a help entry


## Bugs Fixed

* `NA_character_` fully supported
* `methods` imported explicitly, apparently it's now necessary
* outer joins don't force users to return only lists from reducers
* `hdfs.cmd` heuristics are less brittle
