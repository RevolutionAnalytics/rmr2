# What's new in `rmr2` 3.3.0

## Features

* `dfs.ls`: backend-independent listing of directories
* [`avro`](avro.apache.org/) input format

## Bugs Fixed

* `NA_character_` fully supported
* `methods` imported explicitly, apparently it's now necessary
* `equijoin` keeps key information in default reducer
* outer joins don't force users to return only lists from reducers
* `hdfs.cmd` heuristics are less brittle
