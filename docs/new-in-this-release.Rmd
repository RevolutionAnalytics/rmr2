# What's new in 3.1.0

## Features

* The way the temporary directory to be used by `rmr` is specified changed. For local files, it uses R's own `tempfile` and `tempdir`, for HDFS, the rmr option `hdfs.tempfile`, defaulting to `"/tmp"`. The option `dfs.tempfile`, that aimed at covering both cases, has been removed.
* Added start and stop row filter and regex filter for the hbase input format

## Bugs Fixed

* Hbase format build on Debian, courtesy @khharut
* Extreme efficiency problem when the key is a data frame with many columns.
* Incomplete reduce groups when factors used as keys.
* Reduce error when key is a single col data frame.
* Crash when calling `keyval(1, NULL)` -- now returns `keyval(NULL, NULL)`
* Compatibility issues with current Hadoop distro, see [[compatbility]].
* Map and reduce scripts not executed in  a vanilla session could load unwanted data or configuration from the work directory (most likely with hadoop in standalone mode).

## Other improvements
* Harmless warning in hdfs dfs syntax check removed
* One speed up in the serialization code, should affect many use cases.
* Eliminated all warnings with clang compile.
