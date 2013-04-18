# What's new in 2.2.0 

## Features

### `rmr.str` returns its argument

The small utility call to safely print expression values and stack information to standard error now returns its argument on top of doing its regular job, making for less intrusive code changes. For instance if you have a map function `function(k,v) keyval(k, v/2)` and you wonder what's going on you can change that to `function(k,v) rmr.str(keyval(k, v/2))` keeping the semantics of the function intact while adding useful information to stderr logs.

### Input and output formats for equijoins

They work just like IO formats in `mapreduce`.

### Configure HDFS tempdir

You can now change the temp directory used on the distributed file system with `rmr.options(dfs.tempdir = some.dfs.path)`. Until now it just used the value returned by `tempdir()`, which is valid for the local file system but not necessarily for the distibuted one (still the default). Trying to find a common ground was a useless exercise in compromise: the two settings should be decoupled and they can be now.

### More explicit error message
The goal is to have some information about the values that triggered the error and the stack at the time of the error. When the errors are triggered by R or underlying libraries we are using, that is not the case, for performance reasons and because it would a lot of work, but for rmr2-triggered errors we should see something informative in stderr. 

## Bugs

* `mapreduce` doesn't drop records silently with malformed CSV files. Now it will stop with error and forward whatever error was produced by `read.table` to standard error.
* `c.keyval` doesn't fail on empty lists and other corner cases for `keyval` objects. Intended semantics is now [documented](https://github.com/RevolutionAnalytics/RHadoop/wiki/Keyval-types-and-combinations).
* The "reduce calls" counter is now accurate.
* Fixed bug #18 that prevented outer equijoins from completing in many cases.
* Fixed a bug that prevented from specifying specific streaming options with the `backend.parameters` option to mapreduce (e.g. `cmdenv`)
* Fixed bug #20 that added an additional TAB at the end of the line with the CSV output format when the seprator wasn't TAB.


## Miscellanea

* Added single pass clustering [example](../pkg/examples/cluster.mr.R) based on [clara](cran.r-project.org/web/packages/cluster/cluster.pdf
), but it should also provide a template for other single pass clustering algorithms. The idea is to cluster the data in chunks as soon as you see the data and then merge the clusters.
* For the git-initiated, we are now managing the dependency from hadoopy-hbase using subtree. You may see a large number of changes related to this (the diff with the previous version is some 15K lines) but most of them are related to this change.
