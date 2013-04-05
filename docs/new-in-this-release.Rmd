# What's new in 2.2.0 

We are starting to see user feedback driving feature development, and that is a good thing.

## Features

### Control read size

When reading binary formats, rmr2 reads a certain number of bytes, checks they contain at least `rmr.options("keyval.length")` records, then reads more if necessary or passes them to user code. The read size is adjusted so as to read the desired number of records in one shot, on average. You can now control the initial value of the read size if you need tighter control on how many records are processed in a map call if, say, are hitting memory or timeout limits. To achieve that, use `rmr.options(read.size = ideal.size.in.bytes)`. I can see reasons why people would want to control this and `keyval.length` on a job by job basis, but I am also wary of making the `mapreduce` interface more complicated. Let us know what you think.

### `rmr.str` returns its argument

The small utility call to safely print expression values and stack information to standard error now returns its argument on top of doing its regular job, making for less intrusive code changes. For instance if you have a map function `function(k,v) keyval(k, v/2)` and you wonder what's going on you can change that to `function(k,v) rmr.str(keyval(k, v/2))` keeping the semantics of the function intact while adding useful information to stderr logs.

### Input and output formats for equijoins

On a specific user request, IO formats for equijoins. They work just like IO formats in mapreduce.

### Options to RScript

On a specific user request, you can now change the command that is used to run the map and reduce programs. This defaults to "Rscript" and it's highly recommended you keep using Rscript unless you really know what you are doing. Some users though, while still using Rscript, wanted to provide options to control memory usage and other performance related options. Use `rmr.options(rscript.cmd = some.cmd.line)` to do this. Advanced use only. You can do a lot of damage with this.

### Configure HDFS tempdir

Again based on user feedback, you can now change the temp directory used on the distributed file system with `rmr.options(dfs.tempdir = some.dfs.path)`. Until now it just used the value returned by `tempdir()`, which is valid for the local file system but not necessarily for the distibuted one (still the default). Trying to find a common ground was a useless exercise in compromise: the two settings should be decoupled and they can be now.


## Bugs

* `mapreduce` doesn't drop records silently with malformed CSV files. Now it will stop with error and forward whatever error was produced by `read.table` to standard error.
* `c.keyval` doesn't fail on empty lists and other corner cases for `keyval` objects. Intended semantics is now [documented](https://github.com/RevolutionAnalytics/RHadoop/wiki/Keyval-types-and-combinations).
* the "reduce calls" counter is now accurate.


## Miscellanea

* Added single pass clustering [example](../pkg/examples/cluster.mr.R)
* For the git-initiated, we are now managing the dependency from hadoopy-hbase using subtree. You may see a large number of changes related to this (the diff with the previous version is some 15K lines) but most of them are related to this change.
