# What's new in 2.1.0

# Speed and more speed

Through a mix of behind-the-scenes changes and few new API extensions we have made rmr2 much faster. As a driving example we used a NLP task, collocations, borrowed from the [Cloudera blog](http://blog.cloudera.com/blog/2013/01/a-guide-to-python-frameworks-for-hadoop/). It's simple, we didn't make it up and we knew it hit some known weaknesses of rmr2.0, specifically the case of small records and small groups in the reduce phase. The good news is that on that example we now are, at least in our tests, within striking distance of a native Java implementation, that is about 20% slower. The other side of the coin is that to reach that number we had to write a tiny bit of problem-specific C++. See details below.

## More vectorization in the reduce phase
The `mapreduce` function has an additional option, `vectorized.reduce`, which makes `rmr2` call the user-supplied reduce function on many reduce groups at once instead of only one. The reduce groups are always complete (all the records for one key are processed in the same call). This is particularly helpful when dealing with small records and small reduce groups, such that vectorizing only on the values is not sufficient to achieve the desired performance. Check out the [collocations example](../pkg/examples/collocations.R) to see it in action and `help(mapreduce)` has the gory details.

## Combine in memory
Also a new option to `mapreduce`, `in.memory.combine` implements a super-early combine before data is serialized and written out. It works best when the cardinality of the key set is small or they happen to be sorted in the input, or same keys are contiguous for some other reason. It can be used with or without a regular combiner. 

## Helpful counters
The goal of vectorization is to have an R program get more work done in the C layer than in the R interpreter, glossing over issues of style and abstraction. For a program using `rmr2` that means processing all your data with few, efficient map and reduce calls that take care of relatively big chunks of data without lengthy loops. To provide guidance toward optimizations of this type, we added two Hadoop counters, "map calls" and "reduce calls" that you can monitor in the normal job tracker web interface together with all other counters. If the number of one or the other has the same order of magnitude as the number of records in input and typical records are small, the answer is more vectorization.

## Fast aggregation
To achieve `rmr2`'s full potential on the collocations example we needed to write a fast reducer, and we couldn't find a library function to help us. The task at hand was simply `sapply(x, sum)` where `x` is a big list of relatively small vectors. We had to replace that single line with 10 lines of [Rcpp](http://dirk.eddelbuettel.com/code/rcpp.html)-enhanced C++, but we think it's a good trade-off as compared to switching to C++ or Java altogether. We packed this function with the library as `vsum` for your convenience. It may be the first in a series of fast aggregators and we seek community input and contributions towards making writing fast reducers ever easier.

# Other friendly features

## HBase input
By popular demand, but still a little experimental, an input format that can read directly from HBase tables. Deserialization is configurable and a few common choices are provided. Please kick the tires and let us know how it's working for you.

## Hadoop status and counters
You can now set the task attempt status with `status` and increment counters with `increment.counter` to better monitor your computations. One additional use for these calls is to tell Hadoop that your map or reduce function is still alive. If your computation is very CPU bound and fails on time-outs for larger data sets but seems correct otherwise, updating the status or incrementing a counter every minute or so can solve the problem.

## Memory profiling
We added the possibility to memory profile the mapper and reducer, in addition to regular profiling.

## Transitioning code from previous releases
The new function `c.keyval` helps people porting programs from rmr 1.3. A list of non-vectorized key-val pairs in rmr < 2 can be converted into a rmr2-friendly vectorized key-value pair with the help of this function. Moreover, a warning is generated every time an implicit conversion to key-val pair is performed. `to.dfs` takes a key-value pair as first argument. If anything else is passed, `to.dfs(x)` is equivalent to `to.dfs(keyval(NULL, x))`. In versions prior to 2.0.0 `to.dfs` accepted lists of key-value pairs which now need to be converted to a single, vectorized key-value pair. The automatic conversion doesn't do that, hence the warning.

# Bugs
The implementation of typedbytes should now cover the complete spec in HADOOP-1722 (unfortunately the Hive team forked its own implementation extending the format, those extensions are not supported). Some interesting corner cases for keyval pairs have been thought out more carefully. The short of it is that mixing 0-length arguments with non-zero length is not supported, as in `keyval(1, integer(0))`. The exceptions are:

  1. `keyval(NULL, x)` with x of length at least 1, which roughly means keys are missing and is a common idiom 
  2. `keyval(NULL, NULL)` which is not allowed. 

Here "length" should be read as the value returned by `length` for lists and vectors and "number of rows" for data frames and matrices, according to the usual rmr convention.
