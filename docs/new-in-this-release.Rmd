# What's new in 2.1.0

# Speed and more speed

Through a mix of behind-the-scenes changes and few new API extensions we have made rmr2 much faster. As a driving example we used a NLP taks, collocations, borrowed from the Cloudera blog. It's simple, we didn't make it up and we knew it hit some of the weaknesses of rmr2.0, specifically the case of small records and small groups in the reduce phase. The good news is that on that example we now are, in our tests, within striking distance of a native Java implementation, that is about 20% slower. The other side of the coin is that to reach that number we had to replace one line of R with 10 lines of C++ (in the example, the library already contains about 14% C++ code), but we think it's a good trade-off as compared to switching to C++ or Java altogether. We packed this little piece of C code in the library as the function `vsum` for your convenience, and it's just a fast version of `sapply(x, sum)` when `x` is a big list of relatively small vectors.

## More vectorization in the reduce phase
The `mapreduce` function has an additional option, `vectorized.reduce`, which, when set to `TRUE`, makes `rmr2` call the user-supplied reduce function on not one but many reduce groups. The reduce groups are always complete (all the records for one key are processed in the same call). This is particularly helpful when dealing with small records and small reduce groups, such that vectorizing only on the values is not sufficient to achieve the desired performance. Check out the [collocations example](../pkg/examples/collocations.R) to see it in action and `help(mapreduce)` has the gory details.

## Combine in memory
Also a new option to `mapreduce`, `in.memory.combine` implements a super-early combine before data is serialized for the shuffle phase. It works best when the cardinality of the key set is small or they happen to be sorted in the input, or same keys are contiguos for some other reason. It can be used with or without a regular combiner. 

## Helpful counters
The goal of vectorization is to have an R program get more work done in the C layer than in the R interpreter. For a program using `rmr2` that means processing all your data with few, efficient map and reduce calls that take care of relatively big chunks of data. As an aid to optimizations of this type, we added two Hadoop counters, "map calls" and "reduce calls" that you can monitor inthe normal jobtracker web interface together with all other counters. If the number of one or the other is the same order of magnitude as the number of records in input and typical records are small, the answer is more vectorization.

# Other friendly features

## Hbase input
By popular demand, but still a little experimental, an input format that can read directly from hbase tables. Deserialization is configurable and a few common choices are provided. Please kick the tires and let us know how it's working for you or otherwise.

## Hadoop status and counters
You can now set the task attempt status with `status` and increment counters with `increment.counter`. One use for these calls is to tell Hadoop that your map or reduce function is still alive. If your computation is very CPU bound and fails on time-outs for larger data sets but seems correct otherwise, updating the status or incrementing a counter every minute or so can solve the problem.

## Memory profiling
We added the possiblity to memory profile the mapper and reducer, in addition to regular profiling.

## Concatenate keyval pairs
The new function `c.keyval` is a convenience for people porting programs from rmr 1.3. A list of non-vectorized key-val pairs in rmr < 2 can be converted into a rmr2-ready vectorized key-value pair with the help of this function.
