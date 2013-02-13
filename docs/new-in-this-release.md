# What's new in 2.1.0

# Speed and more speed

Through a mix of behind-the-scenes changes and few new API extensions we have made rmr2 much faster. As a driving example we used a NLP taks, collocations, borrowed from the Cloudera blog. It's simple, we didn't make it up and we knew it hit some of the weaknesses of rmr2.0, specifically the case of small records and small groups in the reduce phase. The good news is that on that example we now are, in our tests, within striking distance of a native Java implementation, that is about 20% slower. The other side of the coin is that to reach that number we had to replace one line of R with 10 lines of C++ (in the example, the library already contains about 14% C++ code), but we think it's a good trade-off as compared to switching to C++ or Java altogether.

## More vectorization in the reduce phase
The `mapreduce` function has an additional option, `vectorized.reduce`, which, when set to `TRUE` make `rmr2` call the user-supplied reduce function on not one but many reduce groups. The reduce groups are always complete (all the records for one key are processed in the same call). This is particularly helpful when dealing with small records and small reduce groups, such that vectorizing only on the values is not sufficient to achieve the desired performance. Check out the [collocations example](../pkg/examples/collocations.R) to see it in action and `help(mapreduce)` has the gory details. 
## Combine in memory

## Helpful counters

## Hbase input

## Hadoop status and counters

## memory profiling

## Concatenate keyval pairs
