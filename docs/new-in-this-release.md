# What's new in 2.2.1

## Compatibility

* Compatible with HDP on Windows

## Speed improvements

* Faster when using list of small objects as keys
* Efficiency improvements in `scatter`

## Bug Fixes

* Always convert reduce return value to be a keyval pair
* Pass all additional arguments to `mapreduce` in `scatter`
* `rmr.sample` works even when the keys are `NULL` and the method is "Bernoulli"
* `dfs.size` now returns an accurate size even for large files
* #44, an incorrect behaviour on the local backend when keys are present in the map phase.
* #41, a failure in `equijoin` when performing an outer join and data to be joined is a vector instead of, say, a data frame.

## Miscellanea

* Added example of how to create Avro IO formats
 

