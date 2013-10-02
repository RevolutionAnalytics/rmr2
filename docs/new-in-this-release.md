# What's new in 2.3.0

* Interchange format with hive and pig ("pig.hive")
* exclude large objects from being broadcasted to the cluster with `rmr.options(exclude.objects = ...)`
* environment setup now leave a trail in stderr, to more easily identify problems (R3.x only).
* better key normalization: in some instances absolutely identical R objects still serialized in a different way, which made Hadoop believe it was dealing with separate keys.
* speed improvements, specifically as related to data.frame as keys and values
* `dfs.mv` and `dfs.rmr` to move and delete files, backend independent.
