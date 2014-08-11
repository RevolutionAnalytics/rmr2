# What's new in `rmr2` 3.2.0

## Features

* `mapreduce` now returns job id and application id (the latter on YARN only) as attributes to the return value when `verbose = F` #105.

## Bugs Fixed

* Crashes on disallowed keyval pairs are prevented by checks. Definition of allowable keyval pair has been slighly relaxed #121.
* Outer joins work again, albeit using two quick postprocessing jobs. Call it a workaround if you want, but it should add only modestly to the runtime of large joins #110.
* The node profiling setting was ineffective because of wrong write location. As a workaround, it now writes under "tmp". Will think harder about this or add a configuration later on if there is demand 51f877d be1ae16.
* Updated build procedure for hbase I/O format de890bd e9aebbf.
* Fixed a problem with dropped factor levels and data bloat also related to factors #128 #129. In the current solution factor order is not preserved. If this is a problem please create an issue.
* Load order for packages on the nodes was opposite of correct one 42b581a.
