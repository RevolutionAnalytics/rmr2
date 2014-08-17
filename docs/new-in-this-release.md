# What's new in `rmr2` 3.2.0

## Features

* `mapreduce` now returns job id and application id (the latter on YARN only) as attributes to the return value when `verbose = F` #105.

## Bugs Fixed

* Crashes on disallowed keyval pairs are prevented by checks. Definition of allowable keyval pair has been slighly relaxed #121.
* Outer joins work again, albeit using two quick postprocessing jobs. Call it a workaround if you want, but it should add only modestly to the runtime of large joins #110.
* The node profiling setting was ineffective because of wrong write location. As a workaround, it now writes under `/tmp`. Will think harder about this or add a configuration later on if there is demand 51f877d be1ae16.
* Updated build procedure for hbase I/O format de890bd e9aebbf.
* Fixed a problem with dropped factor levels and data bloat also related to factors #128 #129. In the current solution factor order is not preserved. If this is a problem please create an issue.
* Load order for packages on the nodes was opposite of correct one 42b581a.

## Configuration

In a break from tradition, we are changing some hadoop settings in our default configuration. Until now, we relied on hadoop to be configured properly, while with the argument `backend.parameters` to either `mapreduce` or `rmr.options` the user had the possibility of changing various settings on a per job or per session basis. This approach worked until recent tests on CDH5.0.2 using YARN. We found that the default settings didn't leave enough memory for R to terminate without error when running as part of a map or reduce task in some instances. Therefore we are adding this to the default configuration:


```r
"mapreduce.map.java.opts=-Xmx400M"    
"mapreduce.reduce.java.opts=-Xmx400M" 
"mapreduce.map.memory.mb=4096"    
"mapreduce.reduce.memory.mb=4096"    
```

These may or may not work for everybody. Your first line of defense is to override these with the above functions and let us know, via google groups or issue tracker.
