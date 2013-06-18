Knit document for some timing results:


```r
zz = rmr2:::interleave(1:10^6, 1:10^6)
con = file("/tmp/n-test", "wb")
system.time({
    rmr2:::typedbytes.writer(zz, con, TRUE)
})
```

```
##    user  system elapsed 
##   0.582   0.033   0.615
```

```r
close(con)
con = file("/tmp/tb-test", "wb")
system.time({
    rmr2:::typedbytes.writer(zz, con, FALSE)
})
```

```
##    user  system elapsed 
##   0.295   0.023   0.317
```

```r
close(con)
system.time({
    save(zz, file = "/tmp/save-test")
})
```

```
##    user  system elapsed 
##   2.365   0.022   2.390
```

```r
system.time({
    rmr2:::make.typedbytes.input.format()(file("/tmp/n-test", "rb"), 10^6)
})
```

```
##    user  system elapsed 
##   9.229   0.374   9.603
```

```r
system.time({
    rmr2:::make.typedbytes.input.format()(file("/tmp/tb-test", "rb"), 10^6)
})
```

```
## Warning: closing unused connection 4 (/tmp/n-test)
```

```
##    user  system elapsed 
##   7.387   0.328   7.716
```

```r
system.time({
    load(file = "/tmp/save-test")
})
```

```
## Warning: closing unused connection 4 (/tmp/tb-test)
```

```
##    user  system elapsed 
##   0.652   0.001   0.653
```

