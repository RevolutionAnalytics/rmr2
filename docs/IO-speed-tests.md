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
##   0.599   0.034   0.632
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
##   0.308   0.020   0.328
```

```r
close(con)
system.time({
    save(zz, file = "/tmp/save-test")
})
```

```
##    user  system elapsed 
##   2.353   0.022   2.374
```

```r
system.time({
    rmr2:::make.typedbytes.input.format()(file("/tmp/n-test", "rb"), 10^6)
})
```

```
##    user  system elapsed 
##   8.955   0.398   9.354
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
##   5.449   0.150   5.599
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
##   0.643   0.010   0.653
```

