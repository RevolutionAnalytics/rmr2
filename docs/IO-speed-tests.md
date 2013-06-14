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
##   0.584   0.039   0.631
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
##   0.299   0.021   0.321
```

```r
close(con)
system.time({
    save(zz, file = "/tmp/save-test")
})
```

```
##    user  system elapsed 
##   2.374   0.024   2.399
```

```r
system.time({
    rmr2:::make.typedbytes.input.format()(file("/tmp/n-test", "rb"), 10^6)
})
```

```
##    user  system elapsed 
##  18.564   0.428  18.994
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
##  16.465   0.431  16.900
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
##   0.743   0.002   0.746
```

