
```r
zz = rmr2:::interleave(1:10^6, 1:10^6)
con = file("/tmp/n-test", "wb")
system.time({
    rmr2:::typedbytes.writer(zz, con, TRUE)
})
```

```
##    user  system elapsed 
##   0.587   0.033   0.621
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
##   0.307   0.021   0.328
```

```r
close(con)
system.time({
    save(zz, file = "/tmp/save-test")
})
```

```
##    user  system elapsed 
##   2.431   0.026   2.457
```

```r
system.time({
    rmr2:::make.typedbytes.input.format()(file("/tmp/n-test", "rb"), 10^6)
})
```

```
##    user  system elapsed 
##   8.906   0.268   9.175
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
##   5.301   0.101   5.402
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
##   0.636   0.001   0.638
```

