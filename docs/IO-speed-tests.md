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
##   0.614   0.073   0.909
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
##   0.310   0.036   0.419
```

```r
close(con)
system.time({
    save(zz, file = "/tmp/save-test")
})
```

```
##    user  system elapsed 
##   2.448   0.055   2.585
```

```r
system.time({
    rmr2:::make.typedbytes.input.format()(file("/tmp/n-test", "rb"), 10^6)
})
```

```
##    user  system elapsed 
##   9.211   0.438  11.081
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
##   5.724   0.144   7.060
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
##   0.693   0.022   0.920
```

