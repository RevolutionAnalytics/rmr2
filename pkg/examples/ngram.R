# Copyright 2011 Revolution Analytics
#    
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Start cluster
# $WHIRR_HOME/bin/whirr  launch-cluster --config ~/Projects/Revolution/RHadoop/rmr/pkg/tools/whirr/hadoop-ec2-lzo.properties 2>&1 
# $WHIRR_HOME/bin/whirr  run-script --script ~/Projects/Revolution/RHadoop/rmr/pkg/tools/whirr/rmr-1.3.sh  --config ~/Projects/Revolution/RHadoop/rmr/pkg/tools/whirr/hadoop-ec2-lzo.properties
# $WHIRR_HOME/bin/whirr  run-script --script ~/Projects/Revolution/RHadoop/rmr/pkg/tools/whirr/lzo.sh  --config ~/Projects/Revolution/RHadoop/rmr/pkg/tools/whirr/hadoop-ec2-lzo.properties


## @knitr fake-data
fake.size = 2000000
writeLines(
  apply(
    cbind(
      sample(sapply(1:20000, function(x) substr(digest(x),start=1,stop=3)), fake.size, replace = TRUE), 
      sample(1800:1819, fake.size, replace = T),
      sample (1:200, fake.size, replace=T), 
      sample (1:200, fake.size, replace=T), 
      sample (1:200, fake.size, replace=T)),
    1, 
    function(x)paste(x, collapse = "\t")), 
  file("/tmp/fake-ngram-data", "w"))
  
source = "/tmp/fake-ngram-data"
# rmr.options(backend = "local")

#Timing for 12 + 1 node EC2 cluster m1.large instances
## @knitr distcp
# hadoop distcp s3n://$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY@datasets.elasticmapreduce/ngrams/books/20090715/eng-all/1gram/ hdfs:///user/antonio/
## @knitr scatter
## source = scatter("/user/antonio/1gram/data")
# 33 mins
## @knitr ngram.format
ngram.format = function(lines){
  data = 
    as.data.frame(
      do.call(rbind, strsplit(lines, "\t"))[,1:3],
      stringsAsFactors = FALSE)
  names(data) = c("ngram", "year", "count")
  data$year = as.integer(data$year)
  data$count = as.integer(data$count)
  data}

## @knitr filter.map 
filter.map = function(., lines) {
  ngram.data = ngram.format(lines)
  ngram.data[
    regexpr(
      "^[A-Za-z]+$", 
      ngram.data$ngram) > -1 & 
      ngram.data$year > 1800,]}
## @knitr end

# use 
# input.format = "text"
# on fake data

## @knitr filtered.data
source = "/user/antonio/1gram/data"
library(rmr2)
rmr.options(keyval.length = 10^5)
filtered.data = 
  mapreduce(input = source,
            map = filter.map)
## @knitr end
#20 mins, 
## @knitr sample-data
from.dfs(rmr.sample(filtered.data, method="any", n = 50))
## @knitr end
#5 mins

## @knitr totals.map
totals.map = 
  function(., ngram.data) {
    total = tapply(as.numeric(ngram.data$count), ngram.data$year, sum, na.rm = TRUE)
    keyval(names(total), as.vector(total))}

## @knitr totals.reduce
totals.reduce = 
  function(year, count) 
    keyval(year, sum(count, na.rm = TRUE))
  
## @knitr year.totals
year.totals.kv = 
  from.dfs(
    mapreduce(input = filtered.data,
              map = totals.map,
              reduce = totals.reduce,
              combine = TRUE))
## @knitr end
#9 mins

## @knitr year.totals-finish
year.totals = c()
year.totals[keys(year.totals.kv)] = values(year.totals.kv)
## @knitr outlier.map
library(bitops)
outlier.map = 
  function(., ngram.data) {
    k = ngram.data$year + cksum(ngram.data$ngram)%%100/100
    c.keyval(
      keyval(k, ngram.data),
      keyval(k + 1, ngram.data))}

## @knitr outlier.reduce
library(robustbase)
library(reshape2)
outlier.reduce =
  function(., ngram.data) {
    years = range(ngram.data$year)
    if(years[1] == years[2])
      NULL
    else {
      ngram.data = dcast(ngram.data, ngram ~ year, fill = 0)
      tryCatch({
        filter = 
          !adjOutlyingness(
            log(
              t(
                t(ngram.data[,2:3] + 1)/
                  as.vector(
                    year.totals[as.character(years)] + 1))),
            alpha.cutoff = .95)$nonOut
        as.character(ngram.data[filter,'ngram'])},
               error = function(e) NULL)}}
## @knitr end

# watch out the next doesn't seem to work beyond 10^5 ngrams
# problem is inefficient assignment, still investigating
## @knitr outlier.ngram
outlier.ngram =  
  unique(
    values(
      from.dfs(
        mapreduce(
          input = filtered.data,
          output = "/user/antonio/1gram/outlier-ngram",
          map = outlier.map,
          reduce = outlier.reduce))))

## @knitr end
# 8 hours

## @knitr plot.data
plot.data = 
  values(
    from.dfs(
      mapreduce(
        input = filtered.data,
        output = "/user/antonio/1gram/plot-data-ngram",
        map = 
          function(., ngram.data) 
            ngram.data[
              is.element(
                as.character(ngram.data$ngram), 
                outlier.ngram),])))
## @knitr end
# 5 mins

## @knitr plot.data.frame
plot.data = 
  melt(
    dcast(
      plot.data, ngram ~ year, fill = 0), 
    variable.name="year",
    value.name = "count")
plot.data$freq  = 
  (plot.data$count + 0.1)/
  year.totals[as.character(plot.data$year)]
plot.data = 
  plot.data[order(plot.data$ngram, plot.data$year),]
plot.data = 
  cbind(
    plot.data[-nrow(plot.data),],  
    plot.data[-1,])
plot.data = 
  plot.data[
    plot.data[,1] == plot.data[,5],
    c(1,2,4,8)]
names(plot.data) = 
  c("id","time","freq", "freq.prev")
plot.data$average = 
  sqrt(plot.data$freq*plot.data$freq.prev)
plot.data$ratio = 
  plot.data$freq/plot.data$freq.prev
plot.data$time = as.integer(as.character(plot.data$time))
## @knitr end

## cheat and get pre-computed data
load("../RHadoop.data/ngram.plot.data")
## throw away some data points
plot.data = plot.data[log(plot.data$average) > -10, ]
summary(plot.data)


## @knitr plot
suppressPackageStartupMessages(library(googleVis))
motion.chart = 
  gvisMotionChart(
    plot.data[,c("id","time","average","ratio")], 
    options = list(height = 1000, width = 2000))
plot(motion.chart)
## @knitr end
print(motion.chart, "chart")  