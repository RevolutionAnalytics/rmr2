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

library(rmr2)
library(testthat)
library(ravro)
library(rhdfs)
hdfs.init()

rmr.options(backend = "hadoop")
test_avro_rmr <-
  function(df, test, write.args = list(),
           input.format.args = list(), map = function(k, v) v ) {
  if(rmr.options("backend") == "local") TRUE
  else {
    tf1 = tempfile(fileext = ".avro")
    expect_true(do.call(ravro:::write.avro, c(list(df, tf1), write.args)))
    tf2 = "/tmp/rmr2.test"
    tf3 = file.path(tf2, "data.avro")
    hdfs.mkdir(tf2)
    hdfs.put(tf1, tf3)
    df.input.format <- do.call(make.input.format,
                               c(list(
                                 format = "avro",
                                 schema.file = tf1),
                                 input.format.args))
    retdf <- values(
      from.dfs(
        mapreduce(
          tf2,
          map = map,
          input.format = df.input.format)))
    retdf <- retdf[row.names(df), ]
    attributes(retdf) <- attributes(retdf)[names(attributes(df))]
    test(retdf)
  }}

expect_equal_avro_rmr <- function(df, ...){
  row.names(df) <- row.names(df) # rmr2 uses row.names function which coerces to character
  # We need to make sure row.names for x is character or else this will always fail
  test_avro_rmr(df, function(x)expect_equal(x, df), ...)
}

expect_equivalent_avro_rmr <- function(df, ...)
  test_avro_rmr(df, function(x)expect_equivalent(x, df), ...)

d <- data.frame(x = 1,
                y = as.factor(1:10),
                fac = as.factor(sample(letters[1:3], 10, replace = TRUE)))
expect_equivalent_avro_rmr(d)


##########################################################################################

context("Basic Avro Read/Write")

### Handeling Factors
# Warnings: Factor levels converted to valid Avro names

test_that("Handling factors", {
  # Factors with non-"name" levels should still work
  d <- data.frame(x = 1,
                  y = as.factor(1:10),
                  fac = as.factor(sample(letters[1:3], 10, replace = TRUE)))
  expect_equivalent_avro_rmr(d) #order of levels can change
})


### Type Translation

test_that("type translation", {
  # All types should translate successfully
  L3 <- LETTERS[1:3]
  fac <- sample(L3, 10, replace = TRUE)
  d <- data.frame(x = 1, y = 1:10, fac = fac, b = rep(c(TRUE, FALSE), 5), c = rep(NA, 10),
                  stringsAsFactors = FALSE)
  expect_equal_avro_rmr(d)

  d <- data.frame(x = 1, y = 1:10, fac = factor(fac, levels = L3),
                  b = rep(c(TRUE, FALSE), 5), c = rep(NA, 10),
                  stringsAsFactors = FALSE)
  expect_equivalent_avro_rmr(d)
})

### write can handle missing values

test_that("write can handle missing values", {
  # NA column (entirely "null" in Avro)
  d <- data.frame(x = 1,
                  y = 1:10,
                  b = rep(c(TRUE, FALSE), 5),
                  c = rep(NA, 10),
                  stringsAsFactors = FALSE)
  expect_equal_avro_rmr(d)

  # NA row (entirely "null" in Avro)
  d <- rbind(data.frame(x = 1,
                        y = 1:10,
                        b = rep(c(TRUE, FALSE), 5)),
             rep(NA, 3))
  expect_equal_avro_rmr(d)
})

### NaNs throw warning

test_that("NaNs throw warning", {
  # NaN row (entirely "null" in Avro)
  d <- rbind(data.frame(x = 1,
                        y = 1:10,
                        b = rep(c(TRUE, FALSE), 5)),
             rep(NaN, 3))
  d[nrow(d), ] <- NA
  expect_equal_avro_rmr(d)

  # NaN row (entirely "null" in Avro)
  d <- cbind(data.frame(x = 1,
                        y = 1:10,
                        b = rep(c(TRUE, FALSE), 5)),
             c = rep(NaN, 10))
  d[, ncol(d)] <- as.numeric(NA) # coerce this type
  expect_equal_avro_rmr(d)
})

### write.avro throws error on infinite values
## Infinite values cannot be serialied to Avro (which is good, what test verifies)

test_that("write.avro throws error on infinite values", {
  d <- rbind(data.frame(x = 1, y = 1:10, b = rep(c(TRUE, FALSE), 5)), rep(NA, 3),
             c(Inf, 11, TRUE, NA))
  expect_that(expect_equal_avro_rmr(d), throws_error())

  d <- rbind(data.frame(x = 1, y = 1:10, b = rep(c(TRUE, FALSE), 5)), rep(NA, 3),
             c(-Inf, 11, TRUE, NA))
  expect_that(expect_equal_avro_rmr(d), throws_error())
})

############################ Read/Write mtcars and iris ###############################

context("Read/Write mtcars and iris")

### mtcars round trip

test_that("mtcars round trip", {
  expect_equal_avro_rmr(mtcars)
})


### factors level that are not Avro names read/write
## mttmp equivalent despite refactorization (good, warnings)
# 1: In (function (x, name = NULL, namespace = NULL, is.union = F, row.names = T, :
# Factor levels converted to valid Avro names: _3_ravro, _4_ravro, _5_ravro

test_that("factors level that are not Avro names read/write", {
  mttmp <- mtcars
  mttmp$gear_factor <- as.factor(mttmp$gear)
  expect_equal_avro_rmr(mttmp)
})


### iris round trip
## iris_avro not equivalent
# Length mismatch: comparison on first 3 components

test_that("iris round trip", {
  # This doesn't work, because rmr2::from.dfs uses rbind to combine the values together
  #expect_equal_avro_rmr(iris, write.args = list(unflatten = T), input.format.args = list(flatten = F))

  expect_equal_avro_rmr(iris, write.args = list(unflatten = T), input.format.args = list(flatten = T))
})

