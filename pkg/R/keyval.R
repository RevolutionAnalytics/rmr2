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

has.rows = function(x) !is.null(nrow(x))
all.have.rows = Curry(all.predicate, P = has.rows)

rmr.length = 
  function(x) if(has.rows(x)) nrow(x) else length(x)

sapply.rmr.length = 
  function(xx)
    .Call("sapply_rmr_length", xx, PACKAGE = "rmr2")

rmr.equal = 
  function(xx, y) {
    if(rmr.length(xx) == 0) logical()
    else {
      if(is.atomic(xx) && !is.matrix(xx)) xx == y
      else {
        if(is.matrix(xx) || is.data.frame(xx))
          rowSums(xx == y[rep.int(1, rmr.length(xx)),]) == ncol(y)
        else
          sapply(xx, function(x) isTRUE(all.equal(list(x), y, check.attributes = FALSE)))}}}

length.keyval = 
  function(kv) 
    max(rmr.length(keys(kv)), 
        rmr.length(values(kv)))

keyval = 
  function(key, val = NULL) {
    if(missing(val)) keyval(key = NULL, val = key)
    else recycle.keyval(list(key = key, val = val))}

keys = function(kv) kv$key
values = function(kv) kv$val

is.keyval = 
  function(x) {
    is.list(x) && 
      length(x) == 2 && 
      !is.null(names(x)) && 
      all(names(x) == qw(key, val))}

as.keyval = 
  function(x) {
    if(is.keyval(x)) x
    else keyval(x)}

rmr.slice = 
  function(x, r) {
    if(has.rows(x))
      x[r, , drop = FALSE]
    else
      x[r]}

rmr.recycle = 
  function(this, upto) {
    if(is.null(this))
      NULL
    else {
      if(is.null(upto))
        this
      else {
        l.this = rmr.length(this)
        l.upto = if(is.null(upto)) 1 else rmr.length(upto)
        if(l.this == l.upto) this
        else {
          if(min(l.this,l.upto) == 0)
            stop("Can't recycle 0-length argument")
          else
            rmr.slice(
              rmr.slice(
                this,
                rep(1:rmr.length(this), ceiling(l.upto/l.this))),
              1:max(l.upto, l.this))}}}}

recycle.keyval =
  function(kv) {
    k = keys(kv)
    v = values(kv)
    if((rmr.length(k) == rmr.length(v)) ||
         is.null(k))
      kv
    else
      keyval(
        rmr.recycle(k, v),
        rmr.recycle(v, k))}

slice.keyval = 
  function(kv, r) {
    keyval(rmr.slice(keys(kv), r),
           rmr.slice(values(kv), r))}

purge.nulls = 
  function(x)
    .Call("null_purge", x, PACKAGE = "rmr2")

rbind.anything = 
  function(...) {
    tryCatch(
      rbind(...), 
      error = function(e) rbind.fill.fast(...))}

lapply.as.character =
  function(xx)
    .Call("lapply_as_character", xx, PACKAGE = "rmr2")

are.data.frame = 
  function(xx)
    .Call("are_data_frame", xx, PACKAGE = "rmr2")

are.matrix = 
  function(xx)
    .Call("are_matrix", xx, PACKAGE = "rmr2")

are.factor = 
  function(xx) 
    .Call("are_factor", xx, PACKAGE = "rmr2")

c.or.rbind = 
  Make.single.or.multi.arg(
    function(x) {
      if(is.null(x))
        NULL 
      else {
        x = purge.nulls(x)
        if(length(x) == 0) 
          NULL
        else { 
          if(any(are.data.frame(x))) {
            X = do.call(rbind.fill.fast, lapply(x, as.data.frame))
            rownames(X) = make.unique(unlist(sapply(x, rownames)))
            X}          
          else {
            if(any(are.matrix(x)))
              do.call(rbind,x)
            else {
              if(all(are.factor(x)))
                as.factor(do.call(c, lapply.as.character(x)))
              else
                do.call(c,x)}}}}})

c.or.rbind.rep =
  function(x, n) {
    ind = rep(1:length(x), n)
    rmr.slice(c.or.rbind(x), ind)}

sapply.length.keyval = 
  function(kvs)
    .Call("sapply_length_keyval", kvs, PACKAGE = "rmr2")

sapply.null.keys = 
  function(kvs)
    .Call("sapply_null_keys", kvs, PACKAGE = "rmr2")

lapply.values = 
  function(kvs)
    .Call("lapply_values", kvs, PACKAGE = "rmr2")

lapply.keys = 
  function(kvs)
    .Call("lapply_keys", kvs, PACKAGE = "rmr2")

c.keyval = 
  Make.single.or.multi.arg(
    function(kvs) {
      zero.length = as.logical(sapply.length.keyval(kvs) == 0)
      null.keys = as.logical(sapply.null.keys(kvs))
      if(!(all(null.keys | zero.length) || !any(null.keys & !zero.length))) {
        stop("can't mix NULL and not NULL key keyval pairs")}
      vv = lapply.values(kvs)
      kk = lapply.keys(kvs)
      keyval(c.or.rbind(kk), c.or.rbind(vv))})

split.data.frame.fast = 
  function(x, ind, drop) {
    y = 
      do.call(
        Curry(
          mapply, 
          function(...) 
            quickdf(list(...)), 
          SIMPLIFY=FALSE), 
        lapply(
          x, 
          Curry(split, f = ind, drop = drop)))
    rn = split(rownames(x), f = ind, drop = drop)
    mapply(function(a, na) {rownames(a) = na; a}, y, rn, SIMPLIFY = FALSE)}

split.data.frame.fastest = 
  function(x, ind, drop) 
    t.list(
      lapply(
        x, 
        Curry(split, f = ind, drop = drop)))

rmr.split = 
  function(x, ind, lossy) {
    spl = 
      switch(
        class(x),
        matrix = split.data.frame,
        data.frame = {
          if(lossy) split.data.frame.fastest
          else split.data.frame.fast},
        split)
    y = spl(x,ind, drop = TRUE)
    if (is.matrix(ind))
      ind = as.data.frame(ind)
    perm = c()
    perm[unlist(split(1:rmr.length(y), unique(ind)))] = 1:rmr.length(y)
    rmr.slice(y, perm)}

key.normalize= function(k) {
  k = rmr.slice(k, 1)
  if (is.data.frame(k) || is.matrix(k))
    rownames(k) = NULL
  if(!is.null(attributes(k)))
    attributes(k) = attributes(k)[sort(names(attributes(k)))]
  k}

split.keyval = function(kv, size, lossy = FALSE) {
  k = keys(kv)
  v = values(kv)
  if(is.null(v))
    keyval(NULL, NULL) 
  else {
    if(length.keyval(kv) == 0)
      keyval(list(), list())
    else {
      if(is.null(k)) {
        k =  ceiling((1:rmr.length(v))/(rmr.length(v) /(object.size(v)/size)))
        keyval(
          NULL,
          unname(rmr.split(v, k, lossy = lossy)))}
      else {
        k = keys(kv)
        v = values(kv)
        ind = {
          if(is.list(k) && !is.data.frame(k)) 
            cksum(k)
          else {
            if(is.matrix(k))
              as.data.frame(k)
            else {
              if(is.raw(k))
                as.integer(k)
              else
                k}}}
        x = k 
        if(has.rows(x)) 
          rownames(x) = NULL
        else
          names(x) = NULL
        x = unique(x)
        x = 
          switch(
            class(x),
            list = x,
            data.frame = if(lossy) t.list(x) else rmr.split(x, x , F),
            matrix = if(lossy) t.list(as.data.frame(x)) else rmr.split(x, as.data.frame(x), F),
            as.list(x))
        keyval(x, unname(rmr.split(v, ind, lossy = lossy)))}}}}

unsplit.keyval = function(kv) {
  c.keyval(mapply(keyval, keys(kv), values(kv), SIMPLIFY = FALSE))}

reduce.keyval = 
  function(
    kv, 
    FUN, 
    split.size = 
      stop("Must specify key when using keyval in map and combine functions")) {
    k = keys(kv)
    kvs = split.keyval(kv, split.size)
    if(is.null(k)) 
      lapply(values(kvs), function(v) FUN(NULL,v))
    else
      mapply(FUN, keys(kvs), values(kvs), SIMPLIFY = FALSE)}
