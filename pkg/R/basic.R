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

#string

qw = function(...) as.character(match.call())[-1]

#assignment

default = function(x, value) if(!is.null(x)) x else value

#functional

Make.single.arg = 
  function(f)
    function(x) do.call(f,x)

Make.multi.arg = 
  function(f)
    function(...) f(list(...))

Make.single.or.multi.arg = function(f, from = c("single", "multi")) {
  from = match.arg(from)
  if (from == "single") {
    f.single = f
    f.multi = Make.multi.arg(f)}
  else {
    f.single = Make.single.arg(f)
    f.multi = f}
  function(...) {
    args = list(...)
    if(length(args) == 1)
      f.single(args[[1]])
    else
      f.multi(...)}}
  

`%:%` = function(f,g) function(...) do.call(f, g(...))

all.predicate = function(x, P) all(sapply(x, P))

mapply.data.frame = function(data, FUN) do.call(function(...) mapply(FUN = FUN, ...), data)

#data structures

make.fast.list = function(l = list()) {
  l1 = l
  l2 = list(NULL)
  i = 1
  function(els = NULL){
    if(missing(els)) c(l1, l2[!sapply(l2, is.null)]) 
    else{
      if(i + length(els) - 1 > length(l2)) {
        l1 <<- c(l1, l2[!sapply(l2, is.null)])
        i <<- 1
        l2 <<- rep(list(NULL), length(l1) + length(els))}
      l2[i:(i + length(els) - 1)] <<- els
      i <<- i + length(els)}}}

named.slice = function(x, n) x[which(names(x) == n)]

#data frame manip

sane.c = 
  function(...) {
    if(all(are.factor(list(...))))
      unlist(list(...))
    else
      c(...)}

rbind.fill.fast = 
  function(...) {
    xx = list(...)
    cols = unique(unlist(lapply(xx, names)))
    ll =  
      lapply(
        cols, 
        function(n) 
          do.call(
            sane.c,
            lapply(
              xx, 
              function(x){
                if(is.null(x[[n]]))
                  rep(NA, nrow(x))
                else
                  x[[n]]})))
    names(ll) = cols
    do.call(
      data.frame, 
      c(
        lapply(
          ll, 
          function(x) 
            if (is.atomic(x)) x
                else I(x)), 
        stringsAsFactors = F))}



every.second = 
  function(pattern)
    function(x) {
      opt = options("warn")[[1]]
      options(warn = -1)
      y = x[pattern]
      options(warn = opt)
      y}

odd = every.second(c(T,F))
even = every.second(c(F,T))

interleave = 
  function(l1, l2) {
    l = list()
    l[2*(1:length(l1)) - 1] = l1
    l[2*(1:length(l1))] = l2
    l}
