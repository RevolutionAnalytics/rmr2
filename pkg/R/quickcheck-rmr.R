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

##app-specific generators
rkeyval = function(keytdg = rdouble, valtdg = rany) keyval(keytdg(), valtdg())
rkeyvalsimple = function() keyval(runif(1), runif(1)) #we can do better than this

## generic sorting for normalized comparisons
gorder = function(...) UseMethod("gorder")
gorder.default = order
gorder.factor = function(x) order(as.character(x))
gorder.data.frame = 
  function(x) splat(gorder)(lapply(x, function(x) if(is.factor(x)) as.character(x) else if(is.list(x) || is.raw(x)) sapply(x, digest) else x))
gorder.matrix = function(x) gorder(as.data.frame(x))
gorder.raw = gorder.list = function(x) gorder(sapply(x, digest))

reorder = function(x, o) if(has.rows(x)) x[o, , drop = FALSE] else x[o]

gsort = function(x) reorder(x, gorder(x))

gsort.keyval = 
  function(kv) {
    k = keys(kv)
    v = values(kv)
    o = {
      if(is.null(k)) gorder(v)
      else 
        gorder(
          data.frame(
            if(is.list(k) && !is.data.frame(k)) sapply(k, digest) else k,
            if(is.list(v) && !is.data.frame(v)) sapply(v, digest) else v))}
    keyval(reorder(k, o), reorder(v, o))}

## keyval compare
kv.cmp = function(kv1, kv2) 
  isTRUE(all.equal(gsort.keyval(kv1), gsort.keyval(kv2), tolerance=1e-4, check.attributes=FALSE))
