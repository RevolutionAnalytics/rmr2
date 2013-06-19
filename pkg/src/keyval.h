//Copyright 2013 Revolution Analytics
//   
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
  //Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS, 
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

#ifndef _RMR_KEYVAL_H
#define _RMR_KEYVAL_H

#include <Rcpp.h>

RcppExport SEXP null_purge(SEXP xx);
RcppExport SEXP lapply_as_character(SEXP xx);
RcppExport SEXP sapply_rmr_length(SEXP xx);
RcppExport SEXP sapply_length_keyval(SEXP kvs);
RcppExport SEXP sapply_null_keys(SEXP kvs);
RcppExport SEXP lapply_keys(SEXP kvs);
RcppExport SEXP lapply_values(SEXP kvs);
RcppExport SEXP are_factor(SEXP xx);
RcppExport SEXP are_data_frame(SEXP xx);
RcppExport SEXP are_matrix(SEXP xx);

#endif
