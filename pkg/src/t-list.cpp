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

#include "t-list.h"

SEXP t_list(SEXP _ll) {
  Rcpp::List ll(_ll);
  Rcpp::List first_col(Rcpp::wrap(ll[0]));  
  std::vector<std::vector<SEXP> > tll(first_col.size());
  for(int i = 0; i < ll.size(); i++) {
    Rcpp::List l(Rcpp::wrap(ll[i]));
    for(int j = 0; j < l.size(); j++)
      tll[j].push_back(l[j]);}
  std::vector<SEXP> results ;
  for(int i = 0; i < tll.size(); i++) {
     results.push_back(Rcpp::wrap(tll[i]));}    
  return(Rcpp::wrap(results));}