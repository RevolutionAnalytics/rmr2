//Copyright 2011 Revolution Analytics
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

#include "typed-bytes.h"
#include <algorithm>
#include <deque>
#include <iostream>
#include <math.h>
#include <stdint.h>
#include <string>
#include <sstream>

using namespace Rcpp;
using std::vector;
using std::deque;
using std::string;
using std::cerr;
using std::endl;
using std::ostringstream;

enum type_code {
  TB_BYTES = 0,
  TB_BYTE = 1,
  TB_BOOLEAN = 2,
  TB_INTEGER = 3,
  TB_LONG = 4,
  TB_FLOAT = 5,
  TB_DOUBLE = 6, 
  TB_STRING = 7,
  TB_VECTOR = 8,
  TB_LIST = 9,
  TB_MAP = 10,
  R_NATIVE = 144,
  R_VECTOR = 145,
  R_CHAR_VECTOR = 146,
  R_WITH_ATTRIBUTES = 147,
  R_NULL = 148,
  RMR_TEMPLATE = 149};

typedef deque<unsigned char> raw;

template<typename T> 
string to_string(T x) {
  ostringstream ss;
  ss << x;
  return ss.str();}

void safe_stop(string message) {
  cerr << message << endl;
  exit(-1);}
    
template<typename T>
void stop_unimplemented(string what){
  safe_stop(what + " unimplemented for " + typeid(T).name());}
    
class ReadPastEnd {
public:
  string type_code;
  int start;
  ReadPastEnd(string _type_code, int _start){
    type_code = _type_code;
    start = _start;}};

class UnsupportedType{
public:
  unsigned char type_code;
  UnsupportedType(unsigned char _type_code){
    type_code = _type_code;}};

class NegativeLength {
public:
  NegativeLength(){}};
  
template <typename T> 
int nbytes(){
  stop_unimplemented<T>("nbytes");
  return T();} //silence compiler

//nbytes provides the size of a typedbytes type corresponding to a C type
template<>
int nbytes<int>(){return 4;}

template<>
int nbytes<long>(){return 8;}

template<>
int nbytes<double>(){return 8;}

template<>
int nbytes<bool>(){return 1;}

template<>
int nbytes<unsigned char>(){return 1;}

template<>
int nbytes<char>(){return 1;}

template <typename T>
void check_length(const raw & data, unsigned int start, unsigned int length = nbytes<T>()) {
  if(data.size() < start + length) {
      throw ReadPastEnd(typeid(T).name(), start);}}

template <typename T> 
T unserialize_integer(const raw & data, unsigned int & start) {
  check_length<T>(data, start);
  int retval = 0;
  for (int i = 0; i < nbytes<T>(); i ++) {
    retval = retval + ((data[start + i] & 255) << (8*((nbytes<T>()-1) - i)));}
  start = start + nbytes<T>();
  return retval;}

template <typename T>
T unserialize_numeric(const raw & data, unsigned int & start){
  stop_unimplemented<T>("unserialize_numeric called");
  return T();} //silence compiler

template<>
double unserialize_numeric<double>(const raw & data, unsigned int & start) {
  union udouble {
    double d;
    uint64_t u;} ud;
  check_length<double>(data, start);
  uint64_t retval = 0;
  for(int i = 0; i < nbytes<double>(); i++) {
    retval = retval + (((uint64_t) data[start + i] & 255) << (8*(7 - i)));}
  start = start + nbytes<double>(); 
  ud.u = retval;
  return ud.d;} 
 
template <typename T>
T unserialize_scalar(const raw & data, unsigned int & start){
  if(nbytes<T>() > 1) {
    stop_unimplemented<T>("Multibyte unserialize_scalar ");
    return T();} //silence compiler
  check_length<T>(data, start);
  start = start + nbytes<T>();
  return (T)data[start - nbytes<T>()];}

template<>
int unserialize_scalar<int>(const raw & data, unsigned int & start){
  return unserialize_integer<int>(data, start);}
  
template<>
long unserialize_scalar<long>(const raw & data, unsigned int & start){
  return unserialize_integer<long>(data, start);}
  
template<>
double unserialize_scalar<double>(const raw & data, unsigned int & start){
  return unserialize_numeric<double>(data, start);}
  
template<>
float unserialize_scalar<float>(const raw & data, unsigned int & start){
  return unserialize_numeric<float>(data, start);}
    
int get_length(const raw & data, unsigned int & start) {
  int len = unserialize_scalar<int>(data, start);
  if(len < 0) {
    throw NegativeLength();}
  return len;}
  
int get_type(const raw & data, unsigned int & start) {
  return (int)unserialize_scalar<unsigned char>(data, start);}
  
template <typename T>
vector<T> unserialize_vector(const raw & data, unsigned int & start, int raw_length) {
  int length = raw_length/nbytes<T>();
  vector<T> vec(length);
  for(int i = 0; i < length; i++) {
    vec[i] = unserialize_scalar<T>(data, start);}
  return vec;}
  
template <>
vector<string> unserialize_vector<string>(const raw & data, unsigned int & start, int raw_length) {
  int v_length = get_length(data, start);
  vector<string> retval(v_length);
  for(int i = 0; i < v_length; i++) {
    get_type(data, start); //we know it's 07 already
    int str_length = get_length(data, start);
    vector<char> tmp_vec_char = unserialize_vector<char>(data, start, str_length);
    string tmp_string(tmp_vec_char.begin(), tmp_vec_char.end());
    retval[i] = tmp_string;}
  return retval;}
      
RObject unserialize(const raw & data, unsigned int & start, int type_code = 255);

List unserialize_list(const raw & data, unsigned int & start) {
  int length = get_length(data, start);
  List list(length);
  for(int i = 0; i < length; i++) {
    list[i] = unserialize(data, start);}
  return list;}
  
List unserialize_255_terminated_list(const raw & data, unsigned int & start) {
  vector<RObject> vec;
  int type_code = get_type(data, start);
  while(type_code != 255){
    vec.push_back(unserialize(data, start, type_code));
    type_code = get_type(data, start);}
  return List(vec.begin(), vec.end());}
      
List unserialize_map(const raw & data, unsigned int & start) {
  int length = get_length(data, start);
    List keys(length);
    List values(length);
    for(int i = 0; i < length; i++) {
      keys[i] = unserialize(data, start);
      values[i] = unserialize(data, start);}
    return  
        List::create(
          Named("key") = keys,
          Named("val") = values);}
       
RObject unserialize_native(const raw & data, unsigned int & start) {
  int length = get_length(data, start);
  check_length<RObject>(data, start, length);
  cerr << "Calling r_unserialize" << endl;
  Function r_unserialize("unserialize");
  raw tmp(data.begin() + start, data.begin() + start + length);
  start = start + length;
  return r_unserialize(tmp);}

RObject unserialize(const raw & data, unsigned int & start, int type_code){
  RObject new_object;
  if(type_code == 255) {
    type_code = get_type(data, start);}
  switch(type_code) {
    case TB_BYTES: { 
      int length = get_length(data, start);
      new_object = wrap(unserialize_vector<unsigned char>(data, start, length));}
      break;
    case TB_BYTE:
      new_object = wrap(unserialize_scalar<unsigned char>(data, start));
      break;
    case TB_BOOLEAN: //boolean
      new_object = wrap(unserialize_scalar<bool>(data, start));
      break;
    case TB_INTEGER: 
      new_object = wrap(unserialize_scalar<int>(data, start));
      break;
    case TB_LONG:
      new_object = wrap(unserialize_scalar<long>(data, start));
      break;
    case TB_FLOAT:
      new_object = wrap(unserialize_scalar<float>(data, start));
      break;
    case TB_DOUBLE:
      new_object = wrap(unserialize_scalar<double>(data, start));
      break;
    case TB_STRING: {
      int length = get_length(data, start);
      vector<char> vec_tmp = unserialize_vector<char>(data, start, length);
      new_object =  wrap(string(vec_tmp.begin(), vec_tmp.end()));}
      break;
    case TB_VECTOR:
      new_object = unserialize_list(data, start);
      break;
    case TB_LIST: 
      new_object = unserialize_255_terminated_list(data, start);
      break;
    case TB_MAP: 
      new_object = unserialize_map(data, start);
      break;
    case R_NULL:{
      new_object = R_NilValue;
      get_length(data, start);} 
      break;
    case R_NATIVE: 
      new_object = unserialize_native(data, start);
      break;
    case R_WITH_ATTRIBUTES: {
      get_length(data, start);
      new_object = unserialize(data, start, 255);
      CharacterVector names(unserialize(data, start, 255));
      List attributes(unserialize(data, start, 255));
      for(int i = 0; i < names.size(); i++) {
        char * c = names[i]; //workaround Rcpp bug now fixed, remove if assuming 0.10.2 and higher
        string s(c); 
        new_object.attr(s) = attributes[i];}}
      break;
    case R_VECTOR: {
      int raw_length = get_length(data, start);
      int vec_type_code  = get_type(data, start);
      raw_length = raw_length - 1;
      switch(vec_type_code) {
        case TB_BYTE:
          new_object = wrap(unserialize_vector<unsigned char>(data, start, raw_length));
        break;
        case TB_BOOLEAN:
          new_object = wrap(unserialize_vector<bool>(data, start, raw_length));
        break;
        case TB_INTEGER:
          new_object = wrap(unserialize_vector<int>(data, start, raw_length));
        break;
        case TB_LONG:
          new_object = wrap(unserialize_vector<long>(data, start, raw_length));
        break;
        case TB_FLOAT:
          new_object = wrap(unserialize_vector<float>(data, start, raw_length));
        break;
        case TB_DOUBLE:
          new_object = wrap(unserialize_vector<double>(data, start, raw_length));
        break;
        default: 
          throw UnsupportedType(vec_type_code);}}
      break;
    case R_CHAR_VECTOR: {
       int raw_length = get_length(data, start);
       new_object = wrap(unserialize_vector<string>(data, start, raw_length));}
      break;
    case RMR_TEMPLATE: {
      new_object = unserialize_native(data, start);}
      break;
    default: {
      throw UnsupportedType(type_code);}}
      return new_object;}

List supersize(const List& x) {
  unsigned int oldsize = x.size() ;
  List y(2*oldsize) ;
  for(unsigned int i = 0; i < oldsize; i++) 
    y[i] = x[i] ;
  return y ;}
  
SEXP typedbytes_reader(SEXP data){
  List objs(1);
  unsigned int objs_end = 0;
	RawVector tmp(data);
	raw rd(tmp.begin(), tmp.end());
	unsigned int start = 0;
  unsigned int parsed_start = 0;
  bool starting_template = false;
	RObject rmr_template = R_NilValue;
	while(rd.size() > start) {
 		try{
      RObject new_object = unserialize(rd, start);
      if(new_object.hasAttribute("rmr.template")) {
        if(objs_end == 0) 
          starting_template = true;
        else
          objs_end--; // discard the key for the template
        rmr_template = new_object;}
      else {
        if(objs_end >= (unsigned int)objs.size())
          objs = supersize(objs);
        objs[objs_end] = new_object;
        objs_end++;}
      parsed_start = start;} //if rpe exception occurs parsed start won't move, unlike start
    catch (ReadPastEnd rpe){
      break;}
		catch (UnsupportedType ue) {
      safe_stop("Unsupported type: " + to_string((int)ue.type_code));}
		catch (NegativeLength nl) {
      safe_stop("Negative length exception");}}
  return wrap(
    List::create(
      Named("objects") = List(objs.begin(), objs.begin() + objs_end),
      Named("length") = parsed_start,
      Named("template") = rmr_template,
      Named("starting.template") = starting_template));}

void T2raw(unsigned char data, raw & serialized) {
  serialized.push_back(data);}

void T2raw(int data, raw & serialized) {
  for(int i = 0; i < 4; i++) {
    serialized.push_back((data >> (8*(3 - i))) & 255);}}

void T2raw(uint64_t data, raw & serialized) {
  for(int i = 0; i < 8; i++) {  
    serialized.push_back((data >> (8*(7 - i))) & 255);}}

void T2raw(double data, raw & serialized) {
  union udouble {
    double d;
    uint64_t u;} ud;
  ud.d = data;
  T2raw(ud.u, serialized);}

void length_header(int len, raw & serialized){
  if(len < 0) {
  	throw NegativeLength();}
  T2raw(len, serialized);}

template <typename T>
void serialize_scalar(const T & data, unsigned char type_code, raw & serialized) {
  if(type_code != 255) serialized.push_back(type_code);
  T2raw(data, serialized);}

template <typename T> 
void serialize_many(const T & data, unsigned char type_code, raw & serialized){
  serialized.push_back(type_code);
  length_header(data.size(), serialized);
  serialized.insert(serialized.end(), data.begin(), data.end());}

void serialize(const RObject & object, raw & serialized, bool native); 

template <typename T> 
void serialize_vector(T & data, unsigned char type_code, raw & serialized, bool native){  
  if(data.size() == 1) {
    serialize_scalar(data[0], type_code, serialized);}
  else {
    if(native) {
      serialized.push_back(R_VECTOR);
      length_header(data.size() * sizeof(data[0]) + 1, serialized); 
      serialized.push_back(type_code);
      for(typename T::iterator i = data.begin(); i < data.end(); i++) {
        serialize_scalar(*i, 255, serialized);}}
    else {
      serialized.push_back(TB_VECTOR);
      length_header(data.size(), serialized); 
      for(typename T::iterator i = data.begin(); i < data.end(); i++) {
        serialize_scalar(*i, type_code, serialized);}}}}

void serialize_list(List & data, raw & serialized, bool native){
  serialized.push_back(TB_VECTOR);
  length_header(data.size(), serialized);
  for(unsigned int i = 0; i < (unsigned int)data.size(); i++) {
    serialize(as<RObject>(data[i]), serialized, native);}}

void serialize_native(const RObject & object, raw & serialized, type_code tc = R_NATIVE) {
  serialized.push_back(tc);
  cerr << "Calling r_serialize" << endl; 
  Function r_serialize("serialize");
  RawVector tmp(r_serialize(object, R_NilValue));
  length_header(tmp.size(), serialized);
  serialized.insert(serialized.end(), tmp.begin(), tmp.end());}

void serialize_null(raw & serialized) {
  serialized.push_back(R_NULL);
  length_header(0, serialized);}

void serialize_noattr(const RObject & object, raw & serialized, bool native) {
  if(native) {
    switch(object.sexp_type()) {
      case NILSXP: {
        serialize_null(serialized);}
      break;
      case RAWSXP: {//raw
      RawVector data(object);
      serialize_many(data, 0, serialized);}
      break;
      case LGLSXP: {
        LogicalVector data(object);  
        vector<unsigned char> bool_data(data.size());
        for(int i = 0; i < data.size(); i++) {
          bool_data[i] = (unsigned char) data[i];} 
        serialize_vector(bool_data, 2, serialized, TRUE);}
      break;
      case REALSXP: {
        NumericVector data(object);  
          serialize_vector(data, 6, serialized, TRUE);}
      break;
      case STRSXP: { //character
        CharacterVector data(object);
        serialized.push_back(R_CHAR_VECTOR);
        int raw_size = data.size() * 5 + 4;
        for(int i = 0; i < data.size(); i++) {
          raw_size += data[i].size();}
        length_header(raw_size, serialized);
        length_header(data.size(), serialized);
        for(int i = 0; i < data.size(); i++) {
          serialize_many(data[i], 7, serialized);}}
      break; 
      case INTSXP: {
        IntegerVector data(object);  
        serialize_vector(data, 3, serialized, TRUE);}
      break;
      case VECSXP: { //list
        List data(object);
        serialize_list(data, serialized, TRUE);}
      break;
      default:
        serialize_native(object, serialized);}}
    else {
      switch(object.sexp_type()) {
      	case NILSXP: {
        	  throw UnsupportedType(NILSXP);}
      	  break;
        case RAWSXP: {//raw
          RawVector data(object);
          serialize_many(data, 0, serialized);}
          break;
        case STRSXP: { //character
          CharacterVector data(object);
          if(data.size() > 1) {
            serialized.push_back(TB_VECTOR);
            length_header(data.size(), serialized);}
          for(int i = 0; i < data.size(); i++) {
            serialize_many(data[i], TB_STRING, serialized);}}
          break; 
        case LGLSXP: { //logical
          LogicalVector data(object);
          vector<unsigned char> bool_data(data.size());
          for(int i = 0; i < data.size(); i++) {
            bool_data[i] = (unsigned char) data[i];}
          serialize_vector(bool_data, TB_BOOLEAN, serialized, FALSE);}
          break;
        case REALSXP: { //numeric
          NumericVector data(object);
          serialize_vector(data, TB_DOUBLE, serialized, FALSE);}
          break;
        case INTSXP: { //factor, integer
          IntegerVector data(object);
          serialize_vector(data, TB_INTEGER, serialized, FALSE);}
          break;
        case VECSXP: { //list 
          List data(object);
          serialize_list(data, serialized, FALSE);}
          break;
        default: {
          throw UnsupportedType(object.sexp_type());}}}}

void serialize_attributes(const RObject & object, raw & serialized) {
  vector<string> names = object.attributeNames();
  serialize(wrap(names), serialized, TRUE);
  vector<RObject> attributes;
  for(unsigned int i = 0; i < names.size(); i++) {
    attributes.push_back(object.attr(names[i]));}
  serialize(wrap(attributes), serialized, TRUE);}
  
void serialize(const RObject & object, raw & serialized, bool native) {
   bool has_attr = object.attributeNames().size() > 0;
  if(has_attr && native) {
    if(object.hasAttribute("rmr.template")) {
      serialize_native(object, serialized, RMR_TEMPLATE);}
    else {
      serialized.push_back(R_WITH_ATTRIBUTES);
      raw serialized_object(0);
      serialize_noattr(object, serialized_object, TRUE);
      raw serialized_attributes(0);
      serialize_attributes(object, serialized_attributes);
      length_header(serialized_object.size() + serialized_attributes.size(), serialized);
      serialized.insert(serialized.end(), serialized_object.begin(), serialized_object.end());
      serialized.insert(serialized.end(), serialized_attributes.begin(), serialized_attributes.end());}}
  else {
    serialize_noattr(object, serialized, native);}}
  
SEXP typedbytes_writer(SEXP objs, SEXP native){
	raw serialized(0);
	List objects(objs);
  LogicalVector is_native(native);
	for(unsigned int i = 0; i < (unsigned int)objects.size(); i++) {
    try{
      serialize(as<RObject>(objects[i]), serialized, is_native[0]);}
    catch(UnsupportedType ut){
      safe_stop("Unsupported type: " + to_string((int)ut.type_code));}}
	return wrap(serialized);}	

