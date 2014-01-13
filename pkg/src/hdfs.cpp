#include <Rcpp.h>
#include <hdfs.h>


using namespace Rcpp;


// int hdfsFileIsOpenForRead(hdfsFile file);
// int hdfsFileIsOpenForWrite(hdfsFile file);
// int hdfsFileGetReadStatistics(hdfsFile file, struct hdfsReadStatistics **stats);
// int64_t hdfsReadStatisticsGetRemoteBytesRead(const struct hdfsReadStatistics *stats);
// void hdfsFileFreeReadStatistics(struct hdfsReadStatistics *stats);
// hdfsFS hdfsConnectAsUser(const char* nn, tPort port, const char *user);
// hdfsFS hdfsConnect(const char* nn, tPort port);
// hdfsFS hdfsConnectAsUserNewInstance(const char* nn, tPort port, const char *user );
// hdfsFS hdfsConnectNewInstance(const char* nn, tPort port);
// hdfsFS hdfsBuilderConnect(struct hdfsBuilder *bld);
// struct hdfsBuilder *hdfsNewBuilder(void);
// void hdfsBuilderSetForceNewInstance(struct hdfsBuilder *bld);
// void hdfsBuilderSetNameNode(struct hdfsBuilder *bld, const char *nn);
// void hdfsBuilderSetNameNodePort(struct hdfsBuilder *bld, tPort port);
// void hdfsBuilderSetUserName(struct hdfsBuilder *bld, const char *userName);
// void hdfsBuilderSetKerbTicketCachePath(struct hdfsBuilder *bld, const char *kerbTicketCachePath);
// void hdfsFreeBuilder(struct hdfsBuilder *bld);
// int hdfsBuilderConfSetStr(struct hdfsBuilder *bld, const char *key, const char *val);    
// int hdfsConfGetStr(const char *key, char **val);
// int hdfsConfGetInt(const char *key, int32_t *val);
// void hdfsConfStrFree(char *val);
// int hdfsDisconnect(hdfsFS fs);
// hdfsFile hdfsOpenFile(hdfsFS fs, const char* path, int flags, int bufferSize, short replication, tSize blocksize);
// int hdfsCloseFile(hdfsFS fs, hdfsFile file);
// [[Rcpp::export("hdfs.exists")]]
bool hdfs_exists(const char * path){
  hdfsFS fs = hdfsConnect("default", 0);
  return hdfsExists(fs, path) >= 0;} 

// int hdfsSeek(hdfsFS fs, hdfsFile file, tOffset desiredPos); 
// tOffset hdfsTell(hdfsFS fs, hdfsFile file);
// tSize hdfsRead(hdfsFS fs, hdfsFile file, void* buffer, tSize length);
// tSize hdfsPread(hdfsFS fs, hdfsFile file, tOffset position, void* buffer, tSize length);
// tSize hdfsWrite(hdfsFS fs, hdfsFile file, const void* buffer, tSize length);
// int hdfsFlush(hdfsFS fs, hdfsFile file);
// int hdfsHFlush(hdfsFS fs, hdfsFile file);
// int hdfsHSync(hdfsFS fs, hdfsFile file);
// int hdfsAvailable(hdfsFS fs, hdfsFile file);

// [[Rcpp::export("hdfs.cp")]]
bool hdfs_copy(const char * src, const char * dst) {
  hdfsFS srcFS = hdfsConnect("default", 0);
  hdfsFS dstFS = hdfsConnect("default", 0);
  return hdfsCopy(srcFS, src, dstFS, dst) >= 0;}
  
// [[Rcpp::export("hdfs.get")]]
bool hdfs_get(const char * src, const char * dst) {
  hdfsFS srcFS = hdfsConnect("default", 0);
  hdfsFS dstFS = hdfsConnect(NULL, 0);
  return hdfsCopy(srcFS, src, dstFS, dst) >= 0;}
  
// [[Rcpp::export("hdfs.put")]]
bool hdfs_put(const char * src, const char * dst) {
  hdfsFS srcFS = hdfsConnect(NULL, 0);
  hdfsFS dstFS = hdfsConnect("default", 0);
  return hdfsCopy(srcFS, src, dstFS, dst) >= 0;}
  
// [[Rcpp::export(hdfs.mv)]]
bool hdfs_mv(const char* src, const char* dst) {
  hdfsFS srcFS = hdfsConnect("default", 0);
  hdfsFS dstFS = hdfsConnect("default", 0);
  return hdfsMove(srcFS, src, dstFS, dst) >= 0;}

// [[Rcpp::export(hdfs.rm)]]
bool hdfs_rm(const char * path, bool recursive = false) {
  hdfsFS fs = hdfsConnect("default", 0);
  return hdfsDelete(fs, path, ((int) recursive) - 1);}

// int hdfsRename(hdfsFS fs, const char* oldPath, const char* newPath);
// char* hdfsGetWorkingDirectory(hdfsFS fs, char *buffer, size_t bufferSize);
// int hdfsSetWorkingDirectory(hdfsFS fs, const char* path);

// [[Rcpp::export(hdfs.mkdir)]]
bool hdfs_mkdir(const char * path) {
  hdfsFS fs = hdfsConnect("default", 0);
  return hdfsCreateDirectory(fs, path) >= 0;}

// int hdfsSetReplication(hdfsFS fs, const char* path, int16_t replication);

List hdfsFileInfoToList(hdfsFileInfo fi) {
  return List::create( 
        (char) fi.mKind,
        std::string(fi.mName),
        fi.mLastMod,        
        fi.mSize,       
        fi.mReplication,          
        fi.mBlockSize,         
        std::string(fi.mOwner),
        std::string(fi.mGroup),
        fi.mPermissions,
        fi.mLastAccess);}

// [[Rcpp::export]]
List hdfs_ls(const char * path) {
  hdfsFS fs = hdfsConnect("default", 0);
  int numEntries = 0;
  hdfsFileInfo * fi = hdfsListDirectory(fs, path, &numEntries);
  List retval(numEntries);
  for(int i = 0; i < numEntries; i++) 
    retval[i] = hdfsFileInfoToList(fi[i]);
  return retval;}

// [[Rcpp::export]]
List hdfs_file_info(const char * path){
  hdfsFS fs = hdfsConnect("default", 0);
  hdfsFileInfo * fi = hdfsGetPathInfo(fs, path);
  return hdfsFileInfoToList(*fi);}
  
// hdfsFileInfo *hdfsGetPathInfo(hdfsFS fs, const char* path);
// void hdfsFreeFileInfo(hdfsFileInfo *hdfsFileInfo, int numEntries);
// char*** hdfsGetHosts(hdfsFS fs, const char* path, tOffset start, tOffset length);
// void hdfsFreeHosts(char ***blockHosts);
// tOffset hdfsGetDefaultBlockSize(hdfsFS fs);
// tOffset hdfsGetDefaultBlockSizeAtPath(hdfsFS fs, const char *path);
// tOffset hdfsGetCapacity(hdfsFS fs);
// tOffset hdfsGetUsed(hdfsFS fs);
// int hdfsChown(hdfsFS fs, const char* path, const char *owner, const char *group);
// int hdfsChmod(hdfsFS fs, const char* path, short mode);
// int hdfsUtime(hdfsFS fs, const char* path, tTime mtime, tTime atime);
