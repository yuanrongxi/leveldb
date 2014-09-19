#ifndef __LEVEL_DB_BUILDER_H_
#define __LEVEL_DB_BUILDER_H_

#include "status.h"

namespace leveldb{

struct Options;
struct FileMetaData;

class Env;
class Iterator;
class TableCache;
class VersionEdit;

extern Status BuildTable(const std::string& dbname, Env* env, const Options& options,
						TableCache* table_cache, Iterator* iter, FileMetaData* meta);

};//leveldb

#endif




