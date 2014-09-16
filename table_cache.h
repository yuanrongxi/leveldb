#ifndef __LEVEL_DB_TABLE_CACHE_H_
#define __LEVEL_DB_TABLE_CACHE_H_

#include <string>
#include <stdint.h>
#include "dbformat.h"
#include "cache.h"
#include "table.h"
#include "port.h"

namespace leveldb{

class env;

class TableCache
{
public:
	TableCache(const std::string& dbname, const Options* opt, int entries);
	~TableCache();

	Iterator* NewIterator(const ReadOptions& opt, uint64_t file_number, uint64_t file_size, Table** tableptr = NULL);
	Status Get(const ReadOptions& opt, uint64_t file_number, uint64_t file_size, const Slice& k, void* arg, 
		void (*handle_result)(void*, const Slice&, const Slice&));

	void Evict(uint64_t file_number);

private:
	Status FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle**);
private:
	Env* const env_;
	const std::string dbname_;
	const Options* options_;
	Cache* cache_;
};

};//leveldb

#endif
