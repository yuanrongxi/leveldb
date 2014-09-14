#ifndef __LEVEL_DB_TABLE_H_
#define __LEVEL_DB_TABLE_H_

#include <stdint.h>
#include "iterator.h"

namespace leveldb{

class Block;
class BlockHandle;
class Footer;
struct Options;
class ReadOptions;
class RandomAccessFile;
class TableCache;

class Table
{
public:
	static Status Open(const Options& options, RandomAccessFile* file, uint64_t file_size, Table** table);

	~Table();

	Iterator* NewIterator(const ReadOptions&) const;
	uint64_t ApproximateOffsetOf(const Slice& key) const;

private:
	struct Rep;
	Rep* rep_;

	friend class TableCache;

private:
	explicit Table(Rep* rep){rep_ = rep;};
	
	static Iterator* BlockReader(void *, const ReadOptions&, const Slice&);

	Status InternalGet(const ReadOptions&, const Slice& key, void* arg,
		void (*hanlde_result)(void* arg, const Slice& k, const Slice&v));

	void ReadMeta(const Footer& footer);
	void ReadFilter(const Slice& filter_handle_value);

	Table(const Table&);
	void operator=(const Table&);
};

}; //leveldb

#endif
