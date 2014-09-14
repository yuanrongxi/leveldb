#ifndef __LEVEL_DB_TABLE_BUILDER_H_
#define __LEVEL_DB_TABLE_BULIDER_H_

#include <stdint.h>
#include "options.h"
#include "status.h"

namespace leveldb{
class BlockBuilder;
class BlockHandle;
class WritableFile;

class TableBuilder
{
public:
	TableBuilder(const Options& ops, WritableFile* file);
	~TableBuilder();

	Status ChangeOptions(const Options& ops);

	void Add(const Slice& key, const Slice& value);
	void Flush();

	Status status() const;

	Status Finish();
	void Abandon();
	uint64_t NumEntries() const;
	uint64_t FileSize() const;

private:
	bool ok() const;
	void WriteBlock(BlockBuilder* block, BlockHandle* handle);
	void WriteRawBlock(const Slice& data, CompressionType type, BlockHandle* handle);

	TableBuilder(const TableBuilder&);
	void operator=(const TableBuilder&);

private:
	struct Rep;
	Rep* rep_;
};

}; //leveldb
#endif
