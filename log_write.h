#ifndef __LEVEL_DB_LOG_WRITE_H_
#define __LEVEL_DB_LOG_WRITE_H_

#include <stdint.h>
#include "log_format.h"
#include "slice.h"
#include "status.h"

namespace leveldb{

class WritableFile;

namespace log
{

class Writer
{
public:
	explicit Writer(WritableFile* dest);
	~Writer();

	Status AddRecord(const Slice& slice);

private:
	Writer(const Writer&);
	void operator=(const Writer&);

	Status EmitPhysicalRecord(RecordType type, const char* ptr, size_t length);

private:
	WritableFile* dest_;
	int block_offset_;
	uint32_t type_crc_[kMaxRecordType + 1];

};

};//log
};//leveldb


#endif
