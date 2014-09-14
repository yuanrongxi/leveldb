#ifndef __LEVEL_DB_LOG_READER_H_
#define __LEVEL_DB_LOG_READER_H_

#include <stdint.h>
#include "log_format.h"
#include "status.h"

namespace leveldb{
class SequentialFile;

namespace log{

class Reader
{
public:
	class Reporter
	{
	public:
		virtual ~Reporter();
		virtual void Corruption(size_t bytes, const Status& status) = 0;
	};

	Reader(SequentialFile* file, Reporter* reporter, bool checksum, uint64_t initial_offset);
	~Reader();

	bool ReadRecord(Slice* record, std::string* scratch);
	uint64_t LastRecordOffset();

private:
	enum{
		kEof = kMaxRecordType + 1,
		kBadRecord = kMaxRecordType + 2,
	};

	bool SkipToInitialBlock();
	unsigned int ReadPhysicalRecord(Slice* result);
	void ReportCorruption(size_t bytes, const char* reason);
	void ReportDrop(size_t bytes, const Status& reason);

	Reader(const Reader&);
	void operator=(const Reader&);

private:
	SequentialFile* const file_;
	Reporter* const reporter_;
	bool const checksum_;
	char* const backing_store_;

	Slice buffer_;
	bool eof_;

	uint64_t last_record_offset_;
	uint64_t end_of_buffer_offset_;
	uint64_t const initial_offset_;
};

};//log

};//leveldb

#endif
