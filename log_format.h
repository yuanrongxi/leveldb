#ifndef __LEVEL_DB_LOG_FORMAT_H_
#define __LEVEL_DB_LOG_FORMAT_H_

namespace leveldb{
namespace log{

enum RecordType{
	kZeroType = 0,
	kFullType = 1,
	kFirstType = 2,
	kMiddleType = 3,
	kLastType = 4,
};

static const int kMaxRecordType = kLastType; //4
static const int kBlockSize = 32768;
static const int kHeaderSize = 4 + 1 + 2; //7

};//log

};//leveldb

#endif







