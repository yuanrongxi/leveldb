#ifndef __LEVEL_DB_FILENAME_H_
#define __LEVEL_DB_FILENAME_H_

#include <stdint.h>
#include <string>
#include "slice.h"
#include "status.h"
#include "port.h"

namespace leveldb{

class Env;
enum FileType{
	kLogFile,
	kDBLockFile,
	kTableFile,
	kDescriptorFile,
	kCurrentFile,
	kTempFile,
	kInfoLogFile,
};
//LOG file name
extern std::string LogFileName(const std::string& dbname, uint64_t number);
//table file name
extern std::string TableFileName(const std::string& dbname, uint64_t number);
//
extern std::string SSTableFileName(const std::string& dbname, uint64_t number);

extern std::string DescriptorFileName(const std::string& dbname, uint64_t number);

extern std::string CurrentFileName(const std::string& dbname);

extern std::string LockFileName(const std::string& dbname);

extern std::string TempFileName(const std::string& dbname, uint64_t number);

extern std::string InfoLogFileName(const std::string& dbname);

extern std::string OldInfoLogFileName(const std::string& dbname);

extern bool ParseFileName(const std::string& filename, uint64_t* number, FileType* type);

extern Status SetCurrentFile(Env* env, const std::string& dbname, uint64_t descriptor_number);

};//leveldb

#endif
