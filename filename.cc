#include <ctype.h>
#include <stdio.h>
#include "filename.h"
#include "dbformat.h"
#include "env.h"
#include "logging.h"

namespace leveldb{

extern Status WriteStringToFileSync(Env* env, const Slice& data, const std::string& fname);

static std::string MakeFileName(const std::string& name, uint64_t number, const char* suffix)
{
	char buf[100];
	snprintf(buf, sizeof(buf), "/%06llu.%s", static_cast<unsigned long long>(number), suffix);
	return name + buf;
}

std::string LogFileName(const std::string& dbname, uint64_t number)
{
	assert(number > 0);
	//dbname/number.log
	return MakeFileName(dbname, number, "log");
}

std::string TableFileName(const std::string& dbname, uint64_t number)
{
	assert(number > 0);
	//dbname/number.ldb
	return MakeFileName(dbname, number, "ldb");
}

std::string SSTableFileName(const std::string& dbname, uint64_t number)
{
	assert(number > 0);
	//dbname/number.sst
	return MakeFileName(dbname, number, "sst");
}

//dbname/MANIFEST-number
std::string DescriptorFileName(const std::string& dbname, uint64_t number)
{
	assert(number > 0);
	char buf[100];
	snprintf(buf, sizeof(buf), "/MANIFEST-%06llu", static_cast<unsigned long long>(number));
	return dbname + buf;
}

//dbname/CURRENT
std::string CurrentFileName(const std::string& dbname)
{
	return dbname + "/CURRENT";
}
//dbname/LOCK
std::string LockFileName(const std::string& dbname, uint64_t number)
{
	return dbname + "/LOCK";
}

//dbname/number.dbtmp
std::string TempFileName(const std::string& dbname, uint64_t number) 
{
	assert(number > 0);
	return MakeFileName(dbname, number, "dbtmp");
}

//dbname/LOG
std::string InfoLogFileName(const std::string& dbname) 
{
	return dbname + "/LOG";
}
//dbname/LOG.old
std::string OldInfoLogFileName(const std::string& dbname) 
{
	return dbname + "/LOG.old";
}

bool ParseFileName(const std::string& fname, uint64_t* number, FileType* type)
{
	Slice rest(fname);
	if(rest == "CURRENT"){
		*number = 0;
		*type = kCurrentFile;
	}
	else if(rest == "LOCK"){
		*number = 0;
		*type = kDBLockFile;
	}
	else if(rest == "LOG" || rest == "LOG.old"){
		*number = 0;
		*type = kInfoLogFile;
	}
	else if(rest.starts_with("MANIFEST-")){
		rest.remove_prefix(strlen("MANIFEST-"));
		uint64_t num;
		if(!ConsumeDecimalNumber(&rest, &num)) //获取number
			return false;

		if(!rest.empty())
			return false;

		*type = kDescriptorFile;
		*number = num;
	}
	else{
		uint64_t num;
		if (!ConsumeDecimalNumber(&rest, &num)) //获得number
			return false; 

		Slice suffix = rest;
		if (suffix == Slice(".log"))
			*type = kLogFile;
		else if (suffix == Slice(".sst") || suffix == Slice(".ldb"))
			*type = kTableFile;

		else if (suffix == Slice(".dbtmp"))
			*type = kTempFile;
		else 
			return false;

		*number = num;
	}

	return true;
}

Status SetCurrentFile(Env* env, const std::string& dbname, uint64_t descriptor_number)
{
	std::string manifest = DescriptorFileName(dbname, descriptor_number);
	Slice contents = manifest;
	assert(contents.starts_with(dbname + "/"));

	std::string tmp = TempFileName(dbname, descriptor_number);
	Status s = WriteStringToFileSync(env, contents.ToString() + "\n", tmp); //将内容同步写入到临时文件中
	if(s.ok())
		s = env->RenameFile(tmp, CurrentFileName(dbname));

	//写入不成功，将文件删除
	if(!s.ok())
		env->DeleteFile(tmp);

	return s;
}

};//leveldb
