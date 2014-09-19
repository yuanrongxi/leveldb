#ifndef __LEVEL_DB_H_
#define __LEVEL_DB_H_

#include <stdint.h>
#include <stdio.h>

#include "iterator.h"
#include "options.h"

namespace leveldb{

static const int kMajorVersion = 1;
static const int kMinorVersion = 15;

struct Options;
struct ReadOptions;
struct WriteOptions;
class WriteBatch;

//一个快照接口
class Snapshot
{
protected:
	virtual ~Snapshot();
};

//定义一个key范围对象
struct Range
{
	Slice start;
	Slice limit;

	Range() {};
	Range(const Slice& s, const Slice& l) : start(s), limit(l){};
};

//DB的接口
class DB
{
public:
	static Status Open(const Options& opt, const std::string& name, DB** dbptr);

	DB(){};
	virtual ~DB();

	virtual Status Put(const WriteOptions& opt, const Slice& key, const Slice& value) = 0;
	virtual Status Delete(const WriteOptions& opt, const Slice& key) = 0;
	virtual Status Write(const WriteOptions& opt, WriteBatch* updates) = 0;
	virtual Status Get(const ReadOptions& opt, const Slice& key, std::string* value) = 0;
	virtual Iterator* NewIterator(const ReadOptions& opt) = 0; 

	virtual const Snapshot* GetSnapshot() = 0;
	virtual void ReleaseSnapshot(const Snapshot* snapshot) = 0;

	virtual bool GetProperty(const Slice& pro, std::string* value) = 0;
	virtual void GetApproximateSizes(const Range* range, int n, uint64_t* sizes) = 0;

	virtual void CompactRange(const Slice* begin, const Slice* end) = 0;

private:
	DB(const DB&);
	void operator=(const DB&);
};
//删除数据库
Status DestroyDB(const std::string& name, const Options& options);
//修复数据库
Status RepairDB(const std::string& dbname, const Options& options);

};//leveldb

#endif
