#ifndef __LEVEL_DB_MEMTABLE_H_
#define __LEVEL_DB_MEMTABLE_H_

#include <string>
#include "db.h"
#include "dbformat.h"
#include "skiplist.h"
#include "arena.h"
#include "iterator.h"

namespace leveldb{

class InternalKeyComparator;
class Mutex;
class MemTableIterator;

class MemTable
{
public:
	explicit MemTable(const InternalKeyComparator& comparator);

	void Ref(){ ++refs_; };
	void Unref() 
	{
		-- refs_;
		assert(refs_ >= 0);
		if(refs_ <=0) //对MemTable做析构
			delete this;
	}

	size_t ApproximateMemoryUsage();

	Iterator* NewIterator();
	
	void Add(SequenceNumber seq, ValueType type, const Slice& key, const Slice& value);

	bool Get(const LookupKey& key, std::string* value, Status* s);

private:
	~MemTable(); //用过Unref来释放

	MemTable(const MemTable&);
	void operator=(const MemTable&);

private:
	struct KeyComparator
	{
		const InternalKeyComparator comparator;
		explicit KeyComparator(const InternalKeyComparator& c) : comparator(c){};
		int operator()(const char* a, const char* b) const;
	};

	friend class MemTableIterator;
	friend class MemTableBackwardIterator;
	
	//内存跳表
	typedef SkipList<const char*, KeyComparator> Table;

	 KeyComparator comparator_;
	 int refs_;
	 Arena arena_;
	 Table table_;
};

};//leveldb

#endif
