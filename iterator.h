#ifndef __LEVEL_DB_ITERATOR_H_
#define __LEVEL_DB_ITERATOR_H_

#include "slice.h"
#include "status.h"

namespace leveldb{

typedef void (*CleanupFunction)(void* arg1, void* arg2);
struct Cleanup //一个clean function列表	
{
	CleanupFunction function;
	void* arg1;
	void* arg2;
	Cleanup* next;
};


class Iterator
{
public:
	Iterator();
	virtual ~Iterator();

	virtual bool Valid() const = 0;
	virtual void SeekToFirst() = 0;
	virtual void SeekToLast() = 0;
	virtual void Seek(const Slice& target) = 0;
	virtual void Next() = 0;
	virtual void Prev() = 0;
	virtual Slice key() const = 0;
	virtual Slice value() const = 0;
	virtual Status status() const = 0;
	void RegisterCleanup(CleanupFunction function, void* arg1, void* arg2);

private:
	Cleanup cleanup_;
	Iterator(const Iterator&);
	void operator=(const Iterator&);
};

extern Iterator* NewEmptyIterator();
extern Iterator* NewErrorIterator(const Status& status);

};

#endif
