#ifndef __LEVEL_DB_WRITE_BATCH_H_
#define __LEVEL_DB_WRITE_BATCH_H_

#include <string>
#include "status.h"

namespace leveldb{

class Slice;

class WriteBatch
{
public:
	WriteBatch();
	~WriteBatch();

	void Put(const Slice& key, const Slice& value);

	void Delete(const Slice& key);

	void Clear();

	class Handler
	{
	public:
		virtual ~Handler();
		virtual void Put(const Slice& key, const Slice& value) = 0;
		virtual void Delete(const Slice& key) = 0;
	};

	Status Iterate(Handler* handler) const;

private:
	friend class WriteBatchInternal;

	std::string rep_;
};

}; //leveldb

#endif
