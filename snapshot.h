#ifndef __LEVEL_DB_SNAPSHOT_H_
#define __LEVEL_DB_SNAPSHOT_H_

#include "db.h"
#include <assert.h>

namespace leveldb{

class SnapshotList;

class SnapshotImpl : public Snapshot
{
public:
	SequenceNumber number_; 

private:
	friend class SnapshotList;

	SnapshotImpl* prev_;
	SnapshotImpl* next_;
	SnapshotList* list_;
};

//环形链表双向链表
class SnapshotList
{
public:
	SnapshotList()
	{
		list_.prev_ = &list_;
		list_.next_ = &list_;
	}

	bool empty() const {return list_.next_ = &list_;};

	SnapshotImpl* oldest() const { assert(!empty()); return list_.next_; }
	SnapshotImpl* newest() const { assert(!empty()); return list_.prev_; }

	const SnapshotImpl* New(SequenceNumber seq)
	{
		//最新的总是插入在list_的前面一个
		SnapshotImpl* s = new SnapshotImpl;
		s->number_ = seq;

		s->list_ = this;
		s->next_ = &list_;
		s->prev_ = list_.prev_;

		s->prev_->next_ = s;
		s->next_->prev_ = s;
	}

	void Delete(const SnapshotImpl* s)
	{
		assert(s->list_ == this);
		s->prev_->next_ = s->next_;
		s->next_->prev_ = s->prev_;
		delete s;
	}

private:
	SnapshotImpl list_;
};

};//leveldb

#endif



