#include "db_iter.h"
#include "filename.h"
#include "dbformat.h"
#include "env.h"
#include "iterator.h"
#include "port.h"
#include "logging.h"
#include "mutexlock.h"
#include "random.h"

namespace leveldb{

#if 0
static void DumpInternalIter(Iterator* iter) 
{
	for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
		ParsedInternalKey k;
		if (!ParseInternalKey(iter->key(), &k)) {
			fprintf(stderr, "Corrupt '%s'\n", EscapeString(iter->key()).c_str());
		} else {
			fprintf(stderr, "@ '%s'\n", k.DebugString().c_str());
		}
	}
}

#endif

namespace {

class DBIter : public Iterator
{
public:
	enum Direction
	{
		kForward,
		kReverse
	};

	DBIter(DBImpl* db, const Comparator* cmp, Iterator* iter, SequenceNumber s, uint32_t seed)
		: db_(db), user_comparator_(cmp), iter_(iter), sequence_(s),
		direction_(kForward), rnd_(seed), bytes_counter_(RandomPeriod())
	{
	}

	virtual ~DBIter()
	{
		delete iter_;
	}

	virtual bool Valid() const {return valid_;};
	virtual Slice key() const
	{
		assert(valid_);
		return (direction_ == kForward) ? ExtractUserKey(iter_->key()) : saved_key_;
	};

	virtual Slice value() const
	{
		assert(valid_);
		return (direction_ == kForward) ? iter_->value() : saved_value_;
	};


	virtual Status status() const
	{
		if(status_.ok())
			return iter_->status();
		else 
			return status_;
	}

	virtual void Next();
	virtual void Prev();
	virtual void Seek(const Slice& target);
	virtual void SeekToFirst();
	virtual void SeekToLast();

private:
	DBIter(const DBIter&);
	void operator=(const DBIter&);

	void FindNextUserEntry(bool skipping, std::string* skip);
	void FindPrevUserEntry();
	bool ParseKey(ParsedInternalKey* key);

	inline void SaveKey(const Slice& k, std::string* dst)
	{
		dst->assign(k.data(), k.size());
	}

	inline void ClearSavedValue()
	{
		if(saved_value_.capacity() > 1048576){ //1M数据，才用swap
			std::string empty;
			swap(empty, saved_value_); //效率更高，内部并没有释放内存，只是做了内容清零，swap使用xor来做交换
		}
		else
			saved_value_.clear();
	}

	ssize_t RandomPeriod()
	{
		return rnd_.Uniform(2 * config::kReadBytesPeriod);
	}

private:
	DBImpl* db_;
	const Comparator* const user_comparator_;
	Iterator* const iter_;
	SequenceNumber const sequence_;

	Status status_;
	std::string saved_key_;
	std::string saved_value_;
	Direction direction_;

	bool valid_;
	Random rnd_;
	ssize_t bytes_counter_;
};

inline bool DBIter::ParseKey(ParsedInternalKey* ikey)
{
	Slice k = iter_->key();
	ssize_t n = k.size() + iter_->value().size();
	bytes_counter_ -= n;

	while(bytes_counter_ < 0){
		bytes_counter_ += RandomPeriod();
		db_->RecordReadSample(k);
	}

	//将key解析到ikey当中
	if(!ParseInternalKey(k, ikey))
		return false;
	else 
		return true;
}

}//namespace

};


