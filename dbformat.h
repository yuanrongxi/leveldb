#ifndef __LEVEL_DB_DBFORMAT_H_
#define __LEVEL_DB_DBFORMAT_H_

#include <stdio.h>
#include <stdint.h>
#include "comparator.h"
#include "db.h"
#include "filter_policy.h"
#include "slice.h"
#include "table_builder.h"
#include "coding.h"
#include "logging.h"

namespace leveldb{
namespace config{

static const int kNumLevels = 7;

static const int kL0_CompactionTrigger = 4;

static const int kL0_SlowdownWritesTrigger = 8;

static const int kL0_StopWritesTrigger = 12;

static const int kMaxMemCompactLevel = 2;

static const int kReadBytesPeriod = 1048576;
}//config

class InternalKey;

enum ValueType
{
	kTypeDeletion = 0x0,
	kTypeValue = 0x1
};

static const ValueType kValueTypeForSeek = kTypeValue;

typedef uint64_t SequenceNumber;

static const SequenceNumber kMaxSequenceNumber = ((0x1ull << 56) - 1); //用56为表示seq,后面8为表示type

//内部KEY表示对象
struct ParsedInternalKey
{
	Slice user_key;			//用户KEY对象
	SequenceNumber sequence;//SEQ序号
	ValueType type;			//类型

	ParsedInternalKey(){};
	ParsedInternalKey(const Slice& u, const SequenceNumber& seq, ValueType t)
		: user_key(u), sequence(seq), type(t)
	{
	}

	std::string DebugString() const;
};

inline size_t InternalKeyEncodingLength(const ParsedInternalKey& key)
{
	return key.user_key.size() + 8;
}

extern void AppendInternalKey(std::string* result, const ParsedInternalKey& key);

extern bool ParseInternalKey(const Slice& internal_key, ParsedInternalKey* result);

inline Slice ExtractUserKey(const Slice& internal_key)
{
	assert(internal_key.size() >= 8);
	return Slice(internal_key.data(), internal_key.size() - 8);
}

//内部KEY的比较器
class InternalKeyComparator : public Comparator
{
private:
	const Comparator* user_comparator_;

public:
	explicit InternalKeyComparator(const Comparator* c) : user_comparator_(c){};
	virtual const char* Name() const;
	virtual int Compare(const Slice& a, const Slice& b) const;

	virtual void FindShortestSeparator(std::string* start, const Slice& limit) const;
	virtual void FindShortSuccessor(std::string* key) const;

	const Comparator* user_comparator() const {return user_comparator_;};

	int Compare(const InternalKey& a, const InternalKey& b) const;
};

//内部key的过滤器
class InternalFilterPolicy : public FilterPolicy
{
private:
	const FilterPolicy* const user_policy_;

public:
	explicit InternalFilterPolicy(const FilterPolicy* p) : user_policy_(p) { }
	virtual const char* name() const;
	virtual void CreateFilter(const Slice* keys, int n, std::string* dst) const;
	virtual bool KeyMayMatch(const Slice& key, const Slice& filter) const;
};

//内部KEY对象
class InternalKey
{
private:
	std::string rep_;

public:
	InternalKey(){};
	InternalKey(const Slice& user_key, SequenceNumber s, ValueType t)
	{
		AppendInternalKey(&rep_, ParsedInternalKey(user_key, s, t));
	}

	void DecodeFrom(const Slice& s)
	{
		rep_.assign(s.data(), s.size());
	}

	Slice Encode() const
	{
		assert(!rep_.empty());
		return rep_;
	}

	Slice user_key() const {return ExtractUserKey(rep_);};

	void SetFrom(const ParsedInternalKey& p)
	{
		rep_.clear();
		AppendInternalKey(&rep_, p);
	}

	void Clear() {rep_.clear();};

	std::string DebugString() const;
};

inline int InternalKeyComparator::Compare(const InternalKey& a, const InternalKey& b) const
{
	return Compare(a.Encode(), b.Encode());
}

inline bool ParseInternalKey(const Slice& internal_key, ParsedInternalKey* result)
{
	const size_t n = internal_key.size();
	if(n < 8)
		return false;

	uint64_t num = DecodeFixed64(internal_key.data() + n - 8);
	unsigned char c = num & 0xff;
	
	//计算seq
	result->sequence = num >> 8;
	//计算type
	result->type = static_cast<ValueType>(c);
	result->user_key = Slice(internal_key.data(), n - 8);

	return (c <= static_cast<unsigned char>(kTypeValue));
}

class LookupKey
{
public:
	LookupKey(const Slice& user_key, SequenceNumber sequence);
	~LookupKey();
	
	//memtable key = length + user_key + seq
	Slice memtable_key() const
	{
		return Slice(start_, end_ - start_);
	}
	//internal key = user_key + seq
	Slice internal_key() const
	{
		return Slice(kstart_, end_ - kstart_);
	}
	//key = user_key
	Slice user_key() const
	{
		return Slice(kstart_, end_ - kstart_ - 8);
	}

private:
	LookupKey(const LookupKey&);
	void operator=(const LookupKey&);

private:
	const char* start_;
	const char* kstart_;
	const char* end_;
	char space_[200];
};

inline LookupKey::~LookupKey()
{
	if(start_ != space_)
		delete []start_;
}

}//leveldb

#endif
