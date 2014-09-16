#include <stdio.h>
#include "dbformat.h"
#include "port.h"
#include "coding.h"

namespace leveldb{

static uint64_t PackSequenceAndType(uint64_t seq, ValueType t)
{
	assert(seq <= kMaxSequenceNumber);
	assert(t < kValueTypeForSeek);

	return (seq << 8) | t;
}

void AppendInternalKey(std::string* result, const ParsedInternalKey& key)
{
	result->append(key.user_key.data(), key.user_key.size());
	PutFixed64(result, PackSequenceAndType(key.sequence, key.type));
}

std::string ParsedInternalKey::DebugString() const
{
	char buf[50];
	snprintf(buf, sizeof(buf), "' @ %llu : %d", (unsigned long long) sequence, int(type));

	std::string result = "'";
	result += EscapeString(user_key.ToString());
	result += buf;
	return result;
}

std::string InternalKey::DebugString() const
{
	std::string result;
	ParsedInternalKey parsed;
	if (ParseInternalKey(rep_, &parsed)){
		result = parsed.DebugString();
	} 
	else{
		result = "(bad)";
		result.append(EscapeString(rep_));
	}
	return result;
}

const char* InternalKeyComparator::Name() const
{
	return "leveldb.InternalKeyComparator";
}

int InternalKeyComparator::Compare(const Slice& a, const Slice& b) const
{
	int r = user_comparator_->Compare(ExtractUserKey(a), ExtractUserKey(b));

	if(r == 0){
		const uint64_t anum = DecodeFixed64(a.data() + a.size() - 8); //解析seq
		const uint64_t bnum = DecodeFixed64(b.data() + b.size() - 8);
		if(anum > bnum)
			r = -1;
		else if(anum < bnum)
			r = +1;
	}

	return r;
}

void InternalKeyComparator::FindShortestSeparator(std::string* start, const Slice& limit) const
{
	Slice user_start = ExtractUserKey(*start); //user_key
	Slice user_limit = ExtractUserKey(limit); //user_key

	std::string tmp(user_start.data(), user_start.size());
	//找到仅仅大于user_key且小于user_limit的字符串
	user_comparator_->FindShortestSeparator(&tmp, user_limit);

	if(tmp.size() < user_start.size() && user_comparator_->Compare(user_start, tmp) < 0){
		PutFixed64(&tmp, PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek)); //从新打包成interkey
		assert(this->Compare(*start, tmp) < 0);
		assert(this->Compare(tmp, limit) < 0);
		start->swap(tmp);
	}
}

void InternalKeyComparator::FindShortSuccessor(std::string* key) const
{
	Slice user_key = ExtractUserKey(*key);
	std::string tmp(user_key.data(), user_key.size());

	//找到仅仅大于user_key字符串tmp
	user_comparator_->FindShortSuccessor(&tmp);

	if(tmp.size() < user_key.size() && user_comparator_->Compare(user_key, tmp) < 0){
		PutFixed64(&tmp, PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
		assert(this->Compare(*key, tmp) < 0);
		key->swap(tmp);
	}
}

const char* InternalFilterPolicy::name() const
{
	return user_policy_->name();
}

void InternalFilterPolicy::CreateFilter(const Slice* keys, int n, std::string* dst) const
{
	Slice* mkey = const_cast<Slice*>(keys);
	for(int i = 0; i < n; i ++){
		mkey[i] = ExtractUserKey(keys[i]); //user key
	}

	//用USER KEY构建过滤器对照表
	user_policy_->CreateFilter(keys, n, dst);
}

bool InternalFilterPolicy::KeyMayMatch(const Slice& key, const Slice& f) const
{
	//对user key进行过滤
	return user_policy_->KeyMayMatch(ExtractUserKey(key), f);
}

LookupKey::LookupKey(const Slice& user_key, SequenceNumber s)
{
	size_t usize = user_key.size();
	size_t needed = usize + 13;

	char* dst;
	if(needed <= sizeof(space_))
		dst = space_;
	else
		dst = new char[needed];

	start_ = dst;
	dst = EncodeVarint32(dst, usize + 8);//7位编码，5个字节才能描述32位
	kstart_ = dst;
	memcpy(dst, user_key.data(), usize);
	dst += usize;
	EncodeFixed64(dst, PackSequenceAndType(s, kValueTypeForSeek));
	dst += 8;
	end_ = dst;
}

}//leveldb







