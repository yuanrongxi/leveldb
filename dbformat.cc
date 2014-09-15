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

}//leveldb







