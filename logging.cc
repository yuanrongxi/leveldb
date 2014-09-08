#include "logging.h"
#include <errno.h>
#include <stdarg.h>
#include <stdlib.h>
#include "env.h"
#include "slice.h"

namespace leveldb{

void AppendNumberTo(std::string* str, uint64_t num)
{
	char buf[30];
	snprintf(buf, sizeof(buf), "%llu", (unsigned long long)num);
	str->append(buf);
}

void AppendEscapedStringTo(std::string* str, const Slice& value)
{
	for(size_t i = 0; i < value.size(); i ++){
		char c = value[i];
		if(c >= ' ' && c <= '~'){
			str->push_back(c);
		}
		else {
			char buf[10];
			snsprintf(buf, sizeof(buf), "\\x%02x", static_cast<unsigned int>(c) & 0xff);
			str->append(buf);
		}
	}
}

std::string NumberToString(uint64_t num)
{
	std::string r;
	AppendNumberTo(&r, num);
	return r;
}

std::string EscapeString(const Slice& value) 
{
	std::string r;
	AppendEscapedStringTo(&r, value);
	return r;
}

bool ConsumeChar(Slice* in, char c)
{
	if(!in->empty() && (*in)[0] == c){
		in->remove_prefix(1);
		return true;
	}
	else
		return false;
}

//将字符串转成数字
bool ConsumeDecimalNumber(Slice* in, uint64_t* val) 
{
	uint64_t v = 0;
	int digits = 0;
	while (!in->empty()) {
		char c = (*in)[0];
		if (c >= '0' && c <= '9') {
			++digits;
			const int delta = (c - '0');
			static const uint64_t kMaxUint64 = ~static_cast<uint64_t>(0);
			if (v > kMaxUint64/10 ||
				(v == kMaxUint64/10 && delta > kMaxUint64%10)) {
					// Overflow
					return false;
			}
			v = (v * 10) + delta;
			in->remove_prefix(1);
		} 
		else {
			break;
		}
	}
	*val = v;
	return (digits > 0);
}

}


