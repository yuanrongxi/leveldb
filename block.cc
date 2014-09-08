#include "block.h"
#include <vector>
#include <algorithm>
#include "comparator.h"
#include "format.h"
#include "coding.h"
#include "logging.h"

namespace leveldb{

inline uint32_t Block::NumRestarts() const
{
	assert(size >= sizeof(uint32_t));
	return DecodeFixed32(data_ + size_ - sizeof(uint32_t));
}

Block::Block(const BlockContents& contents)
	: data_(contents.data.data()), size_(contents.data.size()), owned_(contents.heap_allocated)
{
	if(size_ < sizeof(uint32_t))
		size_ = 0;
	else {
		size_t max_restarts_allowed = (size_ - sizeof(uint32_t)) / sizeof(uint32_t);
		if(NumRestarts() > max_restarts_allowed)
			size_ = 0;
		else
			restart_offset_ = size_ + (1 + NumRestarts()) * sizeof(uint32_t);
	}
}

Block::~Block()
{
	if(owned_)
		delete[] data_;
}

};


