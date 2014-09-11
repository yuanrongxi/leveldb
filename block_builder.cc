#include <algorithm>
#include <assert.h>
#include "comparator.h"
#include "block_builder.h"
#include "coding.h"
#include "options.h"
#include "table_builder.h"

namespace leveldb{

BlockBuilder::BlockBuilder(const Options* options)
: options_(options), counter_(0), finished_(false)
{
	assert(options_->block_restart_interval >= 1);
	restarts_.push_back(0);
}

void BlockBuilder::Reset()
{
	restarts_.clear();
	restarts_.push_back(0);

	buffer_.clear();
	last_key_.clear();

	counter_ = 0;
	finished_ = false;
}

size_t BlockBuilder::CurrentSizeEstimate() const
{
	return (buffer_.size() + restarts_.size() * sizeof(uint32_t) + sizeof(uint32_t));
}

Slice BlockBuilder::Finish()
{
	for(size_t i = 0; i < restarts_.size(); i++){
		PutFixed32(&buffer_, restarts_[i]);
	}

	PutFixed32(&buffer_, restarts_.size());
	finished_ = true;

	return Slice(buffer_);
}

void BlockBuilder::Add(const Slice& key, const Slice& value)
{
	Slice last_key_piece(last_key_);
	assert(!finished_);
	assert(counter_ <= options_->block_restart_interval);
	assert(buffer_.empty() || options_->comparator->Compare(key, last_key_piece) > 0); //一定是比现有的KEY大

	size_t shared = 0;
	if(counter_ < options_->block_restart_interval){ //每block_restart_interval个KV组成一个restart
		const size_t min_length = std::min(last_key_piece.size(), key.size());
		//定位相同的长度
		while(shared < min_length && last_key_piece[shared] == key[shared])
			shared++;
	}
	else { //启动一个restart，key相似的放在一起
		restarts_.push_back(buffer_.size());
		counter_ = 0;
	}

	//计算不相同的长度
	const size_t non_shared = key.size() - shared;

	PutVarint32(&buffer_, shared);
	PutVarint32(&buffer_, non_shared);
	PutVarint32(&buffer_, value.size());

	buffer_.append(key.data() + shared, non_shared); //KEY不同的部分加入到头上
	buffer_.append(value.data(), value.size());

	//udpate state
	last_key_.resize(shared);
	last_key_.append(key.data() + shared, non_shared);

	assert(Slice(last_key_) == key);
	counter_ ++;
}

}//leveldb




