#include "block.h"
#include <vector>
#include <algorithm>
#include "comparator.h"
#include "format.h"
#include "coding.h"
#include "logging.h"
#include "slice.h" 

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
			restart_offset_ = size_ - ((1 + NumRestarts()) * sizeof(uint32_t));
	}
}

Block::~Block()
{
	if(owned_)
		delete[] data_;
}

static const char* DecodeEntry(const char* p, cosnt char* limit, 
	uint32_t* shared, uint32_t* non_shared, uint32_t* value_length)
{
	if(limit - p < 3)
		return NULL;

	//动态存储了shared non_shared length,7位描述存储法
	*shared = reinterpret_cast<const unsigned char*>(p)[0];
	*non_shared = reinterpret_cast<const unsigned char*>(p)[1];
	*value_length = reinterpret_cast<const unsigned char*>(p)[2];
	if((*shared | *non_shared | *value_length) < 128){
		p += 3; 
	}
	else {
		if ((p = GetVarint32Ptr(p, limit, shared)) == NULL) return NULL;
		if ((p = GetVarint32Ptr(p, limit, non_shared)) == NULL) return NULL;
		if ((p = GetVarint32Ptr(p, limit, value_length)) == NULL) return NULL;
	}
	//合法性判断，key + value的长度一定小于或等于entry后块的长度（limit - p）
	if(static_cast<uint32_t>(limit - p) < (*non_shared + *value_length)){
		return NULL;
	}

	return p;
}

class Block::Iter : public Iterator
{

private:
	const Comparator* const comparator_;		//key比较器
	const char* const data_;					//block数据句柄
	uint32_t const restarts_;					//block restart偏移量
	uint32_t const num_restarts_;				//entries的数量

	uint32_t current_;							//当前指向entry的位置
	uint32_t restart_index_;					//当前block的restart index的位置
	std::string key_;							//当前entry的key
	Slice value_;								//当前entry的value
	Status status_;								//上次操作的错误码

private:
	inline int Compare(const Slice& a, const Slice& b) const 
	{
		return comparator_->Compare(a, b);
	}

	uint32_t NextEntryOffset() const
	{
		return (value_.data() + value_.size()) - data_;
	}

	uint32_t GetRestartPoint(uint32_t index)
	{
		assert(index < num_restarts_);
		return DecodeFixed32(data_ + restarts_ + index * sizeof(uint32_t));
	}

	void SeekToRestartPoint(uint32_t index)
	{
		key_.clear();
		restart_index_ = index;

		uint32_t offset = GetRestartPoint(index); //计算restarts的位置
		value_ = Slice((char*)(data_ + offset), 0);
	}

public:
	Iter(const Comparator* comparator, const char* data, uint32_t restarts, uint32_t num_restarts)
		: comparator_(comparator), data_(data), restarts_(restarts), num_restarts_(num_restarts),
		  current_(restarts), restart_index_(num_restarts_)
	{
		assert(num_restarts_ > 0);
	}

	virtual bool Valid() const {return current_ < restarts_;}
	virtual Status status() const {return status_;}

	virtual Slice key() const
	{
		assert(Valid());
		return key_;
	}

	virtual Slice value() const 
    {
		assert(Valid());
		return value_;
	}

	virtual Next()
	{
		assert(Valid());
		ParseNextKey();
	}

	virtual void Prev()
	{
		assert(Valid());
		const uint32_t original = current_;
		while(GetRestartPoint() >= original){
			if(restart_index_ == 0){ //回到初始位置
				current_ = restarts_;
				restart_index_ = num_restarts_;
				return;
			}
			restart_index_ --;
		}
		
		//SEEK到restart_index_指向的entry位置
		SeekToRestartPoint(restart_index_);
		do{
		}while(ParseNextKey() && NextEntryOffset() < original);
	}

	//定位target作为KEY在block的位置
	virtual void Seek(const Slice& target)
	{
		uint32_t left = 0;
		uint32_t rigtht = num_restarts_ - 1;
		//2分查找法定位目标的位置(index)
		while(left < right){
			uint32_t mid = (left + right + 1) / 2;
			//定位entry偏移量
			uint32_t region_offset = GetRestartPoint(mid);
			uint32_t shared, non_shared, value_length;
			//获得KEY的指针
			const char* key_ptr = DecodeEntry(data_ + region_offset, data_ + restarts_, &shared, &non_shared, &value_length);
			if(key_ptr == NULL || (shared != 0)){
				CorruptionError(); //产生一个错误
				return;
			}

			//进行KEY比较
			Slice mid_key(key_ptr, non_shared);
			if(Compare(mid_key, target) < 0)
				left = mid;
			else
				right = mid  - 1;
		}

		//定位到匹配到的位置
		SeekToRestartPoint(left);
		//进行偏移修正
		while(true){
			if (!ParseNextKey())
				return;
			if (Compare(key_, target) >= 0)
				return;
		}
	}

	virtual void SeekToFirst()
	{
		SeekToRestartPoint(0); //定位到开始
		ParseNextKey();
	}

	virtual void SeekToLast()
	{
		SeekToRestartPoint(num_restarts_ - 1); //定位到最后一个entry
		while(ParseNextKey() && NextEntryOffset() < restarts_){
		}
	}

	void CorruptionError()
	{
		current_ = restart_;
		restart_index_ = num_restarts_;
		status_ = Status("bad entry in block");
		key_.clear();
		value_.clear();
	}

	bool ParseNextKey()
	{
		current_ = NextEntryOffset();
		const char* p = data_ + current_;
		const char* limit = data_ + restarts_;
		if(p > limit){
			current_ = restarts_;
			restart_index_ = num_restarts_;
		}

		//获得entry key的ptr和数据参数
		uint32_t shared, non_shared, value_length;
		p = DecodeEntry(p, limit, &shared, &non_shared, &value_length);
		if(p == NULL || key_.size() < shared){
			CorruptionError();
			return false;
		}
		else{
			//保存KEY和value
			key_.resize(shared);
			key_.append(p, non_shared);
			value_ = Slice(p + non_shared, value_length);
			while(restart_index_ + 1 < num_restarts_ && GetRestartPoint(restart_index_ + 1) < current_)
				++ restart_index_;

			return true;
		}
	}
};

//构建一个block::iter
Iterator* Block::NewIterator(const Comparator* cmp)
{
	if(size_ < sizeof(uint32_t))
		return NewErrorIterator(Status::Corruption("bad block contents"));

	const uint32_t num_restarts = NumRestarts();
	if(num_restarts == 0)
		return NewEmptyIterator();
	else
		return new Iter(cmp, data_, restart_offset_, num_restarts);
}
};


