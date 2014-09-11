#include "filter_block.h"
#include "filter_policy.h"
#include "coding.h"

namespace leveldb{

static const size_t kFilterBaseLg = 11; 
static const size_t kFilterBase = 1 << kFilterBaseLg; //2KB

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* p) : policy_(p)
{
}

void FilterBlockBuilder::StartBlock(uint64_t block_offset)
{
	uint64_t filter_index = (block_offset / kFilterBase);
	assert(filter_index >= filter_offsets_.size());
	while(filter_index > filter_offsets_.size()){ //每2K生成一个filter
		GenerateFilter();
	}
}

void FilterBlockBuilder::AddKey(const Slice& key)
{
	Slice k = key;
	start_.push_back(keys_.size());
	keys_.append(k.data(), k.size());
}
/* 存储结构：|filter1|filter2|...|filtern|filter size array|result size|Base log flag|*/
//将过滤器编码到result_当中并作为Slice返回
Slice FilterBlockBuilder::Finish()
{
	if(!start_.empty()) //进行KEY的过滤器表构建
		GenerateFilter();

	const uint32_t array_offset = result_.size();
	for(size_t i = 0; i < filter_offsets_.size(); i++){ //将各个filter的偏移量写入result_当中
		PutFixed32(&result_, filter_offsets_[i]);
	}

	PutFixed32(&result_, array_offset); //将对照表的数据长度写入result当中,4字节
	result_.push_back(kFilterBaseLg); //1字节

	return Slice(result_);
}

void FilterBlockBuilder::GenerateFilter()
{
	//计算KEY的个数
	const size_t num_keys = start_.size();
	if(num_keys == 0){
		filter_offsets_.push_back(result_.size()); 
		return;
	}
	//将最后一个key的长度 + 原来总的长度压入到start_的最后，便以后面for的计算
	start_.push_back(keys_.size());

	tmp_keys_.resize(num_keys);
	for(size_t i = 0; i < num_keys; i ++){
		const char* base = keys_.data() + start_[i];
		size_t length = start_[i + 1] - start_[i];
		tmp_keys_[i] = Slice(base, length); //分离KEYS,并将key存入tmp_keys_当中
	}

	filter_offsets_.push_back(result_.size());
	policy_->CreateFilter(&tmp_keys_[0], num_keys, &result_); //创建一个过滤器对照表

	//清空构建对照表的参数
	tmp_keys_.clear();
	keys_.clear();
	start_.clear();
}

FilterBlockReader::FilterBlockReader(const FilterPolicy* policy, const Slice& contents)
: policy_(policy), data_(NULL), offset_(NULL), num_(0), base_lg_(0)
{
	size_t n = contents.size();
	if(n < 5) //一个是整个filters的编码后的长度4字节，还有一个是kFilterBaseLg，1字节
		return ;

	base_lg_ = contents[n - 1];
	uint32_t last_word = DecodeFixed32(contents.data() + n - 5); //整个filters 对照表接结尾位置
	if(last_word > n - 5) 
		return ;

	data_ = contents.data();
	offset_ = data_ + last_word;					//获得块的index开始的位置
	num_ = (n - 5 - last_word) / sizeof(uint32_t);	//计算filter的个数(对应FilterBlockBuilder::filter_offsets_)
}

//过滤器方法
bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key)
{
	uint64_t index = block_offset >> base_lg_;
	if(index < num_){
		uint32_t start = DecodeFixed32(offset_ + index * 4);     //filter的开始位置
		uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4); //filter的结束位置(其实就是下一个filter的开始位置)
		if(start <= limit && limit <=(offset_ - data_)){		//取出过滤器对key进行过滤
			Slice filter = Slice(data_ + start, limit - start);
			return policy_->KeyMayMatch(key, filter);
		}
		else if(start == limit)
			return false;
	}

	return true;
}


}//leveldb

