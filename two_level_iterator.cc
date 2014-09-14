#include "iterator_wrapper.h"
#include "table.h"
#include "block.h"
#include "format.h"
#include "options.h"
#include "assert.h"
#include "iterator_wrapper.h"

namespace leveldb{

typedef Iterator* (*BlockFunction)(void*, const ReadOptions&, const Slice&);

class TwoLevelIterator : public Iterator
{
public:
	TwoLevelIterator(Iterator* index_iter, BlockFunction block_fun, void* arg, const ReadOptions& ops);
	virtual ~TwoLevelIterator();

	virtual void Seek(const Slice& target);
	virtual void SeekToFirst();
	virtual void SeekToLast();
	virtual void Next();
	virtual void Prev();

	virtual bool Valid() const
	{
		return data_iter_.Valid();
	};

	virtual Slice value() const
	{
		assert(Valid());
		return data_iter_.value();
	};

	virtual Slice key() const
	{
		assert(Valid());
		return data_iter_.key();
	};

	virtual Status status() const
	{	
		if(!index_iter_.status().ok()){ //index iter出错
			return index_iter_.status();
		}
		else if(data_iter_.iter() != NULL && !data_iter_.status().ok()){//data iter出错
			return data_iter_.status();
		}
		else 
			return status_;
	}

private:
	void SaveError(const Status& s)
	{
		if(status_.ok() && !s.ok())
			status_ = s;
	}

	void SkipEmptyDataBlocksForward();
	void SkipEmptyDataBlocksBackward();
	void SetDataIterator(Iterator* data_iter);
	void InitDataBlock();

private:
	BlockFunction			block_fun_;
	void*					arg_;
	const ReadOptions		ops_;
	Status					status_;
	IteratorWrapper			index_iter_; //一层index iter
	IteratorWrapper			data_iter_;  //二层数据iter
	std::string				data_block_handle_;
};

TwoLevelIterator::TwoLevelIterator(Iterator* index_iter, BlockFunction block_fun, void* arg, const ReadOptions& ops)
	: block_fun_(block_fun), arg_(arg), ops_(ops), index_iter_(index_iter), data_iter_(NULL)
{
}

TwoLevelIterator::~TwoLevelIterator()
{
}

//基于二分查找法的seek,O(logN)
void TwoLevelIterator::Seek(const Slice& target)
{
	//先在index上SEEK
	index_iter_.Seek(target);

	InitDataBlock();
	if(data_iter_.iter() != NULL)
		data_iter_.Seek(target);

	SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToFirst()
{
	index_iter_.SeekToFirst();
	InitDataBlock();
	if(data_iter_.iter() != NULL)
		data_iter_.SeekToFirst();

	SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToLast()
{
	index_iter_.SeekToLast();
	if(data_iter_.iter() != NULL)
		data_iter_.SeekToLast();

	SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::Next()
{
	assert(Valid());
	data_iter_.Next();
	SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::Prev()
{
	assert(Valid());
	data_iter_.Prev();
	SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::SkipEmptyDataBlocksForward()
{
	while(data_iter_.iter() == NULL || !data_iter_.Valid()){
		if(!index_iter_.Valid()){ //index必须是有值的
			SetDataIterator(NULL);
			return;
		}
		
		//移向下一个Data
		index_iter_.Next();
		InitDataBlock();

		if(data_iter_.iter() != NULL)
			data_iter_.SeekToFirst();
	}
}

void TwoLevelIterator::SkipEmptyDataBlocksBackward()
{
	while(data_iter_.iter() == NULL && !data_iter_.Valid()){
		if(!index_iter_.Valid()){
			SetDataIterator(NULL);
			return;
		}
		//移向前一个Data
		index_iter_.Prev();
		InitDataBlock();

		if(data_iter_.iter() != NULL)
			data_iter_.SeekToLast();
	}
}

void TwoLevelIterator::SetDataIterator(Iterator* data_iter)
{
	//保存错误码
	if(data_iter_.iter() != NULL)
		SaveError(data_iter_.status());

	data_iter_.Set(data_iter);
}

void TwoLevelIterator::InitDataBlock()
{
	if(!index_iter_.Valid())
		SetDataIterator(NULL);
	else{
		Slice handle = index_iter_.value();
		if(data_iter_.iter() != NULL && handle.compare(data_block_handle_) == 0){
			
		}
		else{
			Iterator* iter = (*block_fun_)(arg_, ops_, handle); //通过index 的value索引到data iter(一般是从磁盘readBlock)
			data_block_handle_.assign(handle.data(), handle.size()); //通过std::string保存iter value,因为slice的拷贝是指针拷贝！！
			SetDataIterator(iter); //设置data iter
		}
	}
}

//生成一个tow level iterator
Iterator* NewTwoLevelIterator(Iterator* index_iter, BlockFunction block_fun, void* arg, const ReadOptions& ops)
{
	return new TwoLevelIterator(index_iter, block_fun, arg, ops);
}

}; //leveldb



