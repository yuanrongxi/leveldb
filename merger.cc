#include "merger.h"
#include "comparator.h"
#include "iterator.h"
#include "iterator_wrapper.h"

namespace leveldb{

enum Direction{
	kForward,
	kReverse,
};
class MerginIterator : public Iterator
{
public:
	MerginIterator(const Comparator* cp, Iterator** children, int n);
	virtual ~MerginIterator();

	virtual bool Valid() const;
	virtual void SeekToFirst();
	virtual void SeekToLast();
	virtual void Seek(const Slice& target);
	virtual void Next();
	virtual void Prev();
	virtual Slice key() const;
	virtual Slice value() const;
	virtual Status status() const;

private:
	void FindSmallest();
	void FindeLargest();

private:
	const Comparator* comparator_;
	int n_;

	IteratorWrapper* children_;
	IteratorWrapper* current_;

	Direction direction_;
};

MerginIterator::MerginIterator(const Comparator* cp, Iterator** children, int n)
	: comparator_(cp), children_(new IteratorWrapper[n]), n_(n), current_(NULL), direction_(kForward)
{
	for(int i = 0; i < n; i ++){
		children_[i].Set(children[i]);
	}
}

MerginIterator::~MerginIterator()
{
	delete []children_;
}

bool MerginIterator::Valid() const
{
	return (current_ != NULL);
}

void MerginIterator::SeekToFirst()
{
	for(int i = 0; i < n_; i++) //所有的iters都回到first
		children_[i].SeekToFirst();

	//在iters中挑选最小的做为第一个
	FindSmallest();
	direction_ = kForward;
}

void MerginIterator::SeekToLast()
{
	for(int i = 0; i < n_; i ++) //所有的iters都定位到LAST
		children_[i].SeekToLast();

	//在iters中挑选最大的作为最后一个
	FindeLargest();
	direction_ = kReverse;
}

void MerginIterator::Seek(const Slice& target)
{
	for(int i = 0; i < n_; i ++)
		children_[i].Seek(target);

	FindSmallest();
	direction_ = kForward;
}

//指向下一个(key,value)，仅次于大于当前key,有可能在不同的IteratorWrapper中
void MerginIterator::Next()
{
	assert(Valid());
	if(direction_ != kForward){ //判断是从头到尾
		for(int i = 0; i < n_; i++){
			IteratorWrapper* child = &children_[i];
			if(child != current_){
				child->Seek(key()); //定位key的值
				if(child->Valid() && (comparator_->Compare(key(), child->key()) == 0)){
					child->Next();
				}
			}
		}
		direction_ = kForward;
	}

	current_->Next();
	//在当前所有的children_->key中找最小的
	FindSmallest();
}

//指向上一个(key,value)，仅次于小于当前key,有可能在不同的IteratorWrapper中
void MerginIterator::Prev()
{
	assert(Valid());
	if(direction_ != kReverse){
		for(int i = 0; i < n_; i ++){
			IteratorWrapper* child = &children_[i];
			if(child != current_){
				child->Seek(key());
				if(child->Valid())//向前走移动
					child->Prev();
				else //定位到最后
					child->SeekToLast();
			}
		}
		direction_ = kReverse;
	}

	current_->Prev();
	//在当前所有children_->key()中找最大的
	FindeLargest();
}

Slice MerginIterator::key() const
{
	assert(Valid());
	return current_->key();
}

Slice MerginIterator::value() const
{
	assert(Valid());
	return current_->value();
}

Status MerginIterator::status() const 
{
	Status status;
	for(int i = 0; i < n_; i ++){
		status = children_[i].status(); 
		if(!status.ok()) //检查是否有错误,有错误立即返回
			break;
	}

	return status;
}

//在所有的children当中找到最小的KEY
void MerginIterator::FindSmallest()
{
	IteratorWrapper* smallest = NULL;
	for(int i = 0; i < n_; i ++){
		IteratorWrapper* child = &children_[i];
		if(child->Valid()){
			if(smallest == NULL)
				smallest = child;
			else if(comparator_->Compare(child->key(), smallest->key()) < 0) //找到更小的
				smallest = child;
		}
	}
	current_ = smallest;
}

//在所有的children当中找到最大的KEY
void MerginIterator::FindeLargest()
{
	IteratorWrapper* largest = NULL;
	for(int i = n_ - 1; i > 0; i --){
		IteratorWrapper* child = &children_[i];
		if(child->Valid()){
			if(largest == NULL)
				largest = child;
			else if(comparator_->Compare(child->key(), largest->key()) > 0)
				largest = child;
		}
	}
	current_ = largest;
}

Iterator* NewMergingIterator(const Comparator* comparator, Iterator** list, int n)
{
	assert(n >= 0);
	if(n == 0)
		return NewEmptyIterator();
	else if(n == 1)
		return list[0];
	else
		return new MerginIterator(comparator, list, n); //产生一个MeringIterator
}


}//leveldb







