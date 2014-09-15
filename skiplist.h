#ifndef __LEVEL_DB_SKIPLIST_H_
#define __LEVEL_DB_SKIPLIST_H_

#include <assert.h>
#include <stdlib.h>
#include "port.h"
#include "arena.h"
#include "random.h"

namespace leveldb{

class Arena;


template<typename Key, class Comparator>
class SkipList
{
private:
	struct Node;

public:
	explicit SkipList(Comparator cmp, Arena* arena);
	void Insert(const Key& key);
	void Contains(const Key& key) const;

	class Iterator
	{
	public:
		Iterator(const SkipList* list);
		bool Valid() const;
		const Key& key() const;
		void Next();
		void Prev();
		void Seek(const Key& target);
		void SeekToFirst();
		void SeekToLast();

	private:
		const SkipList* list;
		Node* node_;
	};

private:
	enum{
		kMaxHeight = 12,
	};

	Comparator const compare_;
	Arena* const arena_;
	Node* const head_;

	port::AtomicPointer max_height_;

	inline int GetMaxHeight() const{
		return static_cast<int>(reinterpret_cast<intptr_t>(max_height_.NoBarrier_Load()));
	}

	Random rnd_;

private:
	Node* NewNode(const key& key, int height);
	int RandomHeigth();
	bool Equal(const Key& a, const Key& b) const 
	{
		return (compare_(a,b) == 0);
	}

	bool KeyIsAfterNode(const Key& key, Node* n) const;
	Node* FindGreaterOrEqual(const Key& key, Node** prev) const;
	Node* FindLessThan(const Key& key) const;
	Node* FindLast() const;

	SkipList(const SkipList&);
	void operator=(const SkipList&);
};

template<typename Key, class Comparator>
struct SkipList<Key, Comparator>::Node
{
	explicit Node(const Key& k) : key(k){};

	Key const key;
	Node* Next(int n)
	{
		assert(n >= 0);
		return reinterpret_cast<Node*>(next_[n].Acquire_Load()); //从内存中重新导入next_[n],房子cpu cache不一致
	}

	void SetNext(int n, Node* x)
	{
		next_[n].Release_Store(x); //回写到内存中
	}

	Node* NoBarrier_Next(int n)
	{
		assert(n >= 0);
		return reinterpret_cast<Node*>(next_[n].NoBarrier_Load()); //直接从变量中来，有可能变量在内存中，有可能变量在寄存器或者CPU CACHE当中
	}

	void NoBarrier_SetNext(int n, Node* x)
	{
		 next_[n].NoBarrier_Store(x);
	}

private:
	port::AtomicPointer next_[1];
};

//创建分配一个NODE,
template<typename Key, class Comparator>
typedef SkipList<Key, Comparator>::Node*
	SkipList<Key, Comparator>::NewNode(const Key& key, int height)
{
	char* mem = arena_->AllocateAligned(sizeof(Node) + sizeof(port::AtomicPointer) * (height - 1));
	return new (mem) Node(key);
}

template<typename Key, class Comparator>
inline SkipList<Key, Comparator>::Iterator::Iterator(const SkipList* list)
{
	list_ = list;
	node_ = NULL;
}

template<typename Key, class Comparator>
inline bool SkipList<Key, Comparator>::Iterator::Valid() const
{
	return Node != NULL;
}

template<typename Key, class Comparator>
inline const Key& SkipList<Key, Comparator>::Iterator::key() const
{
	assert(Valid());
	return node_->key;
}

template<typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Next()
{
	assert(Valid());
	node_ = node_->Next(0);
}

template<typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Prev()
{
	assert(Valid());
	node_ = list_->FindLessThan(node_->key);
	if(node_ == list_->head_){
		node_ = NULL;
	}
}

template<typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Seek(const Key& target)
{
	node_ = list_->FindGreaterOrEqual(target, NULL);
}

template<typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToFirst()
{
	node_ = list_->head_->Next(0);
}

template<typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToLast()
{
	node_ = list_->FindLast();
	if(node_ == list_->head_)
		node_ = NULL;
}

template<typename Key, class Comparator>
int SkipList<Key, Comparator>::RandomHeigth()
{
	static const unsigned int kBranching = 4;
	int height = 1;
	while(height < kMaxHeight && (rnd_.Next() % kBranching))
		height ++;

	assert(height > 0);
	assert(height <= kMaxHeight);
	return height;
}

//判断key是否处于node的后面，因为skiplist是有序的(n.key < key)
template<typename Key, class Comparator>
bool SkipList<Key, Comparator>::KeyIsAfterNode(const Key& key, Node* n) const
{
	return (n != NULL) && (compare_(n->key, key) < 0);
}

//找到key在跳表中的位置,复杂度近似O(logN)
template<typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::FindGreaterOrEqual(const Key& key, Node** prev) const
{
	Node* x = head_;
	int level = GetMaxHeight() - 1; //从最大跳跃的地方开始找
	while(true){
		Node* next = x->Next(level);
		if(KeyIsAfterNode(key, next)){ //key还是比next大，继续向后找
			x = next;
		}
		else{
			if(prev != NULL) //保存前一个层的节点，一遍建立层级关系
				prev[level] = x;

			if(level == 0) //已经到跳表最底一层了，此位置就是
				return next;
			else  //向下一层搜索
				level --;
		}
	}
}

template<typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::FindLast() const
{
	Node* x = head_;
	int level = GetMaxHeight() - 1; //从最高层一直往最底层移动
	while(true){
		Node* next = x->Next(level);
		if(next == NULL){
			if(level == 0) //已经到最底层的最后一个node
				return x;
			else
				level --;
		} 
		else 
			x = next;
	}
} 

template<typename Key, class Comparator>
SkipList<Key, Comparator>::SkipList(Comparator cmp, Arena* arena)
	: compare_(cmp), arena_(arena), head_(NewNode(0, kMaxHeight)),
	max_height_(reinterpret_cast<void*>(1)), rnd_(0xdeadbeef)
{
	for(int i = 0; i < kMaxHeight; i++)
		head_->SetNext(i, NULL);
}

template<typename Key, class Comparator>
void SkipList<Key, Comparator>::Insert(const Key& key)
{
	Node* prev[kMaxHeight];
	//找到key在跳表中的位置
	Node* x = FindGreaterOrEqual(key, prev);
	assert(x == NULL || !Equal(key, x->key));

	int height = RandomHeigth();
	if(height > GetMaxHeight()){
		for(int i = GetMaxHeight(); i < height; i ++)
			prev[i] = head_;

		//设置新的最大的高度
		max_height_.NoBarrier_Store(reinterpret_cast<void*>(height));
	}

	//新建一个key node
	x = NewNode(key, height);
	for(int i = 0; i < height; i ++){
		x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i)); //还未在skip list中，可以不必强制回写
		prev[i]->SetNext(i, x); //prev是在跳表中，必须强制CPU回写内存
	}
}

template<typename Key, class Comparator>
bool SkipList<Key, Comparator>::Contains(const Key& key) const
{
	Node* x = FindGreaterOrEqual(key, NULL); //找到对应key的skip list位置
	if(x != NULL && Equal(key, x->key))
		return true;
	else
		return false;
}

};

#endif
