#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "cache.h"
#include "port.h"
#include "hash.h"
#include "mutexlock.h"

namespace leveldb {
namespace{

//冷热置换算法(LRU)
struct LRUHandle
{
	void* value;
	void (deleter)(const Slice& key, void* value);
	LRUHandle* next_hash;
	LRUHandle* next;
	LRUHandle* prev;
	size_t charge;		
	size_t key_length;	//key的长度
	uint32_t refs;		//引用计数
	uint32_t hash;		//key的HASH
	char key_data[1];	//KEY数据起始位置

	Slice key() const
	{
		if(next == this){
			return *(reinterpret_cast<Slice *>(value));
		}
		else{
			return Slice(key_data, key_length);
		}
	}
};

//一个LRUHandle的HASH TABLE
class HandleTable
{
public:
	HandleTable():length_(0), elems_(0), list_(NULL)
	{
		Resize();
	}

	~HandleTable()
	{
		delete []list_;
	}

	LRUHandle* Lookup(const Slice& key, uint32_t hash)
	{
		return *FindPointer(key, hash);
	}

	LRUHandle* Insert(LRUHandle* h)
	{
		LRUHandle** ptr = FindPointer(h->key(), h->hash);
		//如果key hash存在，置换掉老的handle
		LRUHandle* old = *ptr;
		h->next_hash = (old == NULL ? NULL : old->next_hash);
		*ptr = h;
		if(old == NULL){ //新开了了一个桶，
			++ elems_;
			if(elems_ > length_) //尺寸不够，rehash
				Resize();

		}

		return old;
	}

	LRUHandle* Remove(const Slice& key, uint32_t hash)
	{
		LRUHandle** ptr = FindPointer(key, hash);
		LRUHandle* result = *ptr;
		if(result != NULL){
			*ptr = result->next_hash;
			--elems_;
		}

		return result;
	}

private:
	LRUHandle** FindPointer(const Slice& key, uint32_t hash)
	{
		LRUHandle** ptr = &list_[hash & (length_ - 1)];
		while(*ptr != NULL && ((*ptr)->hash != hash || key != (*ptr)->key())){ //查找是否已经存在hash的handle，如果不存在，直接返回起始位置
			ptr = &(*ptr)->next_hash;
		}

		return ptr;
	}

	//rehash过程
	void Resize()
	{
		uint32_t new_length = 4;
		while(new_length < elems_)
			new_length *= 2;

		//开辟一个更大的数组
		LRUHandle** new_list = new LRUHandle*[new_length];
		memset(new_list, 0x00, sizeof(new_list[0]) * new_length);
		//将list_中的数据重新设置到new list当中
		uint32_t count = 0;
		for(uint32_t i = 0; i < length_; i ++){
			LRUHandle* h = list_[i];
			while(h != NULL){
				//进行h的状态保存
				LRUHandle* next = h->next_hash;
				uint32_t hash = h->hash;
				//查找在new list的位置
				LRUHandle** ptr = &new_list[hash & (new_length - 1)];
				//插入到对应位置的起始
				h->next_hash = *ptr;
				*ptr = h;
				//进行下一个handle的设置
				h = next;
				count ++;
			}
		}

		//更新list的信息，并释放掉原先的list空间
		assert(elems_ == count);
		delete []list_;
		list_ = new_list;
		length_ = new_length;
	}

private:
	uint32_t length_;		//list数组长度
	uint32_t elems_;		//list有效单元个数
	LRUHandle** list_;	
};


class LRUCache
{
public:
	LRUCache();
	~LRUCache();

	void SetCapacity(size_t capacity) 
	{
		capacity_ = capacity;
	}

	Cache::Handle* Insert(const Slice& key, uint32_t hash, void* value, size_t charge, void (*deleter)(const Slice& key, void* v));
	Cache::Handle* Lookup(const Slice& key, uint32_t hash);
	void Release(Cache::Handle* handle);
	void Erase(const Slice& key, uint32_t hash);

private:
	void LRU_Remove(LRUHandle* e);
	void LRU_Append(LRUHandle* e);
	void Unref(LRUHandle* e);

private:
	size_t capacity_;

	port::Mutex mutex_;
	size_t usage_;

	LRUHandle lru_;

	HandleTable table_;
};

LRUCache::LRUCache() : usage_(0)
{
	lru_.next = &lru_;
	lru_.prev = &lru_;
}

LRUCache::~LRUCache()
{
	for(LRUHandle* e = lru_.next; e != &lru_; ){
		LRUHandle* next = e->next;
		assert(e->refs == 1);
		Unref(e);
		e = next;
	}
}

void LRUCache::Unref(LRUHandle* e)
{
	assert(e->refs > 0);
	e->refs --;
	if(e->refs <= 0){ //引用计数结束，释放掉handle
		usage_ -= e->charge;
		(e->deleter)(e->key(), e->value);
		free(e);
	}
}

void LRUCache::LRU_Remove(LRUHandle* e)
{
	e->next->prev = e->prev;
	e->prev->next = e->next;
}

void LRUCache::LRU_Append(LRUHandle* e)
{
	e->next = &lru_;
	e->prev = lru_.prev;
	e->prev->next = e;
	e->next->prev = e;
}

Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash)
{
	MutexLock l(&mutex_);
	LRUHandle* e = table_.Lookup(key, hash);
	if(e != NULL){
		e->refs ++;
		LRU_Remove(e);
		LRU_Append(e);
	}

	return reinterpret_cast<Cache::Handle*>(e);
}

void LRUCache::Release(Cache::Handle* handle)
{
	MutexLock l(&mutex_);
	Unref(reinterpret_cast<LRUHandle *>(handle));
}

Cache::Handle* LRUCache::Insert(const Slice& key, uint32_t hash, void* value, size_t charge, void (*deleter)(const Slice& key, void* v))
{
	MutexLock l(&mutex_);
	//减1是LRUHandle::key_data[1]定义了1个字节的空间
	LRUHandle* e = (LRUHandle *)malloc(sizeof(LRUHandle) - 1 + key.size());
	e->value = value;
	e->deleter = deleter;
	e->charge = charge;
	e->key_length = key.size();
	e->hash = hash;
	//LRUCahce应用了1次，return e也算1次
	e->refs = 2;
	memcpy(e->key_data, key.data(), key.size());
	LRU_Append(e);
	usage_ += charge;

	//删除置换出来的handle
	LRUHandle* old = table_.Insert(e);
	if(old != NULL){
		LRU_Remove(old);
		Unref(old);
	}

	//容量大小判断，如果超标，进行删除
	while(usage_ > capacity_ && lru_.next != &lru_){
		LRUHandle* old = lru_.next;
		LRU_Remove(old);
		table_.Remove(old->key(), old->hash);
		Unref(old);
	}

	return reinterpret_cast<Cache::Handle*>(e);
}

void LRUCache::Erase(const Slice& key, uint32_t hash)
{
	MutexLock l(&mutex_);
	LRUHandle* e = table_.Remove(key, hash);
	if(e != NULL){
		LRU_Remove(e);
		Unref(e);
	}
}

static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;

class ShardedLRUCache : public Cache
{
public:
	explicit ShardedLRUCache(size_t capacity) : last_id_(0)
	{
		//计算每一个shard的capacity
		const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
		for(int s = 0; s < kNumShards; s ++){
			shard_[s].SetCapacity(per_shard);
		}
	}

	virtual ~ShardedLRUCache(){};

	virtual Handle* Insert(const Slice& key, void* value, size_t charge, void (*deleter)(const Slice& key, void* value))
	{
		const uint32_t hash = HashSlice(key);
		return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
	}

	virtual Handle* Lookup(const Slice& key)
	{
		const uint32_t hash = HashSlice(key);
		return shard_[Shard(hash)].Lookup(key, hash);
	}

	virtual void Release(Handle* handle)
	{
		LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
		shard_[Shard(h->hash)].Release(handle);
	}

	virtual void Erase(const Slice& key)
	{
		const uint32_t hash = HashSlice(key);
		shard_[Shard(hash)].Erase(key);
	}

	virtual void* Value(Handle* handle) 
	{
		return reinterpret_cast<LRUHandle*>(handle)->value;
	}

	virtual uint64_t NewId() 
	{
		MutexLock l(&id_mutex_);
		return ++(last_id_);
	}

private:
	static inline uint32_t HashSlice(const Slice& s)
	{
		return Hash(s.data(), s.size(), 0);
	}

	static uint32_t Shard(uint32_t hash)
	{
		return hash >> (32 - kNumShardBits); //hash % 16
	}

private:
	LRUCache shard_[kNumShards];
	port::Mutex id_mutex_;
	uint64_t last_id_;

};
}

//创建LRUCache实例
Cache* NewLRUCache(size_t capacity) 
{
		return new ShardedLRUCache(capacity);
}

}




