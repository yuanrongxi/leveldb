#include "memtable.h"
#include "dbformat.h"
#include "comparator.h"
#include "env.h"
#include "coding.h"

namespace leveldb{

static Slice GetLengthPrefixedSlice(const char* data)
{
	uint32_t len;
	const char* p = data;
	p = GetVarint32Ptr(p, p + 5, &len);

	return Slice(p, len);
}

MemTable::~MemTable()
{
	assert(refs_ == 0);
}

//总的内存分配数量
size_t MemTable::ApproximateMemoryUsage()
{
	return arena_.MemoryUsage();
}

int MemTable::KeyComparator::operator()(const char* aptr, const char* bptr) const
{
	Slice a = GetLengthPrefixedSlice(aptr);
	Slice b = GetLengthPrefixedSlice(bptr);
	return comparator.Compare(a, b);
}

//target size + target
static const char* EncodeKey(std::string* scratch, const Slice& target) 
{
	scratch->clear();
	PutVarint32(scratch, target.size());
	scratch->append(target.data(), target.size());
	return scratch->data();
}

//定义一个内存table的迭代器
class MemTableIterator : public Iterator
{
public:
	explicit MemTableIterator(MemTable::Table* table) : iter_(table){};

	virtual bool Valid() const {return iter_.Valid();};
	
	virtual void Seek(const Slice& k) {iter_.Seek(EncodeKey(&tmp_, k));};
	virtual void SeekToFirst(){iter_.SeekToFirst();};
	virtual void SeekToLast(){iter_.SeekToLast();};
	virtual void Next() {iter_.Next();};
	virtual void Prev() {iter_.Prev();};

	virtual Slice key() const
	{
		return GetLengthPrefixedSlice(iter_.key());
	}

	virtual Slice value() const
	{
		Slice key_slice = GetLengthPrefixedSlice(iter_.key());
		return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
	}

	virtual Status status() const{return Status::OK();};

private:
	MemTableIterator(const MemTableIterator&);
	void operator=(const MemTableIterator&);

private:
	MemTable::Table::Iterator iter_; //跳表的迭代器
	std::string tmp_;
};

Iterator* MemTable::NewIterator()
{
	return new MemTableIterator(&table_);
}

void MemTable::Add(SequenceNumber s, ValueType type, const Slice& key, const Slice& value)
{
	size_t key_size = key.size();
	size_t val_size = value.size();
	size_t internal_key_size = key_size + 8; //seq + type
	const size_t encode_len = VarintLength(internal_key_size) + internal_key_size + VarintLength(val_size) + val_size;
	//分配一个缓冲区存key + value
	char* buf = arena_.Allocate(encode_len);
	char* p = EncodeVarint32(buf, internal_key_size);
	memcpy(p, key.data(), key_size);
	p += key_size;
	EncodeFixed64(p, (s << 8) | type);
	p += 8;
	p = EncodeVarint32(p, val_size);
	memcpy(p, value.data(), val_size);
	//对长度进行检验
	assert((p + val_size) - buf == encoded_len);
	//将编码后的值（key_size + key + seq + value_size + value）插入到内存跳表中
	table_.Insert(buf);
}

bool MemTable::Get(const LookupKey& key, std::string* value, Status* s)
{
	Slice memkey = key.memtable_key();
	Table::Iterator iter(&table_);
	
	//定位到key对应的iter
	iter.Seek(memkey.data());
	if(iter.Valid()){
		const char* entry = iter.key();
		uint32_t key_length;
		//获得KEY的长度
		const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
		//比较user key
		if(comparator_.comparator.user_comparator()->Compare(Slice(key_ptr, key_length - 8), key.user_key()) == 0){
			//解码seq + type
			const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
			//计算type
			switch(static_cast<ValueType>(tag & 0xff))
			{
			case kTypeValue:
				{
					Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
					value->assign(v.data(), v.size());
					return true;
				} 

			case kTypeDeletion: //被删除了
				*s = Status::NotFound(Slice());
				return true;
			}
		}
	}

	return false;
}

};//leveldb



