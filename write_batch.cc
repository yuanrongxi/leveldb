#include "write_batch.h"
#include "db.h"
#include "dbformat.h"
#include "memtable.h"
#include "Write_batch_internal.h"
#include "coding.h"

namespace leveldb{

//前面8个字节是SequenceNumber，紧接着4字节是表示key value的个数
static const size_t kHeader = 12;

WriteBatch::WriteBatch()
{
	Clear();
}

WriteBatch::~WriteBatch()
{
}

void WriteBatch::Clear() 
{
	rep_.clear();
	rep_.resize(kHeader);
}

//进行批量写
Status WriteBatch::Iterate(Handler* handler) const
{
	Slice input(rep_);

	if(input.size() < kHeader)
		return Status::Corruption("malformed WriteBatch (too small)");

	input.remove_prefix(kHeader);

	Slice key, value;
	int found = 0;
	while(!input.empty()){
		found ++;
		char tag = input[0];
		input.remove_prefix(1);

		switch(tag)
		{
		case kTypeValue: //增加或者修改？
			if(GetLengthPrefixedSlice(&input, &key) && GetLengthPrefixedSlice(&input, &value))
				handler->Put(key, value);
			else
				return Status::Corruption("bad WriteBatch Put");
			break;

		case kTypeDeletion: //删除
			if(GetLengthPrefixedSlice(&input, &key))
				handler->Delete(key);
			else
				return Status::Corruption("bad WriteBatch Delete");
			break;


		default:
			return Status::Corruption("unknown WriteBatch tag");
		}
	}

	if(found != WriteBatchInternal::Count(this))
		return Status::Corruption("WriteBatch has wrong count");
	else
		return Status::OK();
}

int WriteBatchInternal::Count(const WriteBatch* b)
{
	return DecodeFixed32(b->rep_.data() + 8);
}

void WriteBatchInternal::SetCount(WriteBatch* b, int n)
{
	EncodeFixed32(&b->rep_[8], n);
}

SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b)
{
	return SequenceNumber(DecodeFixed64(b->rep_.data()));
}

void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq)
{
	EncodeFixed64(&b->rep_[0], seq);
}

void WriteBatch::Put(const Slice& key, const Slice& value)
{
	WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
	rep_.push_back(static_cast<char>(kTypeValue));
	PutLengthPrefixedSlice(&rep_, key);
	PutLengthPrefixedSlice(&rep_, value);
}

void WriteBatch::Delete(const Slice& key)
{
	WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
	rep_.push_back(static_cast<char>(kTypeDeletion)); //删除标志
	PutLengthPrefixedSlice(&rep_, key);
}

namespace {

class MemTableInserter : public WriteBatch::Handler
{
public:
	SequenceNumber sequence_; //序号？
	MemTable* mem_; //memtable句柄

	virtual void Put(const Slice& key, const Slice& value)
	{
		mem_->Add(sequence_, kTypeValue, key, value);
		sequence_ ++;
	}

	virtual void Delete(const Slice& key)
	{
		mem_->Add(sequence_, kTypeDeletion, key, Slice());
		sequence_ ++;
	}
};
};

Status WriteBatchInternal::InsertInto(const WriteBatch* b, MemTable* memtable)
{
	MemTableInserter inserter;
	inserter.sequence_ = WriteBatchInternal::Sequence(b);
	inserter.mem_ = memtable;
	return b->Iterate(&inserter);
}

void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents)
{
	assert(contents.size() >= kHeader);
	b->rep_.assign(contents.data(), contents.size());
}

void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src)
{
	SetCount(dst, Count(dst) + Count(src));
	assert(src->rep_.size() >= kHeader);
	dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
}


}; //leveldb







