#ifndef __LEVEL_DB_FORMAT_H_
#define __LEVEL_DB_FORMAT_H_

#include <string>
#include <stdint.h>
#include "slice.h"
#include "status.h"
#include "table_builder.h"

namespace leveldb{
class Block;
class RandomAccessFile; //pread-mmapģʽ
struct ReadOptions;

class BlockHandle
{
public:
	BlockHandle();

	uint64_t offset() const {return offset_;}
	void set_offset(uint64_t offset){offset_ = offset;}

	uint64_t size() const {return size_;}
	void set_size(uint64_t size){size_ = size;};

	void EncodeTo(std::string* dst) const;
	Status DecodeFrom(Slice* input);

	enum{
		kMaxEncodedLength = 10 + 10 //20
	};
private:
	uint64_t offset_;
	uint64_t size_;
};

class Footer
{
public:
	Footer() {}

	const BlockHandle& metaindex_handle() const { return metaindex_handle_; };
	void set_metaindex_handle(const BlockHandle& h){ metaindex_handle_ = h; };

	const BlockHandle& index_handle() const
	{
		return index_handle_;
	}

	void set_index_handle(const BlockHandle& h)
	{
		index_handle_ = h;
	}

	void EncodeTo(std::string* dst) const;
	Status DecodeFrom(Slice* input);

	enum{ kEncodedLength = 2 * BlockHandle::kMaxEncodedLength + 8}; //48

private:
	BlockHandle metaindex_handle_;
	BlockHandle index_handle_;
};

}

#endif
