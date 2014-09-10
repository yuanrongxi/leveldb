#include "format.h"
#include "env.h"
#include "port.h"
#include "block.h"
#include "coding.h"
#include "crc32c.h"

namespace leveldb{

//将blockhandle编码到dst当中
void BlockHandle::EncodeTo(std::string* dst) const
{
	assert(offset_ != ~static_cast<uint64_t>(0));
	assert(size_  != ~static_cast<uint64_t>(0));
	PutVarint64(dst, offset_);
	PutVarint64(dst, size_);
}

//从input当中解码block handle的信息
Status BlockHandle::DecodeFrom(Slice* input)
{
	if(GetVarint64(input, &offset_) && GetVarint64(input, &size_)) //解码成功
		return Status::OK();
	else
		return Status::Corruption("bad block handle");
}

//将footer进行编码到dst
void Footer::EncodeTo(std::string* dst) const
{
#ifndef NDEBUG
	const size_t orginal_size = dst->size();
#endif
	metaindex_handle_.EncodeTo(dst); //16字节
	index_handle_.EncodeTo(dst); //16字节

	dst->resize(2 * BlockHandle::kMaxEncodedLength);
	//写入一个特征字
	PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu)); //4字节
	PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32)); //4字节

	assert(dst->size() == orginal_size + kEncodedLength);
}

//从input解码footer的信息
Status Footer::DecodeFrom(Slice* input)
{
	const char* magic_ptr = input->data() + kEncodedLength - 8;
	const uint32_t magic_lo = DecodeFixed32(magic_ptr);
	const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
	//特征字校验
	const uint64_t magic = (static_cast<uint64_t>(magic_hi) << 32 | static_cast<uint64_t>(magic_lo));
	if(magic != kTableMagicNumber){
		return Status::InvalidArgument("not an sstable(bad magic number)");
	}

	//解码meta handler
	Status result = metaindex_handle_.DecodeFrom(input);
	if(result.ok()) //解码handler
		result = index_handle_.DecodeFrom(input);

	if(result.ok()){
		const char* end = magic_ptr + 8;
		*input = Slice(end, input->data() + input->size() - end); //更新input的值，去掉了头的值
	}
}

Status ReadBlock(RandomAccessFile* file, const ReadOptions& options, const BlockHandle& handle, BlockContents* result)
{
	result->data = Slice();
	result->cachable = false;
	result->heap_allocated = false;

	size_t n = static_cast<size_t>(handle.size());
	char* buf = new char[n + kBlockTrailerSize];
	//从file的索引偏移位置读取n + kBlockTrailerSize长度的数据到buf中，并隐射contents
	Slice contents;
	Status s = file->Read(handle.offset(), n + kBlockTrailerSize, &contents, buf);
	if (!s.ok()) {
		delete[] buf;
		return s;
	}

	const char* data = contents.data();
	if(options.verify_checksums){
		const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1)); //获得CRC
		const uint32_t actual = crc32c::Value(data, n + 1); //计算DATA的CRC
		if(actual != crc){ //CRC校验
			delete []buf;
			s = Status::Corruption("block checksum mismatch");
			return s;
		}
	}

	switch(data[n]){
	case kNoCompression: //无压缩数据
		if(data != buf){ //MMAP模式
			delete []buf;
			result->data = Slice(data, n);
			result->heap_allocated = false;
			result->cachable = false;
		}
		else{ //pread模式
			result->data = Slice(buf, n);
			result->heap_allocated = true;
			result->cachable = true;
		}
		break;

	case kSnappyCompression:{ //snappy压缩
			size_t ulength = 0;
			if(!port::Snappy_GetUncompressedLength(data, n, &ulength)){ //snappy 解压长度
				delete []buf;
				return Status::Corruption("corrupted compressed block contents");
			}

			//进行数据解压
			char* ubuf = new char[ulength];
			if(!port::Snappy_Uncompress((data, n, ubuf))){
				delete []buf;
				delete []ubuf;
				return Status::Corruption("corrupted compressed block contents");
			}

			delete buf;

			result->data = Slice(ubuf, ulength);
			result->heap_allocated = true;
			result->cachable = true;
		}
		break;

	default:
		delete []buf;
		return Status::Corruption("bad block type");
	}

	return Status::OK();
}


} //leveldb
