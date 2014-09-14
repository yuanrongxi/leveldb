#include "log_write.h"
#include <stdint.h>
#include "env.h"
#include "coding.h"
#include "crc32c.h"

namespace leveldb{
namespace log{

Writer::Writer(WritableFile* dest) : dest_(dest), block_offset_(0)
{
	for(int i = 0; i <= kMaxRecordType; i++){
		char t = static_cast<char>(i);
		type_crc_[i] = crc32c::Value(&t, 1);
	}
}

Writer::~Writer()
{
}

Status Writer::AddRecord(const Slice& slice)
{
	const char* ptr = slice.data();
	size_t left = slice.size();

	Status s;
	bool begin = true;
	do{
		const int leftover = kBlockSize - block_offset_;
		assert(leftover >=0);

		//32K为一块
		if(leftover < kHeaderSize){
			if(leftover > 0){
				assert(kHeaderSize == 7);
				dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover)); //最后不足7个字节，全部补齐0,让32k的块完整
			}
			//从新启动一个新的32K的block
			block_offset_ = 0;
		}

		assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

		//计算可以用的字节数量
		const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
		const size_t fragment_length = (left < avail) ? left : avail;

		//剩下的长度能一次存储slice
		/*|kFullType|*/
		//剩下的长度无法存储slice就会按照以下方式存储
		/*|kFistType|kMiddleType1|kMiddleType2|....|kLastType|*/
		RecordType type;
		const bool end = (left == fragment_length);
		if(begin && end)
			type = kFullType; //开始位置将日志一次写入文件中
		else if(begin)
			type = kFirstType;//第一块标志，表示开始块
		else if(end)
			type = kLastType; //最后一块标志，表示日志结束块
		else	
			type = kMiddleType; //中间块标志，可能多块

		//进行写入fragment
		s = EmitPhysicalRecord(type, ptr, fragment_length);
		//位置进行前移
		ptr += fragment_length;
		left -= fragment_length;
		//更改日志开始标志
		begin = false;
	}while(s.ok() && left > 0);

	return s;
}

/*日志分片格式:|crc32|fragment size|type|data|*/
Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr, size_t n)
{
	assert(n <= 0xffff);
	assert(block_offset_ + kHeaderSize + n <= kBlockSize);

	//格式化日志头
	char buf[kHeaderSize];
	//将长度打包
	buf[4] = static_cast<char>(n & 0xff);
	buf[5] = static_cast<char>(n >> 8);
	//打包fragment type（日志分片类型）
	buf[6] = static_cast<char>(t);

	//前面4字节是CRC校验码,计算CRC
	uint32_t crc = crc32c::Extend(type_crc_[t], ptr, n);
	crc = crc32c::Mask(crc);
	EncodeFixed32(buf, crc);
	
	Status s = dest_->Append(Slice(buf, kHeaderSize));
	if(s.ok()){
		s = dest_->Append(Slice(ptr, n));
		if(s.ok())
			s = dest_->Flush();
	}
	//改变正在写块的偏移量
	block_offset_ += kHeaderSize + n;
	return s;
}

};

};










