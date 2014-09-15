#include <stdio.h>
#include "log_reader.h"
#include "env.h"
#include "coding.h"
#include "crc32c.h"

namespace leveldb{
namespace log{

Reader::Reporter::~Reporter()
{
}

Reader::Reader(SequentialFile* file, Reporter* reporter, bool checksum, uint64_t initial_offset)
	: file_(file), reporter_(reporter), checksum_(checksum),
	  buffer_(), eof_(false), end_of_buffer_offset_(0),
	  initial_offset_(initial_offset), backing_store_(new char[kBlockSize])
{
	
}

Reader::~Reader()
{
	delete []backing_store_;
}

bool Reader::SkipToInitialBlock()
{
	//计算所在块的偏移
	size_t offset_in_block = initial_offset_ % kBlockSize;
	uint64_t block_start_location = initial_offset_ - offset_in_block; //initial_offset_偏移位置上所有整块的总长度
	
	//末尾可能是0填充过的，最后不携带头信息（7字节）
	if(offset_in_block > kBlockSize - 6){
		offset_in_block = 0;
		block_start_location += kBlockSize;
	}

	//确定最后完整块的总偏移
	end_of_buffer_offset_ = block_start_location;

	if(block_start_location > 0){
		Status skip_status = file_->Skip(block_start_location); //检查是读文件中否有block_start_location这么长的位置，如果有就seek到这个偏移上
		if(!skip_status.ok()){
			ReportDrop(block_start_location, skip_status);
			return false;
		}
	}
	
	return true;
}

bool Reader::ReadRecord(Slice* record, std::string* scratch)
{
	if(last_record_offset_ < initial_offset_){
		if(!SkipToInitialBlock())
			return false;
	}

	scratch->clear();
	record->clear();
	bool in_fragment_record = false;

	uint64_t prospective_record_offset = 0;

	//32K为一个块，一块中有多个fragment
	Slice fragment;
	while(true){
		uint64_t physical_record_offset = end_of_buffer_offset_ - buffer_.size();
		
		//获得分片类型
		const unsigned int record_type = ReadPhysicalRecord(&fragment);
		switch(record_type)
		{
		case kFullType:
			if(in_fragment_record){
				if(scratch->empty())
					in_fragment_record = false;
				else
					ReportCorruption(scratch->size(), "partial record without end(1)");
			}
			prospective_record_offset = physical_record_offset;
			scratch->clear();
			
			*record = fragment;
			last_record_offset_ = physical_record_offset;
			return true;

		case kFirstType:
			if(in_fragment_record){ //连续fragment标识（由first middle last等多个fragment组成的一个log）
				if(scratch->empty()){
					in_fragment_record = false;
				}
				else{
					ReportCorruption(scratch->size(), "partial record without end(2)");
				}
			}

			prospective_record_offset = physical_record_offset;
			scratch->assign(fragment.data(), fragment.size());
			in_fragment_record = true;
			break;

		case kMiddleType:
			if(!in_fragment_record) //不连续标识，middle是一定连续的
				ReportCorruption(fragment.size(), "missing start of fragmented record(1)");
			else
				scratch->append(fragment.data(), fragment.size());
			break;

		case kLastType:
			if(!in_fragment_record){ //LastType前面一定是first和middle
				ReportCorruption(fragment.size(), "missing start of fragmented record(2)");
			}
			else{
				scratch->append(fragment.data(), fragment.size());
				*record = Slice(*scratch);
				last_record_offset_ = prospective_record_offset;
				return true;
			}
			break;

		case kEof:
			if(in_fragment_record){ //假如是连续的，但是文件读取到了末尾，这是个磁盘错误
				ReportCorruption(scratch->size(), "partial record without end(3)");
				scratch->clear();
			}
			return false;

		case kBadRecord:
			if (in_fragment_record){
				ReportCorruption(scratch->size(), "error in middle of record");
				in_fragment_record = false;
				scratch->clear();
			}
			break;

		default:
			{
				char buf[40];
				snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
				ReportCorruption((fragment.size() + (in_fragment_record ? scratch->size() : 0)), buf);
				in_fragment_record = false;
				scratch->clear();

				break;
			}
		}
	}

	return false;
}

uint64_t Reader::LastRecordOffset()
{
	return last_record_offset_;
}

void Reader::ReportCorruption(size_t bytes, const char* reason)
{
	ReportDrop(bytes, Status::Corruption(reason));
}

void Reader::ReportDrop(size_t bytes, const Status& reason)
{
	if(reporter_ != NULL && end_of_buffer_offset_ - buffer_.size() - bytes >= initial_offset_){
		reporter_->Corruption(bytes, reason);
	}
}

unsigned int Reader::ReadPhysicalRecord(Slice* result)
{
	while(true){
		if(buffer_.size() < kHeaderSize){
			if(!eof_){
				buffer_.clear();
				//从文件中读出一块数据到buffer_内存中
				Status s = file_->Read(kBlockSize, &buffer_, backing_store_);
				end_of_buffer_offset_ += buffer_.size();
				if(!s.ok()){ //读取失败，直接返回末尾标识
					buffer_.clear();
					ReportDrop(kBlockSize, s);
					eof_ = true;
					return kEof;
				}
				else if(buffer_.size() < kBlockSize){//读到末尾了
					eof_ = true;
				}
				continue;
			}
			else if(buffer_.size() == 0){ //缓冲区中无数据
				return kEof;
			}
			else{ //缓冲区中有数据
				size_t drop_size = buffer_.size();
				buffer_.clear();
				ReportCorruption(drop_size, "truncated record at end of file");
				return kEof;
			}
		}

		//对log 块的头进行解析
		const char* header = buffer_.data();
		const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
		const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
		const unsigned int type = header[6];
		const uint32_t length = a | (b << 8);	
		if(kHeaderSize + length > buffer_.size()){ //kHeaderSize + length 一定要等于或者大于缓冲区的数据块长度（分多fragment就有可能会大于）
			size_t drop_size = buffer_.size();
			buffer_.clear();
			ReportCorruption(drop_size, "bad record length");
			return kBadRecord;
		}

		if(type == kZeroType && length == 0){
			buffer_.clear();
			return kBadRecord;
		}

		//校验LOG 数据块的SRC
		if(checksum_){
			uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
			uint32_t actual_crc = crc32c::Value(header + 6, 1 + length);
			if(actual_crc != expected_crc){
				size_t drop_size = buffer_.size();
				buffer_.clear();
				ReportCorruption(drop_size, "checksum mismatch");
			}
		}
		//移动slice中的数据，剩下多余的数据，然后做对应的长度校验
		buffer_.remove_prefix(kHeaderSize + length);
		if(end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length < initial_offset_){
			result->clear();
			return kBadRecord;
		}

		//拼凑返回结果
		*result = Slice(header + kHeaderSize, length);
		return type;
	}
}

};//log
};//leveldb




