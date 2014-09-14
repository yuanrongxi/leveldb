#include "table_builder.h"
#include <assert.h>
#include "comparator.h"
#include "env.h"
#include "filter_policy.h"
#include "options.h"
#include "filter_block.h"
#include "format.h"
#include "coding.h"
#include "crc32c.h"
#include "block_builder.h"

namespace leveldb{

struct TableBuilder::Rep{
	Options options;				//配置选项
	Options index_block_options;	//index配置选项，与options一样
	WritableFile* file;				//文件对象
	uint64_t offset;				//文件偏移
	Status status;					//错误码
	BlockBuilder data_block;		//当前的数据块
	BlockBuilder index_block;		//当前的索引块
	std::string last_key;			//最大的KEY??
	int64_t num_entries;			//key value个数
	bool closed;
	FilterBlockBuilder* filter_block;

	bool pending_index_entry;
	BlockHandle pending_handle;

	std::string compressed_output;		//作为snappy输入临时存储的地方

	Rep(const Options& opt, WritableFile* f) : options(opt), index_block_options(opt),
		file(f), offset(0), data_block(&options), index_block(&options),
		num_entries(0), closed(false), 
		filter_block(opt.filter_policy == NULL ? NULL : new FilterBlockBuilder(opt.filter_policy)),
		pending_index_entry(false)
	{
		index_block_options.block_restart_interval = 1;
	}
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
	: rep_(new Rep(options, file))
{
	if(rep_->filter_block != NULL)
		rep_->filter_block->StartBlock(0); //启动过滤器
}

TableBuilder::~TableBuilder()
{
	assert(rep_->closed);
	delete rep_->filter_block;
	delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& opt)
{
	if(opt.comparator != rep_->options.comparator) //比较器不匹配
		return Status::InvalidArgument("changing comparator while building table");

	rep_->options = opt;
	rep_->index_block_options = opt;
	rep_->index_block_options.block_restart_interval = 1; //索引block不做KEY叠加

	return Status::OK();
}

Status TableBuilder::status() const
{
	return rep_->status;
}

bool TableBuilder::ok() const
{
	return status().ok();
}

//放弃数据？？
void TableBuilder::Abandon() 
{
	Rep* r = rep_;
	assert(!r->closed);
	r->closed = true;
}

void TableBuilder::Add(const Slice& key, const Slice& value)
{
	Rep* r = rep_;
	assert(r->closed);

	//前面IO操作异常了，不能加入数据
	if(!ok())
		return;

	if(r->num_entries > 0)
		assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0); //key必须必rep中的last key大

	if(r->pending_index_entry){
		assert(r->data_block.empty());
		r->options.comparator->FindShortestSeparator(&r->last_key, key); //找到r->last_key最小不同的字符串且小于key，并改变last key,
		//编码一个索引块
		std::string handle_encoding;
		r->pending_handle.EncodeTo(&handle_encoding); //编码pending handle
		r->index_block.Add(r->last_key, Slice(handle_encoding));//将索引数据加入到index block当中
		r->pending_index_entry = false;
	}

	//将key加入到过滤器中
	if(r->filter_block != NULL)
		r->filter_block->AddKey(key);

	//对last_key = key
	r->last_key.assign(key.data(), key.size());
	r->num_entries ++;
	//加入到数据块中
	r->data_block.Add(key, value);
	
	//计算添加数据后的block size
	const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
	if(estimated_block_size >= r->options.block_size){ //到了配置块的大小，需要对块Flush
		Flush();
	}
}

//对数据进行flush固话到磁盘上
void TableBuilder::Flush()
{
	Rep* r = rep_;
	assert(r->closed);

	//有磁盘IO错误
	if(!ok())
		return;

	//数据块是空的
	if(r->data_block.empty())
		return;
	
	//对pending index entry进行校验
	assert(!r->pending_index_entry);

	WriteBlock(&r->data_block, &r->pending_handle);
	if(ok()){
		r->pending_index_entry = true; //写入标志？
		r->status = r->file->Flush();
	}

	//从新新开一个过滤器的段
	if(r->filter_block != NULL){
		r->filter_block->StartBlock(r->offset);
	}
}

void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle)
{
	assert(ok());
	Rep* r = rep_;
	Slice raw = block->Finish(); //对数据块进行打包，并复制给raw

	Slice block_contents;
	CompressionType type = r->options.compression;
	switch(type){
	case kNoCompression: //无压缩
		block_contents = raw;
		break;

		//snappy压缩
	case kSnappyCompression:{
			std::string* compressed = &r->compressed_output;
			if(port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
				compressed->size() < raw.size() - (raw.size() / 8u)){
				block_contents = *compressed;
			}
			else{ //压缩失败，用无压缩模式写入
				block_contents = raw;
				type = kNoCompression;
			}
			break;
		}
	}

	WriteRawBlock(block_contents, type, handle);
	r->compressed_output.clear();
	block->Reset(); //将写入的块对象复位，从新接受新的数据
}

void TableBuilder::WriteRawBlock(const Slice& block_contents, CompressionType type, BlockHandle* handle)
{
	Rep* r = rep_;
	//设置索引块的位置记录, pending handle
	handle->set_offset(r->offset);
	handle->set_size(block_contents.size());
	//数据写入文件page cache
	r->status = r->file->Append(block_contents);
	if(r->status.ok()){
		//进行format尾处理
		char trailer[kBlockTrailerSize];
		trailer[0] = type; //记录压缩模式
		//计算CRC并编码到trailer中
		uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
		crc = crc32c::Extend(crc, trailer, 1);
		EncodeFixed32(trailer + 1, crc32c::Mask(crc));
		//尾写入
		r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
		if(r->status.ok())
			r->offset += block_contents.size() + kBlockTrailerSize; //更新文件偏移量
	}
}

Status TableBuilder::Finish()
{
	Rep* r = rep_;
	Flush(); //块写入文件

	assert(!r->closed);
	r->closed = true;

	BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;
	
 // Write filter block
  if (ok() && r->filter_block != NULL) {
		WriteRawBlock(r->filter_block->Finish(), kNoCompression, &filter_block_handle);
  }

	//写入meta index block,主要是过滤的名字和过滤器的位置
	if(ok()){
		BlockBuilder meta_index_block(&r->options);
		if(r->filter_block != NULL){
			//构建一个过滤器key
			std::string key = "filter.";
			key.append(r->options.filter_policy->name());

			std::string handle_encoding;
			filter_block_handle.EncodeTo(&handle_encoding);
			meta_index_block.Add(key, handle_encoding);
		}

		WriteBlock(&meta_index_block, &metaindex_block_handle);
	}

	//write index block
	if(ok()){
		if(r->pending_index_entry){ //将最后一个块的offset索引写入index block
			r->options.comparator->FindShortSuccessor(&r->last_key); //找到一个仅仅比r->last_key大的key
			std::string handle_encoding;
			r->pending_handle.EncodeTo(&handle_encoding); //对pending handle的位置进行编码
			r->index_block.Add(r->last_key, Slice(handle_encoding)); //作为key value加入到index block中
			r->pending_index_entry = false;
		}
		//将索引信息写入文件中
		WriteBlock(&r->index_block, &index_block_handle);
	}

	//对metaindex_block_handle index_block_handle的数据进行文件写入 footer对象写入
	if(ok()){
		Footer footer;
		footer.set_metaindex_handle(metaindex_block_handle);
		footer.set_index_handle(index_block_handle);
		std::string footer_encoding;
		footer.EncodeTo(&footer_encoding);
		r->status = r->file->Append(footer_encoding);
		if(r->status.ok())
			r->offset += footer_encoding.size();
	}

	return r->status;
}

uint64_t TableBuilder::NumEntries() const
{
	return rep_->num_entries;
}

uint64_t TableBuilder::FileSize() const
{
	return rep_->offset;
}

};//leveldb




