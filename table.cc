#include "table.h"
#include "cache.h"
#include "comparator.h"
#include "env.h"
#include "filter_policy.h"
#include "options.h"
#include "filter_block.h"
#include "format.h"
#include "two_level_iterator.h"
#include "coding.h"

namespace leveldb{

struct Table::Rep
{
	Options options;		//配置选项
	Status status;			//错误码
	
	RandomAccessFile* file;	//文件句柄
	uint64_t cache_id;		//cache id

	FilterBlockReader* filter; //过滤器句柄
	const char* filter_data;	

	BlockHandle metaindex_handle;
	Block* index_block;

	~Rep()
	{
		delete filter;
		delete []filter_data;
		delete index_block;
	}
};

//对应table builder中的Finish函数
Status Table::Open(const Options& options, RandomAccessFile* file, uint64_t size, Table** table)
{
	*table = NULL;
	if(size < Footer::kEncodedLength)
		return Status::InvalidArgument("file is too short to be an sstable");

	char footer_space[Footer::kEncodedLength];
	Slice footer_input;
	//读取最后kEncodedLength个字节，这个是footer的存储空间
	Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength, &footer_input, footer_space);
	if(!s.ok())
		return s;

	//对footer的解析
	Footer footer;
	s = footer.DecodeFrom(&footer_input);
	if(!s.ok())
		return s;

	//对index block的读取
	BlockContents contents;
	Block* index_block = NULL;
	s = ReadBlock(file, ReadOptions(), footer.index_handle(), &contents);
	if(s.ok()){
		index_block = new Block(contents);
	}

	if(s.ok()){
		Rep* r = new Table::Rep;
		r->options = options;
		r->file = file;
		r->metaindex_handle = footer.metaindex_handle();
		r->index_block = index_block;
		r->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
		r->filter_data = NULL;
		r->filter = NULL;
		*table = new Table(r);
		//读取meta index block
		(*table)->ReadMeta(footer);
	}
	else if(index_block != NULL){
		delete index_block;
	}

	return s;
}

//对meta index block的读取
void Table::ReadMeta(const Footer& footer)
{
	if(rep_->options.filter_policy == NULL)
		return;

	//读取meta index block的数据
	ReadOptions opt;
	BlockContents contents;
	if(!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()){
		return;
	}

	Block* meta = new Block(contents);

	//对过滤器信息的读取
	Iterator* iter = meta->NewIterator(BytewiseComparator());
	std::string key = "filter.";
	key.append(rep_->options.filter_policy->name());
	iter->Seek(key);
	if(iter->Valid() && iter->key() == Slice(key)){
		ReadFilter(iter->value()); //对过滤器对照表的读取，并构建过滤器
	}

	delete iter;
	delete meta;
}

void Table::ReadFilter(const Slice& filter_handle_value)
{
	Slice v = filter_handle_value;
	BlockHandle filter_handle;
	if(!filter_handle.DecodeFrom(&v).ok()){
		return ;
	}

	ReadOptions opt;
	BlockContents block;
	if(!ReadBlock(rep_->file, opt, filter_handle, &block).ok()){
		return ;
	}

	//判断是否需要释放data,如果是mmap方式就不需要释放
	if(block.heap_allocated){
		rep_->filter_data = block.data.data();
	}
	//构建一个过滤器对象
	rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table()
{
	delete rep_;
}

static void DeleteBlock(void* arg, void * ignored)
{
	delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value)
{
	Block* block = reinterpret_cast<Block*>(value);
	delete block;
}

static void ReleaseBlock(void* arg, void* h)
{
	Cache* cache = reinterpret_cast<Cache*>(arg);
	Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
	cache->Release(handle);
}

//读取一个块，并返回其迭代器
Iterator* Table::BlockReader(void* arg, const ReadOptions& opt, const Slice& index_value)
{
	Table* table = reinterpret_cast<Table*>(arg);
	Cache* block_cache = table->rep_->options.block_cache;
	Block* block = NULL;
	Cache::Handle* cache_handle = NULL;
	//解码块的位置
	BlockHandle handle;
	Slice input = index_value;

	Status s = handle.DecodeFrom(&input);
	if(s.ok()){
		BlockContents contents;
		if(block_cache != NULL){
			char cache_key_buffer[16];
			//id + offset = cache key
			EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
			EncodeFixed64(cache_key_buffer + 8, handle.offset());
			Slice key(cache_key_buffer, sizeof(cache_key_buffer));
			//在LRU CACHE中找BLOCK
			cache_handle = block_cache->Lookup(key);
			if(cache_handle != NULL){//在CACHE中找到了
				block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
			}
			else{//在CACHE中没找到，在磁盘中读取
				s = ReadBlock(table->rep_->file, opt, handle, &contents);
				if(s.ok()){
					s = ReadBlock(table->rep_->file, opt, handle, &contents);
					if(s.ok()){
							//块对象生成
						block = new Block(contents);
						if(contents.cachable && opt.fill_cache) //将从磁盘中得到的block写入到lru cache当中
							cache_handle = block_cache->Insert(key, block, block->size(), &DeleteCachedBlock);
					}
				}
			}
		}
		else{ //配置选项无cache,直接冲磁盘中读取
			s = ReadBlock(table->rep_->file, opt, handle, &contents);
			if(s.ok())
				block = new Block(contents);
		}
	}
	
	//创建一个block迭代器
	Iterator* iter = NULL;
	if(block != NULL){
		iter = block->NewIterator(table->rep_->options.comparator);
		if(cache_handle == NULL) //cache和没有cache的不同方式释放block
			iter->RegisterCleanup(&DeleteBlock, block, NULL);
		else
			iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
	}
	else
		iter = NewErrorIterator(s);

	return iter;
}
//返回一个two level iter
Iterator* Table::NewIterator(const ReadOptions& opt) const
{
	return NewTowLevelIterator(rep_->index_block->NewIterator(rep_->options.comparator), &Table::BlockReader,
		const_cast<Table*>(this), opt);
}

Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg, void (*saver)(void*, const Slice&, const Slice&))
{
	Status s;
	Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
	iiter->Seek(k);
	if(iiter->Valid()){
		Slice handle_value = iiter->value();
		FilterBlockReader* filter = rep_->filter;
		BlockHandle handle;

		//过滤器检查
		if(filter != NULL && handle.DecodeFrom(&handle_value).ok() && !filter->KeyMayMatch(handle.offset(), k)){
			//未找到
		}
		else{
			//读取data block，并且seek到KEY的位置，最后调用SAVE函数保存数据
			Iterator* block_iter = BlockReader(this, options, iiter->value());
			block_iter->Seek(k);
			if(block_iter->Valid())
				(*saver)(arg, block_iter->key(), block_iter->value());

			s = block_iter->status();
			delete block_iter;
		}
	}

	if(s.ok())
		s = iiter->status();

	delete iiter;

	return s;
}

//估算key对应bock的偏移量offset
uint64_t Table::ApproximateOffsetOf(const Slice& key) const
{
	//定位块的索引信息
	Iterator* index_iter = rep_->index_block->NewIterator(rep_->options.comparator);
	index_iter->Seek(key);
	uint64_t result;
	//通过索引信息定位偏移量
	if(index_iter->Valid()){
		BlockHandle handle;
		Slice input = index_iter->value();
		Status s = handle.DecodeFrom(&input);
		if(s.ok())
			result = handle.offset();
		else
			result = rep_->metaindex_handle.offset();
	}
	else{ //key是表示index block的KEY，直接返回meta block的偏移量
		result = rep_->metaindex_handle.offset();
	}

	delete index_iter;
	return result;
}

};



