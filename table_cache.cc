#include "table_cache.h"
#include "filename.h"
#include "env.h"
#include "table.h"
#include "coding.h"

namespace leveldb{

struct TableAndFile
{
	RandomAccessFile* file;
	Table* table;
};

static void DeleteEntry(const Slice& key, void* value)
{
	TableAndFile* tf = reinterpret_cast<TableAndFile *>(value);
	delete tf->table;
	delete tf->file;
	delete tf;
}

static void UnrefEntry(void* arg1 ,void* arg2)
{
	Cache* cache = reinterpret_cast<Cache*>(arg1);
	Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
	cache->Release(h); //LRU Cache进行释放,里面采用了引用计数作为释放标识
}

TableCache::TableCache(const std::string& dbname, const Options* opt, int entries) : dbname_(dbname), options_(opt), env_(opt->env)
{
	cache_ = NewLRUCache(entries);
}

TableCache::~TableCache()
{
	delete cache_;
}

Status TableCache::FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle** handle)
{
	Status s;
	char buf[sizeof(file_number)];
	EncodeFixed64(buf, file_number);

	//通过文件号hash来确定CACHE_handle,cache_底下有16个cache handler
	Slice key(buf, sizeof(buf));
	*handle = cache_->Lookup(key);
	//如果cache中没有记录table的信息，从文件中导入table信息，并记录到cache中
	if(handle == NULL){
		//生成一个//dbname/number.ldb文件名
		std::string fname = TableFileName(dbname_, file_number);
		RandomAccessFile* file = NULL;
		Table* table = NULL;

		s = env_->NewRandomAccessFile(fname, &file); //打开一个随机写的文件
		if(!s.ok()){ //打开ldb文件失败,尝试打开sst文件
			std::string old_fname = SSTableFileName(dbname_, file_number);
			if(env_->NewRandomAccessFile(old_fname, &file).ok())
				s = Status.OK();
		}

		if(s.ok())
			s = Table::Open(*options_, file, file_size, &table); //从文件中读取数据构建TABALE

		if(!s.ok()){
			assert(table == NULL);
			delete file;
		}
		else{
			TableAndFile* tf = new TableAndFile();
			tf->file = file;
			tf->table = table;

			//设置一个cache的handle,参数为TableAndFile,相当于将table的基本访问元数据设置到cache当中
			*handle = cache_->Insert(key, tf, 1, &DeleteEntry);
		}
	}

	return s;
}

//返回一个tow level iterator迭代器
Iterator* TableCache::NewIterator(const ReadOptions& opt, uint64_t file_number, uint64_t file_size, Table** tableptr)
{
	if(tableptr != NULL)
		*tableptr = NULL;

	Cache::Handle* handle = NULL;
	Status s = FindTable(file_number, file_size, &handle); //会产生一个handle的引用计数
	if(!s.ok())
		return NewErrorIterator(s);

	//从cache中查找到value就是TableAndFile对象的指针
	Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
	Iterator* result = table->NewIterator(opt);  //产生一个two level iterator
	result->RegisterCleanup(&UnrefEntry, cache_, handle); //设置个释放迭代器的回调，handle的引用计数会在iter析构的时候调用UnrefEntry释放

	if(tableptr != NULL)
		*tableptr = table;

	return result;
}

//saver第一个参数是arg,第二个阐述是k,第三个参数是在table中查找到value
Status TableCache::Get(const ReadOptions& opt, uint64_t file_number, uint64_t file_size, const Slice& k, void* arg, 
	void(*saver)(void*, const Slice&, const Slice&))
{
	Cache::Handle* handle = NULL;
	Status s = FindTable(file_number, file_size, &handle);
	if(s.ok()){
		Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
		s = t->InternalGet(opt, k ,arg, saver); //在table当中查找一个k对应的value,并调用saver进行返回
		cache_->Release(handle);//释放引用计数
	}

	return s;
}

void TableCache::Evict(uint64_t file_number)
{
	char buf[sizeof(file_number)];
	EncodeFixed64(buf, file_number);
	cache_->Erase(Slice(buf, sizeof(buf))); //从cache中删除file_number的table, 会调用DeleteEntry对tableAndFile结构进行释放
}

};//leveldb



