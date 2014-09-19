#include "builder.h"
#include "filename.h"
#include "dbformat.h"
#include "table_cache.h"
#include "version_set.h"
#include "db.h"
#include "env.h"
#include "iterator.h"

namespace leveldb{

Status BuilderTable(const std::string& dbname, Env* env, const Options& opt, TableCache* table_cache,
					Iterator* iter, FileMetaData* meta)
{
	Status s;
	meta->file_size = 0;
	//定位到表的最前面
	iter->SeekToFirst();
	//构建一个/dbname/number.ldb的文件
	std::string fname = TableFileName(dbname, meta->number);
	if(iter->Valid()){
		//对ldb文件进行打开
		WritableFile* file;
		s = env->NewWritableFile(fname, &file); //打开一个可写的文件
		if(!s.ok())
			return s;

		//构建一个table builder对象
		TableBuilder* builder = new TableBuilder(opt, file);
		//确定最小的key
		meta->smallest.DecodeFrom(iter->key());
		for(; iter->Valid(); iter->Next()){
			Slice key = iter->key();
			//确定最大的KEY
			meta->largest.DecodeFrom(key);
			//将key value写入到table builder当中（这个过程会写文件）
			builder->Add(key, iter->value());
		}

		if(s.ok()){
			//最后写入block index等信息到文件
			s = builder->Finish();
			if(s.ok()){
				//获得文件的大小
				meta->file_size = builder->FileSize();
				assert(meta->file_size > 0);
			}
		}
		else //文件操作失败了，builder table放弃文件
			builder->Abandon();

		delete builder;
		//文件从page cache中写入到磁盘
		if(s.ok())
			s = file->Sync();

		if(s.ok())
			s = file->Close();

		delete file;
		file = NULL;

		if(s.ok()){
			//校验table是否可用
			Iterator* it = table_cache->NewIterator(ReadOptions(), meta->number, meta->file_size);
			s = it->status();
			delete it;
		}
	}

	//检查iter是否有效
	if(!iter->status().ok())
		s = iter->status();

	
	if(s.ok() && meta->file_size > 0){

	}
	else//如果输入的iter无效，将生成的文件删除
		env->DeleteFile(fname);

	return s;
}

};




