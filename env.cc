#include "env.h"

namespace leveldb{

void Log(Logger* info_log, const char* format, ...)
{
	if(info_log != NULL){
		va_list ap;
		va_start(ap, format);
		info_log->Logv(format, ap);
		va_end(ap);
	}
}

//将数据写入文件，并获得操作结果
static Status DoWriteStringToFile(Env* env, const Slice& data, const std::string& fname, bool should_sync)
{
	WritableFile* file;
	//获得一个文件写对象
	Status s = env->NewWritableFile(fname, &file);
	if(!s.ok())
		return s;
	//进行文件追加
	s = file->Append(data);
	if(s.ok() && should_sync){ //同步写入硬盘
		s = file->Sync();
	}

	if(s.ok()){
		s = file->Close();
	}

	delete file;
	if(!s.ok()){ //写失败，直接删除文件？
		env->DeleteFile(fname);
	}

	return s;
}

Status WriteStringToFile(Env* env, const Slice& data, const std::string& fname)
{
	return DoWriteStringToFile(env, data, fname, false);
}

Status WriteStringToFileSync(Env* env, const Slice& data, const std::string& fname) 
{
	return DoWriteStringToFile(env, data, fname, true);
}

Status ReadFileToString(Env* env, const std::string& fname, std::string* data)
{
	data->clear();

	SequentialFile* file;
	//获得一个文件读对象
	Status s = env->NewSequentialFile(fname, &file);
	if(!s.ok()){
		return s;
	}

	//每次读取数据块大小
	static const int kBufferSize = 8192;
	char* space = new char[kBufferSize];
	//进行数据读取
	while(true){
		Slice fragment;
		s = file->Read(kBufferSize, &fragment, space);
		if(!s.ok()){ //读取失败
			break;
		}
		//拼接数据
		data->append(fragment.data(), fragment.size());
		if(fragment.empty()){
			break;
		}
	}

	delete []space;
	delete file;

	return s;
}



}




