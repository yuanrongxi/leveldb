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
	Status s = env->NewWritableFile(fname, &file);
	if(!s.ok())
		return s;

	s = file->Append(data);
	if(s.ok() && should_sync){
		s = file->Sync();
	}

	if(s.ok()){
		s = file->Close();
	}

	delete file;
	if(!s.ok()){
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

}

}




