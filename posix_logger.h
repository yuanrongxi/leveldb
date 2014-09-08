#ifndef __POSIX_LOGGER_H_
#define __POSIX_LOGGER_H_

#include <algorithm>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include "env.h"

namespace leveldb{
//POSIX模式下的日志实现
class PosixLogger : public Logger
{
public:
	PosixLogger(FILE* f, uint64_t (*gettid)()) : file_(f), gettid_(gettid){};
	virtual ~PosixLogger()
	{
		fclose(file_);
	};

	virtual void Logv(const char* format, va_list ap)
	{
		const uint64_t thread_id = (*gettid_)();
		char buffer[500];
		for(int iter = 0; iter < 2; iter ++){
			char* base;
			int bufsize;
			if(iter == 0){
				bufsize = sizeof(buffer);
				base = buffer;
			}
			else{
				bufsize = 30000;
				base = new char[bufsize];
			}

			char* p = base;
			char* limit = base + bufsize;

			struct timeval now_tv;
			gettimeofday(&now_tv, NULL);
			const time_t seconds = now_tv.tv_sec;
			struct tm t;
			localtime_r(&seconds, &t);
			//获得日志时间
			p += snprintf(p, limit - p,
                    "%04d/%02d/%02d-%02d:%02d:%02d.%06d %llx ",
                    t.tm_year + 1900,
                    t.tm_mon + 1,
                    t.tm_mday,
                    t.tm_hour,
                    t.tm_min,
                    t.tm_sec,
                    static_cast<int>(now_tv.tv_usec),
                    static_cast<long long unsigned int>(thread_id));

			if(p < limit){
				if(iter == 0){ //数据没有完全拼接完成，开辟一个更大的缓冲区
					continue;
				}
				else {
					p = limit - 1;
				}
			}

			//判断换行
			if(p == base || p[-1] != '\n'){
				*p++ = '\n';
			}
			//文件写入
			assert(p < limit);
			fwrite(base, 1, p - base, file_);
			fflush(file_);
			if(base != buffer){ //重新开辟了新的缓冲区来做日志写入
				delete []base;
			}
			break;
		}
	}

private:
	FILE* file_;
	uint64_t (*gettid_)();
};

#endif

