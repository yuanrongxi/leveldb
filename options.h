#ifndef __LEVEL_DB_OPTION_H_
#define __LEVEL_DB_OPTION_H_

namespace leveldb{

class Cache;
class Comparator;
class Env;
class Logger;
class FilterPolicy;
class Snapshot;

enum CompressionType
{
	kNoCompression		= 0x00,
	kSnappyCompression	= 0x01
};

struct Options
{
	//比较器
	const Comparator* comparator;
	//true - 加入数据库不存在或者丢失，将自动创建？
	bool create_if_missing;
	//true - 假如数据库存在，将触发一个错误，
	bool error_if_exists;

	bool paranoid_checks;
	//PORT环境操作对象
	Env* env;
	
	//日志对象
	Logger* info_log;

	size_t write_buffer_size;
	//打开文件的最大数目
	int max_open_files;
	//cache LRU CACHE
	Cache* block_cache;
	//块大小
	size_t block_size;

	int block_restart_interval;
	//数据压缩类型
	CompressionType compression;
	//过滤器对象，bloom filter
	const FilterPolicy* filter_policy;

	Options();
};

//读选项
struct ReadOptions
{
	//数据是否检验check sums（CRC）
	bool verfy_checksums;

	bool fill_cache;

	const Snapshot* snapshot;

	ReadOptions() : verfy_checksums(false), fill_cache(true), snapshot(NULL)
	{
	}
};

//写选项
struct WriteOptions
{
	//内存和银盘完全同步标识
	bool sync;
	WriteOptions() : sync(false){};
};

}//leveldb

#endif
