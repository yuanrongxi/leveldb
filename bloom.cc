#include "filter_policy.h"
#include "slice.h"
#include "hash.h"

namespace leveldb{
namespace {
	static uint32_t BloomHash(const Slice& key)
	{
		return Hash(key.data(), key.size(), 0xbc9f1d34);
	}
};

class BloomFilterPolicy : public FilterPolicy
{
public:
	explicit BloomFilterPolicy(int bits_per_key) : bits_per_key_(bits_per_key)
	{
		k_ = static_cast<size_t>(bits_per_key * 0.69);
		if(k_ < 1)
			k_ = 1;
		else if(k_ > 30)
			k_ = 30;
	}

	virtual const char* Name() const
	{
		return "leveldb.BuiltinBloomFilter";
	}

	//构建bloom过滤器表
	virtual void CreateFilter(const Slice* keys, int n, std::string* dst) const
	{
		size_t bits = n * bits_per_key_; //计算bloom对照表的存储空间
		if(bits < 64)
			bits = 64;

		//进行8位对齐
		size_t bytes = (bits + 7) / 8;
		bits = bytes * 8;

		//在dst的最后面加入bloom对照表
		const size_t init_size = dst->size();
		dst->resize(init_size + bytes, 0);
		dst->push_back(static_cast<char>(k_));

		char* array = &(*dst)[init_size];
		for(size_t i = 0; i < n; i ++){ //构建N个key的bloom对照表
			uint32_t h = BloomHash(keys[i]);
			const uint32_t delta = (h >> 17) | (h << 15);
			for(size_t j = 0; j < k_; j++){
				const uint32_t bitpos = h % bits;
				array[bitpos/8] = (1 << (bitpos % 8));
				h += delta;
			}
		}
	}

	//过滤
	virtual bool KeyMayMatch(const Slice& key, const Slice& bloom_filter) const
	{
		const size_t len = bloom_filter.size();
		if(len < 2)
			return false;

		const char* array = bloom_filter.data();
		const size_t bits = (len - 1) * 8;

		//得到bloom 的K
		const size_t k = array[len - 1];
		if(k > 30){
			return false;
		}

		//进行bloom校验
		uint32_t h = BloomHash(key);
		const uint32_t delta = (h >> 17) | (k << 15);
		for(size_t j = 0; j < k; j ++){ //连续k次匹配命中
			const uint32_t bitpos = h % bits;
			if((array[bitpos / 8] & (1 << (bitpos % 8))) == 0) //检查key是否匹配
				return false;
			h += delta;
		}
	}

private:
	size_t bits_per_key_;			//KEY的位数
	size_t k_;
};

//创建一个bloom过滤器实例
const FilterPolicy* NewBloomFilterPolicy(int bits_per_key)
{
	return new BloomFilterPolicy(bits_per_key);
}

};


