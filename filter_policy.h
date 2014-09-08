#ifndef __FILTER_POLICY_H_
#define __FILTER_POLICY_H_

namesapce leveldb{
class Slice;

//过滤器接口
class FilterPolicy
{
public:
	virtual ~FilterPolicy(){};
	virtual const char* name() const = 0;
	//建立一个过滤器
	virtual void CreateFilter(const Slice* keys, int n, std::string* dst) const = 0;
	//数据过滤方法
	virtual bool KeyMayMatch(const Slice& key, const Slice& filter) const = 0;
};

extern const FilterPolicy* NewBloomFilterPolicy(int bits_per_key);

};

#endif