#ifndef __LEVEL_DB_SLICE_H_
#define __LEVEL_DB_SLICE_H_

#include <assert.h>
#include <stddef.h>
#include <string.h>
#include <string>

namespace leveldb{

//数据片对象
class Slice
{
public:
	Slice() : data_(""), size_(0){};
	Slice(const char* d, size_t n) : data_(d), size_(n) {};
	Slice(const std::string& s) : data_(s.data()), size_(s.size()){};
	Slice(const char* s) : data_(s), size_(strlen(s)) {};

	const char* data() const{return data_;};
	size_t size() const {return size_;};
	bool empty() const {return size_ == 0;};

	char operator[](size_t n) const{
		assert(n < size_);
		return data_[n];
	}

	void clear(){
		data_ = ""; 
		size_ = 0;
	};

	//本数据片后移n个字节
	void remove_prefix(size_t n){
		assert(n < size());
		data_ += n;
		size_ -= n;
	}

	std::string ToString() const{
		return std::string(data_, size_);
	}

	//本数据片开始部分包换x片
	bool starts_with(const Slice& x) const{
		return (size_ >= x.size() && memcmp(data_, x.data_, x.size_) == 0);
	}

	// <  0 iff "*this" <  "b",
	// == 0 iff "*this" == "b",
	// >  0 iff "*this" >  "b"
	int compare(const Slice& b) const{
		size_t min_len = (size_ < b.size_) ? size_ : b.size_;
		int r = memcmp(data_, b.data_, min_len);
		if(r == 0){
			if(size_ < b.size_)
				r = -1;
			else if(size_ > b.size_)
				r = 1;
		}
		return r;
	}

private:
	const char* data_;	//数据(起始指针)
	size_t size_;		//数据长度
};

inline bool operator==(const Slice& x, const Slice& y)
{
	return (x.size() == y.size() && memcmp(x.data(), x.data(), x.size()) == 0);
}

inline bool operator!=(const Slice& x, const Slice& y) 
{
	return !(x == y);
}


};

#endif




