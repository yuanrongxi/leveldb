#include "arena.h"

namespace leveldb{

static const int kBlockSize = 4096;

Arena::Arena()
{
	blocks_memory_ = 0;
	alloc_ptr_ = NULL;
	alloc_bytes_remaining_ = 0;
}

Arena::~Arena()
{
	for(size_t i = 0; i < blocks_.size(); i ++){
		delete []blocks_[i];
	}
}

char* Arena::AllocateFallback(size_t bytes)
{
	if(bytes > kBlockSize / 4){ // > 1KB,直接分配等同大小的内存
		return AllocateNewBlock(bytes);
	}

	//新建一个4K的内存页
	alloc_ptr_ = AllocateNewBlock(kBlockSize);
	alloc_bytes_remaining_ = kBlockSize;

	//在内存页上分配bytes的大小空间给申请者
	char* result = alloc_ptr_;
	alloc_ptr_ += bytes;
	alloc_bytes_remaining_ -= bytes;

	return result;
}

char* Arena::AllocateAligned(size_t bytes)
{
	const int align = (sizeof(void*) > 8) ? sizeof(void *) : 8; //判断CPU处理对齐的字节数，32位对齐 64位对齐
	assert((align & (align - 1)) == 0);
	//计算alloc_ptr_与align对齐的位置
	size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align - 1);
	size_t slop = (current_mod == 0 ? 0 : align - current_mod);
	size_t needed = bytes + slop; //补成align对齐

	//进行内存申请
	char* result;
	if(needed <= alloc_bytes_remaining_){
		result = alloc_ptr_ + slop;
		alloc_ptr_ += needed;
		alloc_bytes_remaining_ -= needed;
	}
	else{
		result = AllocateFallback(bytes);
	}
	//对齐检查
	assert((reinterpret_cast<uintptr_t>(result) & (align - 1)) == 0);
	return result;
}

//分配新的页
char* Arena::AllocateNewBlock(size_t block_bytes)
{
	char* result = new char[block_bytes];
	blocks_memory_ += block_bytes;
	blocks_.push_back(result);
	return result;
}

}
