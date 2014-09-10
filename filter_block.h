#ifndef __LEVEL_DB_FILTER_BLOCK_H_
#define __LEVEL_DB_FILTER_BLOCK_H_

#include <stddef.h>
#include <stdint.h>
#include <vector>
#include <string>
#include "slice.h"
#include "hash.h"

namespace leveldb{

class FilterPolicy;

class FilterBlockBuilder
{
public:
	explicit FilterBlockBuilder(const FilterPolicy*);

	void StartBlock(uint64_t block_offset);
	void AddKey(const Slice& key);

	Slice Finish();

private:
	// No copying allowed
	FilterBlockBuilder(const FilterBlockBuilder&);
	void operator=(const FilterBlockBuilder&);

	void GenerateFilter();

private:
	const FilterPolicy* policy_;
	std::string keys_;
	std::vector<size_t> start_;
	std::string result_;
	std::vector<Slice> tmp_keys_;
	std::vector<uint32_t> filter_offsets_;
};

class FilterBlockReader
{
public:
	FilterBlockReader(const FilterPolicy*policy, const Slice& contents);
	bool KeyMayMatch(uint64_t block_offset, const Slice& key);

private:
	const FilterPolicy* policy_;
	const char* data_;
	const char* offset_;
	size_t num_;
	size_t base_lg_;
};

}//leveldb

#endif


