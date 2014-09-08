#ifndef __LEVEL_DB_BLOCK_H_
#define __LEVEL_DB_BLOCK_H_

#include <stddef.h>
#include <stdint.h>
#include "iterator.h"

namespace leveldb{

struct BlockContents;
class Comparator;

class Block
{
public:
	explicit Block(const BlockContents& contents);
	~Block();

	size_t size() const {return size_;};
	Iterator* NewIterator(const Comparator* comp);

private:
	uint32_t NumRestarts() const;
	Block(const Block&);
	void operator=(const Block&);

private:
	const char* data_;
	size_t size_;
	uint32_t restart_offset_;
	bool owned_;

	class Iter;
};

};

#endif
