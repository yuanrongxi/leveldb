#ifndef __LEVEL_DB_RANDOM_H_
#define __LEVEL_DB_RANDOM_H_

#include <stdint.h>

namespace leveldb{

class Random
{
public:
	explicit Random(uint32_t s) : seed_(s & 0x7fffffffu)
	{
		if(seed_ == 0 || seed_ == 2147483647L)
			seed_ = 1;
	}

	uint32_t Next()
	{
		static const uint32_t M = 2147483647L;
		static const uint32_t A = 16807;

		uint64_t product = seed_ * A;
		seed_ = (product >> 31) + (product & M);
		if(seed_ > M)
			seed_ -= M;

		return seed_;
	};

	uint32_t Uniform(int n)
	{
		return Next() % n;
	}

	bool OneIn(int n)
	{
		return (Next() % n) == 0;
	}

	uint32_t Skewed(int max_log)
	{
		return Uniform(1 << Uniform(max_log + 1));
	}

private:
	uint32_t seed_;
};
};

#endif
