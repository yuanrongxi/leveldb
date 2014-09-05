#ifndef __LEVEL_DB_HASH_H_
#define __LEVEL_DB_HASH_H_

#include <stddef.h>
#include <stdint.h>

namespace leveldb{

extern uint32_t Hash(const char* data, size_t size_t n, uint32_t seed);

}

#endif
