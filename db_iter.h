#ifndef __LEVEL_DB_ITER_H_
#define __LEVEL_DB_ITER_H_

#include <stdint.h>
#include "db.h"
#include "dbformat.h"

namespace leveldb{

class DBImpl;

extern Iterator* NewDBIterator(DBImpl* db, const Comparator* user_key_comparator, Iterator* internal_iter,
								SequenceNumber sequence, uint32_t seed);

};//leveldb


#endif




