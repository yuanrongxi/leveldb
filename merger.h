#ifndef __LEVEL_DB_MERGER_H_
#define __LEVEL_DB_MERGER_H_

namespace leveldb{

class Comparator;
class Iterator;

extern Iterator* NewMergingIterator(const Comparator* compatator, Iterator** children, int n)

}//leveldb

#endif
