#ifndef __LEVEL_DB_IMPL_H_
#define __LEVEL_DB_IMPL_H_

#include <deque>
#include <set>
#include "dbformat.h"
#include "log_write.h"
#include "snapshot.h"
#include "db.h"
#include "env.h"
#include "port.h"
#include "thread_annatations.h"

namespace leveldb{

class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

class DBImpl : public DB
{
public:
	DBImpl(const Options& opt, const std::string& dbname);
	virtual ~DBImpl();

	virtual Status Put(const WriteOptions& opt, const Slice& key, const Slice& value);
	virtual Status Delete(const WriteOptions& opt, const Slice& key);
	virtual Status Write(const WriteOptions& opt, WriteBatch* updates);

	virtual Status Get(const ReadOptions& opt, const Slice& key, std::string* value);
	virtual Iterator* NewIterator(const ReadOptions& opt);

	virtual const Snapshot* GetSnapshot();
	virtual void ReleaseSnapshot(const Snapshot* snapshot);

	virtual bool GetProerty(const Slice& property, std::string* value);
	virtual void GetApproximateSizes(const Range* range, int n, uint64_t* sizes);
	virtual void CompactRange(const Slice* begin, const Slice* end);

	//≤‚ ‘∑Ω∑®
	void TEST_CompactRange(int level, const Slice* begin, const Slice* end);

	Status TEST_CompactMemTable();

	Iterator* TEST_NewInternalIterator();
	
	int64_t TEST_MaxNextLevelOverlappingBytes();

	void RecordReadSample(Slice key);

private:
	friend class DB;
	struct CompactionState;
	struct Writer;

	DBImpl(const DBImpl&);
	void operator=(const DBImpl&);

	Iterator* NewInternalIterator(const ReadOptions& opt, SequenceNumber* last_snapshot, uint32_t* seed);
	
	Status NewDB();

	Status Recover(VersionEdit* edit) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

	void MaybeIgnoreError(Status* s) const;

	void DeleteObsoleteFiles();

	void CompactMemTable() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
	
	Status RecoverLogFile(uint64_t log_number, VersionEdit* edit, SequenceNumber* max_sequence) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

	Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base)  EXCLUSIVE_LOCKS_REQUIRED(mutex_);

	Status MakeRoomForWrite() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

	WriteBatch* BuildBatchGroup(WriteBatch);

	void RecordBackgroundError(const Status& s);

	void MaybeScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

	static void BGWork(void* db);

	void BackgroundCall();

	void BackgroundCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

	void CleanupCompaction(CompactionState* compact) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

	Status DoCompactionWork(CompactionState* compact) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

	Status OpenCompactionOutputFile(CompactionState* compact);

	Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);

	Status InstallCompactionResults(CompactionState* compact) EXCLUSIVE_LOCKS_REQUIRED(mutex_);
private:

	Env* const env_;
	const InternalKeyComparator internal_comparator_;
	const InternalFilterPolicy internal_filter_policy_;

	const Options options_;
	bool owns_info_log_;
	bool owns_cache_;

	const std::string dbname_;

	TableCache* table_cache_;

	FileLock* db_lock_;

	port::Mutex mutex_;
	port::AtomicPointer shutting_down_;
	port::CondVar bg_cv_;

	MemTable* mem_;
	MemTable* imm_;

	port::AtomicPointer has_imm_;

	WritableFile* logfile_;
	uint64_t logfile_number_;
	log::Writer* log_;
	uint32_t seed_;

	std::deque<Writer*> writers_;
	WriteBatch* tmp_batch_;

	SnapshotList snapshots_;
	std::set<uint64_t> pending_outputs_;
	bool bg_compaction_scheduled_;

	struct ManualCompaction
	{
		int level;
		bool done;
		const InternalKey* begin;
		const InternalKey* end;
		InternalKey tmp_storage;
	};

	ManualCompaction* manual_compaction_;

	VersionSet* versions_;

	Status bg_error_;

	struct CompactionStats {
		int64_t micros;
		int64_t bytes_read;
		int64_t bytes_written;

		CompactionStats() : micros(0), bytes_read(0), bytes_written(0) { }

		void Add(const CompactionStats& c) 
		{
			this->micros += c.micros;
			this->bytes_read += c.bytes_read;
			this->bytes_written += c.bytes_written;
		}
	};
	CompactionStats stats_[config::kNumLevels];
	const Comparator* user_comparator() const
	{
		return internal_comparator_.user_comparator();
	}
};

extern Options SanitizeOptions(const std::string& db, const InternalKeyComparator* icmp,
							   const InternalFilterPolicy* ipolicy, const Options& src);

};

#endif
