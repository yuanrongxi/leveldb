#include "db_impl.h"
#include <algorithm>
#include <set>
#include <string>
#include <stdint.h>
#include <stdio.h>
#include <vector>

#include "builder.h"
#include "db_iter.h"
#include "dbformat.h"
#include "filename.h"
#include "log_reader.h"
#include "log_write.h"
#include "memtable.h"
#include "table_cache.h"
#include "version_edit.h"
#include "version_set.h"
#include "write_batch_internal.h"
#include "db.h"
#include "env.h"
#include "status.h"
#include "table.h"
#include "table_builder.h"
#include "port.h"
#include "block.h"
#include "two_level_iterator.h"
#include "coding.h"
#include "logging.h"
#include "mutexlock.h"

namespace leveldb{

const int kNumNonTableCacheFiles = 10;

struct DBImpl::Writer
{
	Status status;
	WriteBatch* batch;
	bool sync;
	bool done;
	port::CondVar cv;

	explicit Writer(port::Mutex* mu) : cv(mu){};
};

struct DBImpl::CompactionState
{
	Compaction* const compaction;
	SequenceNumber smallest_snapshot;

	struct Output
	{
		uint64_t number;
		uint64_t file_size;
		InternalKey smallest, largest;
	};

	std::vector<Output> outputs;

	WritableFile* outfile;
	TableBuilder* builder;

	uint64_t total_bytes;

	Output* current_output()
	{
		return &outputs[outputs.size() - 1];
	};

	explicit CompactionState(Compaction* c) : compaction(c), outfile(NULL), builder(NULL), total_bytes(0)
	{
	}
};

template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue)
{
	if(static_cast<V>(*ptr) > maxvalue)
		*ptr = maxvalue;

	if(static_cast<V>(*ptr) < minvalue)
		*ptr = minvalue;
}

//对配置的检查
Options SanitizeOptions(const std::string& dbname, const InternalKeyComparator* icmp,
						const InternalFilterPolicy* ipolicy, const Options& src)
{
	Options result = src;
	result.comparator = icmp;
	result.filter_policy = (src.filter_policy != NULL) ? ipolicy : NULL;
	
	ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000); //74 ~ 50000
	ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30); //64K ~ 256M
	ClipToRange(&result.block_size, 1 << 10, 4 << 20); //1K ~ 4M

	if(result.info_log == NULL){
		src.env->CreateDir(dbname);
		src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
		//建立一个文本log文件
		Status s= src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
		if(!s.ok()) //日志文件打开失败
			result.info_log = NULL;
	}

	if(result.block_cache == NULL)
		result.block_cache = NewLRUCache(8 << 20); //LRU CACHE为8M?是不是太小了？

	return result;
}

DBImpl::DBImpl(const Options& raw_opt, const std::string& dbname) : env_(raw_opt.env),
	internal_comparator_(raw_opt.comparator), internal_filter_policy_(raw_opt.filter_policy),
	options_(SanitizeOptions(dbname, &internal_comparator_, &internal_filter_policy_, raw_opt)),
	owns_info_log_(options_.info_log != raw_opt.info_log),
	owns_cache_(options_.block_cache != raw_opt.block_cache),
	dbname_(dbname), db_lock_(NULL), shutting_down_(NULL),
	bg_cv_(&mutex_), mem_(new MemTable(internal_comparator_)), imm_(NULL),
	logfile_(NULL), logfile_number_(0), log_(NULL), seed_(0),
	tmp_batch_(new WriteBatch)
{
	bg_compaction_scheduled_ = false;
	manual_compaction_ = NULL;
	//memtable 引用计数
	mem_->Ref();
	has_imm_.Release_Store(NULL);

	//构建一个table cache
	const int table_cache_size = options_.max_open_files - kNumNonTableCacheFiles;
	table_cache_ = new TableCache(dbname_, &options_, table_cache_size);

	versions_ = new VersionSet(dbname_, &options_, table_cache_, &internal_comparator_);
}

DBImpl::~DBImpl()
{
	mutex_.Lock();
	shutting_down_.Release_Store(this);
	while(bg_compaction_scheduled_){
		bg_cv_.Wait();
	}
	mutex_.Unlock();

	if(db_lock_ != NULL)
		env_->UnlockFile(db_lock_);

	delete versions_;
	if(mem_ != NULL)
		mem_->Unref();

	if(imm_ != NULL)
		imm_->Unref();

	delete tmp_batch_;
	delete log_;
	delete logfile_;
	delete table_cache_;

	if(owns_info_log_)
		delete options_.info_log;

	if(owns_cache_)
		delete options_.block_cache;
}

Status DBImpl::NewDB()
{
	VersionEdit new_db;
	new_db.SetComparatorName(user_comparator()->Name());
	new_db.SetLogNumber(0);
	new_db.SetNextFile(2);
	new_db.SetLastSequence(0);

	//MANIFEST-1并对其打开
	const std::string manifest = DescriptorFileName(dbname_, 1);
	WritableFile* file;
	Status s= env_->NewWritableFile(manifest, &file);
	if(!s.ok())
		return s;

	{//将version edit中的初始化数据作为日志写入到MANIFEST-1当中
		log::Writer log(file);
		std::string record;
		new_db.EncodeTo(&record);
		s = log.AddRecord(record);
		if(s.ok())
			s = file->Close();
	}

	delete file;
	if(s.ok())
		s = SetCurrentFile(env_, dbname_, 1);
	else
		env_->DeleteFile(manifest);

	return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const
{
	if(s->ok() || options_.paranoid_checks){
	}
	else{
		Log(options_.info_log, "Ignaring error %s", s->ToString().c_str());
		*s = Status::OK();
	}
}

//删除废弃的文件
void DBImpl::DeleteObsoleteFiles()
{
	if(!bg_error_.ok())
		return ;

	//获取当前有效的文件
	std::set<uint64_t> live = pending_outputs_;
	versions_->AddLiveFiles(&live);
	
	std::vector<std::string> filenames;
	env_->GetChildren(dbname_, &filenames);
	uint64_t number;
	FileType type;
	
	for(size_t i = 0; i < filenames.size(); i ++){
		if(ParseFileName(filenames[i], &number, &type)){ //对NUMBER进行校验，如果不是当前版本有效的number就表示可以删除
			bool keep = true;
			switch(type){
			case kLogFile:
				keep = ((number >= versions_->LogNumber()) || (number == versions_->PrevLogNumber()));
				break;

			case kDescriptorFile:
				keep = (number >= versions_->ManifestFileNumber());
				break;

			case kTableFile:
				keep = (live.find(number) != live.end());
				break;

			case kTempFile:
				keep = (live.find(number) !=live.end());
				break;

			case kCurrentFile:
			case kDBLockFile:
			case kInfoLogFile:
				keep = true;
				break;

			}

			//文件需要删除
			if(!keep){
				if(type == kTableFile) //删除table cache中对应的单元
					table_cache_->Evict(number);
			}

			Log(options_.info_log, "Delete type=%d #%lld\n", int(type), static_cast<unsigned long long>(number)); 
			//删除文件
			env_->DeleteFile(dbname_ + "/" + filenames[i]);
		}
	}
}

Status DBImpl::Recover(VersionEdit* edit)
{
	mutex_.AssertHeld();

	env_->CreateDir(dbname_);
	assert(db_lock_ == NULL);

	//产生一个文件锁
	Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
	if(!s.ok())
		return s;

	if(!env_->FileExists(CurrentFileName(dbname_))){
		if(options_.create_if_missing){
			s = NewDB();
			if(s.ok())
				return s;
		}
		else
			return Status::InvalidArgument(dbname_, "does not exist (create_if_missing is false)");
	}
	else{
		if(options_.error_if_exists)
			return Status::InvalidArgument(dbname_, "exists (error_if_exists is true)");
	}

	//进行version set读取日志恢复
	s = versions_->Recover();
	if(s.ok()){
		SequenceNumber max_sequence(0);
		const uint64_t min_log = versions_->LogNumber();
		const uint64_t prev_log = versions_->PrevLogNumber();

		std::vector<std::string> filenames;
		s = env_->GetChildren(dbname_, &filenames);
		if(!s.ok())
			return s;

		std::set<uint64_t> expected;
		versions_->AddLiveFiles(&expected);
		uint64_t number;
		FileType type;
		
		//对生成后的文件与内存中记录的file meta Data个数进行一一对比
		std::vector<uint64_t> logs;
		for (size_t i = 0; i < filenames.size(); i++) {
			if (ParseFileName(filenames[i], &number, &type)) {
				expected.erase(number);
				if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
					logs.push_back(number);
			}
		}

		//内存中的meta data还有剩，说明磁盘上的文件可能有问题
		if(!expected.empty()){
			char buf[50];
			snprintf(buf, sizeof(buf), "%d missing files; e.g.", static_cast<int>(expected.size()));
			return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
		}

		//进行数据库日志文件的读取，并回复
		std::sort(logs.begin(), logs.end());
		for(size_t i = 0; i < logs.size(); i++){
			s = RecoverLogFile(logs[i], edit, &max_sequence);
			versions_->MarkFileNumberUsed(logs[i]); //标记version set的文件序号
		}

		if(s.ok()){
			if(versions_->LastSequence() < max_sequence)
				versions_->SetLastSequence(max_sequence);
		}
	}

	return s;
}

Status DBImpl::RecoverLogFile(uint64_t log_number, VersionEdit* edit, SequenceNumber* max_sequence)
{
	struct LogReporter : public log::Reader::Reporter
	{
		Env* env;
		Logger* info_log;
		const char* fname;
		Status* status;

		virtual void Corruption(size_t bytes, const Status& s)
		{
			Log(info_log, "%s%s: dropping %d bytes; %s", (this->status == NULL ? "(ignoring error) " : ""), 
				fname, static_cast<int>(bytes), s.ToString().c_str());

			if(this->status != NULL && this->status->ok())
				*this->status = s;
		}
	};

	mutex_.AssertHeld();

	//打开顺序读取的log文件
	std::string fname = LogFileName(dbname_, log_number);
	SequentialFile* file;
	Status status = env_->NewSequentialFile(fname, &file);
	if(!status.ok()){
		MaybeIgnoreError(&status);
		return status;
	}

	//构建一个Log Reader
	LogReporter reporter;
	reporter.env = env_;
	reporter.info_log = options_.info_log;
	reporter.fname = fname.c_str();
	reporter.status = (options_.paranoid_checks ? &status : NULL);

	log::Reader reader(file, &reporter, true, 0);
	Log(options_.info_log, "Recovering log #%llu", (unsigned long long) log_number);

	//读出所有LOG中的记录并加到memtable当中
	std::string scratch;
	Slice record;
	WriteBatch batch;
	MemTable* mem = NULL;

	while(reader.ReadRecord(&record, &scratch) && status.ok()){
		if(record.size() < 12){ //LOG的记录头为12个字节
		   reporter.Corruption(record.size(), Status::Corruption("log record too small"));
			continue;
		}

		WriteBatchInternal::SetContents(&batch, record);

		if(mem == NULL){
			mem = new MemTable(internal_comparator_);
			mem->Ref();
		}
		//记录写入memtable
		status = WriteBatchInternal::InsertInto(&batch, mem);
		MaybeIgnoreError(&status);
		if(!status.ok())
		break;

		//计算max sequence
		const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) + WriteBatchInternal::Count(&batch) - 1;
		if(last_seq > *max_sequence)
			 *max_sequence = last_seq;

		if(mem->ApproximateMemoryUsage() > options_.write_buffer_size){ //内存使用达到上限
			status = WriteLevel0Table(mem, edit, NULL); 
			if(!status.ok())
				break;

			//写入完成后，可以释放掉mem对象，等下一次循环再开辟一个新的mem
			mem->Unref();
			mem = NULL;
		}
	}
	
	//写入level 0当中
	if(status.ok() && mem != NULL)
		status = WriteLevel0Table(mem, edit, NULL);

	if(mem != NULL)
		mem->Unref();

	delete file;
	
	return status;
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base)
{
	mutex_.AssertHeld();

	const uint64_t start_micros = env_->NowMicros();
	FileMetaData meta;
	meta.number = versions_->NewFileNumber();
	//记录正在生成的file numner
	pending_outputs_.insert(meta.number);

	Iterator* iter = mem->NewIterator();
	Log(options_.info_log, "Level-0 table #%llu: started", (unsigned long long) meta.number);

	Status s;
	{
		mutex_.Unlock();
		//将memtable中的数据写入到meta file当中，并制造成block文件格式
		s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
		mutex_.Lock();
	}

	Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s", (unsigned long long) meta.number,
      (unsigned long long) meta.file_size, s.ToString().c_str());

	delete iter;
	//把file number从中删除,因为已经完成了写入
	pending_outputs_.erase(meta.number);

	int level = 0;
	if(s.ok() && meta.file_size > 0){
		const Slice min_user_key = meta.smallest.user_key();
		const Slice max_user_key = meta.largest.user_key();
		if(base != NULL)
			level = base->PickLevelForMemTableOutput(min_user_key, max_user_key); //选择一个合适的可以Compact的层，
		//加入到version edit当中
		edit->AddFile(level, meta.number, meta.file_size, meta.smallest, meta.largest);
	}

	//记录Compaction的统计信息
	CompactionStats stats;
	stats.micros = env_->NowMicros() - start_micros;
	stats.bytes_written = meta.file_size;
	stats_[level].Add(stats);

	return s;
}

void DBImpl::CompactMemTable()
{
	mutex_.AssertHeld();
	assert(imm_ != NULL);

	VersionEdit edit;
	Version* base = versions_->current();
	base->Ref();

	Status s = WriteLevel0Table(imm_, &edit, base);
	base->Unref();

	if(s.ok() && shutting_down_.Acquire_Load())
		s = Status::IOError("Deleting DB during memtable compaction");

	if(s.ok()){
		edit.SetPrevLogNumber(0);
		edit.SetLogNumber(logfile_number_);
		s = versions_->LogAndApply(&edit, &mutex_);
	}

	if(s.ok()){
		imm_->Unref();
		imm_ = NULL;
		has_imm_.Release_Store(NULL);
		DeleteObsoleteFiles();
	}
	else
		RecordBackgroundError(s);
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end)
{
	int max_level_with_files = 1;
	{
		MutexLock l(&mutex_);
		Version* base = versions_->current();
		for(int level = 1; level < config::kNumLevels; level ++){
			if(base->OverlapInLevel(level, begin, end))
				max_level_with_files = level;
		}
	}

	TEST_CompactMemTable();
	for(int level = 0; level < max_level_with_files; level++)
		TEST_CompactRange(level, begin, end);
}

Status DBImpl::TEST_CompactMemTable()
{
	Status s = Write(WriteOptions(), NULL);
	if(s.ok()){
		MutexLock l(&mutex_);
		while(imm_ != NULL && bg_error_.ok())
			bg_cv_.Wait();

		if(imm_ != NULL)
			s = bg_error_;
	}

	return s;
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin, const Slice* end)
{
	assert(level > 0);
	assert(level + 1 < config::kNumLevels);

	InternalKey begin_storage, end_storage;

	ManualCompaction manual;
	manual.level = level;
	manual.done = false;
	if(begin == NULL)
		manual.begin = NULL;
	else{
		begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
		manual.begin = &begin_storage;
	}

	if(end == NULL)
		manual.end = NULL;
	else{
		end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
		manual.end = &end_storage;
	}

	//等待compact完成？
	MutexLock l(&mutex_);
	while(!manual.done && !shutting_down_.Acquire_Load() && bg_error_.ok()){
		if(manual_compaction_ == NULL){
			manual_compaction_ = &manual;
			MaybeScheduleCompaction();
		}
		else
			bg_cv_.Wait();
	}

	if(manual_compaction_ == &manual)
		manual_compaction_ = NULL;
}

void DBImpl::RecordBackgroundError(const Status& s)
{
	mutex_.AssertHeld();
	if(bg_error_.ok()){
		bg_error_ = s;
		bg_cv_.SignalAll();
	}
}

void DBImpl::MaybeScheduleCompaction()
{
	mutex_.AssertHeld();

	//已经开始Compact scheduled
	if(bg_compaction_scheduled_){
	}
	else if(shutting_down_.Acquire_Load()){ //DB正在被删除
	}
	else if(!bg_error_.ok()){ //已经产生一个错误，不能进行Compact
	}
	else if(imm_ == NULL && manual_compaction_ == NULL && !versions_->NeedsCompaction()){ //没有需要compact的条件或者数据
	}
	else{
		bg_compaction_scheduled_ = true;
		env_->Schedule(&DBImpl::BGWork, this); //产生一个线程进行对应的compact，相当于数据库的后台工作线程
	}
}

void DBImpl::BGWork(void* db)
{
	reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

void DBImpl::BackgroundCall()
{
	MutexLock l(&mutex_);
	assert(bg_compaction_scheduled_);

	if(shutting_down_.Acquire_Load()){
	}
	else if(!bg_error_.ok()){
	}
	else
		BackgroundCompaction(); //后台Compact

	bg_compaction_scheduled_ = false;

	//再次检查compact Schedule
	MaybeScheduleCompaction();
}

void DBImpl::BackgroundCompaction()
{
	mutex_.AssertHeld();

	//先对imm_ table进行写入文件
	if(imm_ != NULL){
		CompactMemTable();
		return ;
	}

	Compaction* c;
	bool is_manual = (manual_compaction_ != NULL);
	InternalKey manual_end;
	if(is_manual){
		ManualCompaction* m = manual_compaction_;
		//通过version set的Compact进行计算得到一个Compaction
		c = versions_->CompactRange(m->level, m->begin, m->end);

		m->done = (c == NULL);
		if(c != NULL) //重新确定manual end
			manual_end = c->input(0, c->num_input_files(0) - 1)->largest;

		Log(options_.info_log,
			"Manual compaction at level-%d from %s .. %s; will stop at %s\n",
			m->level,
			(m->begin ? m->begin->DebugString().c_str() : "(begin)"),
			(m->end ? m->end->DebugString().c_str() : "(end)"),
			(m->done ? "(end)" : manual_end.DebugString().c_str()));
	}
	else //如果不是手动制定Compact范围的话，在versions进行Compact检测得到一个Compaction
		c = versions_->PickCompaction();

	Status status;
	if(c == NULL){
	}
	else if (!is_manual && c->IsTrivialMove()){ //有Compact的需求,将Compact需求的文件移动到下一个LEVEL
		assert(c->num_input_files(0) == 1);

		FileMetaData* f = c->input(0, 0);
		c->edit()->DeleteFile(c->level(), f->number);
		c->edit()->AddFile(c->level() + 1, f->number, f->file_size, f->smallest, f->largest);

		status = versions_->LogAndApply(c->edit(), &mutex_);
		if(!status.ok()) //出现一个错误
			RecordBackgroundError(status);

		VersionSet::LevelSummaryStorage tmp;
		Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
			static_cast<unsigned long long>(f->number), c->level() + 1, static_cast<unsigned long long>(f->file_size),
			status.ToString().c_str(), versions_->LevelSummary(&tmp));
	}
	else{ //进行相关的数据合并？
		CompactionState* compact = new CompactionState(c);
		status = DoCompactionWork(compact);
		if(!status.ok())
			RecordBackgroundError(status);

		CleanupCompaction(compact);
		c->ReleaseInputs();
		//删除掉废弃的文件
		DeleteObsoleteFiles();
	}

	delete c;

	if(status.ok()){
	}
	else if(shutting_down_.Acquire_Load()){
	}
	else
		Log(options_.info_log, "Compaction error: %s", status.ToString().c_str());

	if(is_manual){
		 ManualCompaction* m = manual_compaction_;
		 if(!status.ok())
			 m->done = true;

		 if(!m->done){
			 m->tmp_storage = manual_end;
			 m->begin = &m->tmp_storage;
		}
		manual_compaction_ = NULL;
	}
}

void DBImpl::CleanupCompaction(CompactionState* compact)
{
	mutex_.AssertHeld();
	if(compact->builder != NULL){
		compact->builder->Abandon();
		delete compact->builder;
	}
	else{
		assert(compact->outfile == NULL);
	}

	delete compact->outfile;
	//从pending中删除正在处理的文件number
	for(size_t i = 0; i < compact->outputs.size(); i ++){
		const CompactionState::Output& out = compact->outputs[i];
		pending_outputs_.erase(out.number);
	}

	delete compact;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact)
{
	assert(compact != NULL);
	assert(compact->builder == NULL);

	uint64_t file_number;
	{
		//产生一个文件序号
		mutex_.Lock();
		file_number = versions_->NewFileNumber();
		pending_outputs_.insert(file_number);
		//打开一个Compact out对象，并加入到compact当中
		CompactionState::Output out;
		out.number = file_number;
		out.smallest.Clear();
		out.largest.Clear();
		compact->outputs.push_back(out);

		mutex_.Unlock();
	};
	//构建一个table file和table builder
	std::string fname = TableFileName(dbname_, file_number);
	Status s = env_->NewWritableFile(fname, &compact->outfile);
	if(s.ok())
		compact->builder = new TableBuilder(options_, compact->outfile);

	return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact, Iterator* input)
{
	assert(compact != NULL);
	assert(compact->outfile != NULL);
	assert(compact->builder != NULL);

	const uint64_t output_number = compact->current_output()->number;
	assert(output_number != 0);

	Status s = input->status();
	const uint64_t current_entries = compact->builder->NumEntries();
	if(s.ok())
		s = compact->builder->Finish();
	else
		compact->builder->Abandon();

	//计算Compact的字节数
	const uint64_t current_bytes = compact->builder->FileSize();
	compact->current_output()->file_size = current_bytes;
	compact->total_bytes += current_bytes;

	delete compact->builder;
	compact->builder = NULL;

	//内容写入磁盘
	if(s.ok())
		s = compact->outfile->Sync();

	if(s.ok())
		s = compact->outfile->Close();
	//释放文件操作句柄
	delete compact->outfile;
	compact->outfile = NULL;

	//校验table是否可用
	if(s.ok() && current_entries > 0){
		Iterator* iter = table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
		s = iter->status();
		delete iter;

		if(s.ok()){
			Log(options_.info_log, "Generated table #%llu: %lld keys, %lld bytes",
			(unsigned long long) output_number,
			(unsigned long long) current_entries,
			(unsigned long long) current_bytes);
		}
	}

	return s;
}

Status DBImpl::InstallCompactionResults(CompactionState* compact)
{
	mutex_.AssertHeld();

	Log(options_.info_log,  "Compacted %d@%d + %d@%d files => %lld bytes",
		compact->compaction->num_input_files(0),
		compact->compaction->level(),
		compact->compaction->num_input_files(1),
		compact->compaction->level() + 1,
		static_cast<long long>(compact->total_bytes));

	//删除输入的Compact files,因为这些文件被Compact了
	compact->compaction->AddInputDeletions(compact->compaction->edit());

	const int level = compact->compaction->level();
	//为version edit增加有效的Compact files
	for(size_t i = 0; i < compact->outputs.size(); i++){
		const CompactionState::Output& out = compact->outputs[i];
		compact->compaction->edit()->AddFile(level + 1, out.number, out.file_size, out.smallest, out.largest);
	}

	return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}

Status DBImpl::DoCompactionWork(CompactionState* compact)
{
	const uint64_t start_micros = env_->NowMicros();
	int64_t imm_micros = 0;

	Log(options_.info_log,  "Compacting %d@%d + %d@%d files",
		compact->compaction->num_input_files(0),
		compact->compaction->level(),
		compact->compaction->num_input_files(1),
		compact->compaction->level() + 1);

	assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
	assert(compact->builder == NULL);
	assert(compact->outfile == NULL);

	if(snapshots_.empty())
		compact->smallest_snapshot = versions_->LastSequence();
	else
		compact->smallest_snapshot = snapshots_.oldest()->number_;

	mutex_.Unlock();

	//获得一个merge iter
	Iterator* input = versions_->MakeInputIterator(compact->compaction);
	input->SeekToFirst();

	Status status;
	ParsedInternalKey ikey;
	std::string current_user_key;
	bool has_current_user_key = false;

	SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
	for(; input->Valid() && !shutting_down_.Acquire_Load(); ){
		if(has_imm_.NoBarrier_Load() != NULL){
			const uint64_t imm_start = env_->NowMicros();
			mutex_.Lock();
			if(imm_ != NULL){ //优先Compact mem table，因为imm中有可能有非常范围大的KEY VALUE或者删除的key
				CompactMemTable();
				bg_cv_.SignalAll();
			}

			mutex_.Unlock();
			imm_micros += (env_->NowMicros() - imm_start);
		}

		//判断是否Compact结束
		Slice key = input->key();
		if(compact->compaction->ShouldStopBefore(key) && compact->builder != NULL){
			status = FinishCompactionOutputFile(compact, input);
			if(!status.ok())
				break;
		}

		bool drop = false;
		if(!ParseInternalKey(key, &ikey)){ //key是一个非法的key
			current_user_key.clear();	  //清空掉key
			has_current_user_key = false; //标记为没有当前的key
			last_sequence_for_key = kMaxSequenceNumber;
		}
		else{
			//判断是否是这个iter的第一个key
			if(!has_current_user_key || user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) != 0){
				current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
				has_current_user_key = true;
				last_sequence_for_key = kMaxSequenceNumber;
			}

			//判断值是否需要抛弃
			if(last_sequence_for_key <= compact->smallest_snapshot) //有多条key重叠，这条可能是后面的记录覆盖了
				drop = true;
			else if(ikey.type == kTypeDeletion && ikey.sequence <= compact->smallest_snapshot
				&& compact->compaction->IsBaseLevelForKey(ikey.user_key)) //key被删除了
				drop = true;

			last_sequence_for_key = ikey.sequence;
		}

		if(!drop){
			if(compact->builder == NULL){ //刚开始，将out file和out table打开
				status = OpenCompactionOutputFile(compact);
				if(!status.ok())
					break;
			}

			if(compact->builder->NumEntries() == 0) //记录out meta file的smallest
				compact->current_output()->smallest.DecodeFrom(key);

			//每一次都记录为最大，因为不知道那一次为结束
			compact->current_output()->largest.DecodeFrom(key);
			compact->builder->Add(key, input->value());
			
			//文件到允许的最大值了,结束这个block table
			if(compact->builder->FileSize() >= compact->compaction->MaxOutputFileSize()){
				status = FinishCompactionOutputFile(compact, input);
				if(!status.ok())
					break;
			}
		}
		//处理下一条记录
		input->Next();
	}

	if(status.ok() && shutting_down_.Acquire_Load())
		status = Status::IOError("Deleting DB during compaction");

	if(status.ok() && compact->builder != NULL)
		status = FinishCompactionOutputFile(compact, input);

	if(status.ok())
		status =input->status();

	//删除掉迭代器
	delete input;
	input = NULL;

	//计算文件Compact的耗时，这里减去了imm Compact的耗时
	CompactionStats stats;
	stats.micros = env_->NowMicros() - start_micros - imm_micros;
	//计算input bytes 和output bytes
	for(int which = 0; which < 2; which ++)
		for(int i = 0; i < compact->compaction->num_input_files(which); i ++)
			stats.bytes_read += compact->compaction->input(which, i)->file_size;

	for(size_t i = 0; i < compact->outputs.size(); i ++)
		stats.bytes_written += compact->outputs[i].file_size;

	mutex_.Lock();
	stats_[compact->compaction->level() + 1].Add(stats);

	//对Compact 后的meta files进行整理
	if(status.ok())
		status = InstallCompactionResults(compact);

	if(!status.ok())
		RecordBackgroundError(status);

	VersionSet::LevelSummaryStorage tmp;
	Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));

	return status;
}

namespace {
struct IterState {
		port::Mutex* mu;
		Version* version;
		MemTable* mem;
		MemTable* imm;
};

static void CleanupIteratorState(void* arg1, void* arg2)
{
	IterState* state = reinterpret_cast<IterState*>(arg1);
	
	state->mu->Lock();

	state->mem->Unref();
	if (state->imm != NULL) 
		state->imm->Unref();

	state->version->Unref();
	state->mu->Unlock();

	delete state;
}

};

};


