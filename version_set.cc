#include <algorithm>
#include <stdio.h>

#include "version_set.h"
#include "filename.h"
#include "log_reader.h"
#include "log_write.h"
#include "memtable.h"
#include "table_cache.h"
#include "table_builder.h"
#include "merger.h"
#include "two_level_iterator.h"
#include "coding.h"
#include "logging.h"

namespace leveldb{

static const int kTargetFileSize = 2 * 1048576; //2M

static const int64_t kMaxGrandParentOverlapBytes = 10 * kTargetFileSize; // 20M

static const int64_t kExpandedCompactionByteSizeLimit = 25 * kTargetFileSize; // 50M

static double MaxBytesForLevel(int level)
{
	double result = 10 * 1048576.0; //层基数10M
	while(level > 1){
		result *= 10;
		level --;
	}

	return result;
}

static uint64_t MaxFileSizeForLevel(int level)
{
	return kTargetFileSize; 
}

static int64_t TotalFileSize(const std::vector<FileMetaData*>& files)
{
	int64_t sum = 0;
	for(size_t i = 0; i < files.size(); i ++)
		sum += files[i]->file_size;

	return sum;
}

namespace{
std::string IntSetToString(const std::set<uint64_t>& s)
{
	std::string result = "{";
	for(std::set<uint64_t>::const_iterator it = s.begin(); it != s.end(); ++ it){
		result += (result.size() > 1) ? "," : "";
		result += NumberToString(*it);
	}

	result += "}";
	return result;
}
}//namesapce

Version::~Version()
{
	assert(refs_ == 0);
	prev_->next_ = next_;
	next_->prev_ = prev_;

	//删除掉这个version obj 的FileMeta
	for(int level = 0; level < config::kNumLevels; level ++){
		for(size_t i = 0; i < files_[level].size(); i ++){
			FileMetaData* f = files_[level][i];
			assert(f->refs > 0);

			f->refs --;
			if(f->refs <= 0)
				delete f;
		}
	}
}

//查找kye所在的文件序号
int FindFile(const InternalKeyComparator& icmp, const std::vector<FileMetaData*>& files, const Slice& key)
{
	uint32_t left = 0;
	uint32_t right = files.size();
	
	//2分查找
	while(left < right){
		uint32_t mid = (left + right) / 2;
		const FileMetaData* f = files[mid];
		if(icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) < 0)
			left = mid + 1;
		else
			right = mid;
	}

	return right;
}

static bool AfterFile(const Comparator* ucmp, const Slice* user_key, const FileMetaData* f)
{
	return (user_key != NULL && ucmp->Compare(*user_key, f->largest.user_key()) > 0);
}

static bool BeforeFile(const Comparator* ucmp, const Slice* user_key, const FileMetaData* f)
{
	return (user_key != NULL && ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
}

//定位smallest_user_key ~ largest_user_key是否在文件files的范围当中当中
bool SomeFileOverlapsRange(const InternalKeyComparator& icmp, bool disjoint_sorted_files, const std::vector<FileMetaData*>& files,
	const Slice* smallest_user_key, const Slice* largest_user_key)
{
	const Comparator* ucmp = icmp->user_comparator();
	if(!disjoint_sorted_files){
		for(size_t i = 0; i < files.size(); i ++){
			const FileMetaData* f = files[i];
			if(AfterFile(ucmp, smallest_user_key, f) || BeforeFile(ucmp, largest_user_key, f)){
				//没有重叠区间，往下层找
			}
			else
				return true; //和此文件有重叠
		}

		return false;
	}

	uint32_t index = 0;
	if(smallest_user_key != NULL){
		InternalKey small(*smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek); //生成一个内部key(user_key + seq + type)
		index = FindFile(icmp, files, small.Encode()); //查找small所在的文件序号
	}

	//未查找到对应的文件序号
	if(index >= files.size())
		return false;

	return !BeforeFile(ucmp, largest_user_key, files[index]);
}

//对files_的iter
class Version::LevelFileNumIterator : public Iterator
{
public:
	LevelFileNumIterator(const InternalKeyComparator& icmp, const std::vector<FileMetaData*>* flist)
		: icmp_(icmp), flist_(flist), index_(flist->size())
	{
	}

	virtual bool Valid() const
	{
		return index_ < flist_->size();
	}

	virtual void Seek(const Slice& target)
	{
		index_ = FindFile(icmp_, *flist_, target);
	}

	virtual void SeekToFirst()
	{
		index_ = 0;
	};

	virtual void SeekToLast()
	{
		index_ = flist_->empty() ? 0 : flist_->size() - 1;
	}

	virtual void Next()
	{
		assert(Valid());
		index_ ++;
	}

	virtual void Prev()
	{
		assert(Valid());
		if(index_ == 0){
			index_ = flist_->size(); //标识为非法
		}
		else{
			index_ --;
		}
	}

	Slice key() const
	{
		assert(Valid());
		return (*flist_)[index_]->largest.Encode();
	}

	Slice value() const
	{
		assert(Valid());
		EncodeFixed64(value_buf_, (*flist_)[index_]->number); //文件序号
		EncodeFixed64(value_buf_+ 8, (*flist_)[index_]->file_size); //文件大小
		return Slice(value_buf_, sizeof(value_buf_));
	}

	virtual Status status() const {return Status::OK();};

private:
	const InternalKeyComparator icmp_;
	const std::vector<FileMetaData*>* const flist_;
	uint32_t index_;

	mutable char value_buf_[16];
};

static Iterator* GetFileIterator(void* arg, const ReadOptions& opt, const Slice& file_value)
{
	TableCache* cache = reinterpret_cast<TableCache*>(arg);
	if(file_value.size() != 16)
		return NewErrorIterator(Status::Corruption("FileReader invoked with unexpected value"));
	else//通过table cache构建一个two level的iter
		return cache->NewIterator(opt, DecodeFixed64(file_value.data()), DecodeFixed64(file_value.data() + 8));

}

Iterator* Version::NewConcatenatingIterator(const ReadOptions& opt, int level) const
{
	return NewTwoLevelIterator(new LevelFileNumIterator(vset_->icmp_, &files_[level]), &GetFileIterator, vset_->table_cache_, opt);
}

void Version::AddIterators(const ReadOptions& opt, std::vector<Iterator*>* iters)
{
	//level 0的table cache迭代器
	for(size_t i = 0; i < files_[0].size(); i ++){
		//获得一个table cache的iter
		Iterator* cache_iter = vset_->table_cache_->NewIterator(opt, files_[0][i]->number, files_[0][i]->file_size);
		iters->push_back(cache_iter);
	}

	//1 ~ level层返回tow level迭代器
	for(int level = 1; level < config::kNumLevels; level ++){
		if(!files_[level].empty()){
			iters->push_back(NewConcatenatingIterator(opt, level));
		}
	}
}

namespace {
enum SaverState{
	kNotFound,
	kFound,
	kDeleted,
	kCorrupt,
};

struct Saver
{
	SaverState state;
	const Comparator* ucmp;
	Slice user_key;
	std::string* value;
};
}; //namespace

//将value存入Saver中
static void SaveValue(void* arg, const Slice& ikey, const Slice& v)
{
	Saver* s = reinterpret_cast<Saver*>(arg);
	ParsedInternalKey parsed_key;
	if(!ParseInternalKey(ikey, &parsed_key)){
		s->state = kCorrupt;
	}
	else{
		if(s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0){
			//判断key是不是被删除了
			s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
			if(s->state == kFound)
				s->value->assign(v.data(), v.size());
		}
	}
}

static bool NewestFirst(FileMetaData* a, FileMetaData* b)
{
	return a->number > b->number;
}

void Version::ForEachOverlapping(Slice user_key, Slice internal_key, void* arg, bool (*func)(void*, int, FileMetaData*))
{
	const Comparator* ucmp = vset_->icmp_.user_comparator();

	//search level 0
	std::vector<FileMetaData*> tmp;
	tmp.reserve(files_[0].size());
	for(uint32_t i = 0; i < files_[0].size(); i ++){
		FileMetaData* f = files_[0][i];
		if(ucmp->Compare(user_key, f->smallest.user_key()) >= 0 && ucmp->Compare(user_key, f->largest.user_key()) <= 0)
			tmp.push_back(f);
	}

	if(!tmp.empty()){
		std::sort(tmp.begin(), tmp.end(), NewestFirst);
		for(uint32_t i = 0; i < tmp.size(); i ++){
			if(!(*func)(arg, 0, tmp[i]))
				return ;
		}
	}

	//查找1 ~ level层
	for(int level =1; level < config::kNumLevels; level ++){
		size_t num_files = files_[level].size();
		if(num_files == 0)
			continue;

		//采用2分查找法进行查找
		uint32_t index = FindFile(vset_->icmp_, files_[level], internal_key);
		if(index < num_files){
			FileMetaData* f = files_[level][index];
			if(ucmp->Compare(user_key, f->smallest.user_key()) < 0){ //user_key 比f->smallest还小，表示应该往下层继续搜索??

			}
			else{
				if (!(*func)(arg, level, f))
					return;
			}
		}
	}
}

Status Version::Get(const ReadOptions& opt, const LookupKey& k, std::string* value, GetStats* stats)
{
	Slice ikey = k.internal_key();
	Slice user_key = k.user_key();

	const Comparator* ucmp = vset_->icmp_.user_comparator();
	Status s;

	stats->seek_file = NULL;
	stats->seek_file_level = -1;

	FileMetaData* last_file_read = NULL;
	int last_file_read_level = -1;

	std::vector<FileMetaData*> tmp;
	FileMetaData* tmp2;

	for(int level = 0; level< config::kNumLevels; level ++){
		size_t num_files = files_[level].size();
		if(num_files)
			continue;

		FileMetaData* const* files = &files_[level][0];
		if(level == 0){ //0层搜索

			tmp.reserve(num_files);
			for(uint32_t i = 0; i < num_files; i ++){ //找到所有存有user_key的file
				FileMetaData* f = files[i];
				if(ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
					ucmp->Compare(user_key, f->largest.user_key())<= 0)
					tmp.push_back(f);
			}

			if(tmp.empty())
				continue;
			std::sort(tmp.begin(), tmp.end(), NewestFirst);
			files = &tmp[0];
			num_files = tmp.size();
		}
		else{ //其他层的搜索，找到ikey所在的文件，
			uint32_t index = FindFile(vset_->icmp_, files_[level], ikey);
			if(index >= num_files){
				files = NULL;
				num_files = 0;
			}
			else{
				tmp2 = files[index];
				if(ucmp->Compare(user_key, tmp2->smallest.user_key()) < 0){
					files = NULL;
					num_files = 0;
				}
				else{
					files = &tmp2;
					num_files = 1;
				}
			}
		}

		for(uint32_t i = 0; i < num_files; ++ i){
			if(last_file_read != NULL && stats->seek_file == NULL){
				stats->seek_file = last_file_read;
				stats->seek_file_level = last_file_read_level;
			}

			FileMetaData* f = files[i];
			last_file_read = f;
			last_file_read_level = level;

			Saver saver;
			saver.state = kNotFound;
			saver.ucmp = ucmp;
			saver.user_key = user_key;
			saver.value = value;
			//从table cache中进行key value从查找
			s = vset_->table_cache_->Get(opt, f->number, f->file_size, ikey, &saver, SaveValue);
			if(!s.ok())
				return s;

			switch(saver.state){
			case kNotFound:
				break;
			case kFound:
				break;
			case kDeleted:
				s = Status::NotFound(Slice());
				break;
			case kCorrupt:
				s = Status::Corruption("corrupted key for ", user_key);
				return s;
			}
		}
	}
	return Status::NotFound(Slice());  // Use an empty error message for speed
}

bool Version::UpdateStats(const GetStats& stats)
{
	FileMetaData* f = stats.seek_file;
	if(f != NULL){
		f->allowed_seeks --;
		if(f->allowed_seeks <= 0 && file_to_compact_ == NULL){
			file_to_compact_ = f;
			file_to_compact_level_ = stats.seek_file_level;
			return true;
		}
	}

	return false;
}

bool Version::RecordReadSample(Slice internal_key)
{
	struct State {
		GetStats stats;  // Holds first matching file
		int matches;

		static bool Match(void* arg, int level, FileMetaData* f) {
			State* state = reinterpret_cast<State*>(arg);
			state->matches++;
			if (state->matches == 1) {
				// Remember first match.
				state->stats.seek_file = f;
				state->stats.seek_file_level = level;
			}
			// We can stop iterating once we have a second match.
			return state->matches < 2;
		}
	};


	ParsedInternalKey ikey;
	if (!ParseInternalKey(internal_key, &ikey))
		return false;

	State state;
	state.matches = 0;
	//查找user_key重叠的次数
	ForEachOverlapping(ikey.user_key, internal_key, &state, &State::Match);
	if(state.matches >= 2) //至少有一次重叠
		return UpdateStats(state.stats);

	return false;
}

void Version::Ref()
{
	++ refs_;
}

void Version::Unref()
{
	assert(this != &vset_->dummy_versions_);
	assert(refs_ >= 1);

	-- refs_;
	if(refs_ == 0)
		delete this;
}

//level层是否与{smallest_user_key, largest_user_key}有重叠
bool Version::OverlapInLevel(int level, const Slice* smallest_user_key, const Slice* largest_user_key)
{
	return SomeFileOverlapsRange(vset_->icmp_, (level > 0), files_[level], smallest_user_key, largest_user_key);
}

int Version::PickLevelForMemTableOutput(const Slice& smallest_user_key, const Slice& largest_user_key)
{
	int level = 0;
	//与第0层有重叠
	if(!OverlapInLevel(0, &smallest_user_key, &largest_user_key)){
		//构建internal key的上下限
		InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
		InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));

		std::vector<FileMetaData*> overlaps;
		while(level < config::kMaxMemCompactLevel){
			if(OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key)){
				break;
			}

			if(level + 2 < config::kNumLevels){
				GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
				const int64_t sum = TotalFileSize(overlaps);
				if(sum > kMaxGrandParentOverlapBytes) //重叠区域超过20M
					break;
			}
		}
		level ++;
	}

	return level;
}

void Version::GetOverlappingInputs(int level, const InternalKey* begin, const InternalKey* end, std::vector<FileMetaData*>* inputs)
{
	assert(level >= 0);
	assert(level < config::kNumLevels);

	inputs->clear();
	Slice user_begin, user_end;
	if(begin != NULL)
		user_begin = begin->user_key();
	if(end != NULL)
		user_end = end->user_key();

	const Comparator* user_cmp = vset_->icmp_.user_comparator();

	for(size_t i = 0; i < files_[level].size();){
		FileMetaData* f = files_[level][i++];
		const Slice file_start = f->smallest.user_key();
		const Slice file_limit = f->smallest.user_key();

		if(begin != NULL && user_cmp->Compare(file_limit, user_begin) < 0){

		}else if(end != NULL && user_cmp->Compare(file_start, user_end) > 0){

		} //上两种情况是完全无重叠的情况
		else{
			inputs->push_back(f);
			if(level == 0){ //{begin, end}没有超过level 0的范围，level 0将不计算到inputs当中
				if(begin != NULL && user_cmp->Compare(file_start, user_begin) < 0){
					user_begin = file_start;
					inputs->clear();
					i = 0;
				}
				else if(end != NULL && user_cmp->Compare(file_limit, user_end) > 0){
					user_end = file_limit;
					inputs->clear();
					i = 0;
				}
			}
		}
	}
}

std::string Version::DebugString() const 
{
	std::string r;
	for (int level = 0; level < config::kNumLevels; level++) {
		r.append("--- level ");
		AppendNumberTo(&r, level);
		r.append(" ---\n");
		const std::vector<FileMetaData*>& files = files_[level];
		for (size_t i = 0; i < files.size(); i++) {
			r.push_back(' ');
			AppendNumberTo(&r, files[i]->number);
			r.push_back(':');
			AppendNumberTo(&r, files[i]->file_size);
			r.append("[");
			r.append(files[i]->smallest.DebugString());
			r.append(" .. ");
			r.append(files[i]->largest.DebugString());
			r.append("]\n");
		}
	}
	return r;
}

class VersionSet::Builder
{
private:
	struct BySmallestKey
	{
		const InternalKeyComparator* internal_comparator;
		bool operator()(FileMetaData* f1, FileMetaData* f2) const
		{
			int r = internal_comparator->Compare(f1->smallest, f2->smallest);
			if(r != 0)
				return r < 0;
			else
				return (f1->number < f2->number);
		}
	};

	typedef std::set<FileMetaData*, BySmallestKey> FileSet;

	struct LevelState
	{
		std::set<uint64_t> deleted_files;
		FileSet* added_files;
	};

	VersionSet* vset_;
	Version* base_;
	LevelState levels_[config::kNumLevels];

public:
	Builder(VersionSet* vset, Version* base) : vset_(vset), base_(base)
	{
		base_->Ref();
		BySmallestKey cmp;
		cmp.internal_comparator = &vset_->icmp_;
		for(int level = 0; level < config::kNumLevels; level ++){
			levels_[level].added_files = new FileSet(cmp);
		}
	}

	~Builder()
	{
		for(int level = 0; level < config::kNumLevels; level ++){
			const FileSet* added = levels_[level].added_files;
			std::vector<FileMetaData *> to_unref;
			
			to_unref.reserve(added->size());
			for(FileSet::const_iterator it = added->begin(); it != added->end(); ++ it)
				to_unref.push_back(*it);

			//释放掉fileSet
			delete added;
			//释放引用计数
			for(uint32_t i = 0; i < to_unref.size(); i ++){
				FileMetaData* f = to_unref[i];

				f->refs --;
				if(f->refs <= 0)
					delete f;
			}
		}

		base_->Unref();
	}

	//将edit中的delete files和add files加入到各层当中，等待SaveTo处理
	void Apply(VersionEdit* edit)
	{
		for(size_t i = 0; i < edit->compact_pointers_.size(); i ++){
			const int level = edit->compact_pointers_[i].first;
			vset_->compact_pointer_[level] = edit->compact_pointers_[i].second.Encode().ToString();
		}

		const VersionEdit::DeletedFileSet& del = edit->deleted_files_;
		for(VersionEdit::DeletedFileSet::const_iterator iter = del.begin(); iter != del.end(); ++ iter){
			const int level = iter->first;
			const uint64_t number = iter->second;
			levels_[level].deleted_files.insert(number); //将要删除的文件number插入到levels当中
		}

		//加入新的文件信息？？
		for(size_t i = 0; i < edit->new_files_.size(); i ++){
			const int level = edit->new_files_[i].first;
			FileMetaData* f = new FileMetaData(edit->new_files_[i].second);
			f->refs = 1;

			// We arrange to automatically compact this file after
			// a certain number of seeks.  Let's assume:
			//   (1) One seek costs 10ms
			//   (2) Writing or reading 1MB costs 10ms (100MB/s)
			//   (3) A compaction of 1MB does 25MB of IO:
			//         1MB read from this level
			//         10-12MB read from next level (boundaries may be misaligned)
			//         10-12MB written to next level
			// This implies that 25 seeks cost the same as the compaction
			// of 1MB of data.  I.e., one seek costs approximately the
			// same as the compaction of 40KB of data.  We are a little
			// conservative and allow approximately one seek for every 16KB
			// of data before triggering a compaction.
			//计算允许SEEK的次数,通过IO能力来计算
			f->allowed_seeks = f->file_size / 16384; //16k允许一次SEEK
			if(f->allowed_seeks < 100) //最小为100
				f->allowed_seeks = 100;

			levels_[level].deleted_files.erase(f->number);
			levels_[level].added_files->insert(f);
		}
	}

	void SaveTo(Version* v)
	{
		BySmallestKey cmp;
		cmp.internal_comparator = &vset_->icmp_;
		for(int level = 0; level < config::kNumLevels; level ++){
			const std::vector<FileMetaData*>& base_files = base_->files_[level];
			std::vector<FileMetaData*>::const_iterator base_iter = base_files.begin();
			std::vector<FileMetaData*>::const_iterator base_end = base_files.end();
			
			const FileSet* added = levels_[level].added_files;
			v->files_[level]->reserve(base_files.size() + added->size());

			//base_->files_ 添加added中新的file,顺序(由小到大)合并
			for(FileSet::const_iterator added_iter = added->begin(); added_iter != added->end(); ++ added_iter){
				for(std::vector<FileMetaData*>::const_iterator bpos  = std::upper_bound(base_iter, base_end, *added_iter, cmp);
					base_iter != bpos; ++ base_iter){
					MaybeAddFile(v, level, *base_iter);
				}
				MaybeAddFile(v, level, *added_iter);
			}

			//合并剩下的
			for(; base_iter != base_end; ++ base_iter)
				 MaybeAddFile(v, level, *base_iter);
		}
	}

	void MaybeAddFile(Version* v, int level, FileMetaData* f)
	{
		if(levels_[level].deleted_files.count(f->number) > 0){ //已经在deleted file set当中，忽略

		}
		else{
			std::vector<FileMetaData*>* files = &v->files_[level];
			if(level > 0 && !files->empty()){
				assert(vset_->icmp_.Compare((*files)[files->size()-1]->largest, f->smallest) < 0); //每次必须超出files的最大范围
			}

			//加入到files当中
			f->refs ++;
			files->push_back(f);
		}
	}
};

VersionSet::VersionSet(const std::string& dbname, const Options* opt, TableCache* table_cache, const InternalKeyComparator* icmp)
	: env_(opt->env), dbname_(dbname), options_(opt), table_cache_(table_cache), icmp_(*icmp), next_file_number_(2), manifest_file_number_(0),
	last_sequence_(0), log_number_(0), prev_log_number_(0), descriptor_file_(NULL), descriptor_log_(NULL), dummy_versions_(this),
	current_(NULL)
{
	AppendVersion(new Version(this));
}

VersionSet::~VersionSet()
{
	current_->Unref();
	assert(dummy_versions_.next_ == &dummy_versions_);

	delete descriptor_log_;
	delete descriptor_file_;
}

void VersionSet::AppendVersion(Version* v)
{
	assert(v->refs_ == 0);
	assert(v != current_);

	if(current_ != NULL)
		current_->Unref();

	current_ = v;
	v->Ref();

	v->prev_ = dummy_versions_.prev_;
	v->next_ = &dummy_versions_;
	v->prev_->next_ = v;
	v->next_->prev_ = v;
}

Status VersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mu)
{

}

};//leveldb



