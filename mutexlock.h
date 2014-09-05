#ifndef __LEVEL_DB_MUTEXLOCK_H_
#define __LEVEL_DB_MUTEXLOCK_H_

namespace leveldb{

class SCOPED_LOCKABLE MutexLock
{
public:
	explicit MutexLock(port::Mutex *mu) EXCLUSIVE_LOCK_FUNCTION(mu) : mu_(mu)  
	{
		this->mu_->Lock();
	}

	~MutexLock() UNLOCK_FUNCTION() 
	{
		this->mu_->Unlock();
	}

private:
	//隐藏拷贝构造和赋值
	MutexLock(const MutexLock&);
	void operator=(const MutexLock&);

	port::Mutex *const mu_;
};

}

#endif
