#include "Thread.h"
#include "CountDownLatch.h"
#include "CurrentThread.h"
#include "Thread.h"

#include <atomic>
#include <cassert>
#include <cstddef>
#include <linux/prctl.h>
#include <pthread.h>
#include <sched.h>
#include <string>
#include <utility>
#include <sys/prctl.h>


namespace clearmoon
{
namespace util
{
struct ThreadData
{
public:
using ThreadFunc = clearmoon::Thread::ThreadFunc;

    ThreadData(ThreadFunc cb, std::string& name, pid_t* tid, CountDownLatch* latch) : threadFunc_(std::move(cb)), name_(name), tid_(tid),latch_(latch)
    {
    }

    void runInThread()
    {
        *tid_ = clearmoon::Current::tid();
        tid_ = nullptr;
        latch_->countDown();

        Current::t_threadName = name_.empty()? "clearmoonThread" : name_.c_str();
        ::prctl(PR_SET_NAME, Current::t_threadName);

        if(threadFunc_) threadFunc_();

        Current::t_threadName = "Finished";
    }

private:
    ThreadFunc threadFunc_;
    pid_t* tid_;
    std::string name_;
    CountDownLatch* latch_;

};


void* startThread(void* obj)
{
    ThreadData* threadData = static_cast<ThreadData*>(obj);
    
    threadData->runInThread();

    delete threadData;
    return NULL;
}

}//util
}//clearmoon


using namespace clearmoon;
using namespace clearmoon::util;

std::atomic<int>Thread::numCurrent_ = 0;

Thread::Thread(ThreadFunc cb, std::string threadName) : started_(false), joined_(false), threadId_(0), tid_(Current::t_cacheTid), threadName_(threadName), threadFunc_(std::move(cb)),latch_(1)
{
    numCurrent_++;
}

Thread::~Thread()
{
    if(started_ && !joined_)
    {
        pthread_detach(threadId_);
    }
}


void Thread::start()
{
    assert(!started_);
    started_ = true;

    util::ThreadData* data = new util::ThreadData(threadFunc_, threadName_, &tid_, &latch_);
    if(pthread_create(&threadId_, NULL, &util::startThread, data))
    {
        started_ = false;
        delete data;
    }
    else {
        latch_.wait();
        assert(tid_ > 0);
    }
}

int Thread::join()
{   
    assert(started_);
    assert(!joined_);
    joined_ = true;
    return pthread_join(threadId_, NULL);
}