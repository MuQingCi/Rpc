#ifndef CLEANMOON_BASE_THREAD_H
#define CLEANMOON_BASE_THREAD_H

#include "CountDownLatch.h"
#include "noncopy.h"
#include <atomic>
#include <functional>
#include <pthread.h>
#include <string>
#include <sys/types.h>
namespace clearmoon
{

class Thread : noncopyable
{
public:
    using ThreadFunc = std::function<void()>;
    explicit Thread(ThreadFunc cb, std::string threadName);
    ~Thread();

    void start();
    int join();

    std::string ThreadName() const { return threadName_; }
    pid_t ThreadTid() const { return  tid_; }
    pthread_t threadId() const { return threadId_; }

    bool Stared() const { return started_; }

private:
    bool started_;
    bool joined_;

    pthread_t threadId_;
    pid_t tid_;
    std::string threadName_;
    
    ThreadFunc threadFunc_;

    CountDownLatch latch_;
    
    static std::atomic<int> numCurrent_;
};

}


#endif
