#include "CountDownLatch.h"
#include "Mutex.h"

using namespace clearmoon;

void CountDownLatch::wait()
{
    MutexGuard mutexGuard(mutex_);
    while (count_ > 0) {
        cond_.wait();
    }
}

void CountDownLatch::countDown()
{
    MutexGuard mutexGuard(mutex_);
    --count_;

    if(count_ == 0)
    {
        cond_.notifyAll();
    }
}

int CountDownLatch::getCount() const
{
    MutexGuard mutexGuard(mutex_);
    return count_;
}