#ifndef CLEARMOON_BASE_COUNTDOWNLATCH_H
#define  CLEARMOON_BASE_COUNTDOWNLATCH_H

#include "Condition.h"
#include "Mutex.h"

namespace clearmoon {

class CountDownLatch
{
public:
    CountDownLatch(int count) : count_(count), mutex_(), cond_(mutex_)
    {
    }

    void wait();
    void countDown();
    int getCount() const;
private:
    mutable Mutex mutex_;
    Condition cond_;
    int count_;
};

}
#endif