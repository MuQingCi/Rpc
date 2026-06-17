#ifndef CLEARMOON_BASE_BLOCKQUEUE_H
#define CLEARMOON_BASE_BLOCKQUEUE_H

#include "Condition.h"
#include "Mutex.h"
#include "noncopy.h"
#include <cstddef>
#include <deque>
#include <functional>
#include <sys/stat.h>

namespace clearmoon
{

class BlockQueue : public noncopyable
{
public:
using Task = std::function<void()>;
    explicit BlockQueue(size_t maxSize = 100);
    ~BlockQueue() { stop(); };

    Task getTask();
    void addTask(Task task);

    size_t getMaxSize() const { return maxSize_; }
    
    void setMaxSize(size_t maxsize) 
    { 
        MutexGuard lock(mutex_);
        maxSize_ = maxsize; 
        if(taskQueue.size() < maxsize)
            fullCond_.notifyAll();
    }

    void stop();



private:
    bool isFull() const { return taskQueue.size() == maxSize_; }
    bool isEmpty() const { return taskQueue.empty(); }
    Mutex mutex_;
    Condition fullCond_;
    Condition emptyCond_;

    size_t maxSize_;
    std::deque<Task> taskQueue;

    bool stopped_ = false;
};

}

#endif