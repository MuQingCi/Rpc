#include "BlockQueue.h"
#include "Condition.h"
#include "Mutex.h"
#include <utility>

using namespace clearmoon;

BlockQueue::BlockQueue(size_t maxSize) : mutex_(), fullCond_(mutex_), emptyCond_(mutex_), maxSize_(maxSize)
{
    
}

BlockQueue::Task BlockQueue::getTask()
{
    MutexGuard lock(mutex_);
    while (isEmpty() && !stopped_) {
        emptyCond_.wait();
    }

    if(taskQueue.empty()) return Task();

    Task task = taskQueue.front();
    taskQueue.pop_front();

    if(taskQueue.size() == maxSize_ - 1)
    {
        fullCond_.notifyAll();
    }

    return task;
}

void BlockQueue::addTask(Task task)
{
    MutexGuard lock(mutex_);

    while(taskQueue.size() >= maxSize_ && !stopped_)
    {
        fullCond_.wait();
    }

    if(stopped_) return;

    bool wasEmpty = isEmpty();
    taskQueue.emplace_back(std::move(task));
    
    if(wasEmpty)
    {
        emptyCond_.notifyAll();
    }
}

void BlockQueue::stop()
{
    MutexGuard lock(mutex_);
    stopped_ = true;

    fullCond_.notifyAll();
    emptyCond_.notifyAll();
}