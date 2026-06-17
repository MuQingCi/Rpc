#include "ThreadPool.h"
#include "BlockQueue.h"

#include "Thread.h"

#include <cassert>
#include <memory>
#include <string>

using namespace clearmoon;

std::atomic<int> ThreadPool::currentThreadNum{0};

ThreadPool::ThreadPool(size_t ThreadMaxSize, std::string& poolName, size_t TaskMaxSize) : threadMaxSize_(ThreadMaxSize), PoolName_(poolName), nextID_(1), start_(false), taskMaxSize_(TaskMaxSize)
{
    threads_.reserve(ThreadMaxSize);
    taskQueuePtr_ = std::make_unique<BlockQueue>(TaskMaxSize);
    //taskQueue_()
}

ThreadPool::~ThreadPool()
{
    if(start_)
        start_ = false;
    if(taskQueuePtr_)
        taskQueuePtr_->stop();

    for(auto& it : threads_)
    {
        it->join();
    }
}


void ThreadPool::start()
{
    assert(threadMaxSize_ >=0);
    assert(taskMaxSize_ >= 0);
    assert(threads_.empty());

    start_ = true;

    for(int i=0; i< threadMaxSize_; ++i)
    {
        char id[32];
        std::string name = PoolName_ + std::to_string(i);
        threads_.emplace_back(std::make_unique<Thread>([this]{ work(); } ,name));
        threads_[i]->start();
        ++currentThreadNum;
    }

    if(threads_.size() <= 0)
    {
        //Log threadPool have error
    }
}

void ThreadPool::work()
{
    while (start_) 
    {
        Task task = taskQueuePtr_->getTask();
        if(!task)
        {
            break;
        }
        
        task();
    }

    --currentThreadNum;
}


void ThreadPool::addTask(Task task)
{
    taskQueuePtr_->addTask(task);
}
