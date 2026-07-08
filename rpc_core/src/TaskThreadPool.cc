#include "../include/TaskThreadPool.h"
#include "net/Log/Logger.h"
#include <exception>


TaskThreadPool::TaskThreadPool(size_t threadNum) : threadNums_(threadNum == 0 ? 1:threadNum)
{
}

void TaskThreadPool::start()
{
    {
        std::unique_lock<std::mutex> lock(mutex_);
        if(started_) return;

        started_ = true;
    }

    for(size_t i=0; i<threadNums_;++i)
    {
        threads_.emplace_back([this]
        {
            while (true) {
                Task task;
                {
                    std::unique_lock<std::mutex> lock(mutex_);

                    cond_.wait(lock, [this]{ return !started_ || !taskQueue_.empty(); });

                    if(!started_ && taskQueue_.empty()) return;
                    
                    task = taskQueue_.front();
                    taskQueue_.pop();
                }
                
                try {
                task();
                } catch (std::exception& e) {
                    LOG_ERROR<<"执行Task的时候出现错误: " << e.what();
                } catch(...)
                {
                    LOG_ERROR<<"执行Task的时候出现未知错误: ";
                }
            }
        });
    }
}

void TaskThreadPool::stop()
{
    {
        std::unique_lock<std::mutex> lock(mutex_);

        if(!started_) return;

        started_ = false;
    }

    cond_.notify_all();
    for(auto& t : threads_)
    {
        if(t.joinable())
            t.join();
    }

    threads_.clear();
}
