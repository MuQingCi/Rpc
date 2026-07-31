#ifndef CLEARMOON_RPC_TASKTHREADPOOL_H
#define CLEARMOON_RPC_TASKTHREADPOOL_H

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <queue>
#include <utility>
#include <vector>
#include <thread>

using Task = std::function<void()>;

class TaskThreadPool
{
public:
    TaskThreadPool(size_t threadNum = 8);
    ~TaskThreadPool(){
        stop();
    }

    void start();
    void stop();

    template<typename T>
    bool tryEnqueue(T&&t);

private:
    template<typename T>
    bool enqueue(T&& t);

    std::vector<std::thread> threads_;
    std::queue<Task> taskQueue_;
    //互斥锁与条件变量
    std::mutex mutex_;
    std::condition_variable cond_;

    bool started_ = false;
    
    size_t threadNums_;

    const size_t kMaxTaskNum_ = 100;
    // static const uint16_t kMaxThreadNumber_ = 8;    //业务线程池分8核，IO线程池分4-6
};

template<typename T>
bool TaskThreadPool::tryEnqueue(T&&t)
{
    std::unique_lock<std::mutex>lock(mutex_);
    if(taskQueue_.size() >= kMaxTaskNum_)
        return false;
    return enqueue(t);
}


template<typename T>
bool TaskThreadPool::enqueue(T&& t)
{
    // {
    //     std::unique_lock<std::mutex> lock(mutex_);
    //     if(!started_ || taskQueue_.size() >= kMaxTaskNum_) return;
    //     taskQueue_.emplace(std::forward<T>(t));
    // }
    taskQueue_.emplace(std::forward<T>(t));

    cond_.notify_one();
    return true;
}

#endif