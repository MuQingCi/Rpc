#ifndef CLEARMOON_BASE_THREADPOOL_H
#define CLEARMOON_BASE_THREADPOOL_H

#include "Thread.h"
#include "noncopy.h"

#include <atomic>
#include <memory>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>
namespace clearmoon
{

class BlockQueue;

class ThreadPool : public noncopyable
{
public:
using Task = std::function<void()>;
    explicit ThreadPool(size_t ThreadMaxSize, std::string& poolName, size_t TaskMaxSize = 100);

    ~ThreadPool();

    void start();

    Thread* getThread();
    static int getCurrentNum() { return currentThreadNum; }

    void work();

    void addTask(Task task);
private:
    bool isFull() const { return threads_.size() == threadMaxSize_; }

    std::vector<std::unique_ptr<Thread>> threads_;

   // BlockQueue taskQueue_;
    // std::vector<Task>
    std::unique_ptr<BlockQueue> taskQueuePtr_;
    std::string PoolName_;
    size_t threadMaxSize_;
    size_t taskMaxSize_;
    size_t nextID_;

    std::atomic<bool> start_;

    static std::atomic<int> currentThreadNum;
};

}
#endif