#ifndef CLEARMOON_NET_EVENTLOOPTHREAD_H
#define CLEARMOON_NET_EVENTLOOPTHREAD_H

#include "../base/noncopy.h"
// #include "../base/Thread.h"

// #include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
namespace clearmoon
{
namespace net 
{

class EventLoop;

class EventLoopThread : public noncopyable
{
public:
using ThreadInitCallback = std::function<void(EventLoop*)>;

    EventLoopThread(const ThreadInitCallback& cb = ThreadInitCallback(), std::string name = std::string());
    
    ~EventLoopThread();

    EventLoop* start();

    //void stop();
    
    void join()
    {
        if(joined_) return;
        thread_.join();
        joined_ = true;
    }
    
    EventLoop* getLoop() const
    {
        return loop_;
    }

    
    // void join() 
    // {
    //     if(isJoinable()) 
    //         thread_.join();
    // }

    // bool isRunning() const
    // {
    //     // return loop_ != nullptr && !exiting_;
    //     return running_.load();
    // }

    // bool isJoinable() const
    // {
    //     return thread_.joinable();
    // }

private:
    void threadFunc();

    EventLoop* loop_;

    std::thread thread_;
    // Thread thread_;
    std::mutex mutex_;
    std::condition_variable cond;

    bool started_;
    bool exiting_;
    bool joined_;
    
    ThreadInitCallback callback_;
    std::string name_;

    // std::atomic<bool> started_;
    // std::atomic<bool> exiting_;
    // std::atomic<bool> running_;
};


}//net
}//clearmoon

#endif