#include "EventLoopThread.h"
#include "EventLoop.h"

#include <cassert>
#include <mutex>
#include <pthread.h>
#include <string>
#include <thread>
#include <utility>

using namespace clearmoon;
using namespace clearmoon::net;


EventLoopThread::EventLoopThread(const ThreadInitCallback& cb, 
                                 std::string name) : loop_(nullptr),
                                                     exiting_(false),
                                                     started_(false),
                                                     joined_(false),
                                                     //   exiting_(false),
                                                     //   running_(false),
                                                     callback_(std::move(cb)),
                                                     name_(name)
                                  
{
}


EventLoopThread::~EventLoopThread()
{
    // exiting_ = true;

    // if(loop_ && started_)
    //     loop_->quit();

    // if(isJoinable())
    //     thread_.join();

    // if(running_)
    // {
    //     stop();
    // }

    exiting_ = true;
    if(started_)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if(loop_)
        {
            loop_->quit();
        }
    }

    if(!joined_)
    {
        thread_.join();
        joined_ = true;
    }
}


EventLoop* EventLoopThread::start()
{
    // bool excepted = false;
    // //如果started_的值还是false的话，将其置为true且返回true
    // //否则将exceped更新为true,返回false
    // if(!started_.compare_exchange_strong(excepted, true))
    // {
    //     std::lock_guard<std::mutex> lock(mutex_);
        
    //     return loop_;
    // }

    // // int res = pthread_create(&threadId_, NULL, &EventLoopThread::threadFunc, nullptr);
    // thread_ = std::thread(&EventLoopThread::threadFunc, this);

    // // started_ = true;
    // {
    //     std::unique_lock<std::mutex> lock(mutex_);
    //     cond.wait(lock, [this] { return loop_ != nullptr; } );
    // }
    
    // running_ = true;
    // return loop_;
    assert(!started_);
    thread_ = std::thread(&EventLoopThread::threadFunc, this);
    started_ = true;

    EventLoop* loop;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        cond.wait(lock,[this]{ return loop_ != nullptr; });
        loop = loop_;
    }

    return loop;
}

// void EventLoopThread::stop()
// {
//     if(!started_) return;

//     {
//         std::lock_guard<std::mutex> lock(mutex_);
//         if(loop_)
//             loop_->quit();
//     }

//     if(!joined_)
//     {
//         thread_.join();
//         joined_ = true;
//     }
        
//     // exiting_ = true;
//     // if(loop_)
//     // {
//     //     loop_->quit();
//     // }
// }


void EventLoopThread::threadFunc()
{
    EventLoop loop;

    std::string threadName = name_.empty()? "ClearMoon" : name_;

    //设置线程名称
    int ret = pthread_setname_np(pthread_self(), threadName.c_str());

    if(ret != 0)
    {
        //Log<<error
    }

    //若设置了线程初始化回调则调用
    if(callback_) callback_(&loop);

    //成员loop_指向生成的Eventloop且通知主线程
    {
        std::lock_guard<std::mutex> lock(mutex_);
        loop_ = &loop;
        cond.notify_one();
    }

    loop.loop();

    //循环结束时将成员Loop_置空
    std::lock_guard<std::mutex> lock(mutex_);
    loop_ = nullptr;
}
