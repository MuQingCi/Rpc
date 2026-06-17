#include "EventLoop.h"
#include "Channel.h"
#include "../base/CurrentThread.h"
#include "Poller/DefaultPoller.h"
#include "Timestamp.h"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <sys/eventfd.h>
#include <unistd.h>
#include <utility>

using namespace clearmoon;
using namespace clearmoon::net;

EventLoop::EventLoop(): tid_(Current::tid()),
                        mutex_(),
                        looping_(false),
                        quit_(false),
                        eventHanding_(false),
                        callingPendingFunc_(false),
                        weakFd_(eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK)), 
                        weakChannel_(std::make_unique<Channel>(this, weakFd_)),
                        poller_(createDefaultPoller(this))
{
    weakChannel_->setReadCallback([this]{ handleWeakup(); });
    weakChannel_->enableReading();
}

EventLoop::~EventLoop()
{
    quit();

}

void EventLoop::loop()
{
    assertInLoopThread();
    looping_ = true;
    quit_ = false;

    while(!quit_)
    {
        activeChannels_.clear();

        Timestamp time;
        time = poller_->poll(0,&activeChannels_);
        
        eventHanding_ = true;

        for(auto ch : activeChannels_)
        {
            ch->handleEvent();
        }

        eventHanding_ = false;

        doPendingFuncs();
    }

    looping_ = false;
}


void EventLoop::quit()
{
    assert(!quit_);
    quit_ = true;
    if(!isInThread())
    {
        weakup();
    }
}


void EventLoop::weakup()
{
    uint64_t one = 1;
    size_t ret = ::write(weakFd_, &one, sizeof one);

    if(ret != sizeof(one))
    {
        //Log<< "error<<
    }
}


void EventLoop::runInLoop(Func cb)
{
    if(isInThread())
    {  
        cb();
    }
    else
    {
        queueInLoop(std::move(cb));
    }
}


void EventLoop::assertInLoopThread()
{
    if(!isInThread())
    {
        abort();
    }
}

void EventLoop::updateChannel(Channel* channel)
{
    assertInLoopThread();
    poller_->updateChannel(channel);
}

void EventLoop::removeChannel(Channel* channel)
{
    poller_->removeChannel(channel);
}




void EventLoop::handleWeakup()
{
    uint64_t one = 1;
    size_t ret = ::read(weakFd_, &one, sizeof one);
    if( ret != sizeof(one))
    {
        //Log<< "error<<
    }
}

void EventLoop::doPendingFuncs()
{
    assert(!callingPendingFunc_);
    FuncList func;

    callingPendingFunc_ = true;
    {
        MutexGuard lock(mutex_);
        func.swap(pendingFunc_);    
    }
    for(auto& f : func)
    {
        f(); 
    }
    callingPendingFunc_ = false;
}


void EventLoop::queueInLoop(Func cb)
{
    {
        MutexGuard lock(mutex_);
        pendingFunc_.push_back(std::move(cb));
    }

    if(!isInThread() || callingPendingFunc_)
    {
        weakup();
    }
}
