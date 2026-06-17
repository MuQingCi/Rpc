#ifndef CLEARMOON_NET_EVENTLOOP_H
#define CLEARMOON_NET_EVENTLOOP_H

#include "../base/noncopy.h"
#include "../base/Mutex.h"

#include <atomic>
#include <functional>
#include <memory>
#include <sys/types.h>
#include <vector>

namespace clearmoon
{
namespace net 
{

class Channel;
class Poller;

class EventLoop : public noncopyable
{
public:
using Func = std::function<void()>;
    EventLoop();
    ~EventLoop();

    void loop();
    void quit();
    void weakup();

    void runInLoop(Func cb);

    void assertInLoopThread();

    bool isInThread() const { return tid_ == Current::tid(); }

    void updateChannel(Channel* channel);

    void removeChannel(Channel* channel);
    
private:
using ChannelList = std::vector<Channel*>; 
using FuncList = std::vector<std::function<void()>>;

    void handleWeakup();
    void doPendingFuncs();
    void queueInLoop(Func cb);

    pid_t tid_;    
    Mutex mutex_;

    std::atomic<bool> looping_;
    std::atomic<bool> quit_;
    std::atomic<bool> eventHanding_;
    std::atomic<bool> callingPendingFunc_;

    int weakFd_;
    std::unique_ptr<Channel> weakChannel_;

    ChannelList activeChannels_;

    FuncList pendingFunc_;

    std::unique_ptr<Poller> poller_;
};


}//net
}//clearmoon

#endif