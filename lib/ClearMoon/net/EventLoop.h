#ifndef CLEARMOON_NET_EVENTLOOP_H
#define CLEARMOON_NET_EVENTLOOP_H

#include "../base/noncopy.h"
#include "../base/Mutex.h"
#include "TimerId.h"
#include "TimerQueue.h"

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
class Timestamp;

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

    // ========== 定时器接口 ==========
    /**
     * @brief 在指定时刻执行回调
     * @param when 绝对时间
     */
    TimerId runAt(Timestamp when, Func cb);

    /**
     * @brief 延迟 delay 秒后执行回调
     * @param delay 秒（支持小数）
     */
    TimerId runAfter(double delay, Func cb);

    /**
     * @brief 每隔 interval 秒执行一次回调
     * @param interval 秒（支持小数）
     */
    TimerId runEvery(double interval, Func cb);

    /**
     * @brief 取消定时器
     */
    void cancel(TimerId timerId);
    
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

    std::unique_ptr<TimerQueue> timerQueue_;
};


}//net
}//clearmoon

#endif
