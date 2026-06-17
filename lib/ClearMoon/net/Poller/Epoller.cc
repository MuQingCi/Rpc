#include "Epoller.h"
#include "Poller.h"
#include "../EventLoop.h"
#include "../Channel.h"
#include "../Timestamp.h"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <sys/epoll.h>
#include <sys/stat.h>
#include <unistd.h>

using namespace clearmoon;
using namespace clearmoon::net;

const int Epoller::kMaxEvents = 10000;

enum ChannelState : int
{
    kDeleted = -1,
    kNew = 0,
    kAdded = 1
};


// =========== Public =========== //
Epoller::Epoller(EventLoop* loop) : Poller(loop),
                                    epfd_(::epoll_create1(EPOLL_CLOEXEC)),
                                    events_(kMaxEvents)
{
}

Epoller::~Epoller()
{
    ::close(epfd_);
}

/**
 * @brief 内部调用epoll_wait()等待事件发生，将其发生时间数传给fillActiveChannel函数并返回事件发生的时间Timestamp now()
 * 
 * @param timeoutMs 
 * @param activeChannels 
 * @return Timestamp 
 */
Timestamp Epoller::poll(int timeoutMs, ChannelList* activeChannels)
{
    assertInThread();
    
    int numEvent = epoll_wait(epfd_, events_.data(), kMaxEvents, timeoutMs);

    if(numEvent == events_.size())
        events_.resize(events_.size() * 2);

    Timestamp now = Timestamp::now();

    if(numEvent > 0)
        fillActiveChannels(numEvent, activeChannels);

    return now;
}

/**
 * @brief 更新该EventLoop上的fd对应的Channel。 先获取Channel的events，然后判断idx处于什么状态(kNew/kDeleted 就重新监听，不然就修改)。
 * 
 * @param channel 
 */
void Epoller::updateChannel(Channel* channel)
{
    assertInThread();
    
    int idx = channel->getIndex();
    int fd = channel->getFd();

    //event用于epoll_ctl函数
    struct ::epoll_event event;
    event.events = static_cast<uint32_t>(channel->getEvents());
    event.data.ptr = channel;       //保留指向channel的指针

    if(idx == kNew || idx == kDeleted)  //刚实例化或者已删除过的Channel
    {
        if(idx == kNew)
        {
            assert(!hasChannel(channel));
            ChannelMap_[fd] = channel;
        }
        else
        {
            assert(ChannelMap_.find(fd) == ChannelMap_.end() || ChannelMap_[fd] == channel);
            ChannelMap_[fd] = channel;
        }

        int ret = epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &event);  

        if(ret < 0)
        {
            //Log
        }

        channel->setIndex(kAdded);
    }
    else //idx == kAdded
    {
        assert(idx == kAdded);
        assert(ChannelMap_.find(fd) != ChannelMap_.end());
        assert(ChannelMap_[fd] == channel);

        if(channel->getEvents() == 0)  //当前关心事件为空，直接移除
        {
            removeChannel(channel); 
            return;
        }
        
        int ret = ::epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &event);

        if(ret < 0)
        {
            //Log
        }
    }
}

/**
 * @brief 调用这个函数的前提就是idx == kAdded,那么ChannelMap_中一定含有对应的(fd,channel*)，先将目标fd移出监听队列再将对应的Channel移出ChannelMap_
 * 
 * @param channel 
 */
void Epoller::removeChannel(Channel* channel)
{
    assertInThread();
    int fd = channel->getFd();
    int idx = channel->getIndex();
    
    assert(idx == kAdded);

    int ret = ::epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, NULL);

    if(ret < 0)
    {
        //Log
    }

    channel->setIndex(kDeleted);
    
    size_t n = ChannelMap_.erase(fd);
    assert(n == 1); (void)n;
}

// =========== private =========== //

void Epoller::fillActiveChannels(int numEvent, ChannelList* activeChannels)
{
    assertInThread();
    for(int i = 0; i < numEvent ; ++i)
    {
        Channel* ch = static_cast<Channel*>(events_[i].data.ptr);
        ch->setRevents(events_[i].events);
        activeChannels->push_back(ch);
    }
}