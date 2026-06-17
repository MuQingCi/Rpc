#ifndef CLEARMOON_NET_POLLER_H
#define CLEARMOON_NET_POLLER_H

#include "../../base/noncopy.h"
#include <map>
#include <vector>

namespace clearmoon
{
namespace net 
{
class EventLoop;
class Channel;
class Timestamp;

class Poller : public noncopyable
{    
public:
using ChannelList = std::vector<Channel*>;
    Poller(EventLoop* loop) : loop_(loop)
    {}
    virtual ~Poller() = default;

    virtual Timestamp poll(int timeoutMs, ChannelList* channels) = 0;
    virtual void updateChannel(Channel* channel) = 0;
    virtual void removeChannel(Channel* channel) = 0;

    void assertInThread() const;
    bool hasChannel(Channel* channel) const;

protected:
    EventLoop* loop_;
    std::map<int , Channel*> ChannelMap_;
};

}
}



#endif