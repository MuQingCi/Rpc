#ifndef CLEARMOON_NET_EPOLL_H
#define CLEARMOON_NET_EPOLL_H

#include "Poller.h"
#include <vector>
#include <sys/epoll.h>
namespace clearmoon 
{
namespace net 
{

class Epoller : public Poller
{
public:
    explicit Epoller(EventLoop* loop);
    ~Epoller();

    Timestamp poll(int timeoutMs, ChannelList* activeChannels);
    void updateChannel(Channel* channel);
    void removeChannel(Channel* channel);

private:
    void fillActiveChannels(int numEvent, ChannelList* activeChannels);

    int epfd_;
    std::vector<::epoll_event> events_;    


    
    static const int kMaxEvents;
};

} //net
} //clearmoon

#endif