#ifndef CLEARMOON_NET_DEFAULTPOLLER_H
#define CLEARMOON_NET_DEFAULTPOLLER_H
#include "Poller.h"
#include "Epoller.h"

namespace clearmoon 
{
namespace net 
{
static Poller* createDefaultPoller(EventLoop* loop)
{
    return new Epoller(loop);
}

}
}

#endif