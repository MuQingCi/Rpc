#include "Poller.h"

#include "../EventLoop.h"
#include "../Channel.h"

using namespace clearmoon;
using namespace clearmoon::net;


// =========== Public =========== //

void Poller::assertInThread() const
{
    loop_->assertInLoopThread();
}


bool Poller::hasChannel(Channel* channel) const
{
    assertInThread();
    auto it = ChannelMap_.find(channel->getFd());
    return it != ChannelMap_.end() && it->second == channel;
}

