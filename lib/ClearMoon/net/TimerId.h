#ifndef CLEARMOON_NET_TIMERID_H
#define CLEARMOON_NET_TIMERID_H

#include <cstdint>

namespace clearmoon
{
namespace net
{

class Timer;

/**
 * @brief 定时器标识符，用于取消定时器
          其他类通过TimerId来cancel且仅cancel定时器
 */
class TimerId
{
public:
    TimerId() : timer_(nullptr), sequence_(0) {}
    TimerId(Timer* timer, int64_t seq) : timer_(timer), sequence_(seq) {}

    bool valid() const { return timer_ != nullptr; }

    friend class TimerQueue;

private:
    Timer* timer_;
    int64_t sequence_;      //Timer的唯一序列号
};

} // namespace net
} // namespace clearmoon

#endif