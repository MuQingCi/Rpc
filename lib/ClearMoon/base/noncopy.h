#ifndef CLEARMOON_BASE_NONCOPY_H
#define CLEARMOON_BASE_NONCOPY_H

namespace clearmoon {

class noncopyable
{
public:
    noncopyable(const noncopyable&) = delete;
    noncopyable& operator=(const noncopyable&) = delete;
protected:
    noncopyable() = default;
    virtual ~noncopyable() = default;
};

}
#endif