#ifndef CLEARMOON_BASE_CURRENTTHREAD_H
#define CLEARMOON_BASE_CURRENTTHREAD_H

#include <sched.h>
#include <unistd.h>
namespace clearmoon
{
namespace Current 
{

extern thread_local int t_cacheTid;
extern thread_local char t_tidString[32];
extern thread_local int t_tidStringLength;
extern thread_local const char* t_threadName;

void cacheTid();

inline int tid()
{
    if(t_cacheTid == 0)
    {
        cacheTid();
    }
    return t_cacheTid;
}

inline const char* tidString() { return t_tidString; }
inline int tidStringLength() { return t_tidStringLength; }
inline const char* threadName() { return t_threadName; }
// bool isMainThread();
}
}

#endif