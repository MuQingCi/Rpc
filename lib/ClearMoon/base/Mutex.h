#ifndef CLEARMOON_BASE_MUTEX_H
#define CLEARMOON_BASE_MUTEX_H

#include "noncopy.h"
#include "CurrentThread.h"

#include <assert.h>
#include <pthread.h>
#include <sched.h>

namespace clearmoon {

class Mutex : public noncopyable
{
public:
    Mutex() : holder_(0) { pthread_mutex_init(&mutex_, NULL);}
    ~Mutex() 
    { 
        assert(holder_ == 0);
        pthread_mutex_destroy(&mutex_); 
    }

    void lock()
    {
        pthread_mutex_lock(&mutex_);
        assign();
    }

    void unlock()
    {
        unassign();
        pthread_mutex_unlock(&mutex_);
    }

    pthread_mutex_t* getMutex()
    {
        return &mutex_;
    }

private:

    class UnassignMutex : public noncopyable
    {
    public:
        explicit UnassignMutex(Mutex& mutex) : mutex_(mutex)
        {
            mutex_.unassign();
        }
        ~UnassignMutex()
        {
            mutex_.assign();
        }

    private:
        Mutex& mutex_;
    };

    void assign()
    {
        holder_ = Current::tid();
    }

    void unassign()
    {
        holder_ = 0;
    }

    pthread_mutex_t mutex_;
    pid_t holder_;
};

class MutexGuard : public noncopyable
{
public:
    MutexGuard(Mutex& mutex) : mutex_(mutex)
    {
        mutex_.lock();
    }
    ~MutexGuard()
    {
        mutex_.unlock();
    }

private:
    Mutex& mutex_;
};


}
#endif