#ifndef CLEARMOON_BASE_CONDITION_H
#define CLEARMOON_BASE_CONDITION_H



#include "Mutex.h"
#include "noncopy.h"
#include <cstddef>
#include <pthread.h>


namespace clearmoon {

class Condition : public clearmoon::noncopyable
{
public:
    Condition(Mutex& mutex) : mutex_(mutex)
    {
        pthread_cond_init(&cond_, NULL);
    }

    ~Condition()
    {
        pthread_cond_destroy(&cond_);
    }

    void wait()
    {
        pthread_cond_wait(&cond_, mutex_.getMutex());
    }

    void notify()
    {
        pthread_cond_signal(&cond_);
    }

    void notifyAll()
    {
        pthread_cond_broadcast(&cond_);
    }


private:
    pthread_cond_t cond_;
    Mutex& mutex_;
};

}
#endif