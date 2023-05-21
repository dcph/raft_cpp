#ifndef LOCKER_H
#define LOCKER_H

#include <pthread.h>//多线程
#include <semaphore.h>//条件变量
#include <iostream>

class locker{
public:
    locker(){
        if(pthread_mutex_init(&m_mutex, NULL) != 0){    //初始化交互锁失败，弹出一个异常
            throw std::exception();
        }
    }
    ~locker(){
        pthread_mutex_destroy(&m_mutex);                  //删除锁
    }
    bool lock(){
        return pthread_mutex_lock(&m_mutex) == 0;        //锁定
    }
    bool unlock(){
        return pthread_mutex_unlock(&m_mutex) == 0;      //解锁
    }
    pthread_mutex_t* getLock(){
        return &m_mutex;                                //得到锁
    }
private:
    pthread_mutex_t m_mutex;
};

class sem{
public:
    sem(){
        if(sem_init(&m_sem, 0 ,0) != 0){                            //初始化信号量
            throw std::exception();
        }
    }
    sem(int num){
        if(sem_init(&m_sem, 0, num) != 0){                          
            throw std::exception();
        }
    }
    ~sem(){
        sem_destroy(&m_sem);                           //删除
    }
    bool wait(){
        return sem_wait(&m_sem) == 0;                       //-1
    }
    bool post(){
        return sem_post(&m_sem) == 0;                     //+1
    }
    void getnum(int *const value)
    {
        sem_getvalue(&m_sem, value);                   //得到数目
    }
private:
    sem_t m_sem;
};

#endif