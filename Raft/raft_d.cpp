#include <iostream>
#include <bits/stdc++.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <chrono>
#include <unistd.h>
#include <fcntl.h>
#include "locker.h"
#include "./buttonrpc-master/buttonrpc.hpp"
using namespace std;

#define HEARTTIME 100000
//raft中数据保存的格式
class Operation{
public:
    string Getcmd();
    string op;
    string key;
    string value;
    int clientid;
    int requistid;
};
string Operation::Getcmd(){
    string cmd = op + " " + key + " " + value;
    return cmd;
}
//raft start返回值
class StartRet{
public:
    StartRet():m_cmdIndex(-1), m_curTerm(-1), isLeader(false){}
    int m_cmdIndex;
    int m_curTerm;
    bool isLeader;
};
class Log_mess
{
    public:
    Log_mess(string log_content="", int term=-1):log_content(log_content), term(term) {}
    string log_content;
    int term;
};
class Long_logmess
{
    public:
    vector<Log_mess> log;
    int current_term;
    int current_vote;
};
class My_server
{
    public:
    pair<int, int> my_port;
    int my_server_id;
};
class Append_entri_args
{
    public:
    int term;
    int my_server_id;
    int pre_log_term;
    int pre_log_index;
    int comm_log_index;
    string send_log;
};
class Append_entri_replay
{
    public:
    int term;
    int success;
    int conficit_term;
    int conficit_index;
};
class Request_vote_args
{
    public:
    int term;
    int my_server_id;
    int last_log_term;
    int last_log_index;
};
class Request_vote_replay
{
    public:
    int term;
    bool vote_result;
}

class Raft
{
    public:
    Raft(vector<My_server> all_server);//构造函数
    // ~Raft();//析构函数
    void Make(const vector<My_server>& server_mess, int id);//客户端传参
    StartRet Start(Operation op);//raft接受client的数据
    bool Kill_raft();//停止raft
    bool Save_long_log();//保存持久化数据
    bool Load_long_log();//加载持久化数据
    //需要开出来的线程
    static void* Lis_vote(void* arg);//监听vote线程
    static void* Lis_log(void* arg);//监听log线程
    static void* Lis_client(void* arg);//监听客户端线程
    static void* Call_vote(void* arg);//请求投票的线程
    static void* Call_log(void* arg);//请求复制的线程
    static void* Handle_vote_loop(void* arg);//处理vote的循环线程
    static void* Handle_log_loop(void* arg);//处理log的循环线程

    bool Set_heart_time();//设置心跳时间
    int Get_leader();//得到当前leader的序号
    bool Push_log(Log_mess log);//放入本机log
    bool Fil_log(string context);//文件转log
    int Get_time(timeval last_time);//用于时间差计算
    bool Save_log();//将log保存到文件中
    Request_vote_replay Req_vote(Request_vote_args* vote_args);//公开反馈投票的线程
    Append_entri_replay Req_log(Append_entri_args log_arg);//公开反馈log的线程
    vector<Log_mess> Get_string_log(string logs);//计算当前log的长度

    enum RAFT_STATE {LEADER=0, CANDIDATE, FOLLOWER}

    private:
    locker my_locker;
    cond my_cond;
    vector<My_server> server_mess; //raft的服务器集群
    bool raft_die;

    vector<Log_mess> my_log_mess;//所有log信息
    Long_logmess my_long_logmess; //需要持久化保存的log信息
    map<int, int> follower_log; //follower的log信息

    int my_term; //任期号
    int my_server_id; //服务器id
    int vote_num;//收集到投票的数目
    int current_vote;//收集到当前投票结果
    int current_voter;//投票对象
    int comm_index; //共识log
    int apply_index;//回复客户端的log
    int leader_id; //leader id
    struct timeval last_heart_time;//上次心跳时间

    int current_follower_id;//每个副本对应不同的follower
    RAFT_STATE my_state;
    
};
Raft::Raft(vector<My_server> all_server):this->server_mess(all_server)
{
    //参数初始化
    this->vote_overtime = this->heart_time*3+1000;
    this->raft_die = false;
    for (int i = 0; i < this->all_server.size(); i++)
    {
        this->follower_log.insert(pair<int,int>(i, 1));
    }
    this->follower_log.erase(this->my_server_id);
    this->my_term = 0;
    this->vote_num = -1;
    this->current_vote = -1;
    this->comm_index = 0;
    this->apply_index = 0;
    this->leader_id = -1;
    gettimeofday(&this->last_heart_time, NULL);
    this->current_follower_id = -1;
    this->my_state = FOLLOWER;
    
    cout << "load long log " << this->Load_long_log() <<endl;
    //创建log，vote，client三个处理线程
    pthread_t vote_tid;
    pthread_t log_tid;
    pthread_t client_tid;
    pthread_creat(&vote_tid, NULL, this->Lis_vote, this);
    pthread_detach(vote_tid);
    pthread_creat(&log_tid, NULL, this->Lis_log, this);
    pthread_detach(log_tid);
    pthread_creat(&client_tid, NULL, this->Lis_client, this);
    pthread_detach(client_tid);
}
void* Raft::Lis_vote(void* arg)
{
    //公开投票线程
    //开启投票循环
    //开启rpc服务
    Raft* raft = (Raft*) arg;
    buttonrpc server;
    server.as_server(raft->server_mess[raft->my_server_id].my_port.first);
    server.bind("Req_vote", &Raft::Req_vote, raft);

    pthread_t req_vote;
    string thread_name = "lis vote";
    pthread_setname_np(&req_vote, thread_name.c_str());
    pthread_creat(&req_vote, NULL, Raft::Handle_vote_loop, raft);

    server.run();
    print("vote exit");
}
void* Raft::Lis_log(void* arg)
{
    //公开log线程
    //开启log循环
    //开启rpc服务
    Raft* raft = (Raft*) arg;
    buttonrpc server;
    server.as_server(raft->server_mess[raft->my_server_id].my_port.second);
    server.bind("Req_log", &Raft::Req_log, raft);

    pthread_t req_log;
    string thread_name = "lis log";
    pthread_setname_np(&req_log, thread_name.c_str());
    pthread_creat(&req_log, NULL, Raft::Handle_log_loop, raft);

    server.run();
    print("log exit");
}
void* Raft::Handle_log_loop(void* arg)
{
    //循环判断自己是不是leader
    //循环的向follower发送log
    //根据返回结果调整
    Raft* raft = (Raft*) arg;
    while (raft->raft_die)
    {
        usleep(1000);
        raft->my_locker.lock();
        if(raft->my_state != LEADER)
        {
            raft->my_locker.unlock();
            continue;
        }
        int durint_time = raft->Get_time(raft->last_heart_time);
        if(durint_time < HEARTTIME)
        {
            raft->my_locker.unlock();
            continue;           
        }
        gettimeofday(this->last_heart_time, NULL);
        raft->my_locker.unlock();
        pthread_t call_log_tid[raft->server_mess.size()-1];
        int i = 0;
        for (const auto& server : raft->server_mess)
        {
            if(server.my_server_id == raft->my_server_id)
            {
                continue;
            }
            pthread_create(call_vote_tid[i], NULL, Raft::Call_log, raft);
            pthread_detach(call_vote_tid[i])
            i++
        }
    }
}
void* Raft::Call_log(void* arg)
{
    //向follower发送log
    //根据返回结果调整
    Raft* raft = (Raft*) arg;
    buttonrpc client;
    Append_entri_replay log_replay;
    Append_entri_args log_args;
    raft->my_locker.lock();

    if(raft->current_follower_id == raft->my_server_id)
    {
        raft->current_follower_id++;
    }
    client.as_client("127.0.0.1", raft->server_mess[raft->current_follower_id].my_port.second);

    log_args.term = raft->my_term;
    log_args.comm_log_index = raft->comm_index;
    log_args.my_server_id = raft->my_server_id;
    log_args.pre_log_index = raft->follower_log[raft->current_follower_id] -1;
    for(int i = log_args.pre_log_index; i < raft->my_log_mess.size() ; i++)
    {
        log_args.send_log += to_string(raft->my_log_mess[i].log_content) +"," + to_string(raft->my_log_mess[i].term) +".";
    }
    if(log_args.pre_log_index == 0)
    {
        log_args.pre_log_term = 0;
        if(raft->my_log_mess.size() != 0)
        {
            log_args.pre_log_term = raft->my_log_mess[0].term;
        }
    }else
    {
        log_args.pre_log_term = raft->my_log_mess[log_args.pre_log_index-1].term;
    }
    printf("[%d] -> [%d]'s prevLogIndex : %d, prevLogTerm : %d\n", raft->my_server_id, raft->current_follower_id, log_args.pre_log_index, log_args.pre_log_term);
    int current_id = raft->current_follower_id;
    raft->current_follower_id++;
    if((raft->current_follower_id == raft->my_log_mess.size()) || ((raft->current_follower_id == raft->my_log_mess.size()-1) && (raft->current_follower_id==raft->my_server_id)))
    {
        raft->current_follower_id = 0;
    }

    raft->my_lock.unlock();
    log_replay = client.call<Append_entri_args>("Req_log", log_args).val();
    raft->my_locker.lock();

    if(log_replay.term > raft->my_term)
    {
        raft->my_term = log_replay.term;
        raft->current_voter = -1;
        raft->current_vote = -1;
        raft->my_state = FOLLOWER;
        raft->vote_num = -1;
        raft->my_locker.unlock()
        return NULL
    }   
    if(log_replay.success)
    {
        raft->follower_log[current_id] += raft->Get_string_log(log_args.send_log).size();
        vector<int> comm_mess = raft->follower_log;
        sort(comm_mess.begin(), comm_mess.end());
        int mid_comm = comm_mess[comm_mess.size()/2];
        if((raft->my_log_mess[mid_comm].term == raft->my_term) && (mid_comm > raft->comm_index))
        {
            raft->comm_index = mid_comm;
        }
    }else{
        if(log_replay.conficit_term != -1)
        {
            int leader_conflict_index = -1;
            for (int i = log_args.pre_log_index; i > 0; i--)
            {
                if(raft->my_log_mess[i-1].term == log_replay.conficit_term)
                {
                    leader_conflict_index = index;
                    break;
                }
            }
            if(leader_conflict_index != -1)
            {
                raft->follower_log[current_id] = leader_conflict_index + 1;
            }else{
                raft->follower_log[current_id] = log_replay.conficit_term;
            }
        }else{
            raft->follower_log[current_id] = log_replay.conficit_index + 1;
        }
    }
    raft->Save_long_log();
    raft->my_locker.unlock();
}
vector<Log_mess> Raft::Get_string_log(string logs)
{
    vector<Log_mess> log_mess;
    int n = logs.size();
    vector<string> str;
    string tmp = "";
    for(int i = 0; i < n; i++){
        if(logs[i] != ';'){
            tmp += logs[i];
        }else{
            if(tmp.size() != 0) str.push_back(tmp);
            tmp = "";
        }
    }
    for(int i = 0; i < str.size(); i++){
        tmp = "";
        int j = 0;
        for(; j < str[i].size(); j++){
            if(str[i][j] != ','){
                tmp += str[i][j];
            }else break;
        }
        string number(str[i].begin() + j + 1, str[i].end());
        int num = atoi(number.c_str());  //字符串转整形
        log_mess.push_back(Log_mess(tmp, num));
    }
    return log_mess;   
}
Append_entri_replay Raft::Req_log(Append_entri_args log_arg)
{
    Append_entri_replay log_replay;
    vector<Log_mess> log_mess = this->Get_string_log(log_arg.send_log);
    this->my_locker.lock();
    log_replay.term = this->my_term;
    log_replay.success = false;
    log_replay.conficit_index = -1;
    log_replay.conficit_term = -1;

    if(log_replay.term > log_arg.term)
    {
        this->my_locker.unlock();
        return NULL;
    }
    this->my_state = FOLLOWER;
    if(log_arg.term > log_replay.term)
    {
        this->current_voter =-1;
        this->Save_long_log();
        this->my_term = log_arg.term;
    }
    gettimeofday(this->last_heart_time, NULL);

    if(this->my_log_mess.size() == 0){
        for(const auto& log : log_mess){
            this->Push_log(log);
        }
        this->Save_long_log;
        if(this->comm_index < log_arg.comm_log_index){
            this->comm_index = min(log_arg.comm_log_index, log_mess.size());
        }
        this->my_locker.unlock();
        log_replay.success = true;
        return log_replay;
    }  
    if(this->my_log_mess.size() < log_arg.pre_log_index)
    {
        log_replay.success = false;
        log_replay.conficit_index = this->my_log_mess.size();
        this->my_locker.unlock();
        return log_replay;
    }
    if(log_arg.pre_log_index > 0 && this->my_log_mess[log_arg.pre_log_index-1].term != log_arg.term)
    {
        log_replay.conficit_term = this->my_log_mess[log_arg.pre_log_index-1].term;
        for(int index = 1; index <= log_arg.pre_log_index; index++){
            if(this->my_log_mess[index - 1].term == log_replay.conficit_term){
                log_replay.conficit_index = index;                         
                break;
            }
        }
        this->my_locker.unlock();
        log_replay.success = false;
        return log_replay;       
    }
    for(int i = log_arg.pre_log_index; i < this->my_log_mess.size(); i++){
        this->my_log_mess.pop_back();
    }    
    for(const auto& log : log_mess){
        this->Push_log(log);
    }
    this->Save_long_log();
    if(this->comm_index < log_arg.comm_log_index){
        this->comm_index = min(log_arg.comm_log_index, log_mess.size());
    }
    this->my_locker.unlock();
    log_replay.success = true;
    return log_replay;
}
void* Raft::Handle_vote_loop(void* arg)
{
    //循环执行
    //检查是否超时
    //超时后转化为cand
    //请求选举结果
    Raft* raft = (Raft*) arg;
    while (raft->raft_die)
    {
        int timeout = rand()%200000 + 200000;
        bool this_vote_over = false; 
        while (1)
        {
            usleep(timeout);
            raft->my_locker.lock();
            int during_time = raft->Get_time(raft->last_heart_time);
            if((during_time > timeout) && (raft->my_state == FOLLOWER))
            {
                raft->my_state == CANDIDATE;
            }
            if((raft->my_state == CANDIDATE) && (during_time > timeout))
            {
                printf(" %d attempt election at term %d, timeOut is %d\n", raft->my_server_id, raft->my_term, timeout);
                gettimeofday(&raft->last_heart_time, NULL);
                raft->my_term++;
                raft->current_voter = raft->my_server_id;
                raft->current_vote = 1;
                raft->vote_num = 1;
                raft->current_follower_id = 0;
                raft->Save_long_log();

                pthread_t call_vote_tid[raft->server_mess.size()-1];
                int i = 0;
                for (const auto& server : raft->server_mess)
                {
                    if(server.my_server_id == raft->my_server_id)
                    {
                        continue;
                    }
                    pthread_create(call_vote_tid[i], NULL, Raft::Call_vote, raft);
                    pthread_detach(call_vote_tid[i])
                    i++
                }
                while((raft->current_vote <= raft->server_mess.size()/2) && (raft->vote_num != raft->server_mess.size()))
                {
                    raft->my_cond.wait(raft->my_locker.getLock());
                }
                if(raft->my_state != CANDIDATE)
                {
                    raft->my_locker.unlock();
                    continue;
                }
                if(raft->current_vote > raft->server_mess.size()/2)
                {
                    raft->my_state = LEADER;
                    for (int i = 0; i < this->all_server.size(); i++)
                    {
                        this->follower_log.insert(pair<int,int>(i, raft->my_log_mess.size()));//初始化follower的log信息为与leader同步
                    }
                    printf(" %d become new leader at term %d\n", raft->my_server_id, raft->my_term);
                    raft->Set_heart_time();
                }
                this_vote_over = true;
            }
            raft->my_locker.unlock();
            if(this_vote_over) break;
        }
    }
}
void* Raft::Call_vote(void* arg)
{
    Raft* raft = (Raft*) arg;
    buttonrpc client;
    Request_vote_args vote_arg;
    Request_vote_replay vote_replay;
    raft.my_locker.lock();

    vote_arg.last_log_term = raft->my_log_mess.size()!=0 ? raft->my_log_mess.back().term : 0;
    vote_arg.last_log_index = raft->my_log_mess.size();
    vote_arg.my_server_id = raft->my_server_id;
    vote_arg.term = raft->my_term;
    if(raft->current_follower_id == raft->my_server_id)
    {
        raft->current_follower_id++;
    }
    client.as_client("127.0.0.1", raft->server_mess[raft->current_follower_id].my_port.first);

    raft->current_follower_id++;
    if((raft->current_follower_id == raft->my_log_mess.size()) || ((raft->current_follower_id == raft->my_log_mess.size()-1) && (raft->current_follower_id==raft->my_server_id)))
    {
        raft->current_follower_id = 0;
    }

    raft->my_locker.unlock();
    vote_replay = client.call<Request_vote_args>("Req_vote", &vote_arg).val();
    raft->my_locker.lock();

    raft->vote_num += 1;
    if(vote_replay.vote_result)
    {
        raft->current_vote += 1;
    }
    raft->my_cond.signal();
    if(vote_replay.term > raft->my_term)
    {
        raft->my_term = vote_replay.term;
        raft->current_voter = -1;
        raft->current_vote = -1;
        raft->my_state = FOLLOWER;
        raft->vote_num = -1;
        raft->my_locker.unlock()
        return NULL
    }
    raft->my_locker.unlock()
}
Request_vote_replay Raft::Req_vote(Request_vote_args* vote_args)
{
    //进行投票
    //如果投过了，不投
    //如果term大，不投
    //如果log序号大，不投
    //投完后保存数据并重置超时时间
    Request_vote_replay vote_replay;
    vote_replay.vote_result = false;
    this->my_locker.lock();

    if(this->my_term >= vote_args->term)
    {
        vote_replay.term = this->my_term;
        this->my_locker.unlock();
        return vote_replay;
    }
    if(this->my_term < vote_args->term)
    {
        this->my_state = FOLLOWER;
        this->my_term = vote_args->term;
        this->current_voter = -1;
    }
    if((this->current_voter == -1) || (this->current_voter == vote_args->my_server_id))
    {
        if((this->my_log_mess.size() == 0) || (this->my_log_mess.back().term < vote_args->last_log_term))
        {
            vote_replay.vote_result = true;
        }
        if((this->my_log_mess.back().term == vote_args->last_log_term) && ((this->my_log_mess.size() <= vote_args->higher_log_index)))
        {
            vote_replay.vote_result = true;
        }
    }
    vote_replay.term = this->my_term;
    if(!vote_replay.vote_result)
    {
        this->my_locker.unlock();
        return vote_replay;
    }
    this->current_voter = vote_args->my_server_id;
    printf("[%d] vote to [%d] at %d, duration is %d\n", this->my_server_id, vote_args->my_server_id, this->my_term, Get_time(this->last_heart_time));
    gettimeofday(&this->last_heart_time, NULL);
    this->Save_long_log();
    this->my_locker.unlock();
    return vote_replay
}
bool Raft::Set_heart_time()
{
    gettimeofday(&this->last_heart_time, NULL);
    if(this->last_heart_time.tv_usec >= 200000){
        this->last_heart_time.tv_usec -= 200000;
    }else{
        this->last_heart_time.tv_sec -= 1;
        this->last_heart_time.tv_usec += (1000000 - 200000);
    }
}
bool Raft::Save_long_log()
{
    this->my_long_logmess.current_term = this->my_term;
    this->my_long_logmess.current_vote = this->current_voter;
    this->my_long_logmess.log = this->my_log_mess;
    this->Save_log();

}
bool Raft::Save_log()
{
    string str;
    str = to_string(this->my_long_logmess.current_term)+";"+to_string(this->my_long_logmess.current_vote)+";";
    for (const auto& log : this->my_long_logmess.log)
    {
        str += to_string(log.log_content)+","+to_string(log.term)+".";
    }
    string filename = "persister-" + to_string(m_peerId);
    int fd = open(filename.c_str(), O_WRONLY | O_CREAT, 0664);
    if(fd == -1){
        perror("open");
        exit(-1);
    }
    int len = write(fd, str.c_str(), str.size());
    close(fd);    
} 
int Raft::Get_time(timeval last_time)
{
    struct timeval now_time;
    gettimeofday(&now_time, NULL);
    return int((now_time.tv_sec - last_time.tv_sec) * 1000000 + (now_time.tv_usec - last_time.tv_usec));   
}
void* Raft::Lis_client(void* arg)
{
    Raft* raft = (Raft*)arg;
    while (!raft->raft_die)
    {
        usleep(10000);
        raft.my_locker.lock();
        for (int i = raft->apply_index; i <= raft->comm_index; i++)
        {
            /* code */
        }
        raft->apply_index = raft->comm_index
        raft.my_locker.unlock();
    }
}
bool Raft::Load_long_log()
{
    //创建一个文件
    //加载内容
    //进行反序列化
    //放到log中
    string filename = "persister-" + to_string(this->my_server_id);
    if(access(filename.c_str(), F_OK) == -1) 
    { return false; }//没有这个文件
    int fd = open(filename.c_str(), O_RDONLY);
    if(fd == -1){
        perror("open");
        return false;
    }  
    int length = lseek(fd, 0, SEEK_END);//记录总偏置量
    lseek(fd, 0, SEEK_SET);//偏置量置为0
    char buf[length];
    bzero(buf, length);
    int len = read(fd, buf, length);//读取文件中内容 
    if(len != length){
        perror("read");
        exit(-1);
    }  
    this->Fil_log(to_string(buf));
    close(filename.c_str());
}
bool Raft::Fil_log(string context)
{
    vector<string> persist;
    string tmp = "";
    for (int i = 0; i < context.size(); i++)
    {
        if(context[i]!=';')
        { 
            tmp += context[i]; 
        }
        else{
            if(tmp.size() != 0)
            { 
                persist.push_back(tmp);
                tmp = "";
            }
        }
    }
    this->my_long_logmess.current_term = atoi(persist[0].c_str());
    this->my_long_logmess.current_vote = atoi(persist[1].c_str());
    vector<string> log;
    vector<Log_mess> logs;
    tmp = "";
    for (int i = 0; i < persist[2].size(); i++)
    {
        if(persist[2][i]!='.')
        { 
            tmp += persist[2][i]; 
        }
        else{
            if(tmp.size() != 0)
            { 
                log.push_back(tmp);
                tmp = "";
            }
        }
    }
    for (int i = 0; i < log.size(); i++)
    {
        tmp = "";
        int j = 0;
        for(; j < log[i].size(); j++){
            if(log[i][j] != ','){
                tmp += log[i][j];
            }else break;
        string number(log[i].begin() + j + 1, log[i].end());
        int num = atoi(number.c_str());
        logs.push_back(Log_mess(tmp, num));
        }
    }
    this->my_long_logmess.log = logs;
    return true;
}
bool Raft::Push_log(Log_mess log)
{
    this->my_log_mess.push_back(log);
}
int Raft::Get_leader()
{
    return this->leader_id;
}
bool Raft::Start(Operation opera)
{
    //判断是不是leader
    //将opera的结果序列化
    //放入log
    this->my_locker.lock();
    RAFT_STATE state = this->my_state;
    if(my_state != LEADER){
        this->my_locker.unlock();
        return false;
    }
    Log_mess log_client;
    log_client.log_content = opera.Getcmd();
    log_client.term = this->my_term;
    this->Push_log(log_client);
    this->my_locker.unlock();
    return true;
}
bool Raft::Kill_raft()
{
    this->raft_die = true;
}
int main(int argc, char* argv[])
{
    if(argc < 2){
        printf("loss parameter of peersNum\n");
        exit(-1);
    }
    int peersNum = atoi(argv[1]);//atoi 字符串转整形
    if(peersNum % 2 == 0){
        printf("the peersNum should be odd\n");  //必须传入奇数，这是raft集群的要求
        exit(-1);
    }
    srand((unsigned)time()NULL); //生随机时间种子
    vector<My_server> all_server;
    for (int i = 0; i < peersNum; i++)
    {
        My_server my_server;
        my_server.my_port.first = i;
        my_server.my_port.second = i+peersNum+1;
        my_server.my_server_id = i;
        all_server.push_back(my_server);
    }
    Raft* raft = new Raft(all_server)[peersNum]; //初始化

    usleep(400000);
    int i = 0;
    //向log中传入记录
    for(int j = 0; j < all_server.size(); j++){
        leader_id = raft[i]Get_leader();
        for (; i < 1000; i++){
            Operation opera;
            opera.op = "put";opera.key = to_string(j);opera.value = to_string(j);
            if(!raft[leader_id].Start(opera)){ break; } 
            usleep(50000);
        }
        if(i == 1000){ break; }
        i--;
        if(j == all_server.size()-1){ j=0; }//一轮循环之后再启
    } 

    usleep(400000);
    leader_id = raft[0].Get_leader();  
    raft[leader_id].Kill_raft();
    
    while(1);
    return 0;
}