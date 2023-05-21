#include "raft.hpp"
#include "common.h"
#include <bits/stdc++.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
using namespace std;

#define EVERY_SERVER_PORT 3
#define EVERY_SERVER_RAFT 5
#define EVERY_MASTER_PORT 6
#define MASTER_PORT 6666

typedef std::chrono::steady_clock myClock;
typedef std::chrono::steady_clock::time_point myTime;
#define  myDuration std::chrono::duration_cast<std::chrono::microseconds>

/**
 * @brief 整体框架类似LAB3中的server端代码，但内容完全不一样了，需要先理解整个shard分配的流程再对照LAB3就很好处理了
 * 一些注释可以看LAB3的代码，许多辅助类的定义和功能都是一样的如select
 */

//打印config函数，便于调试
void printConfig(Config config){
    cout<<"begin print -----------------------------------------------"<<endl;
    cout<<"configNum is "<<config.configNum<<endl;
    cout<<"groups.size() is :"<<config.groups.size()<<endl;
    for(auto a : config.groups){
        cout<<"idx : "<<a.first<<" -> ";
        for(auto v : a.second){
            cout<<v<<" ";
        }
        cout<<endl;
    }
    cout<<"shards is ";
    for(auto a : config.shards){
        cout<<a<<" ";
    }
    cout<<endl;
    cout<<"end print -----------------------------------------------"<<endl;
}

class kvServerInfo{    //kv服务器的info
public:
    PeersInfo peersInfo; //服务器raft层信息
    vector<int> m_kvPort; //用于get等服务的端口
};

//从给的kvserverInfo中获得对应raft端口信息的PeersInfo
vector<PeersInfo> getRaftPort(vector<kvServerInfo>& kvInfo){
    int n = kvInfo.size();
    vector<PeersInfo> ret(n);
    for(int i = 0; i < n; i++){
        ret[i] = kvInfo[i].peersInfo;
    }
    return ret;
}

class Select{
public:
    Select(string fifoName);
    string fifoName;
    bool isRecved;
    static void* work(void* arg);
};

Select::Select(string fifoName){
    this->fifoName = fifoName;
    isRecved = false;
    int ret = mkfifo(fifoName.c_str(), 0664);
    pthread_t test_tid;
    pthread_create(&test_tid, NULL, work, this);
    pthread_detach(test_tid);
}

void* Select::work(void* arg){
    Select* select = (Select*)arg;
    char buf[100];
    int fd = open(select->fifoName.c_str(), O_RDONLY);
    read(fd, buf, sizeof(buf));
    select->isRecved = true;
    close(fd);
    unlink(select->fifoName.c_str());
}


class OpContext{
public:
    OpContext(Operation op);
    Operation op; //操作名
    string fifoName; //管道文件信息
    bool isWrongLeader; //是否leader
    bool isIgnored; //是否快照
    Config config; //配置信息
};

OpContext::OpContext(Operation op){
    this->op = op;
    fifoName = "fifo-" + to_string(op.clientId) + + "-" + to_string(op.requestId);
    isWrongLeader = false;
    isIgnored = false;
}



class ShardMaster{
public:
    static void* RPCserver(void* arg); //接受rpc的服务
    static void* applyLoop(void* arg); //应用
    void StartShardMaster(vector<kvServerInfo>& kvInfo, int me, int maxRaftState);  //初始化master
    JoinReply Join(JoinArgs args);
    LeaveReply Leave(LeaveArgs args);
    MoveReply Move(MoveArgs args);
    QueryReply Query(QueryArgs args);
    bool JoinLeaveMoveRpcHandler(Operation operation);      //将更改配置的操作抽象成一个RPC处理函数，因为处理流程几乎一致，只需知道完没完成即可
    void doJoinLeaveMove(Operation operation);              //通过start传入raft共识后再applyLoop中得到的msg还原到operation:Join，Leave，Move进入对应的处理函数
    void balanceWorkLoad(Config& config);                   //对Join和Leave的操作都要进行负载均衡，通过map及2个优先队列实现
    int getConfigsSize();

private:
    locker m_lock;
    Raft m_raft;
    int m_id;
    vector<Config> configs;//各个组的配置

    vector<int> m_port; //对外提供服务的端口，有6个
    int cur_portId;//当前对外服务的端口

    unordered_map<int, int> m_clientSeqMap;    //只记录特定客户端已提交的最大请求ID
    unordered_map<int, OpContext*> m_requestMap;  //记录当前RPC对应的上下文
};

void ShardMaster::StartShardMaster(vector<kvServerInfo>& kvInfo, int me, int maxRaftState){
    
    this->m_id = me;
    m_port = kvInfo[me].m_kvPort;
    vector<PeersInfo> peers = getRaftPort(kvInfo);
    configs.resize(1);//将config调整为1个Config大小

    m_raft.setRecvSem(1);  //开启接受
    m_raft.setSendSem(0);  //关闭发送
    m_raft.Make(peers, me);//初始化raft

    m_clientSeqMap.clear();
    m_requestMap.clear();

    // dead = false;
    pthread_t listen_tid1[m_port.size()];
    for(int i = 0; i < m_port.size(); i++){
        pthread_create(listen_tid1 + i, NULL, RPCserver, this);
        pthread_detach(listen_tid1[i]);
    }
    pthread_t listen_tid2;
    pthread_create(&listen_tid2, NULL, applyLoop, this);
    pthread_detach(listen_tid2);

}

void* ShardMaster::RPCserver(void* arg){
    ShardMaster* master = (ShardMaster*) arg;
    buttonrpc server;
    master->m_lock.lock();
    int port = master->cur_portId++;
    master->m_lock.unlock();

    server.as_server(master->m_port[port]);
    server.bind("Join", &ShardMaster::Join, master);
    server.bind("Leave", &ShardMaster::Leave, master);
    server.bind("Query", &ShardMaster::Query, master);
    server.bind("Move", &ShardMaster::Move, master);
    server.run();
}


//PRChandler for get-request
JoinReply ShardMaster::Join(JoinArgs args){
    JoinReply reply;
    reply.isWrongLeader = false;
    Operation operation;
    operation.op = "Join";
    operation.key = "random";
    operation.value = "random";
    operation.clientId = args.clientId;
    operation.requestId = args.requestId;
    operation.args = args.serversShardInfo;
    printf("join leader id is %d, client is %d\n", m_id, operation.clientId);
    reply.isWrongLeader = JoinLeaveMoveRpcHandler(operation);
    // if(!reply.isWrongLeader) printConfig(this->configs[configs.size() - 1]);
    return reply;
}

LeaveReply ShardMaster::Leave(LeaveArgs args){
    LeaveReply reply;
    reply.isWrongLeader = false;
    Operation operation;
    operation.op = "Leave";
    operation.key = "random";
    operation.value = "random";
    operation.clientId = args.clientId;
    operation.requestId = args.requestId;
    operation.args = args.groupIdsInfo;

    reply.isWrongLeader = JoinLeaveMoveRpcHandler(operation);
    return reply;
}

MoveReply ShardMaster::Move(MoveArgs args){
    MoveReply reply;
    reply.isWrongLeader = false;
    Operation operation;
    operation.op = "Move";
    operation.key = "random";
    operation.value = "random";
    operation.clientId = args.clientId;
    operation.requestId = args.requestId;
    operation.args = args.shardAndGroupIdInfo;

    reply.isWrongLeader = JoinLeaveMoveRpcHandler(operation);
    return reply;
}

bool ShardMaster::JoinLeaveMoveRpcHandler(Operation operation){
    StartRet ret = m_raft.start(operation); //向raft初始化这一log
    operation.term = ret.m_curTerm;
    operation.index = ret.m_cmdIndex;
    bool isWrongLeader = false;
    if(ret.isLeader == false){//leader返回结果
        // printf("client %d's JoinLeaveMove request is wrong leader %d\n", operation.clientId, m_id);
        return true;
    }

    OpContext opctx(operation);//对log封装管道
    m_lock.lock();
    m_requestMap[ret.m_cmdIndex] = &opctx;
    m_lock.unlock();
    Select s(opctx.fifoName);
    myTime curTime = myClock::now();
    while(myDuration(myClock::now() - curTime).count() < 2000000){
        if(s.isRecved){
            // printf("client %d's get->time is %d\n", args.clientId, myDuration(myClock::now() - curTime).count());
            break;
        }
        usleep(10000);
    }

    if(s.isRecved){
        if(opctx.isWrongLeader){
            isWrongLeader = true;
        }
    }
    else{
        isWrongLeader = true;
        printf("in get --------- timeout!!!\n");
    }
    m_lock.lock();
    m_requestMap.erase(ret.m_cmdIndex);
    m_lock.unlock();
    return isWrongLeader;
}

//PRChandler for put/append-request
QueryReply ShardMaster::Query(QueryArgs args){
    QueryReply reply;
    reply.isWrongLeader = false;
    Operation operation;
    operation.op = "Query";
    operation.key = "random";
    operation.value = "random";
    operation.clientId = args.clientId;
    operation.requestId = args.requestId;
    operation.args = to_string(args.configNum);

    StartRet ret = m_raft.start(operation);//raft初始化log

    operation.term = ret.m_curTerm;
    operation.index = ret.m_cmdIndex;
    if(ret.isLeader == false){//只有leader会处理log信息
        // printf("client %d's Query request is wrong leader %d\n", args.clientId, m_id);
        reply.isWrongLeader = true;
        return reply;
    }

    OpContext opctx(operation);
    m_lock.lock();
    m_requestMap[ret.m_cmdIndex] = &opctx;
    m_lock.unlock();

    Select s(opctx.fifoName);
    myTime curTime = myClock::now();
    while(myDuration(myClock::now() - curTime).count() < 2000000){
        if(s.isRecved){
            // printf("client %d's putAppend->time is %d\n", args.clientId, myDuration(myClock::now() - curTime).count());
            break;
        }
        usleep(10000);
    }

    if(s.isRecved){
        // printf("opctx.isWrongLeader : %d\n", opctx.isWrongLeader ? 1 : 0);
        if(opctx.isWrongLeader){
            reply.isWrongLeader = true;
        }else{
            // printf("in query rpc cfgNum : %d\n", opctx.config.configNum);
            reply.configStr = getStringFromConfig(opctx.config);
        }
    }
    else{
        reply.isWrongLeader = true;
        printf("int putAppend --------- timeout!!!\n");
    }
    m_lock.lock();
    m_requestMap.erase(ret.m_cmdIndex);
    m_lock.unlock();
    return reply;
}
void ShardMaster::doJoinLeaveMove(Operation operation){
    Config config = configs.back();//取得最近的配置，在此基础上更改
    printf("[%d] in doJoinLeaveMove size : %d, num: %d\n", m_id, configs.size(), config.configNum);
    unordered_map<int, vector<string>> newMap;//记录之前配置的group
    for(const auto& group : config.groups){
        newMap[group.first] = group.second; //之前配置的group
    }
    config.groups = newMap;
    config.configNum++;

    if(operation.op == "Join"){   //插入新的group配置
        unordered_map<int, vector<string>> newGroups = getMapFromServersShardInfo(operation.args);
        for(const auto& group : newGroups){             //加入新config的groups中(gid -> [servers])
            config.groups[group.first] = group.second; //更新新的group
        }
        if(config.groups.empty()){                      //说明此前必然是空的,args也是空的，直接return
            return;
        }
        balanceWorkLoad(config);                        //进行负载均衡
        configs.push_back(config);                      //将最新配置插入configs数组
        return;
    }
    if(operation.op == "Leave"){  //删除指定的group
        vector<int> groupIds = GetVectorOfIntFromString(operation.args);
        unordered_map<int, int> hash;
        for(const auto& id : groupIds){
            config.groups.erase(id);//删除指定group
            hash[id] = 1;
        }
        for(int i = 0; i < NShards; i++){
            if(hash.count(config.shards[i])){
                config.shards[i] = 0;   //将该group切片转换为未分配状态
            }
        }
        if(config.groups.empty()){      //说明此时为空，可能是都移出去了，需要清理config的其他成员变量
            config.groups.clear();
            config.shards.resize(NShards, 0);
            return;
        }
        balanceWorkLoad(config);
        configs.push_back(config);
        return;
    }
    if(operation.op == "Move"){         //Move不做负载均衡，那只会破坏Move的语义  //将指定的切片放入指定的group
        vector<int> moveInfo = getShardAndGroupId(operation.args);
        config.shards[moveInfo[0]] = moveInfo[1];
        // printConfig(config);
        configs.push_back(config);
    }
}

void* ShardMaster::applyLoop(void* arg){
    ShardMaster* master = (ShardMaster*)arg;
    while(1){

        master->m_raft.waitSendSem(); //关闭发送
        ApplyMsg msg = master->m_raft.getBackMsg();//提取最近共识的msg
        
        Operation operation = msg.getOperation();//得到log
        // printf("op is %s\n", operation.op.c_str());
        int index = msg.commandIndex;
        int term = msg.commandTerm;

        master->m_lock.lock();
        bool isOpExist = false, isSeqExist = false;
        int prevRequestIdx = INT_MAX;
        OpContext* opctx = NULL;
        if(master->m_requestMap.count(index)){//查找该log是否存在
            isOpExist = true;//标记log是否存在
            opctx = master->m_requestMap[index];
            if(opctx->op.term != operation.term){
                opctx->isWrongLeader = true;//term不对说明该服务器不是当时log的leader
                printf("not euqal term -> wrongLeader : opctx %d, op : %d\n", opctx->op.term, operation.term);
            }
        }
        if(master->m_clientSeqMap.count(operation.clientId)){
            isSeqExist = true;//该client是否通信过
            prevRequestIdx = master->m_clientSeqMap[operation.clientId]; //之前的发送id
        }
        master->m_clientSeqMap[operation.clientId] = operation.requestId; //更新发送id

        if(operation.op == "Join" || operation.op == "Leave" || operation.op == "Move"){
            //非leader的server必然不存在命令，同样处理状态机，leader的第一条命令也不存在
            printf("[%d]'s prevIdx is %d, opIdx is %d, isSeqExist is %d, cliendID is %d, op is %s\n", 
                master->m_id, prevRequestIdx, operation.requestId, (isSeqExist ? 1 : 0), operation.clientId, operation.op.c_str());
            if(!isSeqExist || prevRequestIdx < operation.requestId){ //本机为该log的leader并且该log是老的就不执行
                master->doJoinLeaveMove(operation);  //进行对应的操作
            }
        }else{
            // printf("[%d]'s prevIdx is %d, opIdx is %d, isSeqExist is %d, cliendID is %d, op is %s\n", 
            //     master->m_id, prevRequestIdx, operation.requestId, (isSeqExist ? 1 : 0), operation.clientId, operation.op.c_str());
            if(isOpExist){
                int queryNum = stoi(operation.args); //字符串转十进制 //confignum
                if(queryNum >= master->configs.size() || queryNum == -1){
                    opctx->config = master->configs[master->configs.size() - 1]; //返回最新的config
                }else{
                    opctx->config = master->configs[queryNum]; //返回对应confignum的config
                }
            }
        }

        master->m_lock.unlock();

        //保证只有存了上下文信息的leader才能唤醒管道，回应clerk的RPC请求(leader需要多做的工作)
        if(isOpExist){  
            int fd = open(opctx->fifoName.c_str(), O_WRONLY);
            char* buf = "12345";
            write(fd, buf, strlen(buf) + 1);
            close(fd);
        }    
        master->m_raft.postRecvSem(); //放开接受
    }
}

//自己定义的优先队列的两种排序方式
class mycmpLower{
public:
    bool operator()(const pair<int, vector<int>>& a, const pair<int, vector<int>>& b){
        return a.second.size() > b.second.size();
    }
};

class mycmpUpper{
public:
    bool operator()(const pair<int, vector<int>>& a, const pair<int, vector<int>>& b){
        return a.second.size() < b.second.size();
    }
};

//太长了typedef一下
//priority_queu元素有序排列的队列，第一个参数是存储对象的类型，第二个参数是存储元素的底层容器，第三个参数是函数对象，小的元素会排在队列前面
typedef priority_queue<pair<int, vector<int>>, vector<pair<int, vector<int>>>, mycmpLower> lowSizeQueue;//从vector size小到大
typedef priority_queue<pair<int, vector<int>>, vector<pair<int, vector<int>>>, mycmpUpper> upSizeQueue;//从vector size大到小

//用workLoad的数据更新lowerLoadSize，更新lower排序方式的优先队列
void syncLowerLoadSize(unordered_map<int, vector<int>>& workLoad, lowSizeQueue& lowerLoadSize){
    while(!lowerLoadSize.empty()){//将lowerLoadSize清空
        lowerLoadSize.pop();
    }
    for(const auto& load : workLoad){
        lowerLoadSize.push(load);//将workLoad中数据加入lowerLoadSize中
    }
}   
//用workLoad的数据更新lowerLoadSize，更新high排序方式的优先队列
void syncUpperLoadSize(unordered_map<int, vector<int>>& workLoad, upSizeQueue& upperLoadSize){
    while(!upperLoadSize.empty()){//将lowerLoadSize清空
        upperLoadSize.pop();
    }
    for(const auto& load : workLoad){//将workLoad中数据加入lowerLoadSize中
        upperLoadSize.push(load);
    }
}

//负载均衡实现，自己写的可能不是很高效，用了两个优先队列不断更新，主要思想就是把负载最大的拿出来给负载最小的，同时更新workLoad
//再将两个优先队列继续按照workLoad更新，迭代直到满足负载全相等或最大差一
void ShardMaster::balanceWorkLoad(Config& config){
    lowSizeQueue lowerLoadSize;
    unordered_map<int, vector<int>> workLoad;//group id，group server信息
    for(const auto& group : config.groups){
        workLoad[group.first] = vector<int>{};          //先记录下总共有哪些gid(注意：leave去除了gid，把对应的shard置0)
    }
    for(int i = 0; i < config.shards.size(); i++){      //先把为0的部分分配给最小的负载，其实相当于就是再处理move和初始化的join(一开始都为0)
        if(config.shards[i] != 0){
            workLoad[config.shards[i]].push_back(i);    //对应gid负责的分片都push_back到value中，用size表示对应gid的负载
        }
    }
    syncLowerLoadSize(workLoad, lowerLoadSize);
    for(int i = 0; i < config.shards.size(); i++){
        if(config.shards[i] == 0){
            auto load = lowerLoadSize.top();     //找负载最小的组
            lowerLoadSize.pop();//从lowerLoadSize中取出该组
            workLoad[load.first].push_back(i);//将0切片放入负载最小组
            load.second.push_back(i);
            lowerLoadSize.push(load);//将该组重新放入lowerLoadSize中
        }
    }
    //如果没有为0的，就不断找到负载最大给分给最小的即可
    upSizeQueue upperLoadSize;
    syncUpperLoadSize(workLoad, upperLoadSize);
    if(NShards % config.groups.size() == 0){        //根据是否正好取模分为所有gid的shards数量相同或者最大最小只差1两种情况
        while(lowerLoadSize.top().second.size() != upperLoadSize.top().second.size()){
            workLoad[lowerLoadSize.top().first].push_back(upperLoadSize.top().second.back());//最小负载组加入最大负载组最后切片
            workLoad[upperLoadSize.top().first].pop_back();//删除最大负载组最后切片
            syncLowerLoadSize(workLoad, lowerLoadSize);
            syncUpperLoadSize(workLoad, upperLoadSize);
        }
    }else{
        while(upperLoadSize.top().second.size() - lowerLoadSize.top().second.size() > 1){
            workLoad[lowerLoadSize.top().first].push_back(upperLoadSize.top().second.back());//最小负载组加入最大负载组最后切片
            workLoad[upperLoadSize.top().first].pop_back();//删除最大负载组最后切片
            syncLowerLoadSize(workLoad, lowerLoadSize);
            syncUpperLoadSize(workLoad, upperLoadSize);
        }
    }
    //传入的是config的引用，按照最终的workLoad更新传入config的shards
    for(const auto& load : workLoad){
        for(const auto& idx : load.second){
            config.shards[idx] = load.first;
        }
    }
}

vector<kvServerInfo> getKvServerPort(int num){
    vector<kvServerInfo> peers(num);
    for(int i = 0; i < num; i++){
        peers[i].peersInfo.m_peerId = i;
        peers[i].peersInfo.m_port.first = MASTER_PORT + i;
        peers[i].peersInfo.m_port.second = MASTER_PORT + i + num;
        peers[i].peersInfo.isInstallFlag = false;
        for(int j = 0; j < EVERY_MASTER_PORT; j++){
            peers[i].m_kvPort.push_back(MASTER_PORT + i + (j + 2) * num);
        }
        // printf(" id : %d port1 : %d, port2 : %d\n", peers[i].peersInfo.m_peerId, peers[i].peersInfo.m_port.first, peers[i].peersInfo.m_port.second);
        // for(auto a : peers[i].m_kvPort){
        //     cout<<a<<" ";
        // }
        // cout<<endl;
    }
    return peers;
}

int ShardMaster::getConfigsSize(){
    m_lock.lock();
    int len = configs.size();
    m_lock.unlock();
    return len;
}