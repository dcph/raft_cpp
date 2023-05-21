#include "myClass.h"
#include <pthread.h>
using namespace std;

/**
 * @brief 注释太多了，写起来太乱了，需要理解整个流程。建议先看我写的LAB4B的md文件，在看代码就能看得懂了
 * 
 */

vector<PeersInfo> getRaftPort(vector<kvServerInfo>& kvInfo){
    int n = kvInfo.size();
    vector<PeersInfo> ret(n);
    for(int i = 0; i < n; i++){
        ret[i] = kvInfo[i].peersInfo;
    }
    return ret;
}

unordered_map<string, int> str2Port;

void initStr2PortMap(int num){
    if(num >= 10 || num <= 0){
        exit(-1);
    }
    for(int i = 1; i <= num; i++){
        for(int j = 1; j <= EVERY_SERVER_RAFT; j++){
            string str = to_string(i) + to_string(j);
            str2Port[str] = COMMOM_PORT + 2 * EVERY_SERVER_RAFT + j - 1 + (i - 1) * 100;
            // printf("%d ", COMMOM_PORT + 2 * EVERY_SERVER_RAFT + j - 1 + (i - 1) * 100);
        }
        // printf("\n");
    }
}

int make_end(string str){
    return str2Port[str];
}

//从特定格式的string获得其对应的config，因为RPC不能传config
Config getConfig(string configStr){
    Config config;
    vector<string> str;
    string tmp = "";
    for(int i = 0; i < configStr.size(); i++){
        if(configStr[i] != '/'){
            tmp += configStr[i];
        }else{
            if(tmp.size() != 0){
                str.push_back(tmp);
                tmp = "";
            }
        }
    }
    if(str.size() == 2){
        config.configNum = stoi(str[0]);
        config.shards = GetVectorOfIntFromString(str[1]);
        config.groups.clear();
        return config;
    }
    config.configNum = stoi(str[0]);
    config.shards = GetVectorOfIntFromString(str[1]);
    config.groups = getMapFromServersShardInfo(str[2]);
    return config;
}

int key2shard(string key){
    int shard = 0;
    if(key.size() > 0){
        shard = key[0] - 'a';
    }
    shard = shard % NShards;
    return shard;
}

//按op拆分字符串的函数，经常用到
vector<string> splitStr(string str, char op){
    vector<string> ret;
    string tmp = "";
    for(int i = 0; i < str.size(); i++){
        if(str[i] != op){
            tmp += str[i];
        }else{
            if(tmp.size() != 0){
                ret.push_back(tmp);
                tmp = "";
            }
        }
    }
    if(tmp.size() != 0) ret.push_back(tmp);
    return ret;
}

//将数据迁移的RPC应答封装成string格式
string MigrateReply2Str(MigrateReply reply){
    string str;
    str += to_string(reply.configNum) + "|" + to_string(reply.shard) + "|" + reply.err + "|";
    if(reply.clientReqId.empty()) str += "empty";
    else{
        for(const auto& req : reply.clientReqId){
            str += to_string(req.first) + ":" + to_string(req.second) + "/";
        }
    }
    str += "|";
    if(reply.database.empty()) str += "empty";
    else{
        for(const auto& data : reply.database){
            str += data.first + ":" + data.second + "/";
        }
    }
    str += "|";
    return str;
}

//将RPC应答收到的string逆转换为正确的reply格式
MigrateReply str2MigrateReply(string str){
    MigrateReply reply;
    vector<string> content;
    string tmp; 
    for(int i = 0; i < str.size(); i++){
        if(str[i] != '|'){
            tmp += str[i];
        }else{
            if(tmp.size() != 0){
                content.push_back(tmp);
                tmp = "";
            }
        }
    }
    reply.configNum = stoi(content[0]);
    reply.shard = stoi(content[1]);
    reply.err = content[2];
    tmp = "";
    if(content[3] == "empty") reply.clientReqId.clear();
    else{
        vector<string> request = splitStr(content[3], '/');
        for(int i = 0; i < request.size(); i++){
            tmp = "";
            int j = 0;
            for(; j < request[i].size(); j++){
                if(request[i][j] != ':'){
                    tmp += request[i][j];
                }else break;
            }
            string number(request[i].begin() + j + 1, request[i].end());
            reply.clientReqId[stoi(tmp)] = stoi(number);
        }
    }
    tmp = "";
    if(content[4] == "empty") reply.database.clear();
    else{
        vector<string> data = splitStr(content[4], '/');
        for(int i = 0; i < data.size(); i++){
            tmp = "";
            int j = 0;
            for(; j < data[i].size(); j++){
                if(data[i][j] != ':'){
                    tmp += data[i][j];
                }else break;
            }
            string value(data[i].begin() + j + 1, data[i].end());
            reply.database[tmp] = value;
        }
    }
    return reply;
}

class ShardKv{
public:
    static void* RPCserver(void* arg);
    static void* applyLoop(void* arg);  //持续向上层应用日志的守护线程
    static void* snapShotLoop(void* arg);  //生成快照
    static void* updateConfigLoop(void* arg); //定时更新config
    static void* pullShardLoop(void* arg);  //pull分片
    static void* garbagesCollectLoop(void* arg);//处理垃圾
    static void* doGarbage(void* arg);
    static void* doPullShard(void* arg);

    void StartKvServer(vector<kvServerInfo>& kvInfo, int me, int gid, int maxRaftState, vector<vector<int>>& masters);
    void getPutAppendOnDataBase(ShardKv* kv, Operation operation, OpContext* opctx, bool isOpExist, bool isKeyExisted, int prevRequestIdx);
    void doSnapShot(ShardKv* kv, ApplyMsg msg);

    GetReply get(GetArgs args);
    PutAppendReply putAppend(PutAppendArgs args);
    MigrateRpcReply shardMigration(MigrateArgs args);  //分片迁移
    GarbagesCollectReply garbagesCollect(GarbagesCollectArgs args); //垃圾收集

    bool isMatchShard(string key);
    void updateComeInAndOutShrads(Config config);
    void updateDataBaseWithMigrateReply(MigrateReply reply);
    void clearToOutData(int cfgNum, int shard);
    // string test(string key){ return m_database[key]; }  //测试其余不是leader的server的状态机

    string getSnapShot();
    void recoverySnapShot(string snapShot);
    void printSnapShot();

    //----------------------------test------------------------------------
    bool getRaftState();
    void killRaft();
    void activateRaft();

private:
    locker m_lock;
    Raft m_raft;
    int m_id;//服务器id
    int m_port; //对外kv服务port，只有一个
    int m_groupId;//group id

    int m_maxraftstate;  //超过这个大小就快照
    int m_lastAppliedIndex; //已应用index

    shardClerk m_masterClerk; //向客户端通信的配置
    Config m_config; //配置信息

    unordered_map<string, string> m_database;  //模拟数据库
    unordered_map<int, int> m_clientSeqMap;    //只记录特定客户端已提交的最大请求ID的去重map，不需要针对分片，迁移时整个发送即可
    unordered_map<int, OpContext*> m_requestMap;  //记录当前RPC对应的上下文

    unordered_map<int, unordered_map<int, unordered_map<string, string>>> toOutShards; //需要送出去的分片，confignum，分片标号，服务器分片内容
    unordered_map<int, int> comeInShards; //需要拉取的分片
    unordered_set<int> m_AvailableShards; //当前可用的分片
    unordered_map<int, unordered_set<int>> garbages; //需要清除的切片
    unordered_map<int, unordered_set<int>> garbagesBackUp;

    int garbageFinished;
    int garbageConfigNum;
    int pullShardFinished;

    unordered_set<int>::iterator garbageIter;
    unordered_map<int, int>::iterator pullShardIter;

};

void ShardKv::StartKvServer(vector<kvServerInfo>& kvInfo, int me, int gid, int maxRaftState, vector<vector<int>>& masters){ //kv服务器信息，id，group，快照状态，shardMaster端口
    
    this->m_id = me;
    this->m_groupId = gid;
    m_port = kvInfo[me].m_kvPort;
    vector<PeersInfo> peers = getRaftPort(kvInfo);
    this->m_maxraftstate = maxRaftState;
    this->m_lastAppliedIndex = 0;

    m_masterClerk.makeClerk(masters);  //初始化向master的clerk通信

    m_raft.setRecvSem(1);//设置为可接受
    m_raft.setSendSem(0);//设置为不可发送
    m_raft.Make(peers, me); //启动raft的三个loop

    m_database.clear();
    m_clientSeqMap.clear();
    m_requestMap.clear();

    toOutShards.clear();
    comeInShards.clear();
    m_AvailableShards.clear();
    garbages.clear();
    garbagesBackUp.clear();
    // dead = false;

    garbageFinished = 0;
    pullShardFinished = 0;
    garbageConfigNum = 0;

    pthread_t listen_tid1;
    pthread_create(&listen_tid1, NULL, RPCserver, this);
    pthread_detach(listen_tid1);

    pthread_t listen_tid2;
    pthread_create(&listen_tid2, NULL, applyLoop, this);
    pthread_detach(listen_tid2);

    pthread_t listen_tid3;
    pthread_create(&listen_tid3, NULL, snapShotLoop, this);
    pthread_detach(listen_tid3);

    pthread_t listen_tid4;
    pthread_create(&listen_tid4, NULL, updateConfigLoop, this);
    pthread_detach(listen_tid4);

    pthread_t listen_tid5;
    pthread_create(&listen_tid5, NULL, pullShardLoop, this);
    pthread_detach(listen_tid5);

    pthread_t listen_tid6;
    pthread_create(&listen_tid6, NULL, garbagesCollectLoop, this);
    pthread_detach(listen_tid6);
}

void* ShardKv::updateConfigLoop(void* arg){  //定时请求更新config
    ShardKv* kv = (ShardKv*)arg;
    while(1){
        bool isleader = kv->m_raft.getState().second;//只有leader请求config信息
        kv->m_lock.lock();
        if(!isleader || kv->comeInShards.size() > 0){ //还没有成功接收完分片不会请求
            kv->m_lock.unlock();
            usleep(50000);
            continue;
        }
        int nextCfgNum = kv->m_config.configNum + 1;
        kv->m_lock.unlock();
        Config config = kv->m_masterClerk.Query(nextCfgNum); //请求最新的配置
        if(config.configNum == nextCfgNum){
            Operation operation;
            operation.op = "UC";   //更新配置的成功标记
            operation.key = "random";
            operation.value = getStringFromConfig(config); //config转string
            operation.args = "random";
            operation.clientId = rand() % 10000 + 10000;
            operation.requestId = rand() % 10000 + 10000;
            operation.index = -1;
            operation.term = -1;
            // printf("cfgnum is %d, next is %d, in %ld-----------\n", kv->m_config.configNum, nextCfgNum, pthread_self());
            kv->m_raft.start(operation);  //也是在自己的集群里调用start，无需考虑线性一致性，那个是对于客户端请求要求保证的
        }
        usleep(50000);
    }
}

void* ShardKv::pullShardLoop(void* arg){  //pull需要的分片
    ShardKv* kv = (ShardKv*)arg;
    while(1){
        bool isleader = kv->m_raft.getState().second;//只有leader进行pull
        kv->m_lock.lock();
        if(!isleader || kv->comeInShards.size() == 0){
            kv->m_lock.unlock();
            usleep(80000);
            continue;
        }
        //遍历所有comeInShards里的shard发RPC
        int tmpShardsSize = kv->comeInShards.size();
        // kv->pullShardSize = tmpShardsSize;
        kv->pullShardFinished = 0;
        kv->pullShardIter = kv->comeInShards.begin();
        kv->m_lock.unlock();

        pthread_t tid[tmpShardsSize];
        for(int i = 0; i < tmpShardsSize; i++){
            pthread_create(tid + i, NULL, doPullShard, kv);
            pthread_detach(tid[i]);
        }
        //线程最后再++，到这里必然已经把逻辑处理完了，哪怕时间片到了被抢走cpu时间也没关系，此处pullShardIndex只读，不加锁也可一旦不满足条件就可以退出循环
        while(kv->pullShardFinished != tmpShardsSize){ 
            usleep(5000);
        }
        usleep(80000);
    }
}

void* ShardKv::doPullShard(void* arg){  //pull分片
    ShardKv* kv = (ShardKv*)arg;    
    MigrateArgs args;

    kv->m_lock.lock();
    auto tmpData = (*kv->pullShardIter++);
    args.shard = tmpData.first;  //需要pull的分片
    // int cfgNum = tmpData.second;
    args.configNum = tmpData.second;
    kv->m_lock.unlock();

    Config config = kv->m_masterClerk.Query(cfgNum);//请求对应config的设置
    int gid = config.shards[args.shard]; //找到分片对应group
    bool isOk = false;
    for(const auto& server : config.groups[gid]){  //对于该group的所有server
        if(isOk) break;
        int port = make_end(server);//对应kv服务接口
        buttonrpc client;
        client.as_client("127.0.0.1", port);
        MigrateRpcReply retReply = client.call<MigrateRpcReply>("shardMigration", args).val();   //向对应服务器发送分片
        // printf("[%d]  pull shard%d in cfg%d from %d call ser%s'sport is %d, recv reply : %s\n", 
        // kv->m_groupId, args.shard, cfgNum, gid, server.c_str(), port, retReply.reply.c_str());
        MigrateReply reply = str2MigrateReply(retReply.reply);  //string转MigrateReply
        if(reply.err == OK){
            //发起pullShard的必然是leader
            isOk = true;
            Operation operation;
            operation.op = "UD";   //pull分片的成功标记
            operation.key = "random";
            operation.value = MigrateReply2Str(reply);  //分片的内容
            operation.args = "random";
            operation.clientId = rand() % 10000 + 10000;
            operation.requestId = rand() % 10000 + 10000;
            operation.index = -1;
            operation.term = -1;
            //每收到一个reply就共识然后通过applyLoop进行DB的更新,是发起方在自己的集群里调raft的start
            //不是在收到RPC的server集群里调用，若是在收到RPC的server集群里调用start就类似kvserver的处理，需要考虑线性一致性
            kv->m_raft.start(operation);         
        }
    }
    kv->m_lock.lock();
    kv->pullShardFinished++;
    // printf("shard%d is finished, finishedNum is %d\n", args.shard, kv->pullShardFinished);
    kv->m_lock.unlock();
    return NULL;
}

MigrateRpcReply ShardKv::shardMigration(MigrateArgs args){
    // printf("[%d]'s shardMigration is be called by other\n", m_groupId);
    MigrateReply reply;
    reply.err = ErrWrongLeader;
    reply.shard = args.shard;
    reply.configNum = args.configNum;
    reply.clientReqId.clear();
    reply.database.clear();

    string str = MigrateReply2Str(reply);
    MigrateRpcReply retReply; //MigrateReply封装为string用于rpc
    retReply.reply = str;
    bool isLeader = m_raft.getState().second;
    if(!isLeader) return retReply;  //不是leader返回

    reply.err = ErrWrongGroup;
    str = MigrateReply2Str(reply);
    retReply.reply = str;
    m_lock.lock();
    if(args.configNum >= this->m_config.configNum){
        m_lock.unlock();
        return retReply;  //不是对应group返回
    }
    reply.err = OK;
    for(const auto& data : toOutShards[args.configNum][args.shard]){//将对应confignum，分片标号的数据放入数据库中
        reply.database.insert(data);
    }
    for(const auto& seq : m_clientSeqMap){//客户端已提交的最大请求ID
        reply.clientReqId.insert(seq);
    }
    str = MigrateReply2Str(reply);
    retReply.reply = str;
    m_lock.unlock();
    return retReply;
}

void* ShardKv::garbagesCollectLoop(void* arg){   //垃圾回收循环
    ShardKv* kv = (ShardKv*)arg;
    //都失败了也没事(网络故障)，while循环无非再过一段时间重新处理garbages直到其size == 0
    while(1){
        bool isleader = kv->m_raft.getState().second;//只有leader进行
        kv->m_lock.lock();
        if(!isleader || kv->garbages.size() == 0){
            kv->m_lock.unlock();
            usleep(100000);
            continue;
        }
        //遍历所有garbages里的shard发RPC 
        printf("gid[%d]'s garbages is : \n", kv->m_groupId);
        for(auto a : kv->garbages){
            printf("cfgNum.size() : %d cfg: %d shard: ", a.second.size(), a.first);
            for(auto b : a.second){
                printf(" %d ", b);
            }
            printf("\n");
        }
        kv->garbagesBackUp = kv->garbages;
        kv->m_lock.unlock();
        for(auto garbage : kv->garbagesBackUp){//对于所有需要清除的切片
            kv->m_lock.lock();
            int tmpGarbagesSize = garbage.second.size();//shard
            // kv->garbageSize = tmpGarbagesSize;
            kv->garbageIter = garbage.second.begin();
            kv->garbageConfigNum = garbage.first;//confignum
            kv->garbageFinished = 0;
            kv->m_lock.unlock();

            pthread_t tid[tmpGarbagesSize];
            for(int i = 0; i < tmpGarbagesSize; i++){
                pthread_create(tid + i, NULL, doGarbage, kv);//删除对应confignum-shard的切片
                pthread_detach(tid[i]);
            }
            //线程最后再++，到这里必然已经把逻辑处理完了，哪怕时间片到了被抢走cpu时间也没关系,也可以不加锁，对于此处garbageFinished只读
            while(kv->garbageFinished != tmpGarbagesSize){ 
                usleep(5000);
            }
        }
        kv->garbagesBackUp.clear();
        usleep(100000);
    }
}

void* ShardKv::doGarbage(void* arg){  //删除切片
    ShardKv* kv = (ShardKv*)arg;
    GarbagesCollectArgs args;//初始化切片信息
    kv->m_lock.lock();
    args.shard = (*kv->garbageIter++);
    args.configNum = kv->garbageConfigNum;
    kv->m_lock.unlock();
    Config config = kv->m_masterClerk.Query(args.configNum);  //找到对应的num的config
    int gid = config.shards[args.shard]; 
    printf("gid[%d] need send shard%d in cfg%d to %d\n", kv->m_groupId, args.shard, args.configNum, gid);
    // printf("next shard : %d\n", kv->garbagesBackUp[kv->garbageConfigNum][(*kv->garbageIter).first]);
    for(const auto& server : config.groups[gid]){ //对于group中的所有id
        int port = make_end(server);//找到对应服务器kv服务端口
        buttonrpc client;
        client.as_client("127.0.0.1", port);
        GarbagesCollectReply reply = client.call<GarbagesCollectReply>("garbagesCollect", args).val();
        if(reply.err == OK){
            kv->m_lock.lock();
            kv->garbages[kv->garbageConfigNum].erase(args.shard);//删除对应切片信息
            if(kv->garbages[kv->garbageConfigNum].empty()){
                kv->garbages.erase(kv->garbageConfigNum);//如果为空，删除对应config信息
            }
            kv->m_lock.unlock();
        }
    }
    kv->m_lock.lock();
    kv->garbageFinished++;
    kv->m_lock.unlock();
}

GarbagesCollectReply ShardKv::garbagesCollect(GarbagesCollectArgs args){  //切片清除过程
    GarbagesCollectReply reply;
    reply.err = ErrWrongLeader;
    bool isLeader = m_raft.getState().second;
    if(!isLeader) return reply;//判断是否leader
    m_lock.lock();
    if(!toOutShards.count(args.configNum)){//判断对应confignum和shard是否需要进行删除
        m_lock.unlock();
        return reply;
    }
    if(!toOutShards[args.configNum].count(args.shard)){
        m_lock.unlock();
        return reply;
    }
    //同样和之前get、putAppend一样定义Op并传入处理，还需要select(fifoName)  
    //是因为此种情况类似kvserver，是在收到RPC的server集群里进行start，收到的相当于其他集群(对自己集群来说是客户端)的请求，
    //对于此种客户端的请求需要保证请求的线性一致性，至于上面的updateConfig以及updateDB就只是相当于集群内raft同步日志
    m_lock.unlock();  //需要判断当前的config中有没有这个切片，如果现在正在进行的config中有这个切片，那就不用删除
    reply.err = OK;
    Operation operation;
    operation.op = "GC";  //定义切片删除标记
    operation.key = to_string(args.configNum);
    operation.value = "random";
    operation.args = "random";
    operation.clientId = rand() % 10000 + 10000;
    operation.requestId = args.shard;
    StartRet ret = m_raft.start(operation);   //必然是leader，不然上面就return，预防极端情况正好这里宕机了又进行了判断

    operation.term = ret.m_curTerm;
    operation.index = ret.m_cmdIndex;
    if(!ret.isLeader){
        reply.err = ErrWrongLeader;
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
            reply.err = ErrWrongLeader;
        }
    }
    else{
        reply.err = ErrWrongLeader;
        printf("int putAppend --------- timeout!!!\n");
    }
    m_lock.lock();
    // m_requestMap.erase(ret.m_cmdIndex);
    if(m_requestMap.count(ret.m_cmdIndex)){
        if(m_requestMap[ret.m_cmdIndex] == &opctx){
            m_requestMap.erase(ret.m_cmdIndex);
        }
    }
    m_lock.unlock();
    return reply;
}

void ShardKv::clearToOutData(int cfgNum, int shard){
    if(this->m_id == 1){
        printf("gid[%d] call clearToOutData cfg : %d, shard : %d\n", m_groupId, cfgNum, shard);
    }
    if(toOutShards.count(cfgNum)){//查找是否存在这一confignum
        toOutShards[cfgNum].erase(shard);//删除对应shard切片
        if(toOutShards[cfgNum].empty()){
            toOutShards.erase(cfgNum);//如果该confignum已经没有shard信息，全删
        }
    }
}

bool ShardKv::isMatchShard(string key){
    m_lock.lock();
    bool ret = (m_groupId == m_config.shards[key2shard(key)]);
    m_lock.unlock();
    return ret;
}

void ShardKv::updateComeInAndOutShrads(Config config){  //更新config
    m_lock.lock();
    //防止raft日志还未同步，updateConfigLoop又进行新一轮循环传入一样的config到start()中
    if(config.configNum <= this->m_config.configNum){  //如果该配置是老的，忽略
        m_lock.unlock();
        return;
    }
    Config oldConfig = this->m_config;//记录老的配置
    unordered_set<int> tmpToOutShardMap = this->m_AvailableShards;//记录原来存在的切片
    this->m_config = config; //配置更新
    // printf("in UC old cfgNUm is %d, newCfgNum is %d in %ld\n", oldConfig.configNum, m_config.configNum, pthread_self());
    this->m_AvailableShards.clear();//重置shard相关list
    for(int i = 0; i < config.shards.size(); i++){
        if(config.shards[i] != this->m_groupId){  //切片不在该group中
            continue;
        }
        if(tmpToOutShardMap.count(i) || oldConfig.configNum == 0){ //更新当前可用的分片
            tmpToOutShardMap.erase(i);//更新删除切片
            m_AvailableShards.insert(i);//更新存在切片
        }else{
            this->comeInShards[i] = oldConfig.configNum; //更新需要拉取的分片
        }
    }
    if(this->m_id == 1){//一个group只输出一次
        if(comeInShards.size() > 0){
                printf("In gid %d comeInShards : \n", m_groupId);
            for(auto a : comeInShards){
                printf("shard : %d -> cfgNum : %d\n", a.first, a.second);
            }
        }
    }
    if(tmpToOutShardMap.size() > 0){
        for(const auto& shard : tmpToOutShardMap){ //对之前的分片中不在本次group中的
            unordered_map<string, string> tmpDataBase;
            tmpDataBase.clear();
            unordered_map<string, string> dataBackUp = this->m_database;
            for(const auto& data : dataBackUp){
                if(key2shard(data.first) == shard){
                    tmpDataBase.insert(data);//将m_database中的值按照分片放入tmpDataBase
                    this->m_database.erase(data.first);//m_database中删除不再需要的key
                }
            }
            this->toOutShards[oldConfig.configNum][shard] = tmpDataBase;//将对应的分片放入toOutShards
            if(this->m_id == 1){
                printf("in gid %d cfg num : %d, shard%d's data \n", m_groupId, oldConfig.configNum, shard);
                for(auto a : tmpDataBase){
                    printf("key : %s -> value : %s\n", a.first.c_str(), a.second.c_str());
                }  
            }
        }
    }
    // if(this->m_id == 1){
    //     printf("[%d] updateConfig num is %d, out is %d, in is %d, avashards is %d in %ld\n", m_id, 
    // config.configNum, toOutShards[oldConfig.configNum].size(), comeInShards.size(), m_AvailableShards.size(), pthread_self());
    // }
    m_lock.unlock();
}

void ShardKv::updateDataBaseWithMigrateReply(MigrateReply reply){
    m_lock.lock();

    if(reply.configNum != this->m_config.configNum - 1){ //校对confignum
        m_lock.unlock();
        return;
    }
    // if(this->m_id == 1){
    //     printf("[%d] before erase %d, In.size() is %d, DB.size() is %d, req.size() is %d\n", 
    //         m_groupId, reply.shard, comeInShards.size(), reply.database.size(), reply.clientReqId.size());
    // }
    this->comeInShards.erase(reply.shard);//记录该切片已被拉取
    // if(this->m_id == 1){
    //     printf("[%d] after erase %d, In.size() is %d, DB.size() is %d, req.size() is %d\n", 
    //         m_groupId, reply.shard, comeInShards.size(), reply.database.size(), reply.clientReqId.size());
    // }
    if(!m_AvailableShards.count(reply.shard)){//如果该切片不可用
        for(const auto& data : reply.database){
            m_database.insert(data); //将对应分片放入数据库中
            // if(this->m_id == 1){
            //     printf("gid[%d] db insert shard%d's key : %s -> value : %s\n", m_groupId, reply.shard, data.first.c_str(), data.second.c_str());
            // }
        }
        for(const auto& seq : reply.clientReqId){
            this->m_clientSeqMap[seq.first] = max(m_clientSeqMap[seq.first], seq.second);//更新对应分片的最新应用
            // if(this->m_id == 1){
            //     printf("gid[%d] db insert shard%d's cli : %d -> seq : %d\n", m_groupId, reply.shard, seq.first, m_clientSeqMap[seq.first]);
            // }
        }
        m_AvailableShards.insert(reply.shard);//更新可用切片
        garbages[reply.configNum].insert(reply.shard);//更新需要删除切片
        // if(this->m_id == 1){
        //     for(auto a : garbages){
        //         int cfgNum = a.first;
        //         for(auto b : a.second){
        //             printf("gid[%d]'s cfgNum : %d -> shard : %d\n", m_groupId, cfgNum, b.first);
        //         }
        //     }
        // }
    }
    m_lock.unlock();
}


void* ShardKv::RPCserver(void* arg){  //发布4个函数
    ShardKv* kv = (ShardKv*) arg;
    buttonrpc server;

    server.as_server(kv->m_port);    //对外提供服务的端口 
    server.bind("get", &ShardKv::get, kv);
    server.bind("putAppend", &ShardKv::putAppend, kv);
    server.bind("shardMigration", &ShardKv::shardMigration, kv);
    server.bind("garbagesCollect", &ShardKv::garbagesCollect, kv);
    server.run();
}


//相应client的get请求
GetReply ShardKv::get(GetArgs args){
    GetReply reply;
    if(!isMatchShard(args.key)){  //首先判断该key是否由该group进行处理
        reply.err = ErrWrongGroup; //不是
        return reply;
    }
    reply.err = OK;
    Operation operation; //初始化raft层所需的命令信息
    operation.op = "get";
    operation.key = args.key;
    operation.value = "random";
    operation.args = "random";
    operation.clientId = args.clientId;
    operation.requestId = args.requestId;

    StartRet ret = m_raft.start(operation); //判断是否leader，记录log，返回m_curTerm等信息
    operation.term = ret.m_curTerm;
    operation.index = ret.m_cmdIndex;

    if(ret.isLeader == false){
        // printf("client %d's get request is wrong leader %d\n", args.clientId, m_id);
        reply.err = ErrWrongLeader;
        return reply;
    }

    OpContext opctx(operation); //将该命令封装一层接收管道
    m_lock.lock();
    m_requestMap[ret.m_cmdIndex] = &opctx;//将该命令以引用形式记录，便于修改
    m_lock.unlock();
    Select s(opctx.fifoName);  //创建判断是否工作完成的线程
    myTime curTime = myClock::now();
    while(myDuration(myClock::now() - curTime).count() < 2000000){ 
        if(s.isRecved){//超时
            // printf("client %d's get->time is %d\n", args.clientId, myDuration(myClock::now() - curTime).count());
            break;
        }
        usleep(10000);
    }

    if(s.isRecved){ //未超时
        if(opctx.isWrongLeader){
            reply.err = ErrWrongLeader;
        }else if(opctx.op.op == ErrWrongGroup){
            reply.err = ErrWrongGroup;
        }else if(!opctx.isKeyExisted){
            reply.err = ErrNoKey;
        }else{
            reply.value = opctx.value;
        }
    }
    else{
        reply.err = ErrWrongLeader;
        printf("in get --------- timeout!!!\n");
    }
    m_lock.lock();
    // m_requestMap.erase(ret.m_cmdIndex);
    if(m_requestMap.count(ret.m_cmdIndex)){//如果该命令尚未完成
        if(m_requestMap[ret.m_cmdIndex] == &opctx){
            m_requestMap.erase(ret.m_cmdIndex);//删除命令，代表已完成
        }
    }
    m_lock.unlock();
    return reply;
}

//PRChandler for put/append-request
PutAppendReply ShardKv::putAppend(PutAppendArgs args){
    PutAppendReply reply;
    if(!isMatchShard(args.key)){
        reply.err = ErrWrongGroup;
        return reply;
    }
    reply.err = OK;
    Operation operation;
    operation.op = args.op;
    operation.key = args.key;
    operation.value = args.value;
    operation.args = "random";
    operation.clientId = args.clientId;
    operation.requestId = args.requestId;
    // printf("in putAppend key is %s, value is %s\n", args.key.c_str(), args.value.c_str());
    StartRet ret = m_raft.start(operation);

    operation.term = ret.m_curTerm;
    operation.index = ret.m_cmdIndex;
    if(ret.isLeader == false){
        printf("client %d's putAppend request is wrong leader %d\n", args.clientId, m_id);
        reply.err = ErrWrongLeader;
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
            reply.err = ErrWrongLeader;
        }else if(opctx.op.op == ErrWrongGroup){
            reply.err = ErrWrongGroup;
        }else if(opctx.isIgnored){
            //啥也不管即可，请求过期需要被忽略，返回ok让客户端不管即可
            printf("request is ignored ????????????????\n");
        }
    }
    else{
        reply.err = ErrWrongLeader;
        printf("int putAppend --------- timeout!!!\n");
    }
    m_lock.lock();
    // m_requestMap.erase(ret.m_cmdIndex);
    if(m_requestMap.count(ret.m_cmdIndex)){
        if(m_requestMap[ret.m_cmdIndex] == &opctx){
            m_requestMap.erase(ret.m_cmdIndex);
        }
    }
    m_lock.unlock();
    return reply;
}

void ShardKv::getPutAppendOnDataBase(ShardKv* kv, Operation operation, OpContext* opctx, bool isOpExist, bool isSeqExist, int prevRequestIdx){
    int shard = key2shard(operation.key);//计算key所在分片
    if(!kv->m_AvailableShards.count(shard)){//查找该分片是否可用
        opctx->op.op = ErrWrongGroup;
    }else{
        if(operation.op == "put" || operation.op == "append"){
            //非leader的server必然不存在命令，同样处理状态机，leader的第一条命令也不存在
            if(!isSeqExist || prevRequestIdx < operation.requestId){//命令存在且不老  
                if(operation.op == "put"){
                    // printf("[%d] in applyLoop put: %s -> %s\n", kv->m_id, operation.key.c_str(), operation.value.c_str());
                    kv->m_database[operation.key] = operation.value;
                }else if(operation.op == "append"){
                    if(kv->m_database.count(operation.key)){//查找该key是否存在
                        // printf("[%d] in applyLoop exist: %s -> %s\n", kv->m_id, operation.key.c_str(), operation.value.c_str());
                        kv->m_database[operation.key] += operation.value;
                    }else{
                        // printf("[%d] in applyLoop noexist: %s -> %s\n", kv->m_id, operation.key.c_str(), operation.value.c_str());
                        kv->m_database[operation.key] = operation.value;
                    }
                }
            }else if(isOpExist){//如果是leader
                printf("prev is %d, reuqest is %d\n", prevRequestIdx, operation.requestId);
                opctx->isIgnored = true;
            }
        }else{
            if(isOpExist){
                if(kv->m_database.count(operation.key)){//get操作
                    opctx->value = kv->m_database[operation.key];
                }else{
                    opctx->isKeyExisted = false;
                    opctx->value = "";
                }
            }
        }
    }

}

void ShardKv::doSnapShot(ShardKv* kv, ApplyMsg msg){
    if(msg.snapShot.size() == 0){   //重置kv
        kv->m_database.clear();
        kv->m_clientSeqMap.clear();
    }else{
        kv->recoverySnapShot(msg.snapShot); //使用快照
    }
    //一般初始化时安装快照，以及follower收到installSnapShot向上层kvserver发起安装快照请求
    kv->m_lastAppliedIndex = msg.lastIncludedIndex;   
    printf("in stall m_lastAppliedIndex is %d\n", kv->m_lastAppliedIndex);
}

void* ShardKv::applyLoop(void* arg){  //对kv服务器进行更改
    ShardKv* kv = (ShardKv*)arg;
    while(1){

        kv->m_raft.waitSendSem();//阻塞发送
        ApplyMsg msg = kv->m_raft.getBackMsg();//接受最近的可应用msg

        if(!msg.commandValid){ //判断是否执行快照
            kv->m_lock.lock();   
            kv->doSnapShot(kv, msg); //执行快照
            kv->m_lock.unlock();
        }else{
            Operation operation = msg.getOperation(); //string转operation
            int index = msg.commandIndex;//命令的index

            if(operation.op == "UC"){   //更新config
                Config config = getConfig(operation.value); //得到config
                // printf("[%d] update config in num : %d\n", kv->m_id, config.configNum);
                kv->updateComeInAndOutShrads(config);       //进行更新操作，该函数内部加锁了 //更新config，并更新需要加入的分片和需要发出的分片
            }
            else if(operation.op == "UD"){ //pull分片
                // if(kv->m_id == 1){
                //     printf("gid[%d] update dataBase\n", kv->m_groupId);
                // }
                MigrateReply reply = str2MigrateReply(operation.value);       //格式转换，str转MigrateReply
                kv->updateDataBaseWithMigrateReply(reply);  //进行更新操作，该函数内部加锁了 //将需要加入的分片加入
            }else{  
                kv->m_lock.lock();
                kv->m_lastAppliedIndex = index;//收到一个msg就更新m_lastAppliedIndex 
                bool isOpExist = false, isSeqExist = false;
                int prevRequestIdx = INT_MAX;
                OpContext* opctx = NULL;
                if(kv->m_requestMap.count(index)){//查找该命令是否存在
                    isOpExist = true;
                    opctx = kv->m_requestMap[index];
                    if(opctx->op.term != operation.term){
                        opctx->isWrongLeader = true;//如果term不对，那么这条命令不能执行
                        printf("not euqal term -> wrongLeader : opctx %d, op : %d\n", opctx->op.term, operation.term);
                    }
                }
                if(kv->m_clientSeqMap.count(operation.clientId)){//查找是否有这个client的记录
                    isSeqExist = true;
                    prevRequestIdx = kv->m_clientSeqMap[operation.clientId];//记录之前这个client的最高回复命令index
                }
                kv->m_clientSeqMap[operation.clientId] = operation.requestId;//更新最新的命令index

                if(operation.op == "GC"){  //切片删除过程
                    //对GC来说添加client的seq序列无所谓，无需进行判断去重，类似get操作，而且会反馈到garbages里面影响gcLoop，有就是有没就是没了
                    int cfgNum = stoi(operation.key);
                    kv->clearToOutData(cfgNum, operation.requestId); //对需要送出切片情况进行更新
                }else{
                    kv->getPutAppendOnDataBase(kv, operation, opctx, isOpExist, isSeqExist, prevRequestIdx);  //get，put，append操作的更新
                }

                kv->m_lock.unlock();
                //保证只有存了上下文信息的leader才能唤醒管道，回应clerk的RPC请求(leader需要多做的工作)
                if(isOpExist){  //如果半数同意，允许应用，但是原先的leader机离线了，这个operater就没法返回了
                    int fd = open(opctx->fifoName.c_str(), O_WRONLY);
                    char* buf = "12345";
                    write(fd, buf, strlen(buf) + 1);
                    close(fd);
                } 
            }
        }
        kv->m_raft.postRecvSem();//开通接受
    }
}

string ShardKv::getSnapShot(){
    string snapShot;
    if(m_database.empty()) snapShot += "empty";
    else{
        for(const auto& ele : m_database){
            snapShot += ele.first + " " + ele.second + ".";
        }
    }
    snapShot += ";";
    if(m_clientSeqMap.empty()) snapShot += "empty";
    else{
        for(const auto& ele : m_clientSeqMap){
            snapShot += to_string(ele.first) + " " + to_string(ele.second) + ".";
        }
    }
    snapShot += ";";
    if(comeInShards.empty()) snapShot += "empty";
    else{
        for(const auto& shard : comeInShards){
            snapShot += to_string(shard.first) + " " + to_string(shard.second) + ".";
        }
    }
    snapShot += ";";
    if(toOutShards.empty()) snapShot += "empty";
    else{
        for(const auto& cfg2shardDB : toOutShards){
            snapShot += to_string(cfg2shardDB.first) + ":";
            for(const auto& shardDB : cfg2shardDB.second){
                snapShot += to_string(shardDB.first) + ",";
                for(const auto& data : shardDB.second){
                    snapShot += data.first + " " + data.second + ".";
                }
                snapShot += "/";
            }
            snapShot += "|";
        }
    }
    snapShot += ";";
    if(m_AvailableShards.empty()) snapShot += "empty";
    else{
        for(const auto& shard : m_AvailableShards){
            snapShot += to_string(shard) + ".";
        }
    }
    snapShot += ";";
    if(garbages.empty()) snapShot += "empty";
    else{
        for(const auto& garbage : garbages){
            snapShot += to_string(garbage.first) + ":";
            for(const auto& shard : garbage.second){
                snapShot += to_string(shard) + ".";
            }
            snapShot += "|";
        }
    }
    snapShot += ";";
    snapShot += getStringFromConfig(this->m_config) + ";";
    // cout<<"int cout snapShot is "<<snapShot<<endl;
    // printf("in kvserver -----------------snapShot is %s\n", snapShot.c_str());
    return snapShot;
}

void ShardKv::recoverySnapShot(string snapShot){
    printf("recovery is called\n");
    vector<string> str = splitStr(snapShot, ';');
    string tmp = "";
    if(str[0] == "empty") m_database.clear();
    else{
        vector<string> kvData = splitStr(str[0], '.');
        for(int i = 0; i < kvData.size(); i++){
            tmp = "";
            int j = 0;
            for(; j < kvData[i].size(); j++){
                if(kvData[i][j] != ' '){
                    tmp += kvData[i][j];
                }else break;
            }
            string value(kvData[i].begin() + j + 1, kvData[i].end());
            m_database[tmp] = value;
        }
    }

    tmp = "";
    if(str[1] == "empty") m_clientSeqMap.clear();
    else{
        vector<string> clientSeq = splitStr(str[1], '.');
        for(int i = 0; i < clientSeq.size(); i++){
            tmp = "";
            int j = 0;
            for(; j < clientSeq[i].size(); j++){
                if(clientSeq[i][j] != ' '){
                    tmp += clientSeq[i][j];
                }else break;
            }
            string value(clientSeq[i].begin() + j + 1, clientSeq[i].end());
            m_clientSeqMap[stoi(tmp)] = stoi(value);
        }
    }

    tmp = "";
    if(str[2] == "empty") comeInShards.clear();
    else{
        vector<string> inShards = splitStr(str[2], '.');
        for(int i = 0; i < inShards.size(); i++){
            tmp = "";
            int j = 0;
            for(; j < inShards[i].size(); j++){
                if(inShards[i][j] != ' '){
                    tmp += inShards[i][j];
                }else break;
            }
            string value(inShards[i].begin() + j + 1, inShards[i].end());
            comeInShards[stoi(tmp)] = stoi(value);
        }
    }
    tmp = "";
    if(str[3] == "empty") toOutShards.clear();
    else{
        vector<string> cfg2Shards = splitStr(str[3], '|');
        for(int i = 0; i < cfg2Shards.size(); i++){
            vector<string> cfgAndData = splitStr(cfg2Shards[i], ':');
            int cfgNum = stoi(cfgAndData[0]);
            for(int j = 1; j < cfgAndData.size(); j++){
                vector<string> shardData = splitStr(cfgAndData[j], '/');
                for(int k = 0; k < shardData.size(); k++){
                    vector<string> data = splitStr(shardData[k], ',');
                    int shard = stoi(data[0]);
                    vector<string> key2Value = splitStr(data[1], '.');
                    for(int ii = 0; ii < key2Value.size(); ii++){
                        tmp = "";
                        int jj = 0;
                        for(; jj < key2Value[ii].size(); jj++){
                            if(key2Value[ii][jj] != ' '){
                                tmp += key2Value[ii][jj];
                            }else break;
                        }
                        string value(key2Value[ii].begin() + jj + 1, key2Value[ii].end());
                        toOutShards[cfgNum][shard][tmp] = value;
                    }
                }
            }
        }
    }

    tmp = "";
    if(str[4] == "empty") m_AvailableShards.clear();
    else{
        vector<string> shards = splitStr(str[4], '.');
        for(int i = 0; i < shards.size(); i++){
            m_AvailableShards.insert(stoi(shards[i]));
        }
    }

    tmp = "";
    if(str[5] == "empty") garbages.clear();
    else{
        vector<string> cfg2garbage = splitStr(str[5], '|');
        for(int i = 0; i < cfg2garbage.size(); i++){
            tmp = "";
            int j = 0;
            for(; j < cfg2garbage[i].size(); j++){
                if(cfg2garbage[i][j] != ':'){
                    tmp += cfg2garbage[i][j];
                }else break;
            }
            string shard(cfg2garbage[i].begin() + j + 1, cfg2garbage[i].end());
            int cfgNum = stoi(tmp);
            vector<string> shards = splitStr(shard, '.');
            for(int k = 0; k < shards.size(); k++){
                garbages[cfgNum].insert(stoi(shards[k]));
            }
        }
    }

    this->m_config = getConfig(str[6]);
}

void ShardKv::printSnapShot(){
    printf("-----------------databegin---------------------------\n");
    for(auto a : m_database){
        printf("data-> key is %s, value is %s\n", a.first.c_str(), a.second.c_str());
    }
    printf("-----------------requSeqbegin-------------------------\n");
    for(auto a : m_clientSeqMap){
        printf("data-> key is %d, value is %d\n", a.first, a.second);
    }
    printf("-----------------comeInshards-------------------------\n");
    for(auto a : comeInShards){
        printf("shard : %d, configNum is %d\n", a.first, a.second);
    }
    printf("---------------toOutShards----------------------------\n");
    for(auto a : toOutShards){
        for(auto b : a.second){
            for(auto c : b.second){
                printf("in config%d, the shard%d's data -> key is %s, value is %s\n",a.first, b.first, c.first.c_str(), c.second.c_str());
            }
        }
    }
    printf("------------------availableShards---------------------\n");
    printf("available shard is :");
    for(auto a : m_AvailableShards){
        printf("%d ", a);
    }
    printf("\n");
    printf("----------------------barbages------------------------\n");
    for(auto a : garbages){
        for(auto b : a.second){
            printf("in config%d, the shard%d need gc\n", a.first, b);
        }
    }
    printf("-----------------------config--------------------------\n");
    printConfig(this->m_config);
}

void* ShardKv::snapShotLoop(void* arg){   //定时进行快照的loop
    ShardKv* kv = (ShardKv*)arg;
    while(1){
        string snapShot = "";
        int lastIncluedIndex;
        // printf("%d not in loop -> kv->m_lastAppliedIndex : %d\n", kv->m_id, kv->m_lastAppliedIndex);
        if(kv->m_maxraftstate != -1 && kv->m_raft.ExceedLogSize(kv->m_maxraftstate)){ //判断大小
            kv->m_lock.lock();
            snapShot = kv->getSnapShot();  //将快照保存为string
            lastIncluedIndex = kv->m_lastAppliedIndex;
            // printf("%d in loop -> kv->m_lastAppliedIndex : %d\n", kv->m_id, kv->m_lastAppliedIndex);
            kv->m_lock.unlock();
        }
        if(snapShot.size() != 0){
            kv->m_raft.recvSnapShot(snapShot, lastIncluedIndex); //进行持久化
            printf("%d called recvsnapShot size is %d, lastapply is %d\n", kv->m_id, snapShot.size(), kv->m_lastAppliedIndex);
        }
        usleep(10000);
    }
}

vector<vector<kvServerInfo>> getShardKvServerPort(int groupsNum){
    vector<vector<kvServerInfo>> peers(groupsNum, vector<kvServerInfo>(EVERY_SERVER_RAFT));
    for(int idx = 0; idx < groupsNum; idx++){
        for(int i = 0; i < EVERY_SERVER_RAFT; i++){
            peers[idx][i].peersInfo.m_peerId = i;
            peers[idx][i].peersInfo.m_port.first = COMMOM_PORT + i + idx * 100;  //投票服务端口
            peers[idx][i].peersInfo.m_port.second = COMMOM_PORT + i + EVERY_SERVER_RAFT + idx * 100; //同步服务端口  
            peers[idx][i].peersInfo.isInstallFlag = false;
            peers[idx][i].m_kvPort = (COMMOM_PORT + i + 2 * EVERY_SERVER_RAFT  + idx * 100);//get等服务的端口
        }
    }
    return peers;
}

bool ShardKv::getRaftState(){
    return m_raft.getState().second;
}

void ShardKv::killRaft(){
    m_raft.kill();
}

void ShardKv::activateRaft(){
    m_raft.activate();
}

int main(){
    srand((unsigned)time(NULL));
    initStr2PortMap(5); //初始化5*5端口
    vector<vector<kvServerInfo>> servers = getShardKvServerPort(5);//初始化5个group，每个5个服务器
    ShardKv** kv = new ShardKv*[servers.size()]; //初始化5个kv组
    vector<vector<int>> masters = getMastersPort(EVERY_SERVER_RAFT); //得到shardMaster的前半部分端口信息

    for(int i = 0; i < servers.size(); i++){
        kv[i] = new ShardKv[EVERY_SERVER_RAFT]; //每个kv组初始化5个shardkv
        // printf("shardKv%d begin print :\n", i);
        for(int j = 0; j < EVERY_SERVER_RAFT; j++){
            // printf("%d %d %d\n", servers[i][j].peersInfo.m_port.first, servers[i][j].peersInfo.m_port.second, 
            //     servers[i][j].m_kvPort);
                kv[i][j].StartKvServer(servers[i], j, i + 1, 4096, masters);
        }
        // printf("shardKv%d end print :\n", i);
    }

    while(1);
}