#include "./shardMaster.hpp"

void* shardCrtlerLoop(void* arg){
    vector<kvServerInfo> servers = getKvServerPort(EVERY_SERVER_RAFT);  //找到master对应的kv信息
    srand((unsigned)time(NULL));
    ShardMaster* master = new ShardMaster[servers.size()];

    for(int i = 0; i < 5; i++){
        master[i].StartShardMaster(servers, i, 1024);
    }
}

int main(){    //开启master
    pthread_t tid;
    pthread_create(&tid, NULL, shardCrtlerLoop, NULL);
    pthread_detach(tid);
    while(1);
}
