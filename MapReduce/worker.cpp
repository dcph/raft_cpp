#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <sys/types.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
#include <string.h>
#include <signal.h>
#include <sys/time.h>
#include "locker.h"
#include "./buttonrpc-master/buttonrpc.hpp"
#include <bits/stdc++.h>
#include <pthread.h>
#include <dlfcn.h>
using namespace std;

#define LIB_CACULATE_PATH "./libmrFunc.so"  //用于加载的动态库的路径
#define MAX_REDUCE_NUM 15
//可能造成的bug，考虑write多次写，每次写1024用while读进buf
//c_str()返回的是一个临时指针，值传递没事，但若涉及地址出错

int disabledMapId = 0;   //用于人为让特定map任务超时的Id
int disabledReduceId = 0;   //用于人为让特定reduce任务超时的Id

//定义master分配给自己的map和reduce任务数，实际无所谓随意创建几个，我是为了更方便测试代码是否ok
int map_task_num;
int reduce_task_num;

//定义实际处理map任务的数组，存放map任务号
//(map任务大于总文件数时，多线程分配ID不一定分配到正好增序的任务号，如10个map任务，总共8个文件，可能就是0,1,2,4,5,7,8,9)

class KeyValue{
public:
    string key;
    string value;
};

//定义的两个函数指针用于动态加载动态库里的map和reduce函数
typedef vector<KeyValue> (*MapFunc)(KeyValue kv);
typedef vector<string> (*ReduceFunc)(vector<KeyValue> kvs, int reduceTaskIdx);
MapFunc mapF;
ReduceFunc reduceF;

//条件变量，一种线程同步机制，和互斥量一起使用时可以无竞争的使用
//条件变量可以看成时一个类
//条件变量被用来自动的阻塞一个线程，直到条件满足触发，因为他是提前被互斥量锁住的，知道触发了才解除
//是为了第一时间得到条件满足的通知，一种触发类，触发后执行线程，并且这一执行的过程也有锁
//给每个map线程分配的任务ID，用于写中间文件时的命名
int MapId = 0;            
pthread_mutex_t map_mutex;//互斥锁
pthread_cond_t cond;//表示多线程的条件变量，用于控制线程等待和就绪的api，可以看成是一个类
int fileId = 0;

//对每个字符串求hash找到其对应要分配的reduce线程
int ihash(string str){
    int sum = 0;
    for(int i = 0; i < str.size(); i++){
        sum += (str[i] - '0');
    }
    return sum % reduce_task_num;
}

//删除所有写入中间值的临时文件
void removeFiles(){
    string path;
    for(int i = 0; i < map_task_num; i++){
        for(int j = 0; j < reduce_task_num; j++){
            path = "mr-" + to_string(i) + "-" + to_string(j);
            int ret = access(path.c_str(), F_OK);//linux下函数，用来判断文件是否存在与文件状态
            if(ret == 0) remove(path.c_str());//用于删除文件//c_str是string 与 const char* 
        }
    }
}

//取得  key:filename, value:content 的kv对作为map任务的输入
KeyValue getContent(char* file){
    int fd = open(file, O_RDONLY);//以只读的方式打开文件
    int length = lseek(fd, 0, SEEK_END);//记录文件内容总偏移量
    lseek(fd, 0, SEEK_SET);//重置文件偏移量
    char buf[length];
    bzero(buf, length);//清零
    int len = read(fd, buf, length);//将文件中内容读出
    if(len != length){
        perror("read");
        exit(-1);//程序异常退出
    }
    KeyValue kv;
    kv.key = string(file);
    kv.value = string(buf);
    close(fd);
    return kv;
}

//将map任务产生的中间值写入临时文件
void writeKV(int fd, const KeyValue& kv){
    string tmp = kv.key + ",1 ";
    int len = write(fd, tmp.c_str(), tmp.size());
    if(len == -1){
        perror("write");
        exit(-1);
    }
    close(fd);
}

//创建每个map任务对应的不同reduce号的中间文件并调用 -> writeKV 写入磁盘
void writeInDisk(const vector<KeyValue>& kvs, int mapTaskIdx){
    for(const auto& v : kvs){
        int reduce_idx = ihash(v.key);
        string path;
        path = "mr-" + to_string(mapTaskIdx) + "-" + to_string(reduce_idx);
        int ret = access(path.c_str(), F_OK);
        if(ret == -1){
            int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0664);//返回个文件描述符//0664分别代表文件类型
            //文件所有者权限，文件所属组权限，其他人权限，r4可读，w2可写，x1可执行
            writeKV(fd, v);
        }else if(ret == 0){
            int fd = open(path.c_str(), O_WRONLY | O_APPEND);
            writeKV(fd, v);
        }   
    }
}

//以char类型的op为分割拆分字符串
vector<string> split(string text, char op){
    int n = text.size();
    vector<string> str;
    string tmp = "";
    for(int i = 0; i < n; i++){
        if(text[i] != op){
            tmp += text[i];
        }else{
            if(tmp.size() != 0) str.push_back(tmp);
            tmp = "";
        }
    }
    return str;
}

//以逗号为分割拆分字符串
string split(string text){
    string tmp = "";
    for(int i = 0; i < text.size(); i++){
        if(text[i] != ','){
            tmp += text[i];
        }else break;
    }
    return tmp;
}

//获取对应reduce编号的所有中间文件
// 想要获取某目录下（比如a目下）b文件的详细信息
// 首先，使用opendir函数打开目录a，返回指向目录a的DIR结构体c。
// 接着，调用readdir( c)函数读取目录a下所有文件（包括目录），返回指向目录a下所有文件的dirent结构体d。
// 然后，遍历d，调用stat（d->name,stat *e）来获取每个文件的详细信息，存储在stat结构体e中。
vector<string> getAllfile(string path, int op){
    DIR *dir = opendir(path.c_str());//打开目录文件，相当于是ls//是一个目录结构体
    vector<string> ret;
    if (dir == NULL)
    {
        printf("[ERROR] %s is not a directory or not exist!", path.c_str());
        return ret;
    }
    struct dirent* entry; //指向目录中具体文件的结构体
    while ((entry=readdir(dir)) != NULL) //readdir读取到的文件名
    {
        int len = strlen(entry->d_name);
        int oplen = to_string(op).size();
        if(len - oplen < 5) continue;
        string filename(entry->d_name);
        if(!(filename[0] == 'm' && filename[1] == 'r' && filename[len - oplen - 1] == '-')) continue;
        string cmp_str = filename.substr(len - oplen, oplen); //返回最后oplen个string
        if(cmp_str == to_string(op)){
            ret.push_back(entry->d_name);
        }
    }
    closedir(dir);
    return ret;
}

//对于一个ReduceTask，获取所有相关文件并将value的list以string写入vector
//vector中每个元素的形式为"abc 11111";
vector<KeyValue> Myshuffle(int reduceTaskNum){
    string path;
    vector<string> str;
    str.clear();
    vector<string> filename = getAllfile(".", reduceTaskNum);
    unordered_map<string, string> hash;
    for(int i = 0; i < filename.size(); i++){
        path = filename[i];
        char text[path.size() + 1];
        strcpy(text, path.c_str());
        KeyValue kv = getContent(text);
        string context = kv.value;
        vector<string> retStr = split(context, ' ');
        str.insert(str.end(), retStr.begin(), retStr.end());
    }
    for(const auto& a : str){
        hash[split(a)] += "1";
    }
    vector<KeyValue> retKvs;
    KeyValue tmpKv;
    for(const auto& a : hash){
        tmpKv.key = a.first;
        tmpKv.value = a.second;
        retKvs.push_back(tmpKv);
    }
    sort(retKvs.begin(), retKvs.end(), [](KeyValue& kv1, KeyValue& kv2){ //这里使用了lamda函数，lambda表达式是一个可调用的对象
    //可以看成是一个未命名的内联函数，[捕获列表] （参数列表）-> 返回类型 ｛函数体｝，其中捕获列表和函数体是必须包括的
        return kv1.key < kv2.key;
    });//第三个是个比较仿函数
    return retKvs;
}


void* mapWorker(void* arg){

//1、初始化client连接用于后续RPC;获取自己唯一的MapTaskID
    buttonrpc client; 
    client.as_client("127.0.0.1", 5555);//ip和接口
    pthread_mutex_lock(&map_mutex);//任务id会被多个线程同时调用，因此要加锁和解锁
    int mapTaskIdx = MapId++; //任务id
    pthread_mutex_unlock(&map_mutex);
    bool ret = false;

    while(1){
    //2、通过RPC从Master获取任务
    //client.set_timeout(10000);
        ret = client.call<bool>("isMapDone").val();//client的传递信息
        if(ret){
            pthread_cond_broadcast(&cond);//将所有等待该条件变量的线程解锁
            return NULL;
        }
        string taskTmp = client.call<string>("assignTask").val();   //通过RPC返回值取得任务，在map中即为文件名
        if(taskTmp == "empty") continue; 
        printf("%d get the task : %s\n", mapTaskIdx, taskTmp.c_str());
        pthread_mutex_lock(&map_mutex);

        //------------------------自己写的测试超时重转发的部分---------------------
        //注：需要对应master所规定的map数量，因为是1，3，5被置为disabled，相当于第2，4，6个拿到任务的线程宕机
        //若只分配两个map的worker，即0工作，1宕机，我设的超时时间比较长且是一个任务拿完在拿一个任务，所有1的任务超时后都会给到0，
        if(disabledMapId == 1 || disabledMapId == 3 || disabledMapId == 5){
            disabledMapId++;
            pthread_mutex_unlock(&map_mutex);
            printf("%d recv task : %s  is stop\n", mapTaskIdx, taskTmp.c_str());
            while(1){
                sleep(2);
            }
        }else{
            disabledMapId++;   
        }
        pthread_mutex_unlock(&map_mutex);
        //------------------------自己写的测试超时重转发的部分---------------------

    //3、拆分任务，任务返回为文件path及map任务编号，将filename及content封装到kv的key及value中
        char task[taskTmp.size() + 1];
        strcpy(task, taskTmp.c_str());
        KeyValue kv = getContent(task);

    //4、执行map函数，然后将中间值写入本地
        vector<KeyValue> kvs = mapF(kv);
        writeInDisk(kvs, mapTaskIdx);

    //5、发送RPC给master告知任务已完成
        printf("%d finish the task : %s\n", mapTaskIdx, taskTmp.c_str());
        client.call<void>("setMapStat", taskTmp); //发送信息

    }
} 

//用于最后写入磁盘的函数，输出最终结果
void myWrite(int fd, vector<string>& str){
    int len = 0;
    char buf[2];
    sprintf(buf,"\n"); //将格式化的字符串输入到目的字符串
    for(auto s : str){
        len = write(fd, s.c_str(), s.size());
        write(fd, buf, strlen(buf));    //write和read，读写二进制文件 //将buffer中的有效内容拷贝到文件中
        if(len == -1){
            perror("write");
            exit(-1);
        }
    }
}

void* reduceWorker(void* arg){
    //removeFiles();
    buttonrpc client;
    client.as_client("127.0.0.1", 5555);//本地的ip地址
    bool ret = false;
    while(1){
        //若工作完成直接退出reduce的worker线程
        ret = client.call<bool>("Done").val();
        if(ret){
            return NULL;
        }
        int reduceTaskIdx = client.call<int>("assignReduceTask").val();
        if(reduceTaskIdx == -1) continue;
        printf("%ld get the task%d\n", pthread_self(), reduceTaskIdx); //用于获得线程自身的ID
        pthread_mutex_lock(&map_mutex);

        //人为设置的crash线程，会导致超时，用于超时功能的测试
        if(disabledReduceId == 1 || disabledReduceId == 3 || disabledReduceId == 5){
            disabledReduceId++;
            pthread_mutex_unlock(&map_mutex);
            printf("recv task%d reduceTaskIdx is stop in %ld\n", reduceTaskIdx, pthread_self());
            while(1){
                sleep(2);
            }
        }else{
            disabledReduceId++;   
        }
        pthread_mutex_unlock(&map_mutex);

        //取得reduce任务，读取对应文件，shuffle后调用reduceFunc进行reduce处理
        vector<KeyValue> kvs = Myshuffle(reduceTaskIdx);
        vector<string> ret = reduceF(kvs, reduceTaskIdx);
        vector<string> str;
        for(int i = 0; i < kvs.size(); i++){
            str.push_back(kvs[i].key + " " + ret[i]);
        }
        string filename = "mr-out-" + to_string(reduceTaskIdx);
        int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0664);
        myWrite(fd, str);
        close(fd);
        printf("%ld finish the task%d\n", pthread_self(), reduceTaskIdx);
        client.call<bool>("setReduceStat", reduceTaskIdx);  //最终文件写入磁盘并发起RPCcall修改reduce状态
    }
}

//删除最终输出文件，用于程序第二次执行时清除上次保存的结果
void removeOutputFiles(){
    string path;
    for(int i = 0; i < MAX_REDUCE_NUM; i++){
        path = "mr-out-" + to_string(i);
        int ret = access(path.c_str(), F_OK);//判断文件是否存在，是为0，否为-1
        if(ret == 0) remove(path.c_str());
    }
}

int main(){
    
    pthread_mutex_init(&map_mutex, NULL);
    pthread_cond_init(&cond, NULL);

    //运行时从动态库中加载map及reduce函数(根据实际需要的功能加载对应的Func)
    void* handle = dlopen("./libmrFunc.so", RTLD_LAZY);  //以指定的模式打开指定的动态链接文件，并返回一个句柄给调用进程
// RTLD_LAZY:在 dlopen 返回前，对于动态库中存在的未定义的变量 (如外部变量 extern，也可以是函数) 不执行解析，就是不解析这个变量的地址。
// RTLD_NOW：与上面不同，他需要在 dlopen 返回前，解析出每个未定义变量的地址，如果解析不出来，在 dlopen 会返回 NULL.
    if (!handle) {
        cerr << "Cannot open library: " << dlerror() << '\n'; //cerr 用于显示错误信息，//当动态链接库操作函数执行失败时，dlerror可以返回出错信息，返回值为NULL时表示操作函数执行成功。
//cout：写到标准输出的ostream对象；经过缓冲之后输出
//cerr：输出到标准错误的ostream对象，常用于程序错误信息；不需要经过缓冲
        exit(-1);
    }
    mapF = (MapFunc)dlsym(handle, "mapF");
// dlsym() 函数根据动态链接库操作句柄 (handle) 与符号 (symbol)，返回符号对应的地址。使用这个函数不但可以获取函数地址，也可以获取变量地址。参数的含义如下:
// handle：由 dlopen 打开动态链接库后返回的指针；
// symbol：要求获取的函数或全局变量的名称。
    if (!mapF) {
        cerr << "Cannot load symbol 'hello': " << dlerror() <<'\n';
        dlclose(handle); //用于关闭指定句柄的动态链接库     mnnn
        dlclose(handle);
        exit(-1);
    }

    //作为RPC请求端
    buttonrpc work_client; //rpc一种远程调用协议，简单地说就是能使应用像调用本地方法一样的调用远程的过程或服务
    work_client.as_client("127.0.0.1", 5555);
    work_client.set_timeout(5000);
    map_task_num = work_client.call<int>("getMapNum").val();
    reduce_task_num = work_client.call<int>("getReduceNum").val();
    removeFiles();          //若有，则清理上次输出的中间文件
    removeOutputFiles();    //清理上次输出的最终文件

    //创建多个map及reduce的worker线程
    pthread_t tidMap[map_task_num]; //表示线程id
    pthread_t tidReduce[reduce_task_num];
    for(int i = 0; i < map_task_num; i++){
        pthread_create(&tidMap[i], NULL, mapWorker, NULL); //创建线程，分别是线程指针，线程属性，需要执行的函数，传给函数的实参，函数的返回值可以由 pthread_join() 接受
        pthread_detach(tidMap[i]); //线程分离，主动与主线程断开关系//和pthread_join不同，终止后直接结束，不会保留终止状态
    }
    pthread_mutex_lock(&map_mutex);
    pthread_cond_wait(&cond, &map_mutex);  //等待,知道isMapDone为真
    pthread_mutex_unlock(&map_mutex);   //这里的目的应该是让至少一个map运行成功再运行reduce吧
    for(int i = 0; i < reduce_task_num; i++){
        pthread_create(&tidReduce[i], NULL, reduceWorker, NULL);
        pthread_detach(tidReduce[i]);
    }
    while(1){
        if(work_client.call<bool>("Done").val()){
            break;
        }
        sleep(1);
    }

    //任务完成后清理中间文件，关闭打开的动态库，释放资源
    removeFiles();
    dlclose(handle);
    pthread_mutex_destroy(&map_mutex);
    pthread_cond_destroy(&cond);
}