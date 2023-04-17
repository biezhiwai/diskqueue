#include <iostream>
#include <string>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <functional>
#include <boost/filesystem.hpp>
#include <boost/format.hpp>
#include <sw/redis++/redis++.h>

void Log(const std::string &str) {
    std::cout << str << std::endl;
}

class diskQueue {
private:
    int64_t readPos = 0;
    int64_t writePos = 0;
    int64_t readFileNum = 0;
    int64_t writeFileNum = 0;
    std::atomic<int64_t> depth{0};

    std::string name;
    boost::filesystem::path dataPath;
    int64_t maxBytesPerFile;
    int64_t maxBytesPerFileRead;
    int32_t minMsgSize;
    int32_t maxMsgSize;
    bool needSync;

    int64_t nextReadPos = 0;
    int64_t nextReadFileNum = 0;

    int readFile = -1;
    int writeFile = -1;

public:
    // 构造函数
    diskQueue(const std::string &name, const std::string &dataPath, int64_t maxBytesPerFile, int32_t minMsgSize,
              int32_t maxMsgSize) : name(name), dataPath(dataPath), maxBytesPerFile(maxBytesPerFile),
                                    minMsgSize(minMsgSize), maxMsgSize(maxMsgSize) {
        ::mkdir(dataPath.c_str(), 0777);
        if (!retrieveMetaData()) {
            Log("retrieveMetaData error");
        }
    }

    // 返回元数据文件名
    std::string metaDataFileName() {
        return (dataPath / (boost::format("%s.diskqueue.meta.dat") % name).str()).string();
    }

    // 返回数据文件名
    std::string fileName(int64_t fileNum) {
        return (dataPath / (boost::format("%s.diskqueue.%06d.dat") % name % fileNum).str()).string();
    }

    // 故障恢复文件队列的元数据
    bool retrieveMetaData() {
        auto fileName = metaDataFileName();
        auto fp = fopen(fileName.c_str(), "r");
        if (fp == nullptr) {
            Log("open file error");
            return false;
        }
        int64_t depthTmp;
        if (5 > fscanf(fp, "%ld\n%ld,%ld\n%ld,%ld\n", &depthTmp, &readFileNum, &readPos, &writeFileNum, &writePos)) {
            Log("fscanf error");
            return false;
        }
        depth.store(depthTmp);
        nextReadFileNum = readFileNum;
        nextReadPos = readPos;

        fileName = this->fileName(writeFileNum);
        struct stat fileInfo;
        if (-1 == ::stat(fileName.c_str(), &fileInfo)) return false;
        auto fileSize = fileInfo.st_size;

        if (writePos < fileSize) {
            Log("metadata writePos < file size of , skipping to new file");
            writeFileNum += 1;
            writePos = 0;
            if (writeFile != -1) {
                ::close(writeFile);
                writeFile = -1;
            }
        }
        fclose(fp);
        return true;
    }

    // 返回队列元素个数
    int64_t Depth() {
        return depth.load();
    }

    // 持久化元数据
    bool persistMetaData() {
        auto fileName = metaDataFileName();
        auto tmpFileName = (boost::format("%s.%d.tmp") % fileName % random()).str();

        auto fp = fopen(tmpFileName.c_str(), "w");
        if (fp == nullptr) {
            Log("open file error");
            return false;
        }

        if (5 > fprintf(fp, "%ld\n%ld,%ld\n%ld,%ld\n", depth.load(), readFileNum, readPos, writeFileNum, writePos)) {
            Log("fprintf error");
            fclose(fp);
            return false;
        }
        fflush(fp);
        fclose(fp);

        return ::rename(tmpFileName.c_str(), fileName.c_str()) == 0;
    }

    // 同步刷盘
    bool sync() {
        if (writeFile != -1) {
            if (::fsync(writeFile) == -1) {
                Log("sync fail");
                ::close(writeFile);
                writeFile = -1;
                return false;
            }
        }
        if (!persistMetaData()) {
            Log("sync fail");
            return false;
        }
        needSync = false;

        return true;
    }

    // 向队列中写入一条消息
    bool writeOne(const void *data, int32_t dataLen) {
        int64_t totalBytes = dataLen + 4;

        if (dataLen < minMsgSize || dataLen > maxMsgSize) {
            Log("invalid message len");
            return false;
        }

        if (writePos > 0 && writePos + totalBytes > maxBytesPerFile) {
            if (readFileNum == writeFileNum) {
                maxBytesPerFileRead = writePos;
            }
            writeFileNum++;
            writePos = 0;

            if (!sync()) Log("failed to sync");
            if (writeFile != -1) {
                ::close(writeFile);
                writeFile = -1;
            }
        }

        if (writeFile == -1) {
            std::string curFileName = fileName(writeFileNum);
            writeFile = ::open(curFileName.c_str(), O_CREAT | O_RDWR, 0600);
            if (writeFile == -1) {
                Log("write open fail");
                ::close(writeFile);
                return false;
            }
            Log("writeOne() opened ");
            if (writePos > 0) {
                if (-1 == ::lseek(writeFile, writePos, SEEK_SET)) {
                    ::close(writeFile);
                    writeFile = -1;
                    return false;
                }
            }
        }

        if (4 > ::write(writeFile, &dataLen, 4) ||
            dataLen > ::write(writeFile, data, dataLen)) {
            ::close(writeFile);
            writeFile = -1;
            return false;
        }

        writePos += totalBytes;
        depth++;
        return true;
    }

    // 从队列中读出一条消息
    void *readOne(int32_t &msgSize) {
        if (readFile == -1) {
            std::string curFileName = fileName(readFileNum);
            readFile = ::open(curFileName.c_str(), O_RDONLY, 0600);
            if (readFile == -1) {
                Log("read open fail");
                return nullptr;
            }



            maxBytesPerFileRead = maxBytesPerFile;
            if (readFileNum < writeFileNum) {
                struct stat fileStat;
                if (-1 != fstat(readFile, &fileStat)) {
                    maxBytesPerFileRead = fileStat.st_size;
                }
            }
        }
        if (readPos >= 0) {
            if (-1 == ::lseek(readFile, readPos, SEEK_SET)) {
                ::close(readFile);
                readFile = -1;
                return nullptr;
            }
        }

        if (4 > ::read(readFile, &msgSize, 4)) {
            ::close(readFile);
            readFile = -1;
            return nullptr;
        }

        if (msgSize < minMsgSize || msgSize > maxMsgSize) {
            ::close(readFile);
            readFile = -1;
            Log("invalid read msgSize");
            return nullptr;
        }

        auto readBuf = new char[msgSize];
        if (msgSize > ::read(readFile, readBuf, msgSize)) {
            ::close(readFile);
            readFile = -1;
            return nullptr;
        }

        int64_t totalBytes = msgSize + 4;
        nextReadPos = readPos + totalBytes;
        nextReadFileNum = readFileNum;

        if (readFileNum < writeFileNum && nextReadPos >= maxBytesPerFileRead) {
            if (readFile != -1) {
                ::close(readFile);
                readFile = -1;
            }
            nextReadFileNum++;
            nextReadPos = 0;
        }

        return readBuf;
    }

    // 出队操作
    void moveForward() {
        auto oldReadFileNum = readFileNum;
        readFileNum = nextReadFileNum;
        readPos = nextReadPos;
        depth--;

        if (oldReadFileNum != nextReadFileNum) {
            needSync = true;

            auto fn = fileName(oldReadFileNum);
            if (-1 == ::remove(fn.c_str())) {
                Log("remove fail");
            }
        }
        checkTailCorruption();
    }

    // 检查队尾异常情况
    void checkTailCorruption() {
        if (readFileNum < writeFileNum || readPos < writePos) {
            return;
        }
        if (depth != 0) {
            if (depth < 0) {
                Log("negative depth, resetting 0");
            } else if (depth > 0) {
                Log("positive depth, resetting 0");
            }
            depth = 0;
            needSync = true;
        }

        if (readFileNum > writeFileNum) {
            Log("readFileNum > writeFileNum,skipping to next writeFileNum and resetting 0");
        }
        if (readPos > writePos) {
            Log("readPos > writePos, corruption, skipping to next writeFileNum and resetting 0");
        }
        skipToNextRWFile();
        needSync = true;
    }

    // 将队头和队尾重置到队尾的下一消息的位置
    bool skipToNextRWFile() {
        if (readFile != -1) {
            ::close(readFile);
            readFile = -1;
        }

        if (writeFile != -1) {
            ::close(writeFile);
            writeFile = -1;
        }

        bool flag = true;

        for (int64_t i = readFileNum; i <= writeFileNum; i++) {
            auto fn = fileName(i);
            if (-1 == ::remove(fn.c_str())) {
                Log("remove fail");
                flag = false;
            }
        }
        writeFileNum=0;
        writePos = 0;
        readFileNum = writeFileNum;
        readPos = 0;
        nextReadFileNum = writeFileNum;
        nextReadPos = 0;
        depth = 0;
        return flag;
    }

    // 处理读取错误
    void handleReadError() {
        if (readFileNum == writeFileNum) {
            if (writeFile != -1) {
                ::close(writeFile);
                writeFile = -1;
            }
            writeFileNum++;
            writePos = 0;
        }

        auto badFn = fileName(readFileNum);
        auto badRenameFn = badFn + ".bad";

        Log("jump to next file and saving bad file");
        if (-1 == ::rename(badFn.c_str(), badRenameFn.c_str())) {
            Log("failed to rename bad diskqueue file");
        }
        readFileNum++;
        readPos = 0;
        nextReadFileNum = readFileNum;
        nextReadPos = 0;
        needSync = true;
        checkTailCorruption();
    }

    bool exit() {
        if (readFile != -1) {
            ::close(readFile);
            readFile = -1;
        }

        if (writeFile != -1) {
            ::close(writeFile);
            writeFile = -1;
        }
        return true;
    }
};

class DiskMQ {
private:
    diskQueue *q;

    std::string name;
    boost::filesystem::path dataPath;

    std::mutex qmt;                  // 队列锁
    std::thread consumer_thread;     // 消费者线程
    std::atomic<bool> stop{false}; // 线程终止标志位

    //    std::condition_variable qcv;
public:
    sw::redis::RedisCluster* redis = nullptr;
    DiskMQ(const std::string &name,const std::string &dataPath):name(name),dataPath(dataPath){
        q = new diskQueue(name, dataPath, 300, 0, 100);
        redis = new sw::redis::RedisCluster(
                "tcp://123@127.0.0.1:6381?keep_alive=true&connect_timeout=2000ms&socket_timeout=2000ms");
        consumer_thread=std::thread([this]{
            while(!stop){
                try{
                    sleep(1);
                    q->sync();
                    redis->set("test","ok");
                    while(q->Depth()){
                        void* readBuf;
                        {
                            std::unique_lock<std::mutex>lock(qmt);
                            int32_t len;
                            readBuf = q->readOne(len);
                            if(!readBuf){
                                q->handleReadError();
                                break;
                            }
                        }
                        // 反序列化
                        auto keyLen = (int32_t *)readBuf;
                        std::string key((char*)readBuf+4,*keyLen);
                        auto dataLen = (int32_t *)((char*)readBuf+4+*keyLen);
                        std::string data((char*)readBuf+4+*keyLen+4,*dataLen);
                        auto seq = (uint64_t*)((char*)readBuf+4+*keyLen+4+*dataLen);
                        redis->zadd(key,data,*seq);
                        {
                            std::unique_lock<std::mutex>lock(qmt);
                            q->moveForward();
                        }
                    }
                }catch(const sw::redis::Error &e){
                    printf("[Error] %s\n",e.what());
                }
            }
        });
    }

    ~DiskMQ(){
        while(q->Depth()) usleep(10000);
        stop.store(true);
        consumer_thread.join();
    }
    // 序列化
    void* serialize(const std::string &key,const void* data,int32_t dataLen,uint64_t seq,int32_t &len){
        int32_t keyLen = key.size();
        len = 4+keyLen+4+dataLen+8;
        auto serializeBuf = new char[len];
        if(nullptr==::memcpy(serializeBuf,&keyLen,4)) return nullptr;
        if(nullptr==::memcpy(serializeBuf + 4,key.c_str(),keyLen)) return nullptr;
        if(nullptr==::memcpy(serializeBuf + 4 + keyLen,&dataLen,4)) return nullptr;
        if(nullptr==::memcpy(serializeBuf + 4 + keyLen + 4,data,dataLen)) return nullptr;
        if(nullptr==::memcpy(serializeBuf + 4 + keyLen + 4 + dataLen,&seq,8)) return nullptr;
        return serializeBuf;
    }
    // 生产者，用于将存储失败的数据（由key，data，seq组成）存入文件队列
    void Produce(const std::string &key,const std::string &data,uint64_t seq){
        int32_t len;
        auto serializeBuf= serialize(key,data.c_str(),data.size(),seq,len);
        std::unique_lock<std::mutex>lock(qmt);
        q->writeOne(serializeBuf,len);
    }
};


int main() {
    DiskMQ dq("Order","/home/cxk/CLionProjects/diskqueue/Order/");
    int n=dq.redis->zcard("aaa");
    for (int i = n; i < n+100000; i++) {
        dq.Produce("aaa",std::to_string(i),i);
    }

//    for (int i = 0; i < 1; i++) {
//        int32_t len;
//        auto str = (char *) d.readOne(len);
//        std::cout << str << std::endl;
//        d.moveForward();
//        d.sync();
//        if(d.Depth()==0) break;
//    }
}