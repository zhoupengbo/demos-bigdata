**前言：**本文主要讲述了如何使用Docker快速上手HBase，省去繁杂的安装部署环境，直接上手，小白必备。适合HBase入门学习及简单代码测试。

## 1. Docker 安装

参考地址：
```aidl
https://yeasy.gitbook.io/docker_practice/install
```

支持常用的操作系统：Centos / ubuntu / Windows / macos 等。

## 2. 拉取镜像

镜像地址：
```aidl
https://hub.docker.com/r/harisekhon/hbase/tags
```

这里推荐使用`harisekho`,支持多个版本(最新支持`HBase2.1.3`)，star数也最多，大家也可以在镜像库中搜索其他镜像。

**以下前提：第一步Docker安装完成。**

### 拉取最新版本：
```
docker pull harisekhon/hbase:latest
```

### 拉取指定版本：
```
docker pull harisekhon/hbase:1.4
```

## 3. 运行容器

Docker安装成功后，直接运行以下命令。
```
docker run -d -h docker-hbase \
        -p 2181:2181 \
        -p 8080:8080 \
        -p 8085:8085 \
        -p 9090:9090 \
        -p 9000:9000 \
        -p 9095:9095 \
        -p 16000:16000 \
        -p 16010:16010 \
        -p 16201:16201 \
        -p 16301:16301 \
        -p 16020:16020\
        --name hbase \
        harisekhon/hbase
```

配置服务器hosts文件，添加如下配置： 
```
127.0.0.1 docker-hbase 
```

配置本地hosts文件，添加如下配置：
```aidl
服务器ip docker-hbase
``` 

## 4. HBase实操

### 4.1 访问HBase WebUI:
```aidl
http://docker-hbase:16010/master-status
```

### 4.2 访问HBase Shell

查看正在运行的容器：
```aidl
docker ps
```

找到容器id，进入容器：

```aidl
docker exec -it <container ID前缀> bash
```

访问HBase Shell，进入容器后直接输入：
```
bash-4.4# hbase shell
2020-05-20 03:59:26,228 WARN  [main] util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
HBase Shell
Use "help" to get list of supported commands.
Use "exit" to quit this interactive shell.
For Reference, please visit: http://hbase.apache.org/2.0/book.html#shell
Version 2.1.3, rda5ec9e4c06c537213883cca8f3cc9a7c19daf67, Mon Feb 11 15:45:33 CST 2019
Took 0.0049 seconds
hbase(main):001:0> 
```
Shell 测试：
```
hbase(main):001:0> create 'test-docker','f'
Created table test-docker
Took 1.4964 seconds
=> Hbase::Table - test-docker
hbase(main):002:0> list
TABLE
test-docker
3 row(s)
Took 0.0281 seconds
=> ["test-docker"]
hbase(main):003:0>
```
### 4.3 访问Zookeeper
输入`exit`退出容器。并执行以下命令进入zk：
```aidl
bash-4.4# hbase zkcli
```
查看zk节点：
```
ls /
[zookeeper, hbase]
```

### 4.4 Java Api 测试

```
public class HBaseHelper {

    public static String ZK_QUORUM = "docker-hbase";
    public static String ZK_ZNODE = "/hbase";
    public static String ZK_PORT = "2181";
    public static String SUPER_USER = "hbase";

    // 配置连接信息
    public Configuration getConfiguration() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", ZK_QUORUM);
        conf.set("zookeeper.znode.parent", ZK_ZNODE);
        conf.set("hbase.zookeeper.property.clientPort", ZK_PORT);
        return conf;
    }
...
...
...
}
```
想查看完整Demo代码可以访问Github地址：
```aidl
https://github.com/zhoupengbo/demos-bigdata/tree/master/hbase/hbase-docker-demo
```

