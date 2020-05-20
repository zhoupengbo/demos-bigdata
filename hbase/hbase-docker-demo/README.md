#### 1. Docker 安装

参考：[安装Docker](https://yeasy.gitbook.io/docker_practice/install)

支持常用的操作系统：Centos / ubuntu / Windows / macos 等。

#### 2. 拉取 HBase Docker 镜像

镜像地址：https://hub.docker.com/r/harisekhon/hbase/tags

这里推荐使用harisekho,star数最多，大家也可以在镜像库中搜索其他镜像。

拉取最新版本：docker pull harisekhon/hbase:latest

或指定版本：docker pull harisekhon/hbase:1.4

#### 3. 运行容器

Docker安装成功后，执行以下命令。
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

配置服务器hosts文件，添加如下： 
```
127.0.0.1 docker-hbase 
```

配置本地hosts文件，添加如下：
```aidl
服务器ip docker-hbase
``` 

访问HBase WebUI:
```aidl
http://docker-hbase:16010/master-status
```

#### 4. HBase实操

查看正在运行的容器：
```aidl
docker ps
```

找到容器id，进入容器：

```aidl
docker exec -it <container ID前缀> bash
```

访问HBase Shell，直接输入
```aidl
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

可以玩了！


