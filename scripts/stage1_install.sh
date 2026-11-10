#!/usr/bin/env bash
# Этап 1: Java 17, JAVA_HOME и три копии ZooKeeper 3.9.6 (вывод — outputs/stage1_install.txt).
set -u
cd "$(dirname "$0")/.."
source zookeeper/env.sh
run() { echo "\$ $*"; (eval "$@") 2>&1; echo; }
{
run 'echo $JAVA_HOME'
run 'java -version'
run 'cd .downloads && shasum -a 512 -c apache-zookeeper-3.9.6-bin.tar.gz.sha512 && echo "78b08c7f5a0598242d5719c022e19479cb4a3aa45c37c19726a757a469ed1c15  OpenJDK17U-jdk_aarch64_mac_hotspot_17.0.21_9.tar.gz" | shasum -a 256 -c - && ls -l *.tar.gz | awk "{print \$5, \$9}"'
run 'ls -d zookeeper/zk*/ && ls zookeeper/zk1'
run 'cd zookeeper/zk1 && bin/zkServer.sh version'
run 'for n in 1 2 3; do echo "zk$n: myid=$(cat zookeeper/zk$n/data/myid) $(grep -E "^(clientPort|admin.serverPort|dataDir)=" zookeeper/zk$n/conf/zoo.cfg | tr "\n" " ")"; done'
run 'grep -h "^server\." zookeeper/zk*/conf/zoo.cfg | sort | uniq -c'
} | tee outputs/stage1_install.txt
