# Окружение для штатных скриптов ZooKeeper: Java 17 и пути к трём копиям.
# Использование: source zookeeper/env.sh
export JAVA_HOME="$(/usr/libexec/java_home -v 17)"
export PATH="$JAVA_HOME/bin:$PATH"
ZK_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
export ZK_ROOT
export ZK_HOSTS="127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"
