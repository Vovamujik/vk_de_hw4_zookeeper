#!/usr/bin/env bash
# Обёртка над штатными скриптами ZooKeeper для трёх копий на одной машине.
#
#   zookeeper/zk.sh start|stop|status|restart [1|2|3|all]   — bin/zkServer.sh в папке копии
#   zookeeper/zk.sh cli <N> [команда zkCli...]               — bin/zkCli.sh к серверу N
#   zookeeper/zk.sh srvr [1|2|3|all]                         — четырёхбуквенная команда srvr
#
# Перед каждой командой печатается её текст: выводы идут в отчёт как есть.
set -u
source "$(dirname "$0")/env.sh"

cmd=${1:?"команда: start|stop|status|restart|cli|srvr"}
shift

targets() {
    local t=${1:-all}
    [ "$t" = all ] && echo "1 2 3" || echo "$t"
}

case "$cmd" in
start | stop | status | restart)
    for n in $(targets "${1:-all}"); do
        echo "\$ cd zookeeper/zk$n && bin/zkServer.sh $cmd"
        (cd "$ZK_ROOT/zk$n" && bin/zkServer.sh "$cmd" 2>&1)
        echo
    done
    ;;
cli)
    n=${1:?"номер сервера"}
    shift
    echo "\$ zookeeper/zk$n/bin/zkCli.sh -server 127.0.0.1:218$n $*"
    # INFO-строки клиента — шум; WARN/ERROR (например, «Connection refused») оставляем.
    (cd "$ZK_ROOT/zk$n" && bin/zkCli.sh -server "127.0.0.1:218$n" "$@" 2>&1) |
        grep -vE '^[0-9]{4}-[0-9]{2}-[0-9]{2} .* INFO ' | grep -v '^$'
    ;;
srvr)
    for n in $(targets "${1:-all}"); do
        echo "\$ echo srvr | nc 127.0.0.1 218$n"
        out=$(echo srvr | nc -w 2 127.0.0.1 "218$n" 2>&1)
        [ -n "$out" ] && echo "$out" || echo "(нет ответа: порт 218$n закрыт)"
        echo
    done
    ;;
*)
    echo "неизвестная команда: $cmd" >&2
    exit 2
    ;;
esac
