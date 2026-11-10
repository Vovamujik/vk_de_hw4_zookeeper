#!/usr/bin/env bash
# Этап 2: роли серверов, потеря одного и двух серверов, повторный запуск.
#
#   scripts/stage2_ensemble.sh roles    — три сервера: кто лидер, кто последователи
#   scripts/stage2_ensemble.sh lose1    — остановить лидера; ансамбль из двух работает
#   scripts/stage2_ensemble.sh lose2    — остановить ещё один; один сервер из трёх не отвечает
#   scripts/stage2_ensemble.sh restart  — запустить оба снова
#
# Вывод каждой фазы дописывается в outputs/stage2_ensemble.txt.
set -u
cd "$(dirname "$0")/.."
ZK=zookeeper/zk.sh
STATE=outputs/.stage2_stopped # какие серверы остановлены, по порядку
OUT=outputs/stage2_ensemble.txt

leader() {
    for n in 1 2 3; do
        echo srvr | nc -w 2 127.0.0.1 "218$n" 2>/dev/null | grep -q 'Mode: leader' && echo "$n"
    done
}
running() { # номера серверов, которые не остановлены
    local stopped
    stopped=$(cat "$STATE" 2>/dev/null)
    for n in 1 2 3; do [[ " $stopped " == *" $n "* ]] || echo "$n"; done
}
step() { echo; echo "===== $(date '+%Y-%m-%d %H:%M:%S') $*"; }

phase=${1:?"фаза: roles|lose1|lose2|restart"}
{
case "$phase" in
roles)
    : >"$STATE"
    step "Три сервера запущены: роли по zkServer.sh status"
    $ZK status all
    ;;
lose1 | lose2)
    L=$(leader)
    [ -n "$L" ] || { echo "лидера нет — нечего останавливать"; exit 1; }
    step "Останавливаю текущего лидера — сервер $L"
    $ZK stop "$L"
    echo -n "$L " >>"$STATE"
    sleep 4
    rest=$(running | tr '\n' ' ')
    step "Работают серверы: $rest"
    for n in $rest; do $ZK status "$n"; done
    first=$(echo $rest | cut -d' ' -f1)
    if [ "$phase" = lose1 ]; then
        step "Кворум 2 из 3 есть: запись через один сервер, чтение через другой"
        $ZK cli "$first" create /stage2-check "written-with-2-of-3-servers"
        last=$(echo $rest | awk '{print $NF}')
        $ZK cli "$last" get /stage2-check
    else
        step "Кворума нет (1 из 3): сервер не обслуживает клиентов"
        $ZK srvr "$first"
        echo "\$ (zkCli с ожиданием подключения, предел 20 с)"
        timeout 20 "$ZK" cli "$first" -waitforconnection ls / || echo "zkCli не дождался подключения за 20 с (код $?)"
    fi
    ;;
restart)
    stopped=$(cat "$STATE")
    step "Запускаю остановленные серверы: $stopped"
    for n in $stopped; do $ZK start "$n"; done
    : >"$STATE"
    sleep 6
    step "Снова три сервера"
    $ZK status all
    step "Узел, записанный при двух серверах, на месте"
    $ZK cli 3 get /stage2-check
    $ZK cli 3 delete /stage2-check
    ;;
*)
    echo "неизвестная фаза: $phase" >&2
    exit 2
    ;;
esac
} 2>&1 | tee -a "$OUT"
