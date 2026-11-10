#!/usr/bin/env bash
# Этап 2, эфемерный узел: во втором окне Terminal запускается интерактивный zkCli и создаёт
# эфемерный узел; окно закрывается, а из первого окна видно, как узел исчезает вместе с сессией.
#
#   report/tools/term_shot.sh report/img/s2_6_window1.png scripts/stage2_ephemeral.sh
#
# Вывод дописывается в outputs/stage2_ephemeral.txt, снимок второго окна — report/img/s2_5_window2.png.
set -u
cd "$(dirname "$0")/.."
ROOT=$(pwd)
ZK=zookeeper/zk.sh
NODE=/ephemeral-demo
OUT=outputs/stage2_ephemeral.txt
step() { echo; echo "===== $(date '+%Y-%m-%d %H:%M:%S') $*"; }
w2() { osascript -e "tell application \"Terminal\" to do script \"$1\" in selected tab of window id $WIN" >/dev/null; }

{
step "Открываю второе окно: интерактивный zkCli к серверу 1"
WIN=$(osascript -e "tell application \"Terminal\"
    set t to do script \"cd $ROOT/zookeeper/zk1 && clear && bin/zkCli.sh -server 127.0.0.1:2181\"
    delay 1
    set w to (first window whose selected tab is t)
    set bounds of w to {760, 120, 1560, 820}
    set index of w to 1
    return id of w
end tell")
sleep 6
w2 "create -e $NODE created-from-window-2"
sleep 1
w2 "get -s $NODE"
sleep 2
screencapture -x -R760,120,800,700 report/img/s2_5_window2.png
echo "второе окно: узел создан, снимок report/img/s2_5_window2.png"

step "Из первого окна (другая сессия, сервер 2): узел виден, у него есть владелец-сессия"
$ZK cli 2 get -s "$NODE" | grep -E "^\\$|ephemeralOwner|created-from|Node does not exist"
$ZK cli 2 ls /

step "Закрываю второе окно"
# Terminal при закрытии окна шлёт процессу в нём SIGHUP; делаем то же явно, чтобы не было диалога
# «завершить процессы?», затем закрываем само окно.
pkill -HUP -f "ZooKeeperMain -server 127.0.0.1:2181" && echo "zkCli второго окна получил SIGHUP"
sleep 1
osascript -e "tell application \"Terminal\" to close window id $WIN" >/dev/null 2>&1 && echo "окно закрыто"
closed=$(date +%s)

step "Жду, пока узел исчезнет: сервер закрывает сессию по тайм-ауту (у zkCli 30 с)"
while true; do
    sleep 3
    now=$(date +%s)
    if $ZK cli 2 stat "$NODE" 2>&1 | grep -q "Node does not exist"; then
        echo "$(date '+%H:%M:%S') +$((now - closed)) с: узла $NODE больше нет"
        break
    fi
    echo "$(date '+%H:%M:%S') +$((now - closed)) с: узел ещё есть — сессия второго окна не истекла"
    [ $((now - closed)) -gt 90 ] && { echo "узел не исчез за 90 с"; break; }
done
$ZK cli 2 ls /
} 2>&1 | tee -a "$OUT"
