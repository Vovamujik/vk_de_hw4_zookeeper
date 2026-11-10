#!/usr/bin/env bash
# Итог запуска этапа 3 или 4: ключевые строки журналов обоих координаторов и указатель
# на результат через штатный zkCli (get -s показывает dataVersion).
#
#   scripts/show_run.sh stage3-no-fencing
#   scripts/show_run.sh stage4-fencing
set -u
cd "$(dirname "$0")/.."
run=${1:?"имя запуска"}
{
    echo "===== logs/$run/coordinator-{A,B}.log: смена лидера, чекпоинт, сверка с ДЗ 3, публикация"
    grep -hE "СТАЛ ЛИДЕРОМ|чекпоинта нет|продолжаю с чекпоинта|SUSPENDED|LOST|сверка с ДЗ 3|ЗАПИСАЛ РЕЗУЛЬТАТ|ПУБЛИКАЦИЯ ОТКЛОНЕНА" \
        logs/"$run"/coordinator-*.log | sort | cut -c12-
    echo
    echo "===== указатель на результат и счётчик эпох в ZooKeeper"
    zookeeper/zk.sh cli 1 get -s "/hw4/$run/result" | grep -vE "^Connecting|^WATCHER|^WatchedEvent"
    zookeeper/zk.sh cli 1 get "/hw4/$run/epoch" | grep -vE "^Connecting|^WATCHER|^WatchedEvent"
} | tee "outputs/$run.txt"
