#!/usr/bin/env bash
# Снимок окна Terminal.app с выполненной командой (взято из ДЗ 3):
#   report/tools/term_shot.sh <файл.png> <команда...>
# Окно открывается в корне репозитория, команда выполняется, после её завершения
# снимается ровно область окна (screencapture -R) и окно закрывается.
set -euo pipefail
out="$1"; shift
root=$(cd "$(dirname "$0")/../.." && pwd)
cmd="cd $root && clear && echo '\$ $*' && $*"
win=$(osascript -e "tell application \"Terminal\"
    activate
    set t to do script \"$cmd\"
    delay 1
    set w to (first window whose selected tab is t)
    set bounds of w to {80, 60, 1280, 900}
    set index of w to 1
    return id of w
end tell")
for _ in $(seq 1 120); do # ждём, пока команда в окне закончится
    busy=$(osascript -e "tell application \"Terminal\" to busy of selected tab of window id $win")
    [ "$busy" = "false" ] && break
    sleep 1
done
osascript -e "tell application \"Terminal\"
    activate
    set index of window id $win to 1
end tell"
sleep 1
screencapture -x -R80,60,1200,840 "$out"
osascript -e "tell application \"Terminal\" to close window id $win" >/dev/null 2>&1 || true
echo "saved $out"
