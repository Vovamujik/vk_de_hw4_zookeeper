"""Этапы 3–4: две копии координатора, приостановка лидера посреди расчёта и замер смены лидера.

    uv run python scripts/failover.py --run stage3-no-fencing
    uv run python scripts/failover.py --run stage4-fencing --fencing

Сценарий: запускается координатор A, после того как он стал лидером — B. Когда в чекпоинте набирается
--pause-after сплитов, лидер получает SIGSTOP (процесс заморожен целиком, вместе с пингами ZooKeeper).
Через тайм-аут сессии (10 с) ZooKeeper удаляет его эфемерный узел, лидером становится B и доделывает
расчёт с чекпоинта. Когда B закончил, лидер получает SIGCONT — и, не зная, что его сместили,
тоже публикует результат. Без защиты это вторая запись (указатель версии 2), с FENCING=1 — BadVersionError.

Журналы: logs/<run>/coordinator-A.log, coordinator-B.log, scenario.log, summary.json.
"""

import argparse
import glob
import json
import logging
import os
import re
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from kazoo.client import KazooClient  # noqa: E402

from coordinator import REFERENCE, ZK_HOSTS, fingerprint, load_pairs_csv  # noqa: E402
from hw import s3, settings  # noqa: E402

TS = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d[.,]\d{3})")
log = logging.getLogger("scenario")


def line_time(line: str) -> float:
    return datetime.strptime(TS.match(line).group(1).replace(",", "."), "%Y-%m-%d %H:%M:%S.%f").timestamp()


def hms(t: float) -> str:
    return datetime.fromtimestamp(t).strftime("%H:%M:%S.%f")[:-3]


def find_line(path: Path, needle: str) -> str | None:
    if not path.exists():
        return None
    for line in path.read_text(encoding="utf-8").splitlines():
        if needle in line:
            return line
    return None


def wait_for(pred, timeout: float, what: str, step: float = 0.1):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = pred()
        if value:
            return value
        time.sleep(step)
    raise TimeoutError(f"не дождался: {what} ({timeout:.0f} с)")


def server_expiry_line(session_hex: str) -> str | None:
    """Строка журнала сервера ZooKeeper об истечении сессии (пишет её лидер ансамбля)."""
    for path in glob.glob(str(ROOT / "zookeeper" / "zk*" / "logs" / "*.out")):
        for line in Path(path).read_text(encoding="utf-8", errors="replace").splitlines():
            if "Expiring session" in line and session_hex in line:
                return f"{Path(path).parent.parent.name}: {line}"
    return None


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--run", required=True)
    ap.add_argument("--fencing", action="store_true", help="FENCING=1 у обоих координаторов")
    ap.add_argument("--pause-after", type=int, default=8, help="приостановить лидера, когда в чекпоинте столько сплитов")
    ap.add_argument("--split-delay", default="1.5")
    args = ap.parse_args()

    log_dir = ROOT / "logs" / args.run
    log_dir.mkdir(parents=True, exist_ok=True)
    for old in log_dir.glob("*"):
        old.unlink()
    fmt = logging.Formatter("%(asctime)s.%(msecs)03d [scenario] %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
    handlers = [logging.FileHandler(log_dir / "scenario.log", encoding="utf-8"), logging.StreamHandler(sys.stdout)]
    for h in handlers:
        h.setFormatter(fmt)
    logging.basicConfig(level=logging.INFO, handlers=handlers)
    logging.getLogger("kazoo").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)

    base = f"/hw4/{args.run}"
    zk = KazooClient(hosts=ZK_HOSTS, timeout=10)
    zk.start(timeout=20)
    if zk.exists(base):
        zk.delete(base, recursive=True)
    log.info("запуск %s: FENCING=%d, тайм-аут сессии 10 с, пауза лидера после %d сплитов в чекпоинте",
             args.run, args.fencing, args.pause_after)

    env = {**os.environ, "FENCING": "1" if args.fencing else "0", "SESSION_TIMEOUT": "10",
           "SPLIT_DELAY": args.split_delay}
    procs: dict[str, subprocess.Popen] = {}

    def spawn(name: str) -> subprocess.Popen:
        err = open(log_dir / f"coordinator-{name}.stderr", "w")  # трассировки, если упадёт
        p = subprocess.Popen([sys.executable, str(ROOT / "coordinator.py"), "--name", name, "--run", args.run],
                             cwd=ROOT, env=env, stdout=subprocess.DEVNULL, stderr=err)
        log.info("запустил координатор %s (pid %d)", name, p.pid)
        return p

    clog = {n: log_dir / f"coordinator-{n}.log" for n in "AB"}
    procs["A"] = spawn("A")
    wait_for(lambda: find_line(clog["A"], "СТАЛ ЛИДЕРОМ"), 60, "A стал лидером")
    procs["B"] = spawn("B")
    wait_for(lambda: find_line(clog["B"], "жду очереди"), 60, "B встал в очередь")

    contenders = zk.Election(f"{base}/election").contenders()
    leader = contenders[0].split(":")[0]
    backup = "B" if leader == "A" else "A"
    log.info("очередь на лидерство: %s → лидер %s, запасной %s", " → ".join(contenders), leader, backup)

    done = wait_for(lambda: (n := len(zk.get_children(f"{base}/done"))) >= args.pause_after and n,
                    180, "чекпоинт набрал сплиты")
    os.kill(procs[leader].pid, signal.SIGSTOP)
    t_pause = time.time()
    log.warning("SIGSTOP: приостановил лидера %s (pid %d) посреди расчёта, в чекпоинте %d сплитов",
                leader, procs[leader].pid, done)

    line = wait_for(lambda: find_line(clog[backup], "СТАЛ ЛИДЕРОМ"), 90, f"{backup} стал лидером", step=0.05)
    t_new = line_time(line)
    failover = t_new - t_pause
    log.warning("смена лидера: %s стал лидером в %s — через %.2f с после приостановки %s",
                backup, hms(t_new), failover, leader)
    session = re.search(r"сессия (0x[0-9a-f]+)", find_line(clog[leader], "сессия 0x")).group(1)
    expiry = server_expiry_line(session)
    t_expire = line_time(expiry.split(": ", 1)[1]) if expiry else None
    if expiry:
        log.info("журнал сервера: %s", expiry.strip())
        log.info("из них: до истечения сессии %.2f с, от истечения до нового лидера %.2f с",
                 t_expire - t_pause, t_new - t_expire)

    wait_for(lambda: procs[backup].poll() is not None, 180, f"{backup} закончил расчёт", step=0.2)
    log.info("запасной %s закончил расчёт и вышел (код %d)", backup, procs[backup].returncode)
    time.sleep(1)
    os.kill(procs[leader].pid, signal.SIGCONT)
    pause = time.time() - t_pause
    log.warning("SIGCONT: возобновил %s, он был приостановлен %.1f с", leader, pause)
    procs[leader].wait(timeout=180)
    log.info("%s вышел (код %d)", leader, procs[leader].returncode)

    # --- итог ---------------------------------------------------------------------------------
    raw, stat = zk.get(f"{base}/result")
    pointer = json.loads(raw)
    _, epoch_stat = zk.get(f"{base}/epoch")
    log.info("указатель %s/result: версия %d, записал %s, эпоха %d, %s",
             base, stat.version, pointer["coordinator"], pointer["epoch"], pointer["result"])
    log.info("узел %s/epoch: версия %d (столько раз сменился лидер)", base, epoch_stat.version)
    writes, rejects = [], []
    for name in "AB":
        for ln in clog[name].read_text(encoding="utf-8").splitlines():
            if "ЗАПИСАЛ РЕЗУЛЬТАТ" in ln:
                writes.append(ln)
            if "ПУБЛИКАЦИЯ ОТКЛОНЕНА" in ln:
                rejects.append(ln)
    for ln in sorted(writes + rejects, key=line_time):
        log.info("  %s", ln)

    obj_key = pointer["result"].split(f"s3://{settings.BUCKET}/", 1)[1]
    got = load_pairs_csv(s3.client().get_object(Bucket=settings.BUCKET, Key=obj_key)["Body"].read())
    ref = load_pairs_csv(REFERENCE.read_bytes())
    matches = got == ref
    log.info("результат по указателю: пар %d, сумма %d, отпечаток %s; ДЗ 3: пар %d, сумма %d, отпечаток %s — %s",
             len(got), sum(got.values()), fingerprint(got), len(ref), sum(ref.values()), fingerprint(ref),
             "СОВПАДАЕТ" if matches else "РАСХОДИТСЯ")

    if args.fencing:
        ok = (len(writes) == 1 and len(rejects) == 1 and "BadVersionError" in rejects[0]
              and stat.version == 1 and pointer["coordinator"] == backup and matches)
        expect = "одна запись (запасной), одна отклонённая с BadVersionError, указатель версии 1"
    else:
        ok = len(writes) == 2 and stat.version == 2 and matches
        expect = "две записи, указатель версии 2"
    log.info("ожидание: %s — %s", expect, "OK" if ok else "НЕ ВЫПОЛНЕНО")

    summary = {
        "run": args.run,
        "fencing": args.fencing,
        "leader": leader,
        "backup": backup,
        "paused_at_checkpoint": done,
        "failover_s": round(failover, 2),
        "session_expiry_after_pause_s": round(t_expire - t_pause, 2) if expiry else None,
        "pause_s": round(pause, 1),
        "pointer_version": stat.version,
        "pointer_by": pointer["coordinator"],
        "pointer_epoch": pointer["epoch"],
        "epoch_version": epoch_stat.version,
        "writes": len(writes),
        "rejects": len(rejects),
        "matches_hw3": matches,
        "ok": ok,
    }
    (log_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    zk.stop()
    zk.close()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
