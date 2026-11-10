"""Координатор расчёта пар навыков из ДЗ 3 с выбором лидера, чекпоинтом и защитой по эпохе в ZooKeeper.

Запускаются две копии с одним --run: одна становится лидером и считает, вторая ждёт своей очереди.
Расчёт — тот же map-таск ДЗ 3 (hw/mapreduce.py) по 32-МиБ сплитам CSV в RustFS; каждый посчитанный
сплит — чекпоинт: частичный итог лежит в RustFS, отметка о готовности — в ZooKeeper.

Узлы ZooKeeper (всё под /hw4/<run>):
  election/          эфемерные последовательные узлы кандидатов (рецепт Election из kazoo)
  epoch              счётчик эпох: каждый новый лидер увеличивает его; версия узла = номер эпохи
  done/split-NNNN    чекпоинт: сплит посчитан, частичный итог — s3://<bucket>/work/hw4/<run>/split-NNNN.json
  result             указатель на опубликованный результат (JSON со ссылкой на CSV в RustFS)

Переменные окружения:
  ZK_HOSTS         строка подключения к ансамблю (по умолчанию три сервера на 127.0.0.1:2181–2183)
  SESSION_TIMEOUT  тайм-аут сессии ZooKeeper, с (по умолчанию 10)
  FENCING          1 — публиковать результат только если эпоха не сменилась (multi: check + setData)
  SPLIT_DELAY      пауза после каждого сплита, с — имитирует тяжёлый расчёт, чтобы успеть приостановить лидера

    uv run python coordinator.py --name A --run stage3-no-fencing
"""

import argparse
import csv
import hashlib
import io
import json
import logging
import os
import pickle
import socket
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import BadVersionError, NodeExistsError
from kazoo.retry import KazooRetry

from hw import mapreduce as mr
from hw import s3, settings
from hw.jobs import SKILL_COUNTS

ROOT = Path(__file__).resolve().parent
ZK_HOSTS = os.environ.get("ZK_HOSTS", "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183")
SESSION_TIMEOUT = float(os.environ.get("SESSION_TIMEOUT", "10"))
FENCING = os.environ.get("FENCING", "0") == "1"
SPLIT_DELAY = float(os.environ.get("SPLIT_DELAY", "1.5"))
SPLIT_SIZE = int(os.environ.get("SPLIT_MB", "32")) * 2**20
REFERENCE = ROOT / "hw3_reference" / "pairs.csv"  # результат ДЗ 3 для сверки

log = logging.getLogger("coordinator")


def now_iso() -> str:
    return datetime.now().isoformat(timespec="milliseconds")


def fingerprint(pairs: dict[tuple[str, str], int]) -> str:
    """Отпечаток как в scripts/verify.py ДЗ 3: SHA-256 по отсортированным парам и частотам."""
    h = hashlib.sha256()
    for key, value in sorted(pairs.items()):
        h.update(("\x1f".join(key) + "\t" + str(value) + "\n").encode())
    return h.hexdigest()[:16]


def pairs_csv(pairs: dict[tuple[str, str], int]) -> bytes:
    """pairs.csv в формате ДЗ 3: по убыванию частоты, при равенстве — по паре."""
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["skill_1", "skill_2", "vacancies"])
    w.writerows([*key, n] for key, n in sorted(pairs.items(), key=lambda kv: (-kv[1], kv[0])))
    return buf.getvalue().encode("utf-8")


def load_pairs_csv(data: bytes) -> dict[tuple[str, str], int]:
    rows = csv.DictReader(io.StringIO(data.decode("utf-8")))
    return {(r["skill_1"], r["skill_2"]): int(r["vacancies"]) for r in rows}


class Coordinator:
    def __init__(self, name: str, run: str):
        self.name, self.run = name, run
        self.base = f"/hw4/{run}"
        self.epoch_path, self.result_path = f"{self.base}/epoch", f"{self.base}/result"
        self.done_path = f"{self.base}/done"
        self.work_prefix = f"work/hw4/{run}"
        self.spill_dir = ROOT / "work" / run / name
        self.epoch = 0
        self.closing = False
        self.zk = KazooClient(hosts=ZK_HOSTS, timeout=SESSION_TIMEOUT)
        # Повторы на время переподключения: после паузы процесса сессия истекла, kazoo открывает новую.
        self.retry = KazooRetry(max_tries=40, delay=0.25, max_delay=2, ignore_expire=True)
        self.zk.add_listener(self._on_state)

    # --- ZooKeeper ---------------------------------------------------------------------------

    def _on_state(self, state: str) -> None:
        if self.closing:
            return  # zk.stop() сам переводит клиент в LOST — это штатное закрытие, а не истечение
        if state == KazooState.LOST:
            log.warning("сессия ZooKeeper истекла (LOST): её эфемерные узлы удалены сервером")
        elif state == KazooState.SUSPENDED:
            log.warning("связь с ZooKeeper прервана (SUSPENDED)")
        else:
            log.info("подключён к ZooKeeper (CONNECTED)")

    def connect(self) -> None:
        self.zk.start(timeout=20)
        # Согласованный тайм-аут kazoo не хранит; сервер допускает 2–20 tickTime (4–40 с), так что 10 с
        # принимается как есть — это видно в журнале сервера: «timeout of 10000ms exceeded».
        log.info("сессия 0x%x, тайм-аут сессии %.0f с, FENCING=%d, ансамбль %s",
                 self.zk.client_id[0], SESSION_TIMEOUT, FENCING, ZK_HOSTS)
        self.zk.ensure_path(f"{self.base}/election")
        self.zk.ensure_path(self.done_path)
        for path, data in ((self.epoch_path, b'{"epoch": 0}'), (self.result_path, b"")):
            try:
                self.zk.create(path, data)  # версия 0; первая запись даст версию 1
            except NodeExistsError:
                pass

    def bump_epoch(self) -> int:
        """Новая эпоха = следующая версия узла epoch; CAS на случай гонки двух лидеров."""
        while True:
            _, stat = self.retry(self.zk.get, self.epoch_path)
            data = json.dumps({"epoch": stat.version + 1, "leader": self.name, "since": now_iso()}).encode()
            try:
                return self.retry(self.zk.set, self.epoch_path, data, version=stat.version).version
            except BadVersionError:
                continue

    # --- расчёт ------------------------------------------------------------------------------

    def map_split(self, task: mr.MapTask) -> tuple[dict, dict]:
        """Map-таск ДЗ 3 над одним сплитом; из его спила берём только пары навыков."""
        stats = mr.run_map_task(task)
        with open(self.spill_dir / f"map-{task.split.index:04d}-part-00.pkl", "rb") as f:
            part = pickle.load(f)
        return {k: v for k, v in part.items() if len(k) == 2}, stats

    def lead(self) -> None:
        started = time.monotonic()
        self.epoch = self.bump_epoch()
        log.info("СТАЛ ЛИДЕРОМ: эпоха %d (ждал очереди %.1f с)", self.epoch, started - self.waiting_since)

        key = settings.RAW_KEY
        size = s3.object_size(key)
        cols = mr.read_header(key)
        splits = mr.make_splits(size, SPLIT_SIZE)
        done = {int(n.split("-")[1]) for n in self.retry(self.zk.get_children, self.done_path)}
        if done:
            log.info("продолжаю с чекпоинта: готово %d из %d сплитов, осталось %d",
                     len(done), len(splits), len(splits) - len(done))
        else:
            log.info("чекпоинта нет: считаю все %d сплитов s3://%s/%s (%d байт)", len(splits), settings.BUCKET, key, size)

        self.spill_dir.mkdir(parents=True, exist_ok=True)
        for split in splits:
            marker = f"{self.done_path}/split-{split.index:04d}"
            if split.index in done:
                continue
            if self.retry(self.zk.exists, marker):
                log.info("сплит %2d/%d уже отмечен в чекпоинте — пропускаю", split.index + 1, len(splits))
                continue
            t0 = time.monotonic()
            task = mr.MapTask(SKILL_COUNTS, key, split, size, cols, 1, str(self.spill_dir), True, True)
            pairs, stats = self.map_split(task)
            time.sleep(SPLIT_DELAY)  # имитация тяжёлого расчёта
            part_key = f"{self.work_prefix}/split-{split.index:04d}.json"
            body = json.dumps(sorted([*k, v] for k, v in pairs.items()), ensure_ascii=False).encode()
            s3.client().put_object(Bucket=settings.BUCKET, Key=part_key, Body=body)
            meta = json.dumps({"by": self.name, "epoch": self.epoch, "key": part_key, "at": now_iso()}).encode()
            try:
                self.retry(self.zk.create, marker, meta)
            except NodeExistsError:
                log.info("сплит %2d/%d: отметка в чекпоинте уже есть (поставил другой координатор)",
                         split.index + 1, len(splits))
            ready = len(self.retry(self.zk.get_children, self.done_path))
            log.info("сплит %2d/%d посчитан: записей %d, пар в сплите %d, %.1f с; в чекпоинте %d/%d",
                     split.index + 1, len(splits), stats["records"], len(pairs), time.monotonic() - t0,
                     ready, len(splits))

        total: Counter = Counter()
        for split in splits:
            raw = s3.client().get_object(Bucket=settings.BUCKET, Key=f"{self.work_prefix}/split-{split.index:04d}.json")
            for s1, s2, n in json.loads(raw["Body"].read()):
                total[(s1, s2)] += n
        self.finish(dict(total))

    def finish(self, pairs: dict[tuple[str, str], int]) -> None:
        body = pairs_csv(pairs)
        fp, n_sum = fingerprint(pairs), sum(pairs.values())
        top = sorted(pairs.items(), key=lambda kv: (-kv[1], kv[0]))[:3]
        log.info("контрольные числа: пар %d, сумма частот %d, отпечаток %s; топ-3: %s", len(pairs), n_sum, fp,
                 ", ".join(f"{a}+{b} {n}" for (a, b), n in top))
        ref = REFERENCE.read_bytes()
        same = ref == body
        log.info("сверка с ДЗ 3 (%s): %s — пар %d, сумма %d, отпечаток %s",
                 REFERENCE.relative_to(ROOT), "СОВПАДАЕТ байт в байт" if same else "РАСХОДИТСЯ",
                 len(load_pairs_csv(ref)), sum(load_pairs_csv(ref).values()), fingerprint(load_pairs_csv(ref)))

        out_key = f"results/hw4/{self.run}/pairs-epoch{self.epoch}-{self.name}.csv"
        s3.client().put_object(Bucket=settings.BUCKET, Key=out_key, Body=body)
        local = ROOT / "results" / self.run / f"pairs-epoch{self.epoch}-{self.name}.csv"
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_bytes(body)
        payload = {
            "result": f"s3://{settings.BUCKET}/{out_key}",
            "epoch": self.epoch,
            "coordinator": self.name,
            "pairs": len(pairs),
            "sum": n_sum,
            "fingerprint": fp,
            "written_at": now_iso(),
        }
        self.publish(json.dumps(payload, ensure_ascii=False).encode())

    def publish(self, data: bytes) -> None:
        if not FENCING:
            stat = self.retry(self.zk.set, self.result_path, data)
            log.info("ЗАПИСАЛ РЕЗУЛЬТАТ: %s версия %d (эпоха %d, без проверки эпохи)",
                     self.result_path, stat.version, self.epoch)
            return

        def commit():  # транзакция одноразовая — на каждую попытку новая
            tx = self.zk.transaction()
            tx.check(self.epoch_path, self.epoch)  # эпоха не сменилась с тех пор, как я стал лидером
            tx.set_data(self.result_path, data)
            return tx.commit()

        results = self.retry(commit)
        errors = [r for r in results if isinstance(r, Exception)]
        if errors:
            raw, stat = self.retry(self.zk.get, self.epoch_path)
            current = json.loads(raw)
            log.error("ПУБЛИКАЦИЯ ОТКЛОНЕНА: %s — моя эпоха %d устарела, текущая эпоха %d (лидер %s); "
                      "указатель %s не изменён",
                      type(errors[0]).__name__, self.epoch, stat.version, current.get("leader"), self.result_path)
            return
        log.info("ЗАПИСАЛ РЕЗУЛЬТАТ: %s версия %d (эпоха %d, проверка эпохи пройдена)",
                 self.result_path, results[1].version, self.epoch)

    # --- жизненный цикл ----------------------------------------------------------------------

    def run_forever(self) -> None:
        self.connect()
        election = self.zk.Election(f"{self.base}/election", identifier=f"{self.name}:{os.getpid()}")
        contenders = election.contenders()
        if contenders:
            log.info("жду очереди на лидерство; сейчас лидер %s", contenders[0])
        else:
            log.info("кандидатов нет — встаю первым в очередь")
        self.waiting_since = time.monotonic()
        election.run(self.lead)
        log.info("работа завершена, закрываю сессию")
        self.closing = True
        self.zk.stop()
        self.zk.close()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--name", required=True, help="имя копии координатора: A, B …")
    ap.add_argument("--run", required=True, help="имя запуска: узлы /hw4/<run>, журналы logs/<run>/")
    args = ap.parse_args()

    log_dir = ROOT / "logs" / args.run
    log_dir.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter(f"%(asctime)s.%(msecs)03d [{args.name} pid {os.getpid()}] %(levelname)s %(message)s",
                            "%Y-%m-%d %H:%M:%S")
    handlers = [logging.FileHandler(log_dir / f"coordinator-{args.name}.log", encoding="utf-8"),
                logging.StreamHandler(sys.stdout)]
    for h in handlers:
        h.setFormatter(fmt)
    logging.basicConfig(level=logging.INFO, handlers=handlers)
    logging.getLogger("kazoo").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    log.info("старт на %s: запуск %s", socket.gethostname(), args.run)
    Coordinator(args.name, args.run).run_forever()


if __name__ == "__main__":
    main()
