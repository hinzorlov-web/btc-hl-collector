"""
btc_hl_stops.py — где стоят стопы BTC сейчас: Hyperliquid, бесплатно.
Столп 3 индекса, метрика «7 дней». PROJECT.md 6.15, 6.18, 6.19.

Как: у Hyperliquid нет запроса «все позиции сразу». Поэтому:
  1. список счетов — публичная таблица лидеров (stats-data.hyperliquid.xyz,
     ~47 тыс. счетов, кто согласился быть в списке);
  2. берём N крупнейших по размеру счёта (--top 0 — все);
  3. по каждому — официальный запрос clearinghouseState: размер позиции BTC,
     цена входа, ЦЕНА ЛИКВИДАЦИИ (её считает сама биржа), плечо;
  4. хранилище HLP (главный контрагент площадки) — родитель и дочерние
     хранилища, в каждом прогоне;
  5. сверяем сумму найденных позиций с общим открытым интересом BTC на
     бирже — это ПОКРЫТИЕ: какую долю рынка Hyperliquid мы видим.
Лимит биржи: 1200 «весов» в минуту, запрос по счёту — вес 2. Запросы идут
параллельно (6 потоков), по открытому соединению, не больше 500 в минуту:
2000 счетов — 4 мин (11.10), все ~47 тыс. — около 95 мин.

Столп 3 — версия D (принята аудитором 15.10, PROJECT.md 6.34): каждая позиция
со стопом в пределах 8 дневных ходов входит СВОИМ размером, умноженным на
вероятность, что цена дойдёт до её стопа за 7 дней (кривая p3_touch_curve.csv).
Сумма по стороне — ожидаемый объём принудительных закрытий за 7 дней:
R↑ (стопы шортов сверху) и R↓ (стопы лонгов снизу). Делится на открытый
интерес выборки (все найденные позиции, и без цены ликвидации тоже):
  D = 100·(R↑ − R↓) / OI выборки — направление: плюс — уязвимее шорты
      (цена скорее прыгнет вверх к их стопам), минус — лонги. Входит в индекс.
  T = 100·(R↑ + R↓) / OI выборки — величина риска в обе стороны, топливо для
      каскада. Печатается рядом с D, в индекс не входит.
  Хрупкость — D без крупнейшего счёта той стороны, что даёт знак.
Ограничение (печатается словами): D — ожидание при обычном распределении цены;
каскад (ликвидации сами двигают цену дальше) не учтён, в каскаде D занижает риск.

Зонная версия «7 дней» (11.10–15.10, отведена: размер входил только через
выбор самой плотной зоны, ошибка №72) считается и пишется в ряд справочно —
колонка pillar3, для сравнения со старыми строками.

Каждый прогон сохраняется в hl_snapshots/ — так копится собственная история.

Режимы:
  py btc_hl_stops.py selftest
  py btc_hl_stops.py run --top 2000 --known      — ежедневно: топ 2000 + держатели BTC
                                                     из последнего полного опроса (≈ 10 мин)
  py btc_hl_stops.py run --top 0                  — раз в неделю: все счета (≈ 95 мин),
                                                     обновляет список держателей hl_known.json
  py btc_hl_stops.py recompute hl_snapshots/hl_ГГГГММДД_ЧЧММ.json   — пересчёт без опроса
  py btc_hl_stops.py widths hl_snapshots/hl_ГГГГММДД_ЧЧММ.json      — ширина окна 0,5/1/2%
  py btc_hl_stops.py cutoffs latest               — D при обрезке 6/8/12σ (один раз, аудитор 15.10)
  py btc_hl_stops.py refill [--csv путь]          — дописать новые колонки ряда по сохранённым снимкам
  py btc_hl_stops.py sync                         — забрать снимки облачного сборщика
  py btc_hl_stops.py recompute latest             — столп 3 по последнему снимку
  python btc_hl_stops.py collect [--mode auto]    — облачный сборщик (GitHub Actions)
  py btc_hl_stops.py depth                        — проверка глубины ежедневного опроса
  py btc_hl_stops.py sigmacheck                   — σ Binance против Coinbase

Облачный сборщик (шаг 2а, репозиторий hinzorlov-web/btc-hl-collector): Supabase
каждый день будит GitHub Actions, тот запускает collect и складывает сжатые
снимки в data/hl/ГГГГ/ММ/. Столп 3 считается прямо в облаке: σ — по дневным
свечам Coinbase (Binance серверам GitHub отказывает), ряд — data/pillar3_daily.csv.
Если Coinbase недоступен, снимок всё равно сохраняется, а столп считает ноутбук
(recompute, σ по Binance). σ — по дням, закрытым к моменту снимка.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import btc_touch_curve as TC                                    # noqa: E402
import btc_magnet_test as MT                                    # noqa: E402

try:
    import requests
except ImportError:                                          # pragma: no cover
    requests = None

INFO = "https://api.hyperliquid.xyz/info"
LEADERBOARD = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"
HLP_VAULT = "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303"   # Hyperliquidity Provider (HLP)
SNAP_DIR = os.path.join(HERE, "hl_snapshots")
WIN_PCT = 0.5              # ширина окна зоны, п.п. расстояния
WIDTHS = (0.5, 1.0, 2.0)   # проверка чувствительности — один раз, для записи
MAX_PER_MIN = 500          # вес запроса 2 → 1000 из 1200 разрешённых в минуту
WORKERS = 6                # параллельных запросов: задержка сети ~1,3 с на запрос
CUTS = (500, 1000, 2000, 5000, 10000, 20000)
B_NOTE = "справочно; отведён 11.10 — сдвиг с глубиной выборки дали отдельные счета (№57)"
REPO = "hinzorlov-web/btc-hl-collector"   # облачный сборщик (GitHub Actions), шаг 2а
DATA_DIR = os.path.join(HERE, "data")       # куда пишет сборщик в репозитории
KNOWN_FILE = None                           # путь к hl_known.json (сборщик задаёт свой)
CUM_K = (1, 2, 4)                           # кумулятивная версия: пределы в дневных ходах
COINBASE = "https://api.exchange.coinbase.com/products/BTC-USD/candles"   # σ в облаке (аудитор, 13.10)
# Строка ряда столпа 3: всё, чтобы объяснить день без повторного прогона (аудитор, 13.10 вечер):
# границы зон, суммы, счета, доля крупнейшего, σ, кумулятивная 1/2/4σ с долями сторон,
# нормировочная база (условные суммы лонгов и шортов в выборке), хрупкость, покрытие.
# cover_long / cover_short — лонги (шорты) выборки, делённые на поле openInterest биржи. Это поле — СУММА обеих
# сторон (найдено 18.10 по Reservoir, ошибка №82), поэтому доля рынка ≈ 2 × cover. Определение колонки не
# менялось с 11.10 — в ряду одно определение; печать с 21.10 показывает обе величины.
# Новые колонки дописываются в конец — старые строки сохраняются (_append_csv).
P3_COLS = ["utc", "kind", "sigma", "sigma_src", "pillar3", "p_short", "p_long", "zone_short_pct",
           "zone_long_pct", "zone_short_usd", "zone_long_usd", "acc_short", "acc_long", "top_short",
           "top_long", "p3_wo_top_short", "p3_wo_top_long", "cum1", "cum2", "cum4",
           "cover_long", "cover_short", "positions", "polled",
           "zone_short_lo", "zone_short_hi", "zone_long_lo", "zone_long_hi", "k_short", "k_long",
           "reach_short", "reach_long", "short_usd", "long_usd",
           "cum1_short", "cum1_long", "cum2_short", "cum2_long", "cum4_short", "cum4_long",
           "near2_short", "near5_short", "near10_short", "near2_long", "near5_long", "near10_long",
           "mark", "oi_btc", "instrument",
           # версия D (15.10): направление, величина, хрупкость, суммы сторон, база
           "d", "t", "d_wo_top", "d_lead", "d_top_share", "r_up_usd", "r_down_usd", "oi_sample_usd",
           # лестница глубин (аудитор, 15.10 вечер): D и T по ступеням, знак — при согласии всех ступеней
           "d_l2000", "d_l10000", "d_l20000", "t_l2000", "t_l10000", "t_l20000", "d_sign_ladder",
           # три состояния знака D и доли крупнейших вкладчиков сторон (аудитор, 16.10)
           "d_state", "top_share_up", "top_share_down", "d_top_usd", "d_top_type", "d_top_lev",
           "d_top_k", "d_top_dist", "d_top_rank",
           # аудитор, 17.10: разброс ФАКТИЧЕСКИХ цен входа (описательно, только открытые позиции — выжившие)
           # и разложение изменения D и T к прошлым суткам: цена (вчерашний состав по сегодняшним цене и σ)
           # + позиции (сегодняшний состав против вчерашнего по сегодняшней цене); сумма точна
           "entry_disp", "entry_disp_long", "entry_disp_short", "entry_n",
           "prev_utc", "prev_kind", "d_prev", "d_px", "d_pos", "t_prev", "t_px", "t_pos"]
ENTRY_NOTE = ("Разброс входов — только по ОТКРЫТЫМ позициям: закрытые и ликвидированные не видны, а это как раз "
              "крайние входы, — наблюдаемый разброс УЖЕ настоящего (выжившие; аудитор, 17.10). Описательно, вне композита.")
LADDER = (2000, 10000, 20000)               # ступени лестницы глубин; последняя ступень — все опрошенные
CUTOFFS = (6.0, 8.0, 12.0)                  # чувствительность D к обрезке — один прогон (аудитор, 15.10)
# Три состояния знака D (аудитор, 16.10): лестница проверяет состав выборки, хрупкость —
# концентрацию; ни одна не заменяет другую. Киты — предмет измерения: концентрацию называть.
D_STATES = ("установлено", "одним счётом", "не установлено")
D_LIMIT = ("Ограничение: D — ожидаемый объём при обычном распределении цены; каскад (ликвидации "
           "сами двигают цену дальше) не учтён — в каскаде D занижает риск. Высокий T — топливо для каскада.")


class Limiter:
    """Общий для всех потоков интервал между стартами запросов."""
    def __init__(self, per_min: int):
        self.gap = 60.0 / per_min
        self.next = 0.0
        self.lock = threading.Lock()

    def wait(self) -> None:
        with self.lock:
            now = time.monotonic()
            start = max(now, self.next)
            self.next = start + self.gap
        time.sleep(max(0.0, start - now))


LIMIT = Limiter(MAX_PER_MIN)
_TL = threading.local()


def session():
    """Своё соединение у каждого потока — без нового рукопожатия на каждый запрос."""
    s = getattr(_TL, "s", None)
    if s is None:
        s = _TL.s = requests.Session()
    return s


# --------------------------------------------------------------------------
# Сеть
# --------------------------------------------------------------------------

def post(body: dict, tries: int = 5):
    for k in range(tries):
        LIMIT.wait()
        try:
            r = session().post(INFO, json=body, timeout=30)
            if r.status_code == 429:
                time.sleep(5 * (k + 1))
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException:
            if k == tries - 1:
                raise
            time.sleep(2 * (k + 1))
    raise RuntimeError("биржа не ответила")


def btc_market() -> tuple[float, float]:
    """Открытый интерес BTC (в монетах) и цена отметки."""
    meta, ctxs = post({"type": "metaAndAssetCtxs"})
    for a, c in zip(meta["universe"], ctxs):
        if a["name"] == "BTC":
            return float(c["openInterest"]), float(c["markPx"])
    raise RuntimeError("BTC не найден в metaAndAssetCtxs")


def leaderboard_rows() -> list[dict]:
    """Все строки таблицы лидеров, от крупного счёта к мелкому."""
    r = requests.get(LEADERBOARD, timeout=120)
    r.raise_for_status()
    rows = r.json()["leaderboardRows"]
    rows.sort(key=lambda x: float(x.get("accountValue") or 0), reverse=True)
    print(f"Таблица лидеров: {len(rows)} счетов")
    return rows


def leaderboard() -> list[str]:
    return [x["ethAddress"] for x in leaderboard_rows()]


def day_volume(row: dict) -> float:
    """Оборот счёта за окно «day» таблицы лидеров (0, если поля нет)."""
    for w in row.get("windowPerformances") or []:
        if isinstance(w, list) and len(w) == 2 and w[0] == "day" and isinstance(w[1], dict):
            return float(w[1].get("vlm") or 0)
    return 0.0


def known_path() -> str:
    return KNOWN_FILE or os.path.join(HERE, "hl_known.json")


def save_known(positions: list, utc: str) -> None:
    with open(known_path(), "w", encoding="utf-8") as fh:
        json.dump({"utc": utc, "addresses": sorted({p["addr"].lower() for p in positions})}, fh)


def load_known() -> tuple:
    """Держатели BTC из последнего полного опроса: hl_known.json, а если его
    нет — снимок с наибольшим охватом (не меньше 20 000 счетов)."""
    if os.path.isfile(known_path()):
        with open(known_path(), encoding="utf-8") as fh:
            k = json.load(fh)
        return k["utc"], set(k["addresses"])
    best = _full_snapshots()
    if best:
        snap = best[-1][1]
        return snap["utc"], {p["addr"].lower() for p in snap["positions"] if p.get("rank", 0) >= 0}
    return None, set()


FULL_MIN = 20000           # полный опрос — не меньше стольких счетов таблицы лидеров


def _read_snap(path: str) -> dict:
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as fh:
        return json.load(fh)


def _is_full(snap: dict) -> bool:
    """Полный опрос: облачный с пометкой full или ноутбучный run --top 0 (опрошены все счета)."""
    if snap.get("kind"):
        return snap["kind"] == "full"
    t = snap.get("leaderboard") or snap.get("top") or 0
    return t >= FULL_MIN and snap.get("top", 0) >= t


def _full_snapshots(before: str | None = None) -> list:
    """Полные снимки в hl_snapshots/ — [(utc, снимок, путь)] по возрастанию времени,
    при before — только более ранние. Ежедневные облачные снимки не читаются."""
    out = []
    if not os.path.isdir(SNAP_DIR):
        return out
    for fn in sorted(os.listdir(SNAP_DIR)):
        if not fn.startswith("hl_") or not (fn.endswith(".json") or fn.endswith(".json.gz")) \
                or fn.endswith("_daily.json.gz") or (before and fn[3:16] >= before):
            continue
        path = os.path.join(SNAP_DIR, fn)
        snap = _read_snap(path)
        if _is_full(snap):
            out.append((snap["utc"], snap, path))
    return sorted(out, key=lambda x: x[0])


def hlp_addresses() -> list[str]:
    """Хранилище HLP: родитель и дочерние хранилища (позиции держат дочерние)."""
    d = post({"type": "vaultDetails", "vaultAddress": HLP_VAULT}) or {}
    rel = d.get("relationship") or {}
    kids = (rel.get("data") or {}).get("childAddresses") or []
    return [HLP_VAULT] + [k for k in kids if k.lower() != HLP_VAULT]


# --------------------------------------------------------------------------
# Разбор
# --------------------------------------------------------------------------

def btc_position(state: dict) -> dict | None:
    for ap in state.get("assetPositions", []):
        p = ap.get("position", {})
        if p.get("coin") == "BTC" and float(p.get("szi") or 0) != 0:
            lev = p.get("leverage") or {}
            liq = p.get("liquidationPx")
            return {"szi": float(p["szi"]), "entry": float(p["entryPx"]),
                    "liq": float(liq) if liq not in (None, "", "null") else None,
                    "lev_type": lev.get("type"), "lev": lev.get("value"),
                    "value": float(p.get("positionValue") or 0)}
    return None


def _k(d_pct: float, sigma: float) -> float:
    """Расстояние в дневных ходах по расстоянию в процентах (вверх и вниз)."""
    return abs(math.log(1 + d_pct / 100.0)) / sigma


def densest_window(rows: list, up: bool, sigma: float, width: float = WIN_PCT) -> dict | None:
    """
    Скользящее окно шириной width п.п. расстояния: максимум суммы размеров
    среди позиций своей стороны в пределах K_MAX дневных ходов.
    rows — (dist_pct, size_usd, idx). При равенстве сумм — ближнее к цене.
    """
    side = sorted((abs(d), s, i) for d, s, i in rows
                  if (d > 0) == up and d != 0 and _k(d, sigma) <= MT.K_MAX)
    if not side:
        return None
    total = sum(s for _, s, _ in side)
    best, run, j = None, 0.0, 0
    for i in range(len(side)):
        while j < len(side) and side[j][0] <= side[i][0] + width + 1e-9:
            run += side[j][1]
            j += 1
        if best is None or run > best[0] * (1 + 1e-12):
            best = (run, i, j)
        run -= side[i][1]
    size, i0, j0 = best
    mem = side[i0:j0]
    a = sum(x * s for x, s, _ in mem) / size
    dist = a if up else -a
    top = max(mem, key=lambda m: m[1])
    return {"dist_pct": dist, "lo_pct": mem[0][0] if up else -mem[0][0],
            "hi_pct": mem[-1][0] if up else -mem[-1][0], "size_usd": size,
            "k": _k(dist, sigma), "share": size / total, "n_acc": len(mem),
            "top_share": top[1] / size, "_top": top[2], "n_reach": len(side)}


def expected_closures(rows: list, sigma: float, curve: dict, k_max: float | None = None,
                      tail: str = "flat") -> tuple:
    """
    R↑, R↓ — ожидаемый объём принудительных закрытий за 7 дней по сторонам
    (True — шорты, стопы сверху): размер позиции × P(дойти за 7 дней) по всем
    позициям в пределах k_max дневных ходов. Ожидание линейно — складывать
    законно, хотя события вложены (дойдя до 5σ, цена прошла и 1σ).
    За последней точкой кривой (8σ) вероятности нет: tail="flat" — как в 8σ
    (верхняя граница, кривая убывает), "zero" — ноль (нижняя граница).
    Возвращает (R, вклады [(вклад, сторона, размер, расстояние %, номер строки)]).
    """
    k_max = MT.K_MAX if k_max is None else k_max
    k_last = curve["k"][-1]
    R, contrib = {True: 0.0, False: 0.0}, []
    for d, size, idx in rows:
        k = _k(d, sigma)
        if not 0 < k <= k_max or (tail == "zero" and k > k_last):
            continue
        c = size * MT.interp(curve["k"], curve[d > 0], k)
        R[d > 0] += c
        contrib.append((c, d > 0, size, d, idx))
    return R, contrib


def d_block(R: dict, contrib: list, oi_s: float) -> dict:
    """
    D, T и хрупкость D (без крупнейшего счёта стороны, что даёт знак).
    Доля крупнейшего вкладчика в R каждой стороны (аудитор, 16.10) — непрерывная мера:
    широкое направление или единичное, не дожидаясь переворота знака.
    """
    o = {"oi_sample_usd": oi_s, "r_up_usd": R[True], "r_down_usd": R[False],
         "d": None, "t": None, "d_wo_top": None, "d_lead": None, "d_top_share": None, "d_top_usd": None,
         "top_share_up": None, "top_share_down": None, "d_top_dist": None, "_top_idx": None}
    for side, key in ((True, "top_share_up"), (False, "top_share_down")):
        cs = [x[0] for x in contrib if x[1] == side]
        if cs and R[side] > 0:
            o[key] = max(cs) / R[side]
    if not oi_s:
        return o
    o["d"] = 100.0 * (R[True] - R[False]) / oi_s
    o["t"] = 100.0 * (R[True] + R[False]) / oi_s
    if R[True] == R[False]:
        return o
    lead = R[True] > R[False]
    top = max((x for x in contrib if x[1] == lead), key=lambda x: x[0])
    c, size = top[0], top[2]
    if len(top) > 4:
        o["d_top_dist"], o["_top_idx"] = top[3], top[4]
    r = dict(R)
    r[lead] -= c
    rest = oi_s - size                      # счёт уходит и из базы
    o["d_wo_top"] = 100.0 * (r[True] - r[False]) / rest if rest > 0 else None
    o["d_lead"], o["d_top_share"], o["d_top_usd"] = ("short" if lead else "long"), c / R[lead], size
    return o


def summarize(positions: list[dict], oi: float, mark: float, sigma: float,
              curve: dict, sens: bool = True, width: float = WIN_PCT) -> dict:
    longs = [p for p in positions if p["szi"] > 0]
    shorts = [p for p in positions if p["szi"] < 0]
    L, S = sum(p["szi"] for p in longs), -sum(p["szi"] for p in shorts)
    no_liq = {True: 0.0, False: 0.0}
    for p in positions:
        if p["liq"] is None or p["liq"] <= 0:
            no_liq[p["szi"] > 0] += abs(p["szi"]) * mark
    with_liq = [p for p in positions if p["liq"] is not None and p["liq"] > 0]
    rows = [(100.0 * (p["liq"] / mark - 1.0), abs(p["szi"]) * mark, i) for i, p in enumerate(with_liq)]
    out = {"oi_btc": oi, "mark": mark, "sigma": sigma, "n_pos": len(positions),
           "long_btc": L, "short_btc": S, "width_pct": width,
           "cover_long": L / oi if oi else None, "cover_short": S / oi if oi else None,
           "no_liq_long_usd": no_liq[True], "no_liq_short_usd": no_liq[False]}
    for up, side in ((True, "short"), (False, "long")):
        z = densest_window(rows, up, sigma, width)
        if z is not None:
            z["p7"] = MT.interp(curve["k"], curve[up], z["k"])
            z["near"] = {th: sum(s for d, s, _ in rows if (d > 0) == up and abs(d) <= th)
                         for th in (2, 5, 10)}
        out[side] = z
    if out["short"] and out["long"]:
        out["pillar3"] = 100.0 * (out["short"]["p7"] - out["long"]["p7"])
    else:
        out["pillar3"] = None
    # Хрупкость (справка, не условие — решение аудитора 11.10): столп 3, если
    # исчезнет крупнейший счёт зоны (закроется или довнесёт залог).
    for side in ("short", "long"):
        z = out[side]
        if z is None:
            continue
        top = with_liq[z.pop("_top")]
        if sens:
            rest = [q for q in positions if q is not top]
            z["p3_without_top"] = summarize(rest, oi, mark, sigma, curve, sens=False,
                                            width=width)["pillar3"]
    # Версия D (принята 15.10): ожидаемый объём закрытий за 7 дней по сторонам,
    # нормировка — открытый интерес выборки (все позиции, и без цены ликвидации).
    # Влияние любого счёта ограничено его долей в этом интересе.
    exp_vol, contrib = expected_closures(rows, sigma, curve)
    out["exp_up_usd"], out["exp_down_usd"] = exp_vol[True], exp_vol[False]
    tot = exp_vol[True] + exp_vol[False]
    out["pillar3_vol"] = 100.0 * (exp_vol[True] - exp_vol[False]) / tot if tot else None   # = D/T
    out.update(d_block(exp_vol, contrib, (L + S) * mark))
    ti = out.pop("_top_idx", None)                          # счёт, что держит знак: плечо, маржа, ранг
    if ti is not None:
        tp = with_liq[ti]
        out.update(d_top_type=tp.get("lev_type"), d_top_lev=tp.get("lev"), d_top_rank=tp.get("rank"),
                   d_top_k=_k(out["d_top_dist"], sigma))
    # Кумулятивная версия (аудитор, 13.10): доля условной суммы стороны, чьи
    # ликвидации в пределах k дневных ходов от цены; нормировка — на всю сторону
    # в выборке (включая позиции без цены ликвидации). Без окна, сетки и максимума.
    side_tot = {True: S * mark, False: L * mark}            # True — шорты (стопы сверху)
    cum = {}
    for kk in CUM_K:
        part = {True: 0.0, False: 0.0}
        for d, size, _ in rows:
            if 0 < _k(d, sigma) <= kk:
                part[d > 0] += size
        sh = part[True] / side_tot[True] if side_tot[True] else 0.0
        lo = part[False] / side_tot[False] if side_tot[False] else 0.0
        cum[str(kk)] = {"short": sh, "long": lo,
                        "value": 100.0 * (sh - lo) / (sh + lo) if sh + lo else None}
    out["cum"] = cum
    out["short_usd"], out["long_usd"] = side_tot[True], side_tot[False]
    return out


def entry_disp(positions: list[dict]) -> dict:
    """
    Разброс фактических цен входа (аудитор, 17.10): взвешенное размером (BTC) стандартное отклонение
    ln(цены входа) ×100 — ≈ % от цены, та же единица, что сырой разброс столпа 2. Все позиции снимка
    (тот же состав, что у D), по сторонам и вместе. Только открытые позиции — выжившие (ENTRY_NOTE).
    """
    out = {"entry_n": 0}
    for key, sel in (("entry_disp", lambda p: True), ("entry_disp_long", lambda p: p["szi"] > 0),
                     ("entry_disp_short", lambda p: p["szi"] < 0)):
        xs = [(abs(p["szi"]), math.log(p["entry"])) for p in positions
              if sel(p) and p.get("szi") and (p.get("entry") or 0) > 0]
        if key == "entry_disp":
            out["entry_n"] = len(xs)
        w = sum(a for a, _ in xs)
        if len(xs) < 5 or w <= 0:                           # техническое минимальное число, не порог сигнала
            out[key] = None
            continue
        m = sum(a * x for a, x in xs) / w
        out[key] = 100.0 * math.sqrt(sum(a * (x - m) ** 2 for a, x in xs) / w)
    return out


def snap_day(utc: str) -> str:
    """Сутки, к которым относится опрос: раньше 06:00 UTC — конец предыдущих (как btc_index.hl_day)."""
    return (datetime.strptime(utc[:13], "%Y%m%d_%H%M") - timedelta(hours=6)).date().isoformat()


def prev_snapshot(out: str, utc: str) -> str | None:
    """Снимок, которым индекс закрывает ПРЕДЫДУЩИЕ сутки (последний опрос этих суток), в out/hl/ГГГГ/ММ/."""
    want = (datetime.fromisoformat(snap_day(utc)) - timedelta(days=1)).date().isoformat()
    best = None
    for mon in {want[:7].replace("-", ""), utc[:6]}:
        d = os.path.join(out, "hl", mon[:4], mon[4:6])
        if not os.path.isdir(d):
            continue
        for f in os.listdir(d):
            m = re.match(r"hl_(\d{8}_\d{4})_(daily|full)\.json(\.gz)?$", f)
            if m and m.group(1) < utc[:13] and snap_day(m.group(1)) == want and (best is None or m.group(1) > best[0]):
                best = (m.group(1), os.path.join(d, f))
    return best[1] if best else None


def decompose(positions: list[dict], oi: float, mark: float, sigma: float, curve: dict, d_now, t_now,
              prev_path: str | None) -> dict:
    """
    Изменение D и T к прошлым суткам (аудитор, 17.10): ценовая часть — вчерашний состав по сегодняшним
    цене и σ минус вчерашнее значение; позиционная — сегодняшнее значение минус вчерашний состав по
    сегодняшней цене. Сумма частей равна изменению точно. Вчера полный опрос — в позиционной части и смена
    состава выборки (prev_kind).
    """
    if not prev_path or d_now is None:
        return {}
    snap = _read_snap(prev_path)
    s0, pp = snap.get("summary") or {}, snap.get("positions") or []
    if not pp or not s0.get("sigma") or not s0.get("mark"):
        return {}
    a = summarize(pp, s0.get("oi_btc"), s0["mark"], s0["sigma"], curve, sens=False)    # вчера как было
    b = summarize(pp, oi, mark, sigma, curve, sens=False)                               # вчерашний состав, сегодня
    if a.get("d") is None or b.get("d") is None:
        return {}
    out = {"prev_utc": snap.get("utc"), "prev_kind": snap.get("kind") or ("full" if _is_full(snap) else "daily"),
           "d_prev": a["d"], "d_px": b["d"] - a["d"], "d_pos": d_now - b["d"]}
    if t_now is not None and a.get("t") is not None and b.get("t") is not None:
        out.update(t_prev=a["t"], t_px=b["t"] - a["t"], t_pos=t_now - b["t"])
    return out


def print_extras(s: dict) -> None:
    """Разложение изменения D и разброс фактических входов — печать (аудитор, 17.10)."""
    if s.get("d_prev") is not None and s.get("d") is not None:
        full = "; вчера полный опрос — в позиционной части и смена состава выборки" if s.get("prev_kind") == "full" else ""
        print(f"Изменение D к прошлым суткам ({s.get('prev_utc')}): {s['d'] - s['d_prev']:+.2f} = цена {s['d_px']:+.2f} "
              f"+ позиции {s['d_pos']:+.2f}{full}")
        if s.get("t_prev") is not None:
            print(f"   T: {s['t'] - s['t_prev']:+.2f} = цена {s['t_px']:+.2f} + позиции {s['t_pos']:+.2f}")
    if s.get("entry_disp") is not None:
        lo, sh = s.get("entry_disp_long"), s.get("entry_disp_short")
        print(f"Разброс фактических цен входа: {s['entry_disp']:.2f}% (лонги {'—' if lo is None else f'{lo:.2f}%'}, "
              f"шорты {'—' if sh is None else f'{sh:.2f}%'}; позиций {s.get('entry_n')})")
        print("   " + ENTRY_NOTE)


def extras(positions: list[dict], oi: float, mark: float, sigma: float, curve: dict, s: dict,
           out: str, utc: str) -> dict:
    """Дополнения строки 17.10; сбой здесь не мешает основной строке (печать и пустые колонки)."""
    res = {}
    try:
        res.update(entry_disp(positions))
    except Exception as exc:                                 # noqa: BLE001
        print(f"[!] разброс входов не посчитан ({str(exc)[:80]})")
    try:
        res.update(decompose(positions, oi, mark, sigma, curve, s.get("d"), s.get("t"), prev_snapshot(out, utc)))
    except Exception as exc:                                 # noqa: BLE001
        print(f"[!] разложение изменения D не посчитано ({str(exc)[:80]})")
    return res


def ladder(positions: list[dict], oi: float, mark: float, sigma: float, curve: dict,
           whole: dict | None = None) -> dict | None:
    """
    Лестница глубин (аудитор, 15.10 вечер): D и T по крупнейшим N счетам таблицы
    лидеров (HLP — во всех ступенях) и по всем опрошенным. Знак D УСТАНОВЛЕН,
    только если он одинаков на всех ступенях, где D есть; иначе «нет». Порога нет.
    В ежедневном снимке ступени выше 2000 — держатели из полного опроса с этим рангом.
    """
    if not positions or not all("rank" in p for p in positions):
        return None
    steps = {}
    for n in LADDER:
        sub = [p for p in positions if p["rank"] < n]
        z = summarize(sub, oi, mark, sigma, curve, sens=False) if sub else {}
        steps[str(n)] = {"d": z.get("d"), "t": z.get("t"), "n": len(sub)}
    w = whole or summarize(positions, oi, mark, sigma, curve, sens=False)
    steps["all"] = {"d": w.get("d"), "t": w.get("t"), "n": len(positions)}
    sg = {(v["d"] > 0) - (v["d"] < 0) for v in steps.values() if v["d"] is not None}
    # Правило аудитора 17.10: запасной путь не даёт более уверенного исхода, чем основной. Ступень без D —
    # не согласие, а нехватка данных: знак не устанавливается (раньше знак ставился по оставшимся ступеням).
    full = all(v["d"] is not None for v in steps.values())
    sign = ("+" if 1 in sg else "-") if full and len(sg) == 1 and 0 not in sg else "нет"
    return {"steps": steps, "sign": sign, "complete": full}


def print_ladder(lad: dict | None) -> None:
    if not lad:
        print("Лестница глубин: в снимке нет ранга счёта — не считается")
        return
    st = lad["steps"]
    cells = " / ".join("—" if st[k]["d"] is None else f"{st[k]['d']:+.2f}" for k in st)
    names = " / ".join(("все" if k == "all" else f"{int(k):,}".replace(",", " ")) for k in st)
    verdict = {"+": "одинаков на всех ступенях: плюс — знак установлен",
               "-": "одинаков на всех ступенях: минус — знак установлен"}.get(
        lad["sign"], "разный — знак НЕ установлен" if lad.get("complete", True) else
        "D посчитан не на всех ступенях — знак НЕ установлен")
    print(f"Лестница глубин D ({names}): {cells} — {verdict}")
    print("   Уровни D и T — свойства таблицы лидеров и не сопоставимы ни с чем вне неё; ряд внутри себя "
          "сопоставим (состав постоянен); знак пригоден там, где устойчив к глубине (аудитор, 15.10).")


def d_state(d, d_wo_top, sign_ladder) -> str | None:
    """
    Три состояния знака D (аудитор, 16.10; печать, не вето):
      «установлено»   — лестница согласна и знак сохраняется без крупнейшего вкладчика стороны;
      «одним счётом»  — лестница согласна, но без крупнейшего вкладчика знак другой (или ноль);
      «не установлено» — ступени лестницы расходятся.
    Когда знак у композита будет включён: первые два состояния знак дают, третье — нет.
    """
    if d is None or not sign_ladder:
        return None
    if sign_ladder not in ("+", "-"):
        return D_STATES[2]
    if d_wo_top is None or d_wo_top == 0 or (d_wo_top > 0) != (d > 0):
        return D_STATES[1]
    return D_STATES[0]


def top_text(s: dict) -> str:
    """Счёт, задающий знак: «изолированный лонг $149,9 млн, 40x, 1,05σ, 55% закрытий лонгов»."""
    if s.get("d_lead") not in ("long", "short"):                # стороны нет — не угадывать (правило 17.10)
        return "крупнейший счёт: сторона в строке не записана"
    lg = s.get("d_lead") == "long"
    kind = {"isolated": "изолированный ", "cross": "кросс-"}.get(s.get("d_top_type") or "", "")
    lev = f", {s['d_top_lev']}x" if s.get("d_top_lev") else ""
    k = f", {s['d_top_k']:.2f}σ до ликвидации" if s.get("d_top_k") is not None else ""
    rank = s.get("d_top_rank")
    rk = "" if rank is None else (", HLP" if rank < 0 else f", ранг {rank}")
    return (f"{kind}{'лонг' if lg else 'шорт'} ${(s.get('d_top_usd') or 0) / 1e6:,.1f} млн{lev}{k}, "
            f"{100 * (s.get('d_top_share') or 0):.0f}% ожидаемых закрытий {'лонгов' if lg else 'шортов'}{rk}")


def print_state(s: dict) -> None:
    """Направление D в трёх состояниях и доли крупнейших вкладчиков сторон (аудитор, 16.10)."""
    lad = s.get("ladder")
    st = d_state(s.get("d"), s.get("d_wo_top"), (lad or {}).get("sign"))
    if st is None:
        return
    dirn = "вверх (уязвимее шорты)" if s["d"] > 0 else "вниз (уязвимее лонги)"
    if st == D_STATES[0]:
        print(f"Направление D: {dirn} — установлено (ступени согласны, без крупнейшего вкладчика знак тот же)")
    elif st == D_STATES[1]:
        print(f"Направление D: {dirn} — задано одним счётом: {top_text(s)}")
    else:
        print("Направление D: не установлено (ступени лестницы расходятся)")
    f = lambda v: "—" if v is None else f"{100 * v:.0f}%"
    print(f"   Доля крупнейшего вкладчика в ожидаемых закрытиях стороны: шорты {f(s.get('top_share_up'))}, "
          f"лонги {f(s.get('top_share_down'))} (широкое направление или единичное; порога нет)")
    print("   Лестница проверяет состав выборки, хрупкость — концентрацию; одна не заменяет другую.")


def print_summary(s: dict, top) -> None:
    print(f"\nHYPERLIQUID, BTC — цена {s['mark']:,.0f}; открытый интерес {s['oi_btc']:,.0f} BTC "
          f"(${s['oi_btc'] * s['mark'] / 1e9:.2f} млрд)")
    print(f"Счетов просмотрено {top}, позиций BTC найдено {s['n_pos']}")
    print(f"ПОКРЫТИЕ: лонги {s['long_btc']:,.0f} BTC ≈ {200 * s['cover_long']:.0f}% всех лонгов рынка, "
          f"шорты {s['short_btc']:,.0f} BTC ≈ {200 * s['cover_short']:.0f}% всех шортов "
          f"(к полю openInterest биржи — {100 * s['cover_long']:.0f}% / {100 * s['cover_short']:.0f}%: поле — сумма обеих сторон, №82)")
    if s["cover_long"] and s["cover_long"] > 0.525:
        print("   [!] Лонги выборки больше половины поля openInterest (> 100% рынка) — проверить единицы")
    print(f"Без цены ликвидации (залог не даёт ликвидации): лонги ${s['no_liq_long_usd'] / 1e6:,.0f} млн, "
          f"шорты ${s['no_liq_short_usd'] / 1e6:,.0f} млн")
    print(f"Типичный дневной ход (30 дней): {100 * s['sigma']:.2f}%\n")
    for side, name in (("short", "СТОПЫ ШОРТОВ (сверху)"), ("long", "СТОПЫ ЛОНГОВ (снизу)")):
        z = s[side]
        if not z:
            print(f"{name}: в пределах 8 дневных ходов нет")
            continue
        n = z["near"]
        print(f"{name}: самая плотная зона {z['dist_pct']:+.1f}% от цены "
              f"(окно {z['lo_pct']:+.1f}…{z['hi_pct']:+.1f}%; ${z['size_usd'] / 1e6:,.1f} млн, "
              f"{100 * z['share']:.0f}% стопов стороны), "
              f"{z['k']:.1f} дн. хода → дойти за 7 дней: {100 * z['p7']:.0f}%")
        print(f"   объём стопов в пределах 2% / 5% / 10%: ${n[2] / 1e6:,.0f} / ${n[5] / 1e6:,.0f} / "
              f"${n[10] / 1e6:,.0f} млн")
        print(f"   в зоне счетов: {z['n_acc']}, крупнейший — {100 * z['top_share']:.0f}% зоны; "
              f"позиций стороны в пределах 8 ходов: {z['n_reach']}")
    print_d(s)
    if s["pillar3"] is not None:
        alt = [(n, (s[sd] or {}).get("p3_without_top")) for sd, n in (("short", "шортов"), ("long", "лонгов"))]
        alt = [(n, v) for n, v in alt if v is not None]
        print(f"Зонная версия «7 дней» (справочно; отведена 15.10 — размер входил только через выбор зоны, "
              f"№72): {s['pillar3']:+.0f}" + ("; без крупнейшего счёта зоны " +
                                             ", ".join(f"{n} {v:+.0f}" for n, v in alt) if alt else ""))
    if s.get("cum"):
        c = s["cum"]
        lim = " / ".join(str(k) for k in CUM_K)
        f = lambda key: " / ".join(f"{100 * c[str(k)][key]:.1f}%" for k in CUM_K)
        v = " / ".join("—" if c[str(k)]["value"] is None else f"{c[str(k)]['value']:+.0f}" for k in CUM_K)
        print(f"Кумулятивная версия (доля стороны со стопами в пределах {lim} дн. ходов; плюс — шорты):")
        print(f"   шорты {f('short')}, лонги {f('long')} → {v}   "
              f"(база: шорты ${s['short_usd'] / 1e6:,.0f} млн, лонги ${s['long_usd'] / 1e6:,.0f} млн)")
        if s.get("d"):
            diff = [str(k) for k in CUM_K if c[str(k)]["value"] is not None
                    and (c[str(k)]["value"] > 0) != (s["d"] > 0)]
            if diff:
                print(f"   [!] Знак расходится с D в пределах {', '.join(diff)} дн. ходов — "
                      f"печатается расхождение, версия не выбирается")
    print("   Выборка: крупнейшие счета Hyperliquid из таблицы лидеров и HLP. Не весь рынок.")


def print_d(s: dict) -> None:
    """Три линии столпа 3 (аудитор, 15.10): D — направление, T — величина, хрупкость."""
    if s.get("d") is None:
        print("\nСТОЛП 3 (D): открытого интереса в выборке нет — не считается")
        return
    base = s["oi_sample_usd"]
    up, dn = 100 * s["r_up_usd"] / base, 100 * s["r_down_usd"] / base
    print(f"\nСТОЛП 3, версия D — ожидаемый объём принудительных закрытий за 7 дней, % от открытого "
          f"интереса выборки (${base / 1e9:.2f} млрд):")
    print(f"   стопы шортов (сверху): ${s['r_up_usd'] / 1e6:,.1f} млн = {up:.2f}%;   "
          f"стопы лонгов (снизу): ${s['r_down_usd'] / 1e6:,.1f} млн = {dn:.2f}%")
    print(f"   D (направление) = {up:.2f} − {dn:.2f} = {s['d']:+.2f}   — плюс: уязвимее шорты (цена скорее "
          f"прыгнет вверх к их стопам); минус — лонги (вниз сквозь их стопы). Входит в индекс.")
    print(f"   T (величина риска) = {up:.2f} + {dn:.2f} = {s['t']:.2f}   — сколько выборки под угрозой "
          f"в обе стороны; в индекс не входит.")
    if s.get("d_wo_top") is not None:
        same = (s["d_wo_top"] > 0) == (s["d"] > 0) and s["d_wo_top"] != 0
        side = "шортов" if s["d_lead"] == "short" else "лонгов"
        print(f"   Хрупкость: без крупнейшего счёта стороны знака ({side}; позиция "
              f"${s['d_top_usd'] / 1e6:,.1f} млн, {100 * s['d_top_share']:.0f}% ожидаемого объёма стороны) "
              f"D = {s['d_wo_top']:+.2f} — " + ("знак сохраняется" if same else "[!] знак меняется"))
    print("   " + D_LIMIT)


def print_hlp(rows: list, oi: float, mark: float) -> None:
    print(f"\nХРАНИЛИЩЕ HLP: адресов {len(rows)} (родитель и дочерние)")
    tot = {True: 0.0, False: 0.0}
    for a, p, note in rows:
        if p:
            side = p["szi"] > 0
            tot[side] += abs(p["szi"])
            liq = f"ликвидация {p['liq']:,.0f}" if p["liq"] else "без цены ликвидации"
            print(f"   {a[:10]}…: {'лонг' if side else 'шорт'} {abs(p['szi']):,.2f} BTC "
                  f"(${abs(p['szi']) * mark / 1e6:,.1f} млн, {100 * abs(p['szi']) / oi:.1f}% интереса), "
                  f"{liq}; {note}")
        else:
            print(f"   {a[:10]}…: {note}")
    print(f"   Итого HLP: лонги {tot[True]:,.1f} BTC, шорты {tot[False]:,.1f} BTC "
          f"({100 * (tot[True] + tot[False]) / oi:.1f}% интереса)")


# --------------------------------------------------------------------------
# Прогон
# --------------------------------------------------------------------------

def cuts_for(n: int) -> tuple:
    return tuple(c for c in CUTS if c < n) + (n,)


def stability(positions: list[dict], oi: float, mark: float, sigma: float, curve: dict,
              cuts=None) -> None:
    """Совпадает ли картина у N крупнейших счетов при разном N."""
    have_rank = cuts is not None and all("rank" in p for p in positions)
    print("\nУСТОЙЧИВОСТЬ — та же картина по меньшей выборке:")
    if not have_rank:
        print("   (в снимке нет ранга счёта — режу по порядку позиций: доля 25/50/100%)")
    elif any(p["rank"] < 0 for p in positions):
        print("   (позиции HLP входят во все строки)")
    print(f"   {'выборка':>16}{'покр. Л/Ш':>12}{'шорты: зона, P7':>20}{'лонги: зона, P7':>20}"
          f"{'зонная':>8}{'D':>8}{'T':>7}")
    n = len(positions)
    variants = ([(f"топ {c}", [p for p in positions if p["rank"] < c]) for c in cuts] if have_rank
                else [(f"{q}% позиций", positions[: max(1, n * q // 100)]) for q in (25, 50, 100)])
    for name, sub in variants:
        if not sub:
            continue
        z = summarize(sub, oi, mark, sigma, curve, sens=False)
        f = lambda w: f"{w['dist_pct']:+.1f}%, {100 * w['p7']:.0f}%" if w else "—"
        p3 = "—" if z["pillar3"] is None else f"{z['pillar3']:+.0f}"
        dv = "—" if z["d"] is None else f"{z['d']:+.2f}"
        tv = "—" if z["t"] is None else f"{z['t']:.2f}"
        print(f"   {name:>16}{100 * z['cover_long']:>6.0f}/{100 * z['cover_short']:<5.0f}"
              f"{f(z['short']):>20}{f(z['long']):>20}{p3:>8}{dv:>8}{tv:>7}")
    print("   Если строки похожи — меньшая выборка даёт ту же картину, и неполное")
    print("   покрытие, скорее всего, картину не искажает. Если расходятся — искажает.")
    if have_rank:
        print_ladder(ladder(positions, oi, mark, sigma, curve))


def fetch_one(rank_addr):
    rank, a = rank_addr
    try:
        p = btc_position(post({"type": "clearinghouseState", "user": a}))
    except Exception as exc:                              # noqa: BLE001
        return rank, a, None, str(exc)[:60]
    return rank, a, p, None


def add_hlp(positions: list, polled: list[str]) -> list:
    """Позиции HLP: отмечает уже найденные, дозапрашивает те, что не в выборке."""
    try:
        addrs = hlp_addresses()
    except Exception as exc:                              # noqa: BLE001
        print(f"[!] HLP: запрос vaultDetails не прошёл: {str(exc)[:60]}")
        return []
    seen = {a.lower() for a in polled}
    by_addr = {p["addr"].lower(): p for p in positions}
    out = []
    for a in addrs:
        low = a.lower()
        if low in by_addr:
            by_addr[low]["hlp"] = True
            out.append((a, by_addr[low], "уже в выборке (таблица лидеров)"))
        elif low in seen:
            out.append((a, None, "в таблице лидеров, позиции BTC нет"))
        else:
            _, _, p, err = fetch_one((-1, a))
            if err:
                out.append((a, None, f"нет ответа: {err}"))
            elif p:
                p.update(addr=a, rank=-1, hlp=True)
                positions.append(p)
                out.append((a, p, "добавлен в выборку"))
            else:
                out.append((a, None, "позиции BTC нет"))
    return out


def sigma_at(rows: list, snap_ms: int) -> float | None:
    """Дневной ход σ по дням, закрытым к моменту снимка (свеча [t, t+1д) закрыта при t+1д ≤ снимка)."""
    idx = None
    for i, r in enumerate(rows):
        if r[0] + TC.DAY <= snap_ms:
            idx = i
    return None if idx is None else TC.daily_vol([r[3] for r in rows], idx)


def _cb_daily(days: int = 60) -> list:
    """Дневные свечи BTC-USD Coinbase: (t_ms, high, low, close) по возрастанию.
    Coinbase отдаёт [время_с, low, high, open, close, объём], новые первыми."""
    end = datetime.now(timezone.utc)
    r = requests.get(COINBASE, params={"granularity": 86400, "start": (end - timedelta(days=days)).isoformat(),
                                       "end": end.isoformat()},
                     headers={"User-Agent": "btc-hl-collector"}, timeout=30)
    r.raise_for_status()
    return sorted((int(c[0]) * 1000, float(c[2]), float(c[1]), float(c[4])) for c in r.json())


def stamp_ms(stamp: str) -> int:
    return int(datetime.strptime(stamp[:13], "%Y%m%d_%H%M").replace(tzinfo=timezone.utc).timestamp() * 1000)


def poll(top: int, workers: int, known: bool, active: bool = False) -> dict:
    """
    Сетевая часть: рынок, таблица лидеров, позиции BTC, HLP. Без расчёта столпа.
    active — добавить счета с оборотом за сутки > 0 (кто торговал, тот мог
    открыть позицию; проверка аудитора 13.10: таких 5–10 тыс. из ~47 тыс.).
    """
    oi, mark = btc_market()
    lb = leaderboard_rows()
    full = [x["ethAddress"] for x in lb]
    n_top = len(full) if top <= 0 else min(top, len(full))
    picked = list(enumerate(full[:n_top]))
    what, utc_k = f"{n_top} крупнейших", None
    if known:
        utc_k, ks = load_known()
        if not ks:
            sys.exit("Нет списка держателей BTC — сначала полный опрос: py btc_hl_stops.py run --top 0")
        extra = [(i, a) for i, a in enumerate(full[n_top:], n_top) if a.lower() in ks]
        picked += extra
        what += f" + {len(extra)} держателей BTC (список от {utc_k})"
    n_active, act = None, None
    if active:
        act = {x["ethAddress"].lower() for x in lb if day_volume(x) > 0}
        n_active = len(act)
        chosen = {a.lower() for _, a in picked}
        extra2 = [(i, a) for i, a in enumerate(full[n_top:], n_top) if a.lower() in act and a.lower() not in chosen]
        picked = sorted(picked + extra2)
        what += f" + {len(extra2)} торговавших за сутки (всего торговавших {n_active})"
    addrs = [a for _, a in picked]
    print(f"Опрашиваю {len(addrs)} счетов ({what}); ≈ {len(addrs) / MAX_PER_MIN:.0f} мин")
    step = max(250, len(addrs) // 20)
    positions, errors, first_err, t0, done = [], 0, None, time.time(), 0
    no_pos = set()
    t_start = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for rank, a, p, err in ex.map(fetch_one, picked):
            done += 1
            if err:
                errors += 1
                first_err = first_err or err
            elif not p:
                no_pos.add(a.lower())
            else:
                p["addr"], p["rank"] = a, rank
                positions.append(p)
            if done % step == 0:
                print(f"   {done}/{len(addrs)} счетов, позиций BTC {len(positions)}, "
                      f"{(time.time() - t0) / 60:.1f} мин")
    if errors:
        print(f"[!] Не ответили {errors} счетов из {len(addrs)}; пример: {first_err}")
    minutes = (time.time() - t0) / 60
    print(f"Опрос занял {minutes:.1f} мин")
    hlp = add_hlp(positions, addrs)
    t_end = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {"oi": oi, "mark": mark, "full": full, "n_top": n_top, "addrs": addrs, "utc_k": utc_k,
            "positions": positions, "hlp": hlp, "errors": errors, "minutes": minutes,
            "t_start": t_start, "t_end": t_end, "no_pos": no_pos, "n_active": n_active, "act": act}


def run(top: int, workers: int, known: bool = False) -> None:
    if requests is None:
        sys.exit("Нужен пакет requests: py -m pip install requests")
    cpath = os.path.join(HERE, "p3_touch_curve.csv")
    if not os.path.isfile(cpath):
        sys.exit("Нет p3_touch_curve.csv — сначала: py btc_touch_curve.py run")
    curve = MT.load_curve(cpath)
    rows = TC.load(False)
    sigma = sigma_at(rows, int(time.time() * 1000))
    P = poll(top, workers, known)
    oi, mark, full, n_top, addrs, utc_k = P["oi"], P["mark"], P["full"], P["n_top"], P["addrs"], P["utc_k"]
    positions, hlp = P["positions"], P["hlp"]
    s = summarize(positions, oi, mark, sigma, curve)
    print_summary(s, len(addrs))
    if hlp:
        print_hlp(hlp, oi, mark)
    stability(positions, oi, mark, sigma, curve, cuts_for(len(full) if known else n_top))
    os.makedirs(SNAP_DIR, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    path = os.path.join(SNAP_DIR, f"hl_{stamp}.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump({"utc": stamp, "top": n_top, "polled": len(addrs), "leaderboard": len(full),
                   "known_from": utc_k, "hlp": [a for a, _, _ in hlp],
                   "t_start": P["t_start"], "t_end": P["t_end"],
                   "summary": s, "positions": positions}, fh)
    print(f"Снимок сохранён: {os.path.relpath(path, HERE)}")
    if n_top == len(full):
        save_known([p for p in positions if p["rank"] >= 0], stamp)
        print(f"Список держателей BTC для ежедневного опроса обновлён: hl_known.json "
              f"({len([p for p in positions if p['rank'] >= 0])} счетов)")


# --------------------------------------------------------------------------
# Облачный сборщик (GitHub Actions) и забор снимков на ноутбук — шаг 2а
# --------------------------------------------------------------------------

def _day_files(out: str, day: str) -> list[str]:
    d = os.path.join(out, "hl", day[:4], day[4:6])
    return sorted(f for f in os.listdir(d) if f.startswith(f"hl_{day}_")) if os.path.isdir(d) else []


LOG_COLS = ["utc", "kind", "polled", "positions", "errors", "minutes", "cover_long", "cover_short",
            "oi_btc", "mark", "active", "known", "t_start", "t_end"]


def _append_csv(path: str, row: dict, cols: list) -> None:
    """Дописать строку; старый заголовок дополняется новыми колонками без потери строк."""
    rows = []
    if os.path.isfile(path):
        with open(path, encoding="utf-8", newline="") as fh:
            rows = list(csv.DictReader(fh))
    rows.append(row)
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, restval="")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in cols})


def _append_log(path: str, row: dict) -> None:
    _append_csv(path, row, LOG_COLS)


def _near(s: dict, side: str, t: int):
    nd = (s.get(side) or {}).get("near") or {}
    return nd.get(t, nd.get(str(t)))


def pillar_row(stamp: str, kind: str, s: dict, sig_src: str, polled: int) -> dict:
    """Строка ежедневного ряда столпа 3 (для индекса)."""
    f = lambda x, n=4: "" if x is None else f"{x:.{n}f}"
    sh, lo, c = s.get("short") or {}, s.get("long") or {}, s.get("cum") or {}
    return {"utc": stamp, "kind": kind, "sigma": f(s["sigma"], 5), "sigma_src": sig_src,
            "pillar3": f(s["pillar3"], 2), "p_short": f(sh.get("p7")), "p_long": f(lo.get("p7")),
            "zone_short_pct": f(sh.get("dist_pct"), 2), "zone_long_pct": f(lo.get("dist_pct"), 2),
            "zone_short_usd": f(sh.get("size_usd"), 0), "zone_long_usd": f(lo.get("size_usd"), 0),
            "acc_short": sh.get("n_acc", ""), "acc_long": lo.get("n_acc", ""),
            "top_short": f(sh.get("top_share"), 3), "top_long": f(lo.get("top_share"), 3),
            "p3_wo_top_short": f(sh.get("p3_without_top"), 2), "p3_wo_top_long": f(lo.get("p3_without_top"), 2),
            "cum1": f((c.get("1") or {}).get("value"), 2), "cum2": f((c.get("2") or {}).get("value"), 2),
            "cum4": f((c.get("4") or {}).get("value"), 2),
            "cover_long": f(s["cover_long"]), "cover_short": f(s["cover_short"]),
            "positions": s["n_pos"], "polled": polled,
            "zone_short_lo": f(sh.get("lo_pct"), 2), "zone_short_hi": f(sh.get("hi_pct"), 2),
            "zone_long_lo": f(lo.get("lo_pct"), 2), "zone_long_hi": f(lo.get("hi_pct"), 2),
            "k_short": f(sh.get("k"), 3), "k_long": f(lo.get("k"), 3),
            "reach_short": sh.get("n_reach", ""), "reach_long": lo.get("n_reach", ""),
            "short_usd": f(s.get("short_usd"), 0), "long_usd": f(s.get("long_usd"), 0),
            **{f"cum{k}_{side}": f((c.get(str(k)) or {}).get(side), 5)
               for k in CUM_K for side in ("short", "long")},
            **{f"near{t}_{side}": f(_near(s, side, t), 0) for t in (2, 5, 10) for side in ("short", "long")},
            "mark": f(s.get("mark"), 1), "oi_btc": f(s.get("oi_btc"), 2), "instrument": "Hyperliquid",
            "d": f(s.get("d")), "t": f(s.get("t")), "d_wo_top": f(s.get("d_wo_top")),
            "d_lead": s.get("d_lead") or "", "d_top_share": f(s.get("d_top_share")),
            "r_up_usd": f(s.get("r_up_usd"), 0), "r_down_usd": f(s.get("r_down_usd"), 0),
            "oi_sample_usd": f(s.get("oi_sample_usd"), 0),
            **{f"{k}_l{n}": f(((s.get("ladder") or {}).get("steps", {}).get(str(n)) or {}).get(k))
               for k in ("d", "t") for n in LADDER},
            "d_sign_ladder": (s.get("ladder") or {}).get("sign", ""),
            "d_state": d_state(s.get("d"), s.get("d_wo_top"), (s.get("ladder") or {}).get("sign")) or "",
            "top_share_up": f(s.get("top_share_up")), "top_share_down": f(s.get("top_share_down")),
            "d_top_usd": f(s.get("d_top_usd"), 0), "d_top_type": s.get("d_top_type") or "",
            "d_top_lev": "" if s.get("d_top_lev") is None else s["d_top_lev"],
            "d_top_k": f(s.get("d_top_k"), 3), "d_top_dist": f(s.get("d_top_dist"), 3),
            "d_top_rank": "" if s.get("d_top_rank") is None else s["d_top_rank"],
            "entry_disp": f(s.get("entry_disp"), 3), "entry_disp_long": f(s.get("entry_disp_long"), 3),
            "entry_disp_short": f(s.get("entry_disp_short"), 3), "entry_n": s.get("entry_n", ""),
            "prev_utc": s.get("prev_utc") or "", "prev_kind": s.get("prev_kind") or "",
            **{k: f(s.get(k)) for k in ("d_prev", "d_px", "d_pos", "t_prev", "t_px", "t_pos")}}


def collect(mode: str, top: int, workers: int, out: str, force: bool = False, now=None,
            active: bool = True) -> int:
    """
    Сбор позиций BTC, открытого интереса, цены, HLP и расчёт столпа 3
    (σ по Coinbase — Binance серверам GitHub отказывает).
    auto: по воскресеньям (UTC), без списка держателей или если полному опросу
    больше 8 суток — полный опрос; иначе ежедневный: топ N + держатели BTC +
    (active) торговавшие за сутки. Список держателей обновляется каждый раз:
    кто опрошен без позиции BTC — выбывает, у кого позиция нашлась — входит.
    Второй запуск за те же сутки пропускается (ежедневный — если снимок за
    сутки уже есть; полный — если уже есть полный).
    """
    global KNOWN_FILE
    now = now or datetime.now(timezone.utc)
    KNOWN_FILE = os.path.join(out, "hl_known.json")
    utc_k, ks, full_utc = None, set(), None
    if os.path.isfile(KNOWN_FILE):
        with open(KNOWN_FILE, encoding="utf-8") as fh:
            k = json.load(fh)
        utc_k, ks, full_utc = k["utc"], set(k["addresses"]), k.get("full_utc", k["utc"])
    if mode == "auto":
        stale = full_utc is None or (now - datetime.strptime(full_utc[:13], "%Y%m%d_%H%M")
                                     .replace(tzinfo=timezone.utc)).days >= 8
        mode = "full" if (now.weekday() == 6 or not ks or stale) else "daily"
    if mode == "daily" and not ks:
        print("Списка держателей нет — делаю полный опрос")
        mode = "full"
    day = now.strftime("%Y%m%d")
    have = _day_files(out, day)
    if not force and (have if mode == "daily" else [f for f in have if f.endswith("_full.json.gz")]):
        print(f"Снимок за {day} ({mode}) уже есть: {have[-1]} — пропускаю")
        return 0
    print(f"Сбор Hyperliquid: режим {mode}, {now:%Y-%m-%d %H:%M} UTC")
    P = poll(top if mode == "daily" else 0, workers, mode == "daily", active)
    pos, n = P["positions"], len(P["addrs"])
    if not pos or P["errors"] > n / 2:
        print(f"[!] Сбор не удался: позиций {len(pos)}, без ответа {P['errors']} из {n} — снимок не сохранён")
        return 1
    L = sum(p["szi"] for p in pos if p["szi"] > 0)
    S = -sum(p["szi"] for p in pos if p["szi"] < 0)
    stamp = now.strftime("%Y%m%d_%H%M")
    summary = {"oi_btc": P["oi"], "mark": P["mark"], "sigma": None, "n_pos": len(pos),
               "long_btc": L, "short_btc": S, "cover_long": L / P["oi"], "cover_short": S / P["oi"]}
    # Столп 3 — прямо в облаке: σ по дневным свечам Coinbase (Binance серверам
    # GitHub отказывает), кривая — p3_touch_curve.csv из репозитория. Сбой здесь
    # не мешает сохранить снимок: тогда столп посчитает ноутбук (recompute).
    sig_src, cpath = None, os.path.join(HERE, "p3_touch_curve.csv")
    try:
        sigma = sigma_at(_cb_daily(), stamp_ms(stamp))
        if sigma and os.path.isfile(cpath):
            curve = MT.load_curve(cpath)
            summary = summarize(pos, P["oi"], P["mark"], sigma, curve)
            summary["ladder"] = ladder(pos, P["oi"], P["mark"], sigma, curve, whole=summary)
            summary.update(extras(pos, P["oi"], P["mark"], sigma, curve, summary, out, now.strftime("%Y%m%d_%H%M")))
            sig_src = summary["sigma_src"] = "coinbase"
    except Exception as exc:                              # noqa: BLE001
        print(f"[!] Столп 3 в облаке не посчитан ({str(exc)[:80]}) — посчитает ноутбук")
    daily_from = None
    if mode == "full":
        # Пометка «попал бы в ежедневный опрос» (топ N + список держателей + торговавшие
        # за сутки) — тот же момент, что и эталон: проверка глубины без разницы во времени
        dset = {a.lower() for a in P["full"][:top]} | ks | (P["act"] or set())
        for p in pos:
            p["daily"] = p["rank"] < 0 or p["addr"].lower() in dset
        daily_from = utc_k
    d = os.path.join(out, "hl", day[:4], day[4:6])
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, f"hl_{stamp}_{mode}.json.gz")
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        json.dump({"utc": stamp, "kind": mode, "top": P["n_top"], "polled": n, "leaderboard": len(P["full"]),
                   "known_from": P["utc_k"], "hlp": [a for a, _, _ in P["hlp"]], "errors": P["errors"],
                   "t_start": P["t_start"], "t_end": P["t_end"], "active": P["n_active"],
                   "minutes": round(P["minutes"], 1), "daily_top": top if mode == "full" else None,
                   "daily_known_from": daily_from, "summary": summary, "positions": pos}, fh)
    found = {p["addr"].lower() for p in pos if p["rank"] >= 0}
    if mode == "full":
        new_known, full_utc = found, stamp
    else:
        new_known = (ks - P["no_pos"]) | found
    with open(KNOWN_FILE, "w", encoding="utf-8") as fh:
        json.dump({"utc": stamp, "full_utc": full_utc, "addresses": sorted(new_known)}, fh)
    if sig_src:
        _append_csv(os.path.join(out, "pillar3_daily.csv"),
                    pillar_row(stamp, mode, summary, sig_src, n), P3_COLS)
        sm = summary
        print(f"Столп 3: D = {sm['d']:+.2f}, T = {sm['t']:.2f}" if sm.get("d") is not None else "Столп 3: D нет",
              f"(σ {100 * sm['sigma']:.2f}%, Coinbase); зонная (справочно)",
              "—" if sm["pillar3"] is None else f"{sm['pillar3']:+.1f}")
        print_ladder(sm.get("ladder"))
        print_state(sm)
        print_extras(sm)
    _append_log(os.path.join(out, "hl_log.csv"), {
        "utc": stamp, "kind": mode, "polled": n, "positions": len(pos), "errors": P["errors"],
        "minutes": f"{P['minutes']:.1f}", "cover_long": f"{L / P['oi']:.4f}",
        "cover_short": f"{S / P['oi']:.4f}", "oi_btc": f"{P['oi']:.2f}", "mark": f"{P['mark']:.1f}",
        "active": "" if P["n_active"] is None else P["n_active"], "known": len(new_known),
        "t_start": P["t_start"], "t_end": P["t_end"]})
    print(f"Снимок: {os.path.relpath(path, out)}; позиций {len(pos)}; покрытие рынка ≈ лонги "
          f"{200 * L / P['oi']:.0f}%, шорты {200 * S / P['oi']:.0f}% (поле openInterest — сумма обеих сторон, №82)")
    return 0


def _gh_tree() -> dict:
    r = requests.get(f"https://api.github.com/repos/{REPO}/git/trees/main?recursive=1", timeout=60)
    r.raise_for_status()
    return r.json()


def _gh_raw(path: str) -> bytes:
    r = requests.get(f"https://raw.githubusercontent.com/{REPO}/main/{path}", timeout=120)
    r.raise_for_status()
    return r.content


def sync() -> None:
    """Забрать на ноутбук новые снимки облачного сборщика в hl_snapshots/."""
    tree = _gh_tree()
    files = [t["path"] for t in tree.get("tree", []) if t.get("type") == "blob"
             and t["path"].startswith("data/hl/") and t["path"].endswith(".json.gz")]
    if tree.get("truncated"):
        print("[!] Список файлов GitHub обрезан — часть снимков не видна")
    os.makedirs(SNAP_DIR, exist_ok=True)
    new = [f for f in files if not os.path.isfile(os.path.join(SNAP_DIR, os.path.basename(f)))]
    for f in new:
        with open(os.path.join(SNAP_DIR, os.path.basename(f)), "wb") as fh:
            fh.write(_gh_raw(f))
    print(f"Снимков в облаке {len(files)}, забрано новых {len(new)}"
          + (f"; последний — {os.path.basename(sorted(files)[-1])}" if files else ""))
    if any(t["path"] == "data/hl_known.json" for t in tree.get("tree", [])):
        cloud = json.loads(_gh_raw("data/hl_known.json"))
        local = None
        if os.path.isfile(os.path.join(HERE, "hl_known.json")):
            with open(os.path.join(HERE, "hl_known.json"), encoding="utf-8") as fh:
                local = json.load(fh)
        if local is None or cloud["utc"] > local["utc"]:
            if local is not None:                      # прежний список — для проверки глубины
                with open(os.path.join(HERE, f"hl_known_{local['utc']}.json"), "w", encoding="utf-8") as fh:
                    json.dump(local, fh)
                print(f"Прежний список держателей сохранён: hl_known_{local['utc']}.json")
            with open(os.path.join(HERE, "hl_known.json"), "w", encoding="utf-8") as fh:
                json.dump(cloud, fh)
            print(f"Список держателей BTC обновлён из облака (на {cloud['utc']}, "
                  f"полный опрос {cloud.get('full_utc', cloud['utc'])})")


def depth(snap_path: str, known_path_arg: str, top: int = 2000) -> tuple:
    """
    Проверка глубины (аудитор, 13.10): воспроизводит ли ежедневный состав —
    топ N + держатели BTC из ПРЕДЫДУЩЕГО полного опроса — эталон (полный опрос).
    Держатели того же опроса дали бы совпадение по построению, поэтому список
    берётся более ранний: сохранённый sync список hl_known_ГГГГММДД_ЧЧММ.json
    или, если его нет, более ранний полный снимок в hl_snapshots/ (держатели —
    счета с позицией BTC в нём). Эталон по умолчанию — последний ПОЛНЫЙ снимок.
    """
    if snap_path == "latest":
        fulls = _full_snapshots()
        if not fulls:
            sys.exit("В hl_snapshots/ нет полного снимка")
        snap_path = fulls[-1][2]
    snap, s0, curve = _load_snap(snap_path)
    if not _is_full(snap):
        print(f"[!] Снимок {snap['utc']} — не полный опрос; эталон из него неполный")
    pos = [p for p in snap["positions"] if p.get("rank", 0) >= 0]
    flagged = any("daily" in p for p in pos)
    if known_path_arg == "auto":
        cands = [(f[9:22], os.path.join(HERE, f)) for f in os.listdir(HERE)
                 if f.startswith("hl_known_") and f.endswith(".json") and f[9:22] < snap["utc"]]
        cands += [(u, p) for u, _, p in _full_snapshots(before=snap["utc"])]
        if not cands and not flagged:
            sys.exit("Нет более раннего списка держателей: ни hl_known_ГГГГММДД_ЧЧММ.json, "
                     "ни более раннего полного снимка в hl_snapshots/")
        known_path_arg = max(cands)[1] if cands else None
    sets = [("эталон: все счета", pos)]
    if flagged:                              # облачный полный снимок с пометкой ежедневного состава
        sets.append((f"ежедневный состав (топ {snap.get('daily_top')} + список + торговавшие)",
                     [p for p in pos if p.get("daily")]))
    head = f"ПРОВЕРКА ГЛУБИНЫ — снимок {snap['utc']} ({snap.get('kind') or 'полный'})"
    if flagged:
        head += f"; список держателей ежедневного состава — от {snap.get('daily_known_from') or '—'}"
    if known_path_arg:
        k = _read_snap(known_path_arg)
        if "addresses" in k:
            ks, src = set(k["addresses"]), "список"
        else:
            ks, src = {p["addr"].lower() for p in k["positions"] if p.get("rank", 0) >= 0}, "полный снимок"
        print(f"Прежние держатели: {src} {os.path.basename(known_path_arg)}")
        sets.append((f"топ {top} + держатели от {k['utc']}",
                     [p for p in pos if p["rank"] < top or p["addr"].lower() in ks]))
        head += f"; прежние держатели от {k['utc']} ({len(ks)} счетов)"
    sets.append((f"топ {top}", [p for p in pos if p["rank"] < top]))
    print(head + "\n")
    print(f"   {'выборка':<56}{'позиций':>8}{'покр. Л/Ш':>11}{'D':>8}{'T':>7}{'D без топ':>10}"
          f"{'шорты: зона':>13}{'лонги: зона':>13}{'зонная':>8}{'в 2/5/10%, $ млн: шорты | лонги':>36}"
          f"{'кум. 1/2/4σ':>16}")
    for name, sub in sets:
        z = summarize(sub, s0["oi_btc"], s0["mark"], s0["sigma"], curve, sens=False)
        zs = lambda w: f"{w['dist_pct']:+.1f}%" if w else "—"
        near = lambda side: "/".join(f"{(z[side] or {}).get('near', {}).get(t, 0) / 1e6:.0f}" for t in (2, 5, 10))
        cum = "/".join("—" if z["cum"][str(kk)]["value"] is None else f"{z['cum'][str(kk)]['value']:+.0f}"
                       for kk in CUM_K)
        p3 = "—" if z["pillar3"] is None else f"{z['pillar3']:+.1f}"
        num = lambda v, fmt: "—" if v is None else format(v, fmt)
        print(f"   {name:<56}{len(sub):>8}{100 * z['cover_long']:>6.0f}/{100 * z['cover_short']:<4.0f}"
              f"{num(z['d'], '+.2f'):>8}{num(z['t'], '.2f'):>7}{num(z['d_wo_top'], '+.2f'):>10}"
              f"{zs(z['short']):>13}{zs(z['long']):>13}{p3:>8}{near('short') + ' | ' + near('long'):>36}{cum:>16}")
    print("\n   Строки ниже эталона сравниваются с ним: совпадают — состав годен. Главная колонка — D "
          "(сдвиг D с глубиной выборки — условие приёмки, аудитор 15.10).")
    return snap_path, known_path_arg


def lbstats() -> None:
    """Что есть в таблице лидеров по каждому счёту — можно ли опрашивать только активные."""
    r = requests.get(LEADERBOARD, timeout=120)
    r.raise_for_status()
    rows = r.json()["leaderboardRows"]
    print(f"Таблица лидеров: {len(rows)} счетов. Поля первой строки:")
    print("   " + json.dumps(rows[0], ensure_ascii=False)[:600])
    wins = {}
    for x in rows:
        for w in x.get("windowPerformances") or []:
            if isinstance(w, list) and len(w) == 2 and isinstance(w[1], dict):
                st = wins.setdefault(w[0], {"vlm>0": 0, "pnl!=0": 0})
                st["vlm>0"] += float(w[1].get("vlm") or 0) > 0
                st["pnl!=0"] += float(w[1].get("pnl") or 0) != 0
    for name, st in wins.items():
        print(f"   окно {name:<8}: оборот > 0 у {st['vlm>0']:>6} счетов, PnL ≠ 0 у {st['pnl!=0']:>6}")
    if not wins:
        print("   [!] Поля windowPerformances нет — активность по таблице не определить")


def sigmacheck(days: int = 60) -> None:
    """σ за 30 дней: Binance BTCUSDT (кривая откалибрована на нём) против Coinbase BTC-USD (облако)."""
    b, c = TC.load(False), _cb_daily(days + 40)
    now = int(time.time() * 1000)
    print("Дневной ход σ (30 дней), по дням, закрытым к 00:10 UTC:")
    print(f"   {'дата':<12}{'Binance':>9}{'Coinbase':>10}{'разница':>9}")
    diffs = []
    for k in range(days, -1, -1):
        t = (now // TC.DAY - k) * TC.DAY + 600_000
        sb, sc = sigma_at(b, t), sigma_at(c, t)
        if sb and sc:
            diffs.append(sc / sb - 1)
            if k < 10:
                print(f"   {datetime.fromtimestamp(t / 1000, timezone.utc):%Y-%m-%d}  {100 * sb:>7.3f}%"
                      f"{100 * sc:>9.3f}%{100 * (sc / sb - 1):>+8.1f}%")
    if diffs:
        print(f"   За {len(diffs)} дней: средняя разница {100 * sum(diffs) / len(diffs):+.2f}%, "
              f"наибольшая по модулю {100 * max(diffs, key=abs):+.2f}%")


def live_check() -> None:
    """Живые запросы (аудитор, 13.10: заглушка разрешает то, что площадка отвергает)."""
    oi, mark = btc_market()
    n = len(leaderboard_rows())
    cb = _cb_daily(40)
    assert oi > 0 and mark > 0 and n > 1000 and len(cb) >= 31, (oi, mark, n, len(cb))
    print(f"живая проверка: OK — Hyperliquid (OI {oi:,.0f} BTC, цена {mark:,.0f}), таблица лидеров "
          f"{n} счетов, Coinbase {len(cb)} дневных свечей, σ {100 * sigma_at(cb, int(time.time() * 1000)):.2f}%")


def latest_snapshot() -> str:
    fs = [f for f in os.listdir(SNAP_DIR) if f.startswith("hl_") and (f.endswith(".json") or f.endswith(".json.gz"))] \
        if os.path.isdir(SNAP_DIR) else []
    if not fs:
        sys.exit("В hl_snapshots/ снимков нет")
    return os.path.join(SNAP_DIR, max(fs, key=lambda f: f[3:16]))


def _load_snap(path: str):
    if path == "latest":
        path = latest_snapshot()
    snap = _read_snap(path)
    curve = MT.load_curve(os.path.join(HERE, "p3_touch_curve.csv"))
    s0 = snap["summary"]
    if s0.get("sigma") is None:                       # снимок облачного сборщика — σ считаем здесь
        s0["sigma"] = sigma_at(TC.load(False), stamp_ms(snap["utc"]))
    return snap, s0, curve


def recompute(path: str) -> None:
    """Пересчёт по сохранённому снимку без опроса биржи."""
    snap, s0, curve = _load_snap(path)
    s = summarize(snap["positions"], s0["oi_btc"], s0["mark"], s0["sigma"], curve)
    top = snap.get("top")
    print(f"Снимок {snap['utc']} UTC")
    print_summary(s, snap.get("polled") or top or "? (в старом снимке не записано)")
    span = snap.get("leaderboard") if snap.get("known_from") else top
    stability(snap["positions"], s0["oi_btc"], s0["mark"], s0["sigma"], curve,
              cuts_for(span) if span else None)
    s["ladder"] = ladder(snap["positions"], s0["oi_btc"], s0["mark"], s0["sigma"], curve, whole=s)
    print_state(s)


def cutoffs(path: str) -> list:
    """
    Чувствительность D к обрезке 6 / 8 / 12σ — один прогон, больше не повторять
    (аудитор, 15.10). Кривая вероятностей есть до 8σ; для 8–12σ две границы:
    вероятность как в 8σ (кривая убывает — это верхняя) и ноль (нижняя).
    """
    snap, s0, curve = _load_snap(path)
    pos = [p for p in snap["positions"] if p["liq"] is not None and p["liq"] > 0]
    mark, sig = s0["mark"], s0["sigma"]
    rows = [(100.0 * (p["liq"] / mark - 1.0), abs(p["szi"]) * mark, i) for i, p in enumerate(pos)]
    oi_s = sum(abs(p["szi"]) for p in snap["positions"]) * mark
    print(f"Снимок {snap['utc']} UTC: D при разной обрезке (σ {100 * sig:.2f}%, OI выборки "
          f"${oi_s / 1e9:.2f} млрд)")
    band = {True: 0.0, False: 0.0}
    for d, size, _ in rows:
        if MT.K_MAX < _k(d, sig) <= max(CUTOFFS):
            band[d > 0] += size
    print(f"   стопы между 8σ и 12σ: шорты ${band[True] / 1e6:,.0f} млн, лонги ${band[False] / 1e6:,.0f} млн "
          f"(до {100 * (math.exp(12 * sig) - 1):.0f}% вверх / {100 * (1 - math.exp(-12 * sig)):.0f}% вниз)")
    print(f"   {'обрезка':<34}{'R↑, $ млн':>11}{'R↓, $ млн':>11}{'D':>8}{'T':>7}{'D без топ':>10}")
    out = []
    for kmax in CUTOFFS:
        for tail in (("flat", "zero") if kmax > curve["k"][-1] else ("flat",)):
            R, contrib = expected_closures(rows, sig, curve, k_max=kmax, tail=tail)
            b = d_block(R, contrib, oi_s)
            name = f"{kmax:g}σ" + ("" if kmax <= curve["k"][-1] else
                                    ", за 8σ — как в 8σ (верхняя)" if tail == "flat" else ", за 8σ — ноль (нижняя)")
            wo = "—" if b["d_wo_top"] is None else f"{b['d_wo_top']:+.2f}"
            print(f"   {name:<34}{R[True] / 1e6:>11,.1f}{R[False] / 1e6:>11,.1f}{b['d']:>+8.2f}{b['t']:>7.2f}{wo:>10}")
            out.append((kmax, tail, b))
    print("   Один прогон для записи (аудитор, 15.10); обрезка остаётся 8σ.")
    return out


def find_snapshot(utc: str, kind: str, out: str = DATA_DIR) -> str | None:
    """Снимок по метке ряда: data/hl/ГГГГ/ММ/ (репозиторий) или hl_snapshots/ (ноутбук)."""
    names = [f"hl_{utc}_{kind}.json.gz", f"hl_{utc}.json", f"hl_{utc}_{kind}.json"]
    for d in (os.path.join(out, "hl", utc[:4], utc[4:6]), SNAP_DIR):
        for n in names:
            if os.path.isfile(os.path.join(d, n)):
                return os.path.join(d, n)
    return None


def refill(csv_path: str | None = None, out: str = DATA_DIR) -> int:
    """
    Дописать в ряд столпа 3 колонки, которых не было, когда строка писалась
    (например D и T до 15.10), — по сохранённому снимку и σ ИЗ САМОЙ СТРОКИ.
    Заполненные значения не трогаются; зонная версия пересчитывается для сверки:
    не совпала — строка не дописывается.
    """
    csv_path = csv_path or os.path.join(out, "pillar3_daily.csv")
    with open(csv_path, encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    curve = MT.load_curve(os.path.join(HERE, "p3_touch_curve.csv"))
    n_fill = 0
    for r in rows:
        empty = [c for c in P3_COLS if not r.get(c)]
        if not empty or not r.get("sigma"):
            continue
        path = find_snapshot(r["utc"], r["kind"], out)
        if path is None:
            print(f"   {r['utc']}: снимка нет — пропуск")
            continue
        snap = _read_snap(path)
        s0 = snap["summary"]
        sig = s0.get("sigma") or float(r["sigma"])         # в облачном снимке σ полной точности
        if abs(sig - float(r["sigma"])) > 6e-6:
            print(f"   [!] {r['utc']}: σ снимка {sig:.5f} не совпадает со строкой {r['sigma']} — пропуск")
            continue
        s = summarize(snap["positions"], s0["oi_btc"], s0["mark"], sig, curve)
        s["ladder"] = ladder(snap["positions"], s0["oi_btc"], s0["mark"], sig, curve, whole=s)
        s.update(extras(snap["positions"], s0["oi_btc"], s0["mark"], sig, curve, s, out, r["utc"]))
        prev_row = next((x for x in rows if x.get("utc") == s.get("prev_utc")), None)
        if prev_row and prev_row.get("d") and s.get("d_prev") is not None and abs(float(prev_row["d"]) - s["d_prev"]) > 1e-3:
            print(f"   [!] {r['utc']}: D прошлых суток по снимку {s['d_prev']:+.4f} ≠ строке ряда {prev_row['d']} — "
                  f"разложение не дописано")
            for k in ("prev_utc", "prev_kind", "d_prev", "d_px", "d_pos", "t_prev", "t_px", "t_pos"):
                s.pop(k, None)
        new = pillar_row(r["utc"], r["kind"], s, r.get("sigma_src", ""), r.get("polled", ""))
        if (r.get("pillar3") or "") != new["pillar3"] and not (
                r.get("pillar3") and new["pillar3"] and abs(float(new["pillar3"]) - float(r["pillar3"])) <= 0.011):
            print(f"   [!] {r['utc']}: зонная версия не воспроизводится ({r['pillar3']} против {new['pillar3']}) — "
                  f"строка не дописана")
            continue
        filled = [c for c in empty if new.get(c) not in (None, "")]
        for c in filled:
            r[c] = new[c]
        n_fill += bool(filled)
        print(f"   {r['utc']}: дописано {len(filled)} колонок; D = {r.get('d') or '—'}, T = {r.get('t') or '—'}"
              f" (зонная {r['pillar3']} воспроизведена)")
    with open(csv_path, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=P3_COLS, restval="")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in P3_COLS})
    print(f"Ряд {os.path.basename(csv_path)}: строк {len(rows)}, дописано {n_fill}")
    return n_fill


def widths(path: str) -> None:
    """Чувствительность зоны к ширине окна — один раз, для записи (аудитор, 11.10)."""
    snap, s0, curve = _load_snap(path)
    print(f"Снимок {snap['utc']} UTC — ширина окна зоны стопов\n")
    print(f"   {'окно':>6}  {'шорты: зона (окно), $ млн, счетов, P7':<44}"
          f"{'лонги: зона (окно), $ млн, счетов, P7':<44}{'столп 3':>8}")

    def cell(z):
        if not z:
            return "—"
        return (f"{z['dist_pct']:+.2f}% ({z['lo_pct']:+.2f}…{z['hi_pct']:+.2f}), "
                f"{z['size_usd'] / 1e6:,.1f}, {z['n_acc']}, {100 * z['p7']:.0f}%")
    for w in WIDTHS:
        z = summarize(snap["positions"], s0["oi_btc"], s0["mark"], s0["sigma"], curve,
                      sens=False, width=w)
        p3 = "—" if z["pillar3"] is None else f"{z['pillar3']:+.1f}"
        print(f"   {w:>5.1f}%  {cell(z['short']):<44}{cell(z['long']):<44}{p3:>8}")


# --------------------------------------------------------------------------
# Самопроверка
# --------------------------------------------------------------------------

SYNTH = ("САМОПРОВЕРКА: ЧИСЛА НИЖЕ ПОДСТАВНЫЕ (синтетические данные) — к рынку отношения не имеют, "
         "проверяется только механика")


def selftest() -> None:
    print(SYNTH + "\n")
    st = {"assetPositions": [
        {"position": {"coin": "ETH", "szi": "10", "entryPx": "3000", "liquidationPx": "2500",
                      "leverage": {"type": "cross", "value": 5}, "positionValue": "30000"}},
        {"position": {"coin": "BTC", "szi": "-2.5", "entryPx": "84000", "liquidationPx": "88000",
                      "leverage": {"type": "isolated", "value": 20}, "positionValue": "210000"}}]}
    p = btc_position(st)
    assert p["szi"] == -2.5 and p["liq"] == 88000.0 and p["lev"] == 20
    assert btc_position({"assetPositions": [{"position": {"coin": "BTC", "szi": "0"}}]}) is None
    nl = btc_position({"assetPositions": [{"position": {"coin": "BTC", "szi": "1", "entryPx": "80000",
                                                        "liquidationPx": None}}]})
    assert nl["liq"] is None
    curve = {"k": list(TC.K_GRID), True: [TC.rw_prob(k, 7) for k in TC.K_GRID],
             False: [TC.rw_prob(k, 7) for k in TC.K_GRID]}
    pos = [{"szi": -3.0, "liq": 86000.0}, {"szi": -1.0, "liq": 95000.0},     # шорты: +2,38% и +13,1%
           {"szi": 4.0, "liq": 70000.0}, {"szi": 1.0, "liq": None}]         # лонги: −16,7%
    s = summarize(pos, oi=10.0, mark=84000.0, sigma=0.02, curve=curve)
    assert abs(s["cover_long"] - 0.5) < 1e-12 and abs(s["cover_short"] - 0.4) < 1e-12
    assert abs(s["short"]["dist_pct"] - 100 * (86000 / 84000 - 1)) < 1e-9
    assert s["long"] is None and s["pillar3"] is None                       # −16,7% дальше 8 ходов
    pos.append({"szi": 2.0, "liq": 81000.0})                                # лонги −3,57%
    s = summarize(pos, oi=10.0, mark=84000.0, sigma=0.02, curve=curve)
    assert abs(s["long"]["dist_pct"] - 100 * (81000 / 84000 - 1)) < 1e-9 and s["pillar3"] > 0
    # Кто в зоне и хрупкость
    assert s["short"]["n_acc"] == 1 and s["short"]["top_share"] == 1.0 and s["short"]["n_reach"] == 2
    assert s["long"]["n_reach"] == 1
    assert s["short"]["p3_without_top"] < s["pillar3"]                     # без ближнего шорта — ниже
    assert s["long"]["p3_without_top"] is None                             # лонгов в зоне не осталось
    s2 = summarize(pos + [{"szi": -1.0, "liq": 86100.0}], oi=10.0, mark=84000.0, sigma=0.02, curve=curve)
    assert s2["short"]["n_acc"] == 2 and abs(s2["short"]["top_share"] - 0.75) < 1e-12
    # Скользящее окно против сетки: +10,74% и +10,76% — в разных корзинах сетки
    # 0,5% (10,5 и 11,0), окно собирает их вместе (100 тыс. против 60 тыс. на +12%)
    m = 100000.0
    cl = [{"szi": -0.5, "liq": m * 1.1074}, {"szi": -0.5, "liq": m * 1.1076}, {"szi": -0.6, "liq": m * 1.12},
          {"szi": 0.1, "liq": m * 0.97}]
    z = summarize(cl, 10.0, m, 0.02, curve)["short"]
    assert abs(z["size_usd"] - 100000) < 1e-6 and abs(z["dist_pct"] - 10.75) < 1e-6, z
    assert abs(z["lo_pct"] - 10.74) < 1e-6 and abs(z["hi_pct"] - 10.76) < 1e-6
    binned = {}
    for q in cl[:3]:
        b = 0.5 * round(100 * (q["liq"] / m - 1) / 0.5)
        binned[b] = binned.get(b, 0) + abs(q["szi"]) * m
    assert max(binned, key=binned.get) == 12.0                              # сетка ошиблась бы
    z2 = summarize(cl, 10.0, m, 0.02, curve, width=2.0)["short"]
    assert abs(z2["size_usd"] - 160000) < 1e-6 and z2["n_acc"] == 3
    # Равенство сумм — берётся ближнее окно
    tie = [{"szi": -1.0, "liq": m * 1.05}, {"szi": -1.0, "liq": m * 1.09}, {"szi": 0.1, "liq": m * 0.9}]
    assert abs(summarize(tie, 10.0, m, 0.02, curve)["short"]["dist_pct"] - 5.0) < 1e-6
    # Вариант B — независимый расчёт по случайному блужданию (кривая — та же формула)
    def rw(liq, up):
        d = math.log(liq / 84000.0) if up else -math.log(liq / 84000.0)
        return TC.rw_prob(d / 0.02, 7)
    up = 3 * 84000 * rw(86000, True) + 84000 * rw(95000, True)
    dn = 2 * 84000 * rw(81000, False)                                       # 70 000 — дальше 8 ходов
    assert abs(s["exp_up_usd"] / up - 1) < 0.03 and abs(s["exp_down_usd"] / dn - 1) < 0.03, (s, up, dn)
    assert abs(s["pillar3_vol"] - 100 * (up - dn) / (up + dn)) < 2
    # Версия D (15.10): база — OI выборки (лонги 7 BTC, шорты 4 BTC, и без цены ликвидации тоже)
    base = 11 * 84000.0
    assert abs(s["oi_sample_usd"] - base) < 1e-6
    assert abs(s["d"] - 100 * (s["r_up_usd"] - s["r_down_usd"]) / base) < 1e-9
    assert abs(s["d"] - 100 * (up - dn) / base) < 0.03 * abs(s["d"]) and abs(s["t"] - 100 * (up + dn) / base) < 0.03 * s["t"]
    assert abs(s["d"] / s["t"] * 100 - s["pillar3_vol"]) < 1e-9 and s["d"] > 0
    # Хрупкость: закрытая формула = честный пересчёт без крупнейшего счёта стороны знака (шорт 3 BTC)
    assert s["d_lead"] == "short" and abs(s["d_top_usd"] - 3 * 84000) < 1e-6
    rest = [q for q in pos if q["liq"] != 86000.0]
    assert abs(summarize(rest, 10.0, 84000.0, 0.02, curve)["d"] - s["d_wo_top"]) < 1e-9
    # Доли крупнейших вкладчиков сторон (аудитор, 16.10): у лонгов в пределах 8σ один счёт — 100%
    assert s["top_share_down"] == 1.0 and abs(s["top_share_up"] - s["d_top_share"]) < 1e-12 < s["top_share_up"] < 1
    assert abs(s["d_top_dist"] - 100 * (86000 / 84000 - 1)) < 1e-9 and abs(s["d_top_k"] - _k(s["d_top_dist"], 0.02)) < 1e-12
    # Три состояния знака D: лестница — состав, хрупкость — концентрация
    assert d_state(1.0, 0.5, "+") == "установлено" and d_state(-3.03, 0.35, "-") == "одним счётом"
    assert d_state(-1.0, -0.5, "нет") == "не установлено" and d_state(1.0, 0.0, "+") == "одним счётом"
    assert d_state(None, None, "+") is None and d_state(1.0, 0.5, "") is None
    tt = top_text({"d_lead": "long", "d_top_type": "isolated", "d_top_lev": 40, "d_top_k": 1.05,
                   "d_top_usd": 149.9e6, "d_top_share": 0.5515, "d_top_rank": 622})
    assert tt == "изолированный лонг $149.9 млн, 40x, 1.05σ до ликвидации, 55% ожидаемых закрытий лонгов, ранг 622", tt
    # Приёмочные свойства (прогон 15.10): размер входит, двусторонний риск виден в T
    near = lambda x: summarize(pos + [{"szi": -x, "liq": 84000 * 1.004}], 10.0, 84000.0, 0.02, curve)
    assert near(50.0)["d"] > near(5.0)["d"] > s["d"]                        # зонная дала бы одно и то же
    both = summarize(pos + [{"szi": -20.0, "liq": 84000 * 1.01}, {"szi": 20.0, "liq": 84000 / 1.01}],
                     10.0, 84000.0, 0.02, curve)
    assert abs(both["d"]) < abs(s["d"]) and both["t"] > 2 * s["t"]
    # Лестница: знак установлен только при согласии ступеней
    rk = lambda first: [dict(q, rank=0 if q["liq"] == first else 30000 + i) for i, q in enumerate(pos)]
    lz = ladder(rk(81000.0), 10.0, 84000.0, 0.02, curve)                   # в топе только лонг у цены
    assert lz["steps"]["2000"]["n"] == 1 and lz["steps"]["all"]["n"] == len(pos)
    assert lz["steps"]["2000"]["d"] < 0 < lz["steps"]["all"]["d"] and lz["sign"] == "нет"
    assert ladder(rk(86000.0), 10.0, 84000.0, 0.02, curve)["sign"] == "+"   # в топе шорт — плюс везде
    assert ladder([{"szi": 1.0, "liq": 80000.0}], 10.0, 84000.0, 0.02, curve) is None     # без ранга
    # Стопы дальше обрезки — только в базе: D сжимается в отношении баз, R не меняются
    far = summarize(pos + [{"szi": -11.0, "liq": 84000 * 1.5}], 10.0, 84000.0, 0.02, curve)
    assert far["r_up_usd"] == s["r_up_usd"] and abs(far["d"] - s["d"] / 2) < 1e-9
    # Кумулятивная версия: шорты 4 BTC (3 в 1,18 хода, 1 в 6,2), лонги 7 BTC
    # (2 в 1,82 хода, 4 дальше 8, 1 без цены ликвидации) — нормировка на всю сторону
    c = s["cum"]
    assert c["1"]["value"] is None and c["1"]["short"] == 0 and c["1"]["long"] == 0
    sh, lo = 3 / 4, 2 / 7
    assert abs(c["2"]["short"] - sh) < 1e-12 and abs(c["2"]["long"] - lo) < 1e-12
    assert abs(c["2"]["value"] - 100 * (sh - lo) / (sh + lo)) < 1e-9 and c["4"] == c["2"]
    # Ограничитель: 30 стартов при 6000/мин из 6 потоков — не быстрее 0,01 с между стартами
    lim, starts = Limiter(6000), []

    def hit(_):
        lim.wait()
        starts.append(time.monotonic())
    with ThreadPoolExecutor(max_workers=6) as ex:
        list(ex.map(hit, range(30)))
    starts.sort()
    assert starts[-1] - starts[0] >= 29 * 0.01 * 0.95, starts[-1] - starts[0]
    # Полный прогон на подставной бирже: потоки, ранги, HLP, устойчивость, снимок, пересчёт
    import random
    import shutil
    import tempfile
    g = globals()
    saved = {k: g[k] for k in ("HERE", "SNAP_DIR", "post", "btc_market", "leaderboard_rows", "requests",
                               "LIMIT", "CUTS", "HLP_VAULT", "KNOWN_FILE", "_gh_tree", "_gh_raw",
                               "_cb_daily", "FULL_MIN")}
    saved_load = TC.load
    tmp = tempfile.mkdtemp(prefix="hl_selftest_")
    try:
        with open(os.path.join(tmp, "p3_touch_curve.csv"), "w", encoding="utf-8") as fh:
            fh.write("horizon_d,k,p_up,p_down\n")
            for k in TC.K_GRID:
                fh.write(f"7,{k},{TC.rw_prob(k, 7)},{TC.rw_prob(k, 7)}\n")
        random.seed(3)
        book = {}
        for i in range(300):
            if random.random() < 0.6:
                sz = random.choice((-1, 1)) * random.uniform(0.1, 5)
                liq = 84000 * (1 + (0.01 if sz < 0 else -0.01) * random.uniform(1, 15))
                book[f"0x{i:040x}"] = {"assetPositions": [{"position": {
                    "coin": "BTC", "szi": str(sz), "entryPx": "84000", "liquidationPx": str(liq),
                    "leverage": {"type": "cross", "value": 10}, "positionValue": str(abs(sz) * 84000)}}]}
            else:
                book[f"0x{i:040x}"] = {"assetPositions": []}
        lead = list(book)
        in_book = next(a for a in lead if book[a]["assetPositions"] and not a.endswith("7"))
        parent, child = "0x" + "a" * 40, "0x" + "b" * 40
        extra = {parent: {"assetPositions": []},
                 child: {"assetPositions": [{"position": {
                     "coin": "BTC", "szi": "-12.5", "entryPx": "84000", "liquidationPx": "99000",
                     "leverage": {"type": "cross", "value": 5}, "positionValue": "1050000"}}]}}

        def fake_post(body, tries=5):
            LIMIT.wait()
            if body["type"] == "vaultDetails":
                return {"relationship": {"type": "parent", "data": {"childAddresses": [in_book, child]}}}
            u = body["user"]
            if u.endswith("7"):                                  # часть счетов «не отвечает»
                raise RuntimeError("тест: нет ответа")
            return book.get(u) or extra[u]
        g.update(HERE=tmp, SNAP_DIR=os.path.join(tmp, "snap"), post=fake_post,
                 btc_market=lambda: (500.0, 84000.0),
                 leaderboard_rows=lambda: [{"ethAddress": a, "windowPerformances": [
                     ["day", {"vlm": "100" if i % 7 == 3 else "0"}]]} for i, a in enumerate(lead)],
                 requests=saved["requests"] or object(), LIMIT=Limiter(60000), CUTS=(100, 200),
                 HLP_VAULT=parent, FULL_MIN=20000)
        rows = [(k * TC.DAY, 84000.0, 84000.0, 84000.0 * math.exp(0.02 * (-1) ** k)) for k in range(60)]
        TC.load = lambda refresh=False: rows
        g["_cb_daily"] = lambda days=60: rows
        print("--- подставная биржа: числа ниже выдуманные, проверяется только механика ---")
        run(0, 6)
        snaps = os.listdir(os.path.join(tmp, "snap"))
        assert len(snaps) == 1 and os.path.isfile(os.path.join(tmp, "hl_known.json"))
        snap = json.load(open(os.path.join(tmp, "snap", snaps[0]), encoding="utf-8"))
        ranks = [p["rank"] for p in snap["positions"]]
        assert ranks[:-1] == sorted(ranks[:-1]) and ranks[-1] == -1 and snap["top"] == 300
        assert not any(p["addr"].endswith("7") for p in snap["positions"])
        hl = [p for p in snap["positions"] if p.get("hlp")]
        assert {p["addr"] for p in hl} == {in_book, child} and len(snap["hlp"]) == 3
        print("\n--- пересчёт по снимку ---")
        recompute(os.path.join(tmp, "snap", snaps[0]))
        print("\n--- ширина окна ---")
        widths(os.path.join(tmp, "snap", snaps[0]))
        old = dict(snap, positions=[{k: v for k, v in p.items() if k not in ("rank", "hlp")}
                                    for p in snap["positions"]])
        del old["top"]
        with open(os.path.join(tmp, "old.json"), "w", encoding="utf-8") as fh:
            json.dump(old, fh)
        print("\n--- пересчёт по снимку старого формата ---")
        recompute(os.path.join(tmp, "old.json"))
        # Ежедневный режим: топ 50 + держатели BTC из полного опроса — та же картина
        full_s = snap["summary"]
        _, ks = load_known()
        holders = {p["addr"].lower() for p in snap["positions"] if p["rank"] >= 0}
        assert ks == holders
        os.remove(os.path.join(tmp, "snap", snaps[0]))
        print("\n--- ежедневный режим: топ 50 + известные держатели ---")
        run(50, 6, known=True)
        new = [f for f in os.listdir(os.path.join(tmp, "snap")) if f.endswith(".json")]
        d = json.load(open(os.path.join(tmp, "snap", new[0]), encoding="utf-8"))
        expect = 50 + sum(1 for a in lead[50:] if a.lower() in holders)
        assert d["polled"] == expect and d["known_from"] == snap["utc"], (d["polled"], expect)
        assert abs(d["summary"]["long_btc"] - full_s["long_btc"]) < 1e-9     # покрытие то же
        assert abs(d["summary"]["short_btc"] - full_s["short_btc"]) < 1e-9
        # Запасной путь: hl_known.json нет — берётся снимок с охватом ≥ 20 000
        os.remove(os.path.join(tmp, "hl_known.json"))
        assert load_known() == (None, set())
        big = dict(snap, top=47355, leaderboard=47355, utc="20991231_0000")
        with open(os.path.join(tmp, "snap", "hl_20991231_0000.json"), "w", encoding="utf-8") as fh:
            json.dump(big, fh)
        assert load_known() == ("20991231_0000", holders)
        # Облачный сборщик: воскресенье → полный; повтор в те же сутки → пропуск;
        # понедельник → ежедневный (топ 50 + держатели); журнал; σ — на «ноутбуке»
        out = os.path.join(tmp, "data")
        sun = datetime(2026, 10, 18, 0, 10, tzinfo=timezone.utc)
        mon = datetime(2026, 10, 19, 0, 10, tzinfo=timezone.utc)
        assert sun.weekday() == 6
        os.makedirs(out, exist_ok=True)
        with open(os.path.join(out, "hl_log.csv"), "w", encoding="utf-8", newline="") as fh:
            fh.write("utc,kind,polled,positions,errors,minutes,cover_long,cover_short,oi_btc,mark\n"
                     "20261013_1022,full,47000,3200,0,95.0,0.4,0.4,37000,83000\n")
        with open(os.path.join(out, "pillar3_daily.csv"), "w", encoding="utf-8", newline="") as fh:
            fh.write(",".join(P3_COLS[:24]) + "\n" + "20261014_0010,daily" + ",1" * 22 + "\n")
        print("\n--- облачный сборщик: воскресенье ---")
        assert collect("auto", 50, 6, out, now=sun) == 0
        f_sun = _day_files(out, "20261018")
        assert len(f_sun) == 1 and f_sun[0].endswith("_full.json.gz"), f_sun
        # Полный опрос помечает, кто попал бы в ежедневный: топ 50 + список (его ещё нет) + торговавшие
        with gzip.open(os.path.join(out, "hl", "2026", "10", f_sun[0]), "rt", encoding="utf-8") as fh:
            ds = json.load(fh)
        act7 = {a.lower() for i, a in enumerate(lead) if i % 7 == 3}
        assert ds["daily_top"] == 50 and ds["active"] == len(act7)
        assert all(p["daily"] == (p["rank"] < 50 or p["addr"].lower() in act7) for p in ds["positions"]
                   if p["rank"] >= 0)
        assert 0 < sum(p["daily"] for p in ds["positions"]) < len(ds["positions"])
        assert os.path.isfile(os.path.join(out, "hl_known.json"))
        assert collect("auto", 50, 6, out, now=sun) == 0 and len(_day_files(out, "20261018")) == 1
        print("\n--- облачный сборщик: понедельник ---")
        assert collect("auto", 50, 6, out, now=mon) == 0
        f_mon = _day_files(out, "20261019")
        assert len(f_mon) == 1 and f_mon[0].endswith("_daily.json.gz"), f_mon
        assert collect("daily", 50, 6, out, now=mon) == 0 and len(_day_files(out, "20261019")) == 1
        with gzip.open(os.path.join(out, "hl", "2026", "10", f_mon[0]), "rt", encoding="utf-8") as fh:
            dm = json.load(fh)
        active_set = {a.lower() for i, a in enumerate(lead) if i % 7 == 3}
        expect = 50 + sum(1 for a in lead[50:] if a.lower() in holders or a.lower() in active_set)
        assert dm["kind"] == "daily" and dm["polled"] == expect, (dm["polled"], expect)
        assert dm["active"] == len(active_set)
        with open(os.path.join(out, "hl_log.csv"), encoding="utf-8") as fh:
            lg = list(csv.DictReader(fh))
        assert len(lg) == 3 and lg[0]["utc"] == "20261013_1022" and lg[0]["active"] == ""   # старая строка цела
        assert lg[2]["kind"] == "daily" and lg[2]["active"] == str(len(active_set))
        with open(os.path.join(out, "hl_known.json"), encoding="utf-8") as fh:
            kn = json.load(fh)
        assert kn["utc"] == "20261019_0010" and kn["full_utc"] == "20261018_0010"
        assert set(kn["addresses"]) == {p["addr"].lower() for p in dm["positions"] if p["rank"] >= 0}
        # Столп 3 посчитан в облаке (σ «Coinbase») и записан в ежедневный ряд
        assert dm["summary"]["sigma_src"] == "coinbase" and dm["summary"]["pillar3"] is not None
        with open(os.path.join(out, "pillar3_daily.csv"), encoding="utf-8") as fh:
            pr = list(csv.DictReader(fh))
        assert [r["kind"] for r in pr] == ["daily", "full", "daily"] and pr[2]["cum2"] != ""
        assert pr[0]["utc"] == "20261014_0010" and pr[0]["short_usd"] == ""        # старая строка цела
        r2, c2 = pr[2], dm["summary"]["cum"]
        assert abs(float(r2["short_usd"]) - dm["summary"]["short_usd"]) < 1
        assert abs(float(r2["cum2_short"]) - c2["2"]["short"]) < 1e-4 and r2["instrument"] == "Hyperliquid"
        assert r2["near10_long"] != "" and r2["zone_short_lo"] != "" and r2["k_short"] != ""
        assert abs(float(r2["d"]) - dm["summary"]["d"]) < 1e-4 and r2["t"] != "" and r2["d_lead"] in ("short", "long")
        assert abs(float(r2["oi_sample_usd"]) - dm["summary"]["oi_sample_usd"]) < 1
        lad = dm["summary"]["ladder"]                                      # лестница: последняя ступень = весь снимок
        assert lad["steps"]["all"]["d"] == dm["summary"]["d"] and r2["d_sign_ladder"] == lad["sign"]
        assert lad["sign"] in ("+", "-", "нет") and r2["d_l2000"] != "" and r2["t_l20000"] != ""
        sg = {(v["d"] > 0) - (v["d"] < 0) for v in lad["steps"].values() if v["d"] is not None}
        assert (lad["sign"] != "нет") == (len(sg) == 1 and 0 not in sg)
        sm_ = dm["summary"]                                                # три состояния и доли сторон
        assert r2["d_state"] == d_state(sm_["d"], sm_["d_wo_top"], lad["sign"]) and r2["d_state"] in D_STATES
        assert r2["top_share_up"] != "" and r2["top_share_down"] != "" and r2["d_top_type"] == "cross"
        assert r2["d_top_lev"] in ("10", "5") and r2["d_top_k"] != "" and r2["d_top_rank"] != ""
        # Дозаполнение: строки без колонок D — по снимку и σ из строки; строка без снимка — пропуск
        p3csv = os.path.join(tmp, "p3_refill.csv")
        dcols = ["d", "t", "d_wo_top", "d_lead", "d_top_share", "r_up_usd", "r_down_usd", "oi_sample_usd",
                 "d_l2000", "d_l10000", "d_l20000", "t_l2000", "t_l10000", "t_l20000", "d_sign_ladder",
                 "d_state", "top_share_up", "top_share_down", "d_top_usd", "d_top_type", "d_top_lev",
                 "d_top_k", "d_top_dist", "d_top_rank"]
        with open(p3csv, "w", encoding="utf-8", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=[c for c in P3_COLS if c not in dcols])
            w.writeheader()
            for r in pr:
                w.writerow({k: v for k, v in r.items() if k not in dcols})
        print("\n--- дозаполнение ряда (D, T) по сохранённым снимкам ---")
        assert refill(p3csv, out) == 2
        with open(p3csv, encoding="utf-8") as fh:
            rf = list(csv.DictReader(fh))
        assert rf[0]["d"] == "" and rf[0]["pillar3"] == pr[0]["pillar3"]                    # снимка нет
        for a_, b_ in zip(rf[1:], pr[1:]):
            assert all(a_[c] == b_[c] for c in P3_COLS), [c for c in P3_COLS if a_[c] != b_[c]]
        assert refill(p3csv, out) == 0                                                     # повтор — ничего
        # «Ноутбук»: забрать снимки из «облака» и посчитать столп по последнему
        tree = {"tree": [{"type": "blob",
                          "path": os.path.relpath(os.path.join(dp, f), tmp).replace(os.sep, "/")}
                         for dp, _, fs in os.walk(out) for f in fs]}
        g.update(_gh_tree=lambda: tree, SNAP_DIR=os.path.join(tmp, "laptop"),
                 _gh_raw=lambda path: open(os.path.join(tmp, path), "rb").read())
        print("\n--- ноутбук: забрать снимки ---")
        sync()
        assert sorted(os.listdir(os.path.join(tmp, "laptop"))) == sorted(f_sun + f_mon)
        with open(os.path.join(tmp, "hl_known.json"), encoding="utf-8") as fh:
            assert json.load(fh)["utc"] == "20261019_0010"                  # список обновляется ежедневно
        sync()                                                              # повтор — ничего нового
        snap_m, s0, _ = _load_snap("latest")
        assert snap_m["kind"] == "daily" and s0["sigma"] is not None
        print("\n--- ноутбук: столп 3 по последнему облачному снимку ---")
        recompute("latest")
        print("\n--- ноутбук: обрезка 6/8/12σ ---")
        cuts = cutoffs("latest")
        sl = summarize(snap_m["positions"], s0["oi_btc"], s0["mark"], s0["sigma"],
                       MT.load_curve(os.path.join(tmp, "p3_touch_curve.csv")))
        b = {(k_, t_): v_ for k_, t_, v_ in cuts}
        assert abs(b[(8.0, "flat")]["d"] - sl["d"]) < 1e-9 and abs(b[(12.0, "zero")]["d"] - sl["d"]) < 1e-9
        assert b[(6.0, "flat")]["t"] <= b[(8.0, "flat")]["t"] <= b[(12.0, "flat")]["t"]
        # Прежний список держателей при обновлении из облака сохраняется — для проверки глубины
        with open(os.path.join(tmp, "hl_known.json"), "w", encoding="utf-8") as fh:
            json.dump({"utc": "20261011_2209", "addresses": sorted(holders)}, fh)
        os.remove(os.path.join(tmp, "laptop", f_mon[0]))
        sync()
        assert os.path.isfile(os.path.join(tmp, "hl_known_20261011_2209.json"))
        with open(os.path.join(tmp, "hl_known.json"), encoding="utf-8") as fh:
            assert json.load(fh)["utc"] == "20261019_0010"
        print("\n--- проверка глубины: полный снимок против топ 50 + прежних держателей ---")
        used = depth(os.path.join(tmp, "laptop", f_sun[0]), "auto", top=50)
        assert os.path.basename(used[1]) == "hl_known_20261011_2209.json"
        # Ноутбук без сохранённого списка (полный опрос 11.10 сделан версией без hl_known.json,
        # ошибка №62): держатели берутся из более раннего полного снимка; эталон по умолчанию —
        # последний ПОЛНЫЙ снимок, а не последний ежедневный
        os.remove(os.path.join(tmp, "hl_known_20261011_2209.json"))
        g["FULL_MIN"] = 100
        with open(os.path.join(tmp, "laptop", "hl_20261011_2209.json"), "w", encoding="utf-8") as fh:
            json.dump(dict(snap, utc="20261011_2209"), fh)
        with open(os.path.join(tmp, "laptop", "hl_20261012_1500.json"), "w", encoding="utf-8") as fh:
            json.dump(dict(snap, utc="20261012_1500", top=50), fh)          # не полный — не годится
        print("\n--- проверка глубины: держатели из более раннего полного снимка ---")
        used = depth("latest", "auto", top=50)
        assert os.path.basename(used[0]) == f_sun[0], used
        assert os.path.basename(used[1]) == "hl_20261011_2209.json", used
        os.remove(os.path.join(tmp, "laptop", "hl_20261011_2209.json"))
        os.remove(os.path.join(tmp, "laptop", "hl_20261012_1500.json"))
        g["FULL_MIN"] = 20000
        print("\n--- проверка глубины: только пометка ежедневного состава ---")
        assert depth(os.path.join(tmp, "laptop", f_sun[0]), "auto", top=50)[1] is None
        # Таблица лидеров: есть ли активность по счетам
        class _R:
            def raise_for_status(self):
                pass

            def json(self):
                return {"leaderboardRows": [
                    {"ethAddress": "0x1", "accountValue": "10", "windowPerformances": [
                        ["day", {"pnl": "1.5", "roi": "0.1", "vlm": "100"}], ["week", {"pnl": "0", "vlm": "0"}]]},
                    {"ethAddress": "0x2", "accountValue": "5", "windowPerformances": [
                        ["day", {"pnl": "0", "roi": "0", "vlm": "0"}], ["week", {"pnl": "2", "vlm": "50"}]]}]}

        class _Req:
            RequestException = Exception

            @staticmethod
            def get(*a, **k):
                return _R()
        g["requests"] = _Req
        print("\n--- таблица лидеров: активность ---")
        lbstats()
        print("\n--- облако: Coinbase недоступен ---")
        # Coinbase недоступен — снимок всё равно сохраняется, столп считает ноутбук
        def _down(days=60):
            raise RuntimeError("тест: Coinbase недоступен")
        g["_cb_daily"] = _down
        tue = datetime(2026, 10, 20, 0, 10, tzinfo=timezone.utc)
        assert collect("auto", 50, 6, out, now=tue) == 0
        with gzip.open(os.path.join(out, "hl", "2026", "10", _day_files(out, "20261020")[0]), "rt",
                       encoding="utf-8") as fh:
            assert json.load(fh)["summary"]["sigma"] is None
        with open(os.path.join(out, "pillar3_daily.csv"), encoding="utf-8") as fh:
            assert len(list(csv.DictReader(fh))) == 3
        # σ — только по закрытым дням: свеча, открытая за сутки до снимка, ещё не закрыта
        r40 = [(k * TC.DAY, 1, 1, 100 * math.exp(0.01 * k + 0.02 * (-1) ** k + (0.05 if k == 39 else 0)))
               for k in range(40)]
        c40 = [r[3] for r in r40]
        assert sigma_at(r40, 39 * TC.DAY + 600_000) == TC.daily_vol(c40, 38) != TC.daily_vol(c40, 39)
        assert sigma_at(r40, 40 * TC.DAY) == TC.daily_vol(c40, 39)
        # Аудитор 17.10: разброс фактических входов (взвешен размером; меньше 5 позиций — нет значения)
        pe = [{"szi": 1.0, "entry": 100.0}, {"szi": 1.0, "entry": 110.0}, {"szi": -2.0, "entry": 105.0},
              {"szi": 1.0, "entry": 90.0}, {"szi": -1.0, "entry": 120.0}, {"szi": 0.5, "entry": 0}]
        ed = entry_disp(pe)
        ws, xs = [1, 1, 2, 1, 1], [math.log(v) for v in (100, 110, 105, 90, 120)]
        mu = sum(w * x for w, x in zip(ws, xs)) / 6
        assert ed["entry_n"] == 5 and abs(ed["entry_disp"] - 100 * math.sqrt(sum(w * (x - mu) ** 2 for w, x in zip(ws, xs)) / 6)) < 1e-9
        assert ed["entry_disp_long"] is None and ed["entry_disp_short"] is None
        # Разложение изменения D: предыдущие сутки — последний опрос тех суток; цена + позиции = изменение точно
        dec = os.path.join(tmp, "dec")
        cv = {"k": list(TC.K_GRID), True: [TC.rw_prob(k, 7) for k in TC.K_GRID],
              False: [TC.rw_prob(k, 7) for k in TC.K_GRID]}
        p0 = [{"szi": 2.0, "liq": 78000.0, "entry": 81000.0}, {"szi": -1.5, "liq": 84000.0, "entry": 79000.0},
              {"szi": 3.0, "liq": 76000.0, "entry": 80500.0}, {"szi": -0.5, "liq": 90000.0, "entry": 80000.0}]
        p1 = p0[:3] + [{"szi": 4.0, "liq": 79000.0, "entry": 81500.0}]
        os.makedirs(os.path.join(dec, "hl", "2026", "10"))
        for u, kind, pp, mk in (("20261018_0010", "daily", p0, 80000.0), ("20261018_1500", "full", p0, 80200.0),
                                ("20261019_0010", "daily", p0, 80500.0)):
            with gzip.open(os.path.join(dec, "hl", "2026", "10", f"hl_{u}_{kind}.json.gz"), "wt", encoding="utf-8") as fh:
                json.dump({"utc": u, "kind": kind, "summary": {"oi_btc": 10.0, "mark": mk, "sigma": 0.02},
                           "positions": pp}, fh)
        assert prev_snapshot(dec, "20261020_0010").endswith("hl_20261019_0010_daily.json.gz")   # конец 18.10
        assert prev_snapshot(dec, "20261019_0010").endswith("hl_20261018_0010_daily.json.gz")   # 1500 — уже 18.10
        assert prev_snapshot(dec, "20261018_0010") is None
        now_s = summarize(p1, 10.0, 81000.0, 0.021, cv)
        de = decompose(p1, 10.0, 81000.0, 0.021, cv, now_s["d"], now_s["t"], prev_snapshot(dec, "20261020_0010"))
        was = summarize(p0, 10.0, 80500.0, 0.02, cv)
        assert abs(de["d_prev"] - was["d"]) < 1e-12 and de["prev_kind"] == "daily"
        assert abs(de["d_px"] + de["d_pos"] - (now_s["d"] - was["d"])) < 1e-12
        assert abs(de["t_px"] + de["t_pos"] - (now_s["t"] - was["t"])) < 1e-12
        same = decompose(p0, 10.0, 81000.0, 0.021, cv, summarize(p0, 10.0, 81000.0, 0.021, cv)["d"], None,
                         prev_snapshot(dec, "20261020_0010"))
        assert abs(same["d_pos"]) < 1e-12 and "t_px" not in same                     # состав тот же — только цена
        assert decompose(p1, 10.0, 81000.0, 0.021, cv, now_s["d"], now_s["t"], None) == {}
        # Запасной путь не увереннее основного (аудитор, 17.10): ступень без D — знак не установлен
        import contextlib
        import io
        assert top_text({"d_lead": None}) == "крупнейший счёт: сторона в строке не записана"
        lad_ok = {"steps": {"2000": {"d": -1.0}, "all": {"d": -2.0}}, "sign": "-", "complete": True}
        lad_gap = {"steps": {"2000": {"d": None}, "all": {"d": -2.0}}, "sign": "нет", "complete": False}
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            print_ladder(lad_ok)
            print_ladder(lad_gap)
        assert "минус — знак установлен" in buf.getvalue() and "не на всех ступенях — знак НЕ установлен" in buf.getvalue()
    finally:
        g.update(saved)
        TC.load = saved_load
        shutil.rmtree(tmp, ignore_errors=True)
    print("\nselftest: OK (числа подставные) — разбор позиции, покрытие, скользящее окно (сетка ошиблась бы), равенство, "
          "хрупкость, вариант B, ограничитель, параллельный прогон, HLP, снимок, пересчёт, ширины, "
          "облачный сборщик (полный/ежедневный/повтор), забор на ноутбук, σ по закрытым дням, "
          "кумулятивная версия, проверка глубины (список, ранний полный снимок, пометка ежедневного состава), "
          "активность таблицы лидеров, "
          "ежедневный режим с известными держателями, версия D (база — OI выборки, хрупкость = честный "
          "пересчёт, размер входит, двусторонний риск в T, дальние стопы только в базе), обрезка 6/8/12σ, "
          "дозаполнение ряда по снимкам, лестница глубин (знак — при согласии ступеней), три состояния знака D "
          "и доли крупнейших вкладчиков сторон, разброс фактических цен входа, разложение изменения D и T "
          "на ценовую и позиционную части (сумма точна; прошлые сутки — последний их опрос)")


def main() -> None:
    ap = argparse.ArgumentParser(description="Стопы BTC на Hyperliquid сейчас")
    sub = ap.add_subparsers(dest="mode", required=True)
    sub.add_parser("selftest").add_argument("--live", action="store_true",
                                            help="плюс живые запросы к Hyperliquid и Coinbase")
    sub.add_parser("sigmacheck", help="σ Binance против Coinbase (облако считает по Coinbase)")
    r = sub.add_parser("run")
    r.add_argument("--top", type=int, default=2000, help="сколько крупнейших счетов; 0 — все")
    r.add_argument("--workers", type=int, default=WORKERS)
    r.add_argument("--known", action="store_true",
                   help="добавить держателей BTC из последнего полного опроса (ежедневный режим)")
    for name in ("recompute", "widths", "cutoffs"):
        sub.add_parser(name).add_argument("snapshot", help="путь к снимку или latest — последний")
    rf = sub.add_parser("refill", help="дописать новые колонки ряда столпа 3 по сохранённым снимкам")
    rf.add_argument("--csv", default=None, help="ряд (по умолчанию data/pillar3_daily.csv)")
    rf.add_argument("--out", default=DATA_DIR)
    c = sub.add_parser("collect", help="облачный сборщик (GitHub Actions)")
    c.add_argument("--mode", dest="kind", choices=("auto", "daily", "full"), default="auto")
    c.add_argument("--top", type=int, default=2000)
    c.add_argument("--workers", type=int, default=WORKERS)
    c.add_argument("--out", default=DATA_DIR)
    c.add_argument("--force", action="store_true")
    c.add_argument("--no-active", dest="active", action="store_false",
                   help="не добавлять торговавших за сутки")
    sub.add_parser("sync", help="забрать снимки облачного сборщика на ноутбук")
    dp = sub.add_parser("depth", help="проверка глубины ежедневного опроса (аудитор, 13.10)")
    dp.add_argument("snapshot", nargs="?", default="latest")
    dp.add_argument("--known", default="auto")
    dp.add_argument("--top", type=int, default=2000)
    sub.add_parser("lbstats", help="поля и активность счетов в таблице лидеров")
    a = ap.parse_args()
    if (a.mode in ("run", "collect", "sync", "lbstats", "sigmacheck") or getattr(a, "live", False)) \
            and requests is None:
        sys.exit("Нужен пакет requests: py -m pip install requests")
    if a.mode == "collect":
        sys.exit(collect(a.kind, a.top, a.workers, a.out, a.force, active=a.active))
    if a.mode == "sync":
        sync()
        return
    if a.mode == "depth":
        depth(a.snapshot, a.known, a.top)
        return
    if a.mode == "lbstats":
        lbstats()
        return
    if a.mode == "sigmacheck":
        sigmacheck()
        return
    if a.mode == "cutoffs":
        cutoffs(a.snapshot)
        return
    if a.mode == "refill":
        refill(a.csv, a.out)
        return
    if a.mode == "selftest":
        selftest()
        if a.live:
            live_check()
    elif a.mode == "recompute":
        recompute(a.snapshot)
    elif a.mode == "widths":
        widths(a.snapshot)
    else:
        run(a.top, a.workers, a.known)


if __name__ == "__main__":
    main()
