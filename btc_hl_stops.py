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

Столп 3 («7 дней», принят аудитором 11.10): самая плотная зона стопов сверху
(шорты) и снизу (лонги), расстояние в дневных ходах, вероятность дойти за
7 дней по кривой p3_touch_curve.csv. Столп 3 = P(вверх) − P(вниз), −100…+100.

ЗОНА (с 12.10, правило записано до прогона): скользящее окно шириной 0,5 п.п.
расстояния вместо фиксированной сетки 0,5% — сетка резала скопление по
границе корзины (ошибка №58). Окно перебирается по позициям в пределах
8 дневных ходов, берётся максимум суммы размеров; при равенстве — ближнее
к цене; расстояние зоны — среднее расстояние её позиций, взвешенное размером.

Справочно, не условие: столп 3 без крупнейшего счёта каждой зоны (хрупкость)
и вариант B «ожидаемый вынос за 7 дней» (отведён 11.10, не развивается).

Каждый прогон сохраняется в hl_snapshots/ — так копится собственная история.

Режимы:
  py btc_hl_stops.py selftest
  py btc_hl_stops.py run --top 2000 --known      — ежедневно: топ 2000 + держатели BTC
                                                     из последнего полного опроса (≈ 10 мин)
  py btc_hl_stops.py run --top 0                  — раз в неделю: все счета (≈ 95 мин),
                                                     обновляет список держателей hl_known.json
  py btc_hl_stops.py recompute hl_snapshots/hl_ГГГГММДД_ЧЧММ.json   — пересчёт без опроса
  py btc_hl_stops.py widths hl_snapshots/hl_ГГГГММДД_ЧЧММ.json      — ширина окна 0,5/1/2%
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
P3_COLS = ["utc", "kind", "sigma", "sigma_src", "pillar3", "p_short", "p_long", "zone_short_pct",
           "zone_long_pct", "zone_short_usd", "zone_long_usd", "acc_short", "acc_long", "top_short",
           "top_long", "p3_wo_top_short", "p3_wo_top_long", "cum1", "cum2", "cum4",
           "cover_long", "cover_short", "positions", "polled"]


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
    # Вариант B (справочно, отведён 11.10): каждая позиция в пределах 8 ходов
    # с весом своей вероятности дойти за 7 дней.
    exp_vol = {True: 0.0, False: 0.0}
    for d, size, _ in rows:
        k = _k(d, sigma)
        if 0 < k <= MT.K_MAX:
            exp_vol[d > 0] += size * MT.interp(curve["k"], curve[d > 0], k)
    out["exp_up_usd"], out["exp_down_usd"] = exp_vol[True], exp_vol[False]
    tot = exp_vol[True] + exp_vol[False]
    out["pillar3_vol"] = 100.0 * (exp_vol[True] - exp_vol[False]) / tot if tot else None
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


def print_summary(s: dict, top) -> None:
    print(f"\nHYPERLIQUID, BTC — цена {s['mark']:,.0f}; открытый интерес {s['oi_btc']:,.0f} BTC "
          f"(${s['oi_btc'] * s['mark'] / 1e9:.2f} млрд)")
    print(f"Счетов просмотрено {top}, позиций BTC найдено {s['n_pos']}")
    print(f"ПОКРЫТИЕ: лонги {s['long_btc']:,.0f} BTC = {100 * s['cover_long']:.0f}% интереса, "
          f"шорты {s['short_btc']:,.0f} BTC = {100 * s['cover_short']:.0f}%")
    if s["cover_long"] and s["cover_long"] > 1.05:
        print("   [!] Покрытие больше 100% — проверить единицы openInterest")
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
    if s["pillar3"] is not None:
        print(f"\nСТОЛП 3 = {s['pillar3']:+.0f}  (плюс — ближе стопы шортов, минус — лонгов; −100…+100)")
        alt = [(n, (s[sd] or {}).get("p3_without_top")) for sd, n in (("short", "шортов"), ("long", "лонгов"))]
        alt = [(n, v) for n, v in alt if v is not None]
        if alt:
            print("   Хрупкость (справка, не условие) — без крупнейшего счёта зоны: " +
                  "; ".join(f"зоны {n} {v:+.0f}" for n, v in alt))
    if s.get("cum"):
        c = s["cum"]
        lim = " / ".join(str(k) for k in CUM_K)
        f = lambda key: " / ".join(f"{100 * c[str(k)][key]:.1f}%" for k in CUM_K)
        v = " / ".join("—" if c[str(k)]["value"] is None else f"{c[str(k)]['value']:+.0f}" for k in CUM_K)
        print(f"Кумулятивная версия (доля стороны со стопами в пределах {lim} дн. ходов; плюс — шорты):")
        print(f"   шорты {f('short')}, лонги {f('long')} → {v}   "
              f"(база: шорты ${s['short_usd'] / 1e6:,.0f} млн, лонги ${s['long_usd'] / 1e6:,.0f} млн)")
        if s["pillar3"] is not None:
            diff = [str(k) for k in CUM_K if c[str(k)]["value"] is not None and s["pillar3"] != 0
                    and (c[str(k)]["value"] > 0) != (s["pillar3"] > 0)]
            if diff:
                print(f"   [!] Знак расходится с зонной версией в пределах {', '.join(diff)} дн. ходов — "
                      f"печатается расхождение, версия не выбирается")
    if s.get("pillar3_vol") is not None:
        print(f"Вариант B ({B_NOTE}): шортов ${s['exp_up_usd'] / 1e6:,.1f} млн, "
              f"лонгов ${s['exp_down_usd'] / 1e6:,.1f} млн → {s['pillar3_vol']:+.0f}")
    print("   Выборка: крупнейшие счета Hyperliquid из таблицы лидеров и HLP. Не весь рынок.")


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
          f"{'столп 3':>9}{'вар. B':>8}")
    n = len(positions)
    variants = ([(f"топ {c}", [p for p in positions if p["rank"] < c]) for c in cuts] if have_rank
                else [(f"{q}% позиций", positions[: max(1, n * q // 100)]) for q in (25, 50, 100)])
    for name, sub in variants:
        if not sub:
            continue
        z = summarize(sub, oi, mark, sigma, curve, sens=False)
        f = lambda w: f"{w['dist_pct']:+.1f}%, {100 * w['p7']:.0f}%" if w else "—"
        p3 = "—" if z["pillar3"] is None else f"{z['pillar3']:+.0f}"
        pv = "—" if z["pillar3_vol"] is None else f"{z['pillar3_vol']:+.0f}"
        print(f"   {name:>16}{100 * z['cover_long']:>6.0f}/{100 * z['cover_short']:<5.0f}"
              f"{f(z['short']):>20}{f(z['long']):>20}{p3:>9}{pv:>8}")
    print("   Если строки похожи — меньшая выборка даёт ту же картину, и неполное")
    print("   покрытие, скорее всего, картину не искажает. Если расходятся — искажает.")


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
            "positions": s["n_pos"], "polled": polled}


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
            summary = summarize(pos, P["oi"], P["mark"], sigma, MT.load_curve(cpath))
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
        print(f"Столп 3 = {summary['pillar3']:+.1f}" if summary["pillar3"] is not None else "Столп 3: зоны нет",
              f"(σ {100 * summary['sigma']:.2f}%, Coinbase); кумулятивная 1/2/4:",
              " / ".join("—" if summary["cum"][str(k)]["value"] is None else f"{summary['cum'][str(k)]['value']:+.0f}"
                         for k in CUM_K))
    _append_log(os.path.join(out, "hl_log.csv"), {
        "utc": stamp, "kind": mode, "polled": n, "positions": len(pos), "errors": P["errors"],
        "minutes": f"{P['minutes']:.1f}", "cover_long": f"{L / P['oi']:.4f}",
        "cover_short": f"{S / P['oi']:.4f}", "oi_btc": f"{P['oi']:.2f}", "mark": f"{P['mark']:.1f}",
        "active": "" if P["n_active"] is None else P["n_active"], "known": len(new_known),
        "t_start": P["t_start"], "t_end": P["t_end"]})
    print(f"Снимок: {os.path.relpath(path, out)}; позиций {len(pos)}; покрытие лонги "
          f"{100 * L / P['oi']:.0f}%, шорты {100 * S / P['oi']:.0f}%")
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
    print(f"   {'выборка':<56}{'позиций':>8}{'покр. Л/Ш':>11}{'шорты: зона':>13}{'лонги: зона':>13}"
          f"{'столп 3':>8}{'в 2/5/10%, $ млн: шорты | лонги':>36}{'кум. 1/2/4σ':>16}")
    for name, sub in sets:
        z = summarize(sub, s0["oi_btc"], s0["mark"], s0["sigma"], curve, sens=False)
        zs = lambda w: f"{w['dist_pct']:+.1f}%" if w else "—"
        near = lambda side: "/".join(f"{(z[side] or {}).get('near', {}).get(t, 0) / 1e6:.0f}" for t in (2, 5, 10))
        cum = "/".join("—" if z["cum"][str(kk)]["value"] is None else f"{z['cum'][str(kk)]['value']:+.0f}"
                       for kk in CUM_K)
        p3 = "—" if z["pillar3"] is None else f"{z['pillar3']:+.1f}"
        print(f"   {name:<56}{len(sub):>8}{100 * z['cover_long']:>6.0f}/{100 * z['cover_short']:<4.0f}"
              f"{zs(z['short']):>13}{zs(z['long']):>13}{p3:>8}{near('short') + ' | ' + near('long'):>36}{cum:>16}")
    print("\n   Строки ниже эталона сравниваются с ним: совпадают — состав годен.")
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

def selftest() -> None:
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
        assert [r["kind"] for r in pr] == ["full", "daily"] and pr[1]["cum2"] != ""
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
            assert len(list(csv.DictReader(fh))) == 2
        # σ — только по закрытым дням: свеча, открытая за сутки до снимка, ещё не закрыта
        r40 = [(k * TC.DAY, 1, 1, 100 * math.exp(0.01 * k + 0.02 * (-1) ** k + (0.05 if k == 39 else 0)))
               for k in range(40)]
        c40 = [r[3] for r in r40]
        assert sigma_at(r40, 39 * TC.DAY + 600_000) == TC.daily_vol(c40, 38) != TC.daily_vol(c40, 39)
        assert sigma_at(r40, 40 * TC.DAY) == TC.daily_vol(c40, 39)
    finally:
        g.update(saved)
        TC.load = saved_load
        shutil.rmtree(tmp, ignore_errors=True)
    print("\nselftest: OK — разбор позиции, покрытие, скользящее окно (сетка ошиблась бы), равенство, "
          "хрупкость, вариант B, ограничитель, параллельный прогон, HLP, снимок, пересчёт, ширины, "
          "облачный сборщик (полный/ежедневный/повтор), забор на ноутбук, σ по закрытым дням, "
          "кумулятивная версия, проверка глубины (список, ранний полный снимок, пометка ежедневного состава), "
          "активность таблицы лидеров, "
          "ежедневный режим с известными держателями")


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
    for name in ("recompute", "widths"):
        sub.add_parser(name).add_argument("snapshot", help="путь к снимку или latest — последний")
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
