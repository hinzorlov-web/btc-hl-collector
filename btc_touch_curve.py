"""
btc_touch_curve.py — столп 3, шаг 2, часть 1: кривая «расстояние → вероятность
дойти». PROJECT.md 6.18, решение заказчика 10.10.2026.

Вопрос: если уровень (зона стопов) стоит на расстоянии d от цены, как часто
цена до него доходит за N дней? Ответ — по истории BTCUSDT Binance с 2017 года.
Для этой кривой годится любой уровень: дойдёт ли цена, зависит только от пути
цены. Где стоят настоящие стопы — вход метрики, а не часть кривой.

Расстояние меряется двумя способами:
  • в процентах от цены;
  • в «дневных ходах»: k = расстояние / типичный дневной ход, где типичный
    дневной ход — разброс дневных изменений цены за предыдущие 30 дней.
    «До стопов 3 дневных хода» значит одинаковую близость и в тихом, и
    в бурном рынке.
Какой способ честнее, показывает проверка по эпохам: у правильной шкалы
кривая в разные годы одна и та же.

Касание: уровень вверху — дневной максимум в следующие N дней дошёл до него;
внизу — дневной минимум. Отсчёт от закрытия дня.

Режимы:
  py btc_touch_curve.py selftest
  py btc_touch_curve.py run [--refresh]
Выход: таблицы в консоли и p3_touch_curve.csv.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import random
import statistics
import sys
import time
from datetime import datetime, timezone

try:
    import requests
except ImportError:                                          # pragma: no cover
    requests = None

BINANCE = "https://api.binance.com"
SYMBOL = "BTCUSDT"
START = datetime(2017, 8, 17, tzinfo=timezone.utc)
DAY = 86_400_000
CACHE = "p3_daily_full.json"
VOL_WIN = 30
HORIZONS = (7, 3, 14)                       # 7 — основной (решение 10.10)
K_GRID = (0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0, 6.0, 8.0)
PCT_GRID = (1.0, 2.0, 3.0, 5.0, 7.0, 10.0, 15.0, 20.0)
ERAS = (("2017–2019", datetime(2017, 1, 1), datetime(2020, 1, 1)),
        ("2020–2022", datetime(2020, 1, 1), datetime(2023, 1, 1)),
        ("2023–2026", datetime(2023, 1, 1), datetime(2027, 1, 1)))


# --------------------------------------------------------------------------
# Расчёт
# --------------------------------------------------------------------------

def daily_vol(closes: list[float], t: int, win: int = VOL_WIN) -> float | None:
    """Разброс дневных лог-изменений за win дней, закончившихся днём t."""
    if t < win:
        return None
    r = [math.log(closes[i] / closes[i - 1]) for i in range(t - win + 1, t + 1)]
    return statistics.pstdev(r)


def touched(c: float, highs: list[float], lows: list[float], t: int, n: int,
            d: float, up: bool) -> bool | None:
    """Дошла ли цена от закрытия дня t до уровня c·e^(±d) за n следующих дней."""
    if t + n >= len(highs):
        return None
    if up:
        lvl = c * math.exp(d)
        return any(highs[i] >= lvl for i in range(t + 1, t + n + 1))
    lvl = c * math.exp(-d)
    return any(lows[i] <= lvl for i in range(t + 1, t + n + 1))


def rw_prob(k: float, n: int) -> float:
    """Случайное блуждание без сноса: P(максимум за n дней ≥ k дневных ходов)."""
    z = k / math.sqrt(n)
    return 2.0 * (1.0 - 0.5 * (1.0 + math.erf(z / math.sqrt(2.0))))


def curve(rows, n: int, mode: str, grid, t_from=None, t_to=None, step: int = 1) -> dict:
    """
    rows: список (t_ms, high, low, close). mode: 'k' или 'pct'.
    Возвращает {x: (p_up, p_down, наблюдений)}. step = n — непересекающиеся окна.
    """
    closes = [r[3] for r in rows]
    highs = [r[1] for r in rows]
    lows = [r[2] for r in rows]
    acc = {x: [0, 0, 0] for x in grid}
    for t in range(VOL_WIN, len(rows) - n, step):
        ts = rows[t][0]
        if t_from is not None and ts < t_from:
            continue
        if t_to is not None and ts >= t_to:
            continue
        s = daily_vol(closes, t)
        if not s:
            continue
        for x in grid:
            d = x * s if mode == "k" else math.log(1 + x / 100.0)
            u = touched(closes[t], highs, lows, t, n, d, True)
            w = touched(closes[t], highs, lows, t, n, d, False)
            if u is None or w is None:
                continue
            acc[x][0] += u
            acc[x][1] += w
            acc[x][2] += 1
    return {x: (a / m, b / m, m) if m else (None, None, 0) for x, (a, b, m) in acc.items()}


def era_spread(per_era: list[dict], grid) -> float:
    """Средний по сетке размах (макс − мин) вероятности между эпохами, в п.п."""
    spreads = []
    for x in grid:
        vals = [(e[x][0] + e[x][1]) / 2 for e in per_era if e[x][2]]
        if len(vals) == len(per_era):
            spreads.append(100 * (max(vals) - min(vals)))
    return sum(spreads) / len(spreads) if spreads else float("nan")


# --------------------------------------------------------------------------
# Цены
# --------------------------------------------------------------------------

def fetch_daily() -> list[tuple[int, float, float, float]]:
    if requests is None:
        sys.exit("Нужен пакет requests: py -m pip install requests")
    out, cur = [], int(START.timestamp() * 1000)
    now = int(time.time() * 1000)
    while True:
        r = requests.get(f"{BINANCE}/api/v3/klines",
                         params={"symbol": SYMBOL, "interval": "1d",
                                 "startTime": cur, "limit": 1000}, timeout=30)
        r.raise_for_status()
        kl = r.json()
        if not kl:
            break
        for k in kl:
            if int(k[6]) < now:
                out.append((int(k[0]), float(k[2]), float(k[3]), float(k[4])))
        nxt = int(kl[-1][0]) + DAY
        if len(kl) < 1000 or nxt > now:
            break
        cur = nxt
        time.sleep(0.2)
    gaps = [(a, b) for (a, *_), (b, *_) in zip(out, out[1:]) if b - a != DAY]
    if gaps:
        print(f"[!] разрывов в дневном ряду: {len(gaps)}, первый "
              f"{datetime.fromtimestamp(gaps[0][0] / 1000, timezone.utc):%d.%m.%Y}")
    return out


def load(refresh: bool):
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), CACHE)
    if not refresh and os.path.isfile(path):
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        if time.time() - data.get("fetched", 0) < 12 * 3600:
            return [tuple(r) for r in data["rows"]]
    print("Загрузка дневных свечей BTCUSDT с Binance с 17.08.2017…")
    rows = fetch_daily()
    with open(path, "w", encoding="utf-8") as fh:
        json.dump({"fetched": time.time(), "rows": rows}, fh)
    return rows


# --------------------------------------------------------------------------
# Прогон
# --------------------------------------------------------------------------

def ms(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


def run(refresh: bool) -> None:
    rows = load(refresh)
    closes = [r[3] for r in rows]
    print(f"Дней: {len(rows)}, с {datetime.fromtimestamp(rows[0][0] / 1000, timezone.utc):%d.%m.%Y} "
          f"по {datetime.fromtimestamp(rows[-1][0] / 1000, timezone.utc):%d.%m.%Y}")
    s_now = daily_vol(closes, len(closes) - 1)
    print(f"Типичный дневной ход сейчас (30 дней): {100 * s_now:.2f}%\n")

    out_rows = []
    for n in HORIZONS:
        main = " — ОСНОВНОЙ" if n == HORIZONS[0] else " — справочно"
        full = curve(rows, n, "k", K_GRID)
        indep = curve(rows, n, "k", K_GRID, step=n)
        print(f"СРОК {n} ДНЕЙ{main}: вероятность дойти до уровня за {n} дн.")
        print(f"   {'дневных ходов':>13}{'сейчас это %':>13}{'вверх':>8}{'вниз':>8}"
              f"{'в среднем':>11}{'неперекр.':>11}{'случ. блужд.':>14}")
        for k in K_GRID:
            u, w, m = full[k]
            ui, wi, mi = indep[k]
            avg = (u + w) / 2
            avg_i = (ui + wi) / 2
            print(f"   {k:>13.2f}{100 * k * s_now:>12.1f}%{100 * u:>7.0f}%{100 * w:>7.0f}%"
                  f"{100 * avg:>10.0f}%{100 * avg_i:>10.0f}%{100 * rw_prob(k, n):>13.0f}%")
            out_rows.append({"horizon_d": n, "k": k, "p_up": round(u, 4), "p_down": round(w, 4),
                             "p_avg": round(avg, 4), "p_avg_nonoverlap": round(avg_i, 4),
                             "n_obs": m, "n_nonoverlap": mi, "p_random_walk": round(rw_prob(k, n), 4)})
        print(f"   наблюдений: {full[K_GRID[0]][2]} (дни подряд, окна перекрываются); "
              f"неперекрывающихся окон: {indep[K_GRID[0]][2]}\n")

    # Проверка шкалы по эпохам: какой способ мерить расстояние устойчивее
    n = HORIZONS[0]
    print(f"ПРОВЕРКА ШКАЛЫ ПО ЭПОХАМ (срок {n} дней): вероятность дойти, среднее вверх/вниз")
    for mode, grid, label in (("pct", PCT_GRID, "%"), ("k", K_GRID, "дн.ходов")):
        per = [curve(rows, n, mode, grid, ms(a), ms(b)) for _, a, b in ERAS]
        print(f"   расстояние в {label}:")
        print(f"   {'':>10}" + "".join(f"{x:>7g}" for x in grid))
        for (name, _, _), e in zip(ERAS, per):
            print(f"   {name:>10}" + "".join(
                f"{100 * (e[x][0] + e[x][1]) / 2:>6.0f}%" if e[x][2] else f"{'—':>7}" for x in grid))
        print(f"   средний разброс между эпохами: {era_spread(per, grid):.1f} п.п.\n")
    print("   Меньше разброс — шкала честнее: одна и та же кривая годится для всех лет.")

    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "p3_touch_curve.csv")
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(out_rows[0].keys()))
        w.writeheader()
        w.writerows(out_rows)
    print(f"\nКривая записана: {path}")


# --------------------------------------------------------------------------
# Самопроверка
# --------------------------------------------------------------------------

def selftest() -> None:
    # Теория: 2(1 − Φ(k/√n)); при k = 0 вероятность 1.
    assert abs(rw_prob(0.0, 7) - 1.0) < 1e-12
    assert abs(rw_prob(2.0, 4) - 2 * (1 - 0.8413447460685429)) < 1e-9
    # Касание вверх и вниз
    hi = [10, 10, 10.5, 10]
    lo = [10, 9.8, 9.9, 9.0]
    assert touched(10.0, hi, lo, 0, 2, math.log(1.04), True) is True
    assert touched(10.0, hi, lo, 0, 1, math.log(1.04), True) is False
    assert touched(10.0, hi, lo, 0, 3, -math.log(0.95), False) is True
    assert touched(10.0, hi, lo, 2, 2, 0.01, True) is None
    # Случайное блуждание с внутридневным путём: эмпирика близка к теории
    random.seed(3)
    rows, c, t = [], 100.0, 0
    for _ in range(3000):
        h = l = c
        for _ in range(48):
            c *= math.exp(random.gauss(0, 0.02 / math.sqrt(48)))
            h, l = max(h, c), min(l, c)
        rows.append((t, h, l, c))
        t += DAY
    cv = curve(rows, 7, "k", (1.0, 2.0, 3.0))
    for k in (1.0, 2.0, 3.0):
        emp = (cv[k][0] + cv[k][1]) / 2
        assert abs(emp - rw_prob(k, 7)) < 0.06, (k, emp, rw_prob(k, 7))
    # Шкала в дневных ходах на блуждании с меняющейся волатильностью устойчивее процентов
    rows2, c = [], 100.0
    for i in range(3000):
        sig = 0.04 if i < 1500 else 0.015
        h = l = c
        for _ in range(48):
            c *= math.exp(random.gauss(0, sig / math.sqrt(48)))
            h, l = max(h, c), min(l, c)
        rows2.append((i * DAY, h, l, c))
    half = 1500 * DAY
    per_k = [curve(rows2, 7, "k", K_GRID, 0, half), curve(rows2, 7, "k", K_GRID, half, None)]
    per_p = [curve(rows2, 7, "pct", PCT_GRID, 0, half), curve(rows2, 7, "pct", PCT_GRID, half, None)]
    assert era_spread(per_k, K_GRID) < era_spread(per_p, PCT_GRID), "шкала дневных ходов должна быть устойчивее"
    print("selftest: OK — теория блуждания, касания, эмпирика против теории, "
          "устойчивость шкалы дневных ходов при смене волатильности")


def main() -> None:
    ap = argparse.ArgumentParser(description="Кривая «расстояние → вероятность дойти»")
    sub = ap.add_subparsers(dest="mode", required=True)
    sub.add_parser("selftest")
    r = sub.add_parser("run")
    r.add_argument("--refresh", action="store_true")
    a = ap.parse_args()
    if a.mode == "selftest":
        selftest()
    else:
        run(a.refresh)


if __name__ == "__main__":
    main()
