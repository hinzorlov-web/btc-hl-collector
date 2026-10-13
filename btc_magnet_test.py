"""
btc_magnet_test.py — столп 3, шаг 2, часть 3: «стопы притягивают цену»?
PROJECT.md 6.18. Правила записаны 11.10.2026 до прогона.

Вопрос: до настоящих зон стопов (GMX v1) цена доходит чаще, чем до обычного
уровня на том же расстоянии? «Обычный уровень» — кривая из btc_touch_curve.py
(p3_touch_curve.csv, срок 7 дней, отдельно вверх и вниз).

Правила:
  • Данные: gmx_zones.csv (btc_gmx_grid.py zones), цены — кэш btc_touch_curve.py.
  • Период: с 01.10.2021 (первый месяц GMX — набор позиций с нуля) по последний
    день, у которого есть 7 дней вперёд.
  • Зона на день: для шортов — полоса 0,5% над ценой с наибольшим объёмом,
    для лонгов — под ценой; только в пределах 8 дневных ходов.
  • Касание: дневной максимум (для шортов) или минимум (для лонгов) в следующие
    7 дней дошёл до зоны.
  • Ожидание: вероятность по кривой для того же расстояния в дневных ходах.
  • Главный счёт — непересекающиеся окна (каждый 7-й день): касаний против
    ожидаемых и вероятность получить столько же или больше случайно.
    Справочно — все дни подряд и разбивка по расстоянию.
Ограничения: GMX — малая площадка с пулом-контрагентом; цена ликвидации без
накопленной платы за заём. Выборка, не рынок.

Режимы:
  py btc_magnet_test.py selftest
  py btc_magnet_test.py run
"""
from __future__ import annotations

import argparse
import csv
import math
import os
import random
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import btc_touch_curve as TC                                    # noqa: E402

HORIZON = 7
K_MAX = 8.0
START = "2021-10-01"


# --------------------------------------------------------------------------
# Кривая и зоны
# --------------------------------------------------------------------------

def load_curve(path: str, n: int = HORIZON) -> dict:
    ks, up, dn = [], [], []
    with open(path, encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            if int(r["horizon_d"]) == n:
                ks.append(float(r["k"]))
                up.append(float(r["p_up"]))
                dn.append(float(r["p_down"]))
    if not ks:
        sys.exit(f"В {path} нет кривой для срока {n} дней — запустите btc_touch_curve.py run")
    return {"k": ks, True: up, False: dn}


def interp(ks: list[float], ps: list[float], k: float) -> float:
    """Линейно между точками кривой; у нуля — к 1, дальше последней — последняя."""
    if k <= ks[0]:
        return 1.0 + (ps[0] - 1.0) * (k / ks[0])
    for (k0, p0), (k1, p1) in zip(zip(ks, ps), zip(ks[1:], ps[1:])):
        if k <= k1:
            return p0 + (p1 - p0) * (k - k0) / (k1 - k0)
    return ps[-1]


def load_zones(path: str) -> dict:
    """date → {'short': [(dist_pct, size)], 'long': [...]}"""
    z: dict = {}
    with open(path, encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            z.setdefault(r["date"], {"short": [], "long": []})[r["side"]].append(
                (float(r["dist_bin_pct"]), float(r["size_usd"])))
    return z


def densest(rows: list, up: bool, sigma: float) -> tuple | None:
    """Полоса с наибольшим объёмом на своей стороне в пределах K_MAX дневных ходов."""
    best = None
    tot = 0.0
    for dist, size in rows:
        if (up and dist <= 0) or (not up and dist >= 0):
            continue
        d = math.log(1 + dist / 100.0) if up else -math.log(1 + dist / 100.0)
        k = d / sigma
        if k > K_MAX:
            continue
        tot += size
        if best is None or size > best[1]:
            best = (dist, size, d, k)
    if best is None:
        return None
    return best + (best[1] / tot if tot else 0.0,)


# --------------------------------------------------------------------------
# Счёт
# --------------------------------------------------------------------------

def poibin_tails(k: int, ps: list[float]) -> tuple[float, float]:
    dist = [1.0]
    for p in ps:
        nxt = [0.0] * (len(dist) + 1)
        for x, q in enumerate(dist):
            nxt[x] += q * (1 - p)
            nxt[x + 1] += q * p
        dist = nxt
    return sum(dist[k:]), sum(dist[:k + 1])


def evaluate(rows, zones: dict, curve: dict, start: str = START) -> list[dict]:
    closes = [r[3] for r in rows]
    highs = [r[1] for r in rows]
    lows = [r[2] for r in rows]
    idx = {datetime.fromtimestamp(r[0] / 1000, timezone.utc).strftime("%Y-%m-%d"): i
           for i, r in enumerate(rows)}
    out = []
    for date in sorted(zones):
        if date < start or date not in idx:
            continue
        t = idx[date]
        sigma = TC.daily_vol(closes, t)
        if not sigma:
            continue
        for up, side in ((True, "short"), (False, "long")):
            z = densest(zones[date][side], up, sigma)
            if z is None:
                continue
            dist, size, d, k, share = z
            hit = TC.touched(closes[t], highs, lows, t, HORIZON, d, up)
            if hit is None:
                continue
            out.append({"date": date, "t": t, "side": side, "dist_pct": dist, "k": round(k, 3),
                        "size_usd": size, "share": round(share, 3), "touched": int(hit),
                        "expected": round(interp(curve["k"], curve[up], k), 4)})
    return out


def report(recs: list[dict]) -> None:
    if not recs:
        print("Нет наблюдений.")
        return
    print(f"Наблюдений (дни × сторона): {len(recs)}; дней с {recs[0]['date']} по {recs[-1]['date']}\n")
    print("ГЛАВНЫЙ СЧЁТ — непересекающиеся окна по 7 дней:")
    t0 = min(r["t"] for r in recs)
    for side in ("short", "long", None):
        g = [r for r in recs if (side is None or r["side"] == side) and (r["t"] - t0) % HORIZON == 0]
        if not g:
            continue
        obs = sum(r["touched"] for r in g)
        ps = [r["expected"] for r in g]
        exp = sum(ps)
        hi, lo = poibin_tails(obs, ps)
        name = {"short": "стопы шортов (вверх)", "long": "стопы лонгов (вниз)", None: "ВСЕГО"}[side]
        print(f"   {name:<22} окон {len(g):>4}: дошла {obs:>4} ({obs / len(g):.0%}), "
              f"ожидалось {exp:6.1f} ({exp / len(g):.0%}); отношение {obs / exp:.2f}; "
              f"случайно столько или больше {hi:.3f}, столько или меньше {lo:.3f}")
    print("\nСПРАВОЧНО — все дни подряд (окна перекрываются, вероятности не считаются):")
    for side in ("short", "long"):
        g = [r for r in recs if r["side"] == side]
        obs, exp = sum(r["touched"] for r in g), sum(r["expected"] for r in g)
        print(f"   {side:<6} дней {len(g):>5}: дошла {obs / len(g):.0%}, ожидалось {exp / len(g):.0%}, "
              f"отношение {obs / exp:.2f}")
    print("\nСПРАВОЧНО — по расстоянию до зоны (все дни, обе стороны):")
    for a, b in ((0, 1), (1, 2), (2, 4), (4, K_MAX)):
        g = [r for r in recs if a <= r["k"] < b]
        if g:
            obs, exp = sum(r["touched"] for r in g), sum(r["expected"] for r in g)
            print(f"   {a:g}–{b:g} дн. ходов: наблюдений {len(g):>5}; дошла {obs / len(g):.0%}, "
                  f"ожидалось {exp / len(g):.0%}, отношение {obs / exp:.2f}")


def run() -> None:
    zpath = os.path.join(HERE, "gmx_zones.csv")
    cpath = os.path.join(HERE, "p3_touch_curve.csv")
    for p, how in ((zpath, "py btc_gmx_grid.py zones"), (cpath, "py btc_touch_curve.py run")):
        if not os.path.isfile(p):
            sys.exit(f"Нет {os.path.basename(p)} — сначала: {how}")
    rows = TC.load(False)
    recs = evaluate(rows, load_zones(zpath), load_curve(cpath))
    report(recs)
    out = os.path.join(HERE, "p3_magnet.csv")
    with open(out, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(recs[0].keys()) if recs else ["date"])
        w.writeheader()
        w.writerows(recs)
    print(f"\nТаблица: {out}")


# --------------------------------------------------------------------------
# Самопроверка
# --------------------------------------------------------------------------

def selftest() -> None:
    ks, ps = [0.25, 1.0, 2.0], [0.9, 0.7, 0.4]
    assert abs(interp(ks, ps, 0.0) - 1.0) < 1e-12 and abs(interp(ks, ps, 1.5) - 0.55) < 1e-12
    assert interp(ks, ps, 5.0) == 0.4
    z = densest([(-2.0, 100), (-4.0, 300), (1.5, 50), (-30.0, 999)], False, 0.02)
    assert z[0] == -4.0 and abs(z[4] - 300 / 400) < 1e-12, z      # −30% дальше 8 ходов
    assert densest([(1.0, 10)], False, 0.02) is None
    # На случайном блуждании с зонами в случайных местах «магнита» нет:
    # отношение касаний к ожидаемому около 1.
    random.seed(5)
    rows, c, t = [], 100.0, 0
    for _ in range(1500):
        h = l = c
        for _ in range(24):
            c *= math.exp(random.gauss(0, 0.025 / math.sqrt(24)))
            h, l = max(h, c), min(l, c)
        rows.append((t, h, l, c))
        t += TC.DAY
    curve = {"k": list(TC.K_GRID),
             True: [TC.rw_prob(k, HORIZON) for k in TC.K_GRID],
             False: [TC.rw_prob(k, HORIZON) for k in TC.K_GRID]}
    zones = {}
    for r in rows:
        d = datetime.fromtimestamp(r[0] / 1000, timezone.utc).strftime("%Y-%m-%d")
        zones[d] = {"short": [(random.uniform(0.5, 12), 100.0)], "long": [(-random.uniform(0.5, 12), 100.0)]}
    recs = evaluate(rows, zones, curve, start="1970-01-01")
    obs, exp = sum(r["touched"] for r in recs), sum(r["expected"] for r in recs)
    assert 0.85 < obs / exp < 1.1, obs / exp
    print(f"selftest: OK — кривая, зона, на случайном блуждании без магнита отношение {obs / exp:.2f}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Проверка: стопы притягивают цену?")
    sub = ap.add_subparsers(dest="mode", required=True)
    sub.add_parser("selftest")
    sub.add_parser("run")
    a = ap.parse_args()
    selftest() if a.mode == "selftest" else run()


if __name__ == "__main__":
    main()
