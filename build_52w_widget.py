from __future__ import annotations

import json
import re
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from pykrx import stock

OUTPUT_DIR = Path("docs")

LOOKBACK_TRADING_DAYS = 252
FETCH_BUFFER_DAYS = 420
MIN_HISTORY_DAYS = 200
REQUEST_SLEEP = 0.20
MARKETS = ("KOSPI", "KOSDAQ")

EXCLUDE_PREFERRED = True
EXCLUDE_SPAC = True
EXCLUDE_REITS = False

COUNT_EQUAL_HIGH_AS_NEW = True
COUNT_EQUAL_LOW_AS_NEW = True


def latest_trading_date(max_back_days: int = 14) -> str:
    """최근 거래일(YYYYMMDD) 찾기 - 삼성전자 기준"""
    today = date.today()
    for n in range(max_back_days):
        d = (today - timedelta(days=n)).strftime("%Y%m%d")
        try:
            df = stock.get_market_ohlcv(d, d, "005930")
            if isinstance(df, pd.DataFrame) and not df.empty:
                return d
        except Exception as e:
            print(f"[latest_trading_date] {d} 조회 실패: {e}")
    raise RuntimeError("최근 거래일을 찾지 못했습니다.")


def is_preferred_stock(name: str) -> bool:
    patterns = [
        r"우$",
        r"\d우$",
        r"우B$",
        r"우C$",
        r"\d우B$",
        r"\d우C$",
        r"전환우",
        r"우선주",
    ]
    return any(re.search(p, name) for p in patterns)


def should_skip_name(name: str) -> bool:
    if EXCLUDE_PREFERRED and is_preferred_stock(name):
        return True
    if EXCLUDE_SPAC and "스팩" in name:
        return True
    if EXCLUDE_REITS and "리츠" in name:
        return True
    return False


def to_int(v):
    if pd.isna(v):
        return None
    return int(v)


def to_float(v, digits: int = 2):
    if pd.isna(v):
        return None
    return round(float(v), digits)


def build_universe(target_date: str) -> list[dict]:
    items = []
    for market in MARKETS:
        tickers = stock.get_market_ticker_list(target_date, market=market)
        for ticker in tickers:
            name = stock.get_market_ticker_name(ticker)
            if should_skip_name(name):
                continue
            items.append({
                "ticker": ticker,
                "name": name,
                "market": market,
            })
    return items


def analyze_one(item: dict, start_date: str, target_date: str) -> dict | None:
    ticker = item["ticker"]
    name = item["name"]
    market = item["market"]

    df = stock.get_market_ohlcv(start_date, target_date, ticker)
    if df is None or df.empty:
        return None

    df = df[df["종가"] > 0].copy()
    if df.empty:
        return None

    window = df.tail(LOOKBACK_TRADING_DAYS).copy()
    if len(window) < MIN_HISTORY_DAYS:
        return None

    today = window.iloc[-1]
    prev = window.iloc[:-1]
    if prev.empty:
        return None

    if int(today["고가"]) == 0 and int(today["저가"]) == 0:
        return None

    prev_high_52w = prev["고가"].max()
    prev_low_52w = prev["저가"].min()
    high_52w = window["고가"].max()
    low_52w = window["저가"].min()

    if COUNT_EQUAL_HIGH_AS_NEW:
        is_new_high = (today["고가"] >= prev_high_52w) and (today["고가"] == high_52w)
    else:
        is_new_high = (today["고가"] > prev_high_52w) and (today["고가"] == high_52w)

    if COUNT_EQUAL_LOW_AS_NEW:
        is_new_low = (today["저가"] <= prev_low_52w) and (today["저가"] == low_52w)
    else:
        is_new_low = (today["저가"] < prev_low_52w) and (today["저가"] == low_52w)

    prev_close = prev.iloc[-1]["종가"]
    if "등락률" in window.columns:
        pct_change = float(today["등락률"])
    else:
        pct_change = ((float(today["종가"]) / float(prev_close)) - 1.0) * 100.0

    return {
        "ticker": ticker,
        "name": name,
        "market": market,
        "close": to_int(today["종가"]),
        "day_high": to_int(today["고가"]),
        "day_low": to_int(today["저가"]),
        "pct_change": to_float(pct_change, 2),
        "high_52w": to_int(high_52w),
        "low_52w": to_int(low_52w),
        "is_new_high": bool(is_new_high),
        "is_new_low": bool(is_new_low),
        "history_days": len(window),
    }


def build_payload(results: list[dict], target_date: str, universe_count: int) -> dict:
    highs = [x for x in results if x["is_new_high"]]
    lows = [x for x in results if x["is_new_low"]]

    highs.sort(key=lambda x: (-(x["pct_change"] or -9999), x["market"], x["name"]))
    lows.sort(key=lambda x: ((x["pct_change"] or 9999), x["market"], x["name"]))

    high_count = len(highs)
    low_count = len(lows)
    denom = high_count + low_count
    ratio = (high_count / denom) if denom > 0 else None

    summary_by_market = {}
    for m in MARKETS:
        mh = len([x for x in highs if x["market"] == m])
        ml = len([x for x in lows if x["market"] == m])
        summary_by_market[m] = {
            "high_count": mh,
            "low_count": ml,
        }

    payload = {
        "as_of": target_date,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "formula": "52주 신고가 수 / (52주 신고가 수 + 52주 신저가 수)",
        "universe_count": universe_count,
        "high_count": high_count,
        "low_count": low_count,
        "ratio": None if ratio is None else round(ratio, 4),
        "ratio_pct": None if ratio is None else round(ratio * 100, 2),
        "summary_by_market": summary_by_market,
        "highs": highs,
        "lows": lows,
    }
    return payload


def save_payload(payload: dict):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUTPUT_DIR / "data.json"
    out.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"저장 완료: {out.resolve()}")


def main():
    target_date = latest_trading_date()
    start_date = (
        datetime.strptime(target_date, "%Y%m%d").date() - timedelta(days=FETCH_BUFFER_DAYS)
    ).strftime("%Y%m%d")

    print(f"기준일: {target_date}")
    print(f"조회 시작일: {start_date}")

    universe = build_universe(target_date)
    print(f"분석 대상 종목 수(필터 적용 후): {len(universe)}")

    results = []
    total = len(universe)

    for i, item in enumerate(universe, start=1):
        try:
            row = analyze_one(item, start_date, target_date)
            if row is not None:
                results.append(row)
        except Exception as e:
            print(f"[{i}/{total}] 오류 - {item['ticker']} {item['name']}: {e}")

        if i % 50 == 0 or i == total:
            print(f"[진행] {i}/{total}")

        time.sleep(REQUEST_SLEEP)

    payload = build_payload(results, target_date, len(universe))
    save_payload(payload)

    print("-" * 60)
    print(f"52주 신고가 수: {payload['high_count']}")
    print(f"52주 신저가 수: {payload['low_count']}")
    print(f"비율: {payload['ratio_pct']}%")


if __name__ == "__main__":
    main()
