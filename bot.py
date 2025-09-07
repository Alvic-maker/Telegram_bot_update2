#!/usr/bin/env python3
# bot.py - fixed version: primary data from yfinance (reliable), optional vnstock for foreign flows/market info
# Purpose: produce market + per-symbol report and send to Telegram.
# Requirements: pip install yfinance pandas numpy requests vnstock3 (optional for foreign flows)
# Env: BOT_TOKEN, CHAT_ID, USE_VNSTOCK (optional)

import os, traceback
from datetime import datetime, timedelta
import requests
import pandas as pd
import numpy as np
import yfinance as yf

# try vnstock
vns = None
try:
    import vnstock as vns_mod
    vns = vns_mod
except Exception:
    try:
        import vnstock3 as vns_mod
        vns = vns_mod
    except Exception:
        vns = None

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("CHAT_ID", "").strip()
USE_VNSTOCK = os.getenv("USE_VNSTOCK", "1").lower() not in ("0", "false", "no", "")

SYMBOLS = ["MBB","HPG","SSI","PVP","KSB","QTP"]

PCT_ALERT_UP = float(os.getenv("PCT_ALERT_UP", "3.0"))
PCT_ALERT_DOWN = float(os.getenv("PCT_ALERT_DOWN", "-3.0"))
VOL_SURGE_MULT = float(os.getenv("VOL_SURGE_MULT", "2.0"))

def dbg(msg):
    print(f"[DEBUG] {msg}", flush=True)

def fm_money_million(v):
    if v is None: return "—"
    try:
        return f"{v/1_000_000:,.0f} Mn"
    except:
        return str(v)

def fm_shares_million(v):
    if v is None: return "—"
    try:
        return f"{v/1_000_000:,.2f} Mn"
    except:
        return str(v)

def fm_pct(x):
    if x is None: return "—"
    return f"{x:+.2f}%"

# vnstock wrappers for foreign flows & market-level (best-effort)
def try_vn_foreign(symbol=None):
    if vns is None:
        return None
    # try common function names and modules
    candidates = []
    if hasattr(vns, "stock"):
        candidates += [("stock","foreign_trade"),("stock","foreign"),("stock","foreign_flow"),("stock","top_foreign_trade")]
    candidates += [(None,"foreign_trade"),(None,"foreign"),(None,"top_foreign_trade")]
    for mod_attr, name in candidates:
        try:
            target = getattr(vns, mod_attr) if mod_attr and hasattr(vns, mod_attr) else vns
            if hasattr(target, name):
                func = getattr(target, name)
                try:
                    if symbol is None:
                        res = func()
                    else:
                        # try both symbol and [symbol]
                        try:
                            res = func(symbol)
                        except TypeError:
                            res = func([symbol])
                except Exception:
                    res = func(symbol)
                # attempt to parse result into dict with buy/sell keys
                if isinstance(res, dict):
                    return res
                try:
                    # maybe pandas DataFrame or series -> convert last row to dict
                    if hasattr(res, "to_dict"):
                        d = res.to_dict()
                        # if dict of lists, take last elements
                        if any(isinstance(v, (list, pd.Series, np.ndarray)) for v in d.values()):
                            d2 = {k: (v[-1] if isinstance(v,(list,pd.Series,np.ndarray)) and len(v)>0 else v) for k,v in d.items()}
                            return d2
                        return d
                except Exception:
                    return None
        except Exception as e:
            continue
    return None

# yfinance per-symbol fetch
def yf_get_symbol(symbol):
    try:
        ticker = f"{symbol}.VN"
        tk = yf.Ticker(ticker)
        hist = tk.history(period="60d", auto_adjust=False)
        if hist is None or hist.empty:
            return None
        close = hist['Close'].astype(float).dropna()
        last = float(close.iloc[-1])
        prev = float(close.iloc[-2]) if len(close)>=2 else last
        pct = (last/prev - 1) * 100 if prev!=0 else 0.0
        vol = int(hist['Volume'].astype(float).iloc[-1]) if 'Volume' in hist else None
        avg5 = float(close.tail(5).mean()) if len(close)>=5 else None
        avgvol20 = int(hist['Volume'].astype(float).tail(20).mean()) if 'Volume' in hist and len(hist['Volume'].dropna())>=20 else None
        # sma and rsi
        sma20 = float(close.rolling(20).mean().iloc[-1]) if len(close)>=20 else None
        sma50 = float(close.rolling(50).mean().iloc[-1]) if len(close)>=50 else None
        rsi14 = None
        if len(close)>=15:
            delta = close.diff().dropna()
            ups = delta.clip(lower=0)
            downs = -delta.clip(upper=0)
            roll_up = ups.ewm(alpha=1/14, adjust=False).mean()
            roll_down = downs.ewm(alpha=1/14, adjust=False).mean()
            if roll_down.iloc[-1] != 0:
                rs = roll_up.iloc[-1] / roll_down.iloc[-1]
                rsi14 = 100 - (100 / (1 + rs))
            else:
                rsi14 = 100.0
        return {"price": last, "pct": pct, "vol": vol, "avg5_price": avg5, "avg5_vol": avgvol20, "sma20": sma20, "sma50": sma50, "rsi14": rsi14}
    except Exception as e:
        dbg(f"yf_get_symbol error for {symbol}: {e}")
        return None

# index via yfinance fallback
def yf_get_index(index_ticker="^VNINDEX"):
    try:
        tk = yf.Ticker(index_ticker)
        hist = tk.history(period="120d", auto_adjust=False)
        if hist is None or hist.empty:
            return None
        close = hist['Close'].astype(float).dropna()
        last = float(close.iloc[-1])
        prev = float(close.iloc[-2]) if len(close)>=2 else last
        pct = (last/prev - 1) * 100 if prev!=0 else 0.0
        return {"last": last, "pct": pct}
    except Exception as e:
        dbg(f"yf_get_index error: {e}")
        return None

def build_report():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = []
    lines.append(f"📊 Báo cáo thị trường — {now}")

    # Part 1 - market summary: try vnstock for market-level, else yfinance
    idx_info = None
    if vns is not None and USE_VNSTOCK:
        try:
            # try vnstock index-like function names
            for cand in ("index","get_index","market_index"):
                if hasattr(vns, "stock") and hasattr(vns.stock, cand):
                    try:
                        df = getattr(vns.stock, cand)("VNINDEX","1D", count=60)
                        if df is not None and hasattr(df, "iloc"):
                            idx_info = {"last": float(df['close'].iloc[-1]), "pct": float(df['pct_change'].iloc[-1]) if 'pct_change' in df.columns else None}
                            break
                    except Exception:
                        continue
        except Exception as e:
            dbg(f"vnstock index attempt failed: {e}")
    if idx_info is None:
        # yfinance fallback
        idx_info = yf_get_index("^VNINDEX")

    if idx_info and idx_info.get("last") is not None:
        lines.append(f"📈 VN-Index: {idx_info['last']:.2f} ({idx_info['pct']:+.2f}%) | GTGD: {fm_money_million(None)}")
    else:
        lines.append("📈 VN-Index: — (lỗi dữ liệu)")

    # foreign market-level
    fmarket = None
    if vns is not None and USE_VNSTOCK:
        try:
            fmarket = try_vn_foreign(None)
        except Exception as e:
            dbg(f"vnstock foreign market failed: {e}")
    if fmarket:
        # attempt to parse keys
        fb = fmarket.get('buy') or fmarket.get('buy_value') or fmarket.get('total_buy') or fmarket.get('buy_total')
        fs = fmarket.get('sell') or fmarket.get('sell_value') or fmarket.get('total_sell') or fmarket.get('sell_total')
        fn = None
        try:
            if fb is not None and fs is not None:
                fn = fb - fs
        except:
            fn = None
        lines.append(f"🔁 Khối ngoại (toàn TT): Mua {fm_money_million(fb)} / Bán {fm_money_million(fs)} → Ròng {fm_money_million(fn)}")
    else:
        lines.append(f"🔁 Khối ngoại (toàn TT): —")

    lines.append("")

    # Part 2 - per symbol: primary yfinance, optional vnstock for NN flows
    lines.append("📌 Chi tiết mã:")
    details = []
    for s in SYMBOLS:
        info = yf_get_symbol(s)
        fb = fs = None
        if vns is not None and USE_VNSTOCK:
            try:
                fr = try_vn_foreign(s)
                if isinstance(fr, dict):
                    fb = fr.get('buy') or fr.get('buy_value') or fr.get('buy_total')
                    fs = fr.get('sell') or fr.get('sell_value') or fr.get('sell_total')
            except Exception as e:
                dbg(f"vn foreign per-symbol failed for {s}: {e}")
                fb = fs = None
        # build line
        if not info:
            lines.append(f"{s}: — (lỗi dữ liệu)")
            continue
        price = info.get('price')
        pct = info.get('pct')
        vol = info.get('vol')
        avg5 = info.get('avg5_price')
        avgvol20 = info.get('avg5_vol')
        sma20 = info.get('sma20'); sma50 = info.get('sma50'); rsi14 = info.get('rsi14')
        vol_ratio = (vol / avgvol20) if vol and avgvol20 else None
        vol_ratio_s = f" (VolRatio={vol_ratio:.2f}×)" if vol_ratio else ""
        lines.append(f"{s}: {price if price else '—'} {fm_pct(pct)} | KL={fm_shares_million(vol)}{vol_ratio_s} | TB tuần: {avg5 if avg5 else '—'} / {fm_shares_million(avgvol20)} | NN: Mua {fm_money_million(fb)} / Bán {fm_money_million(fs)}")
    # Alerts simple
    lines.append("")
    lines.append("⚠️ Alerts:")
    alerts = []
    for s in SYMBOLS:
        info = yf_get_symbol(s)
        if info:
            if info['pct'] >= PCT_ALERT_UP:
                alerts.append(f"🔥 {s} tăng mạnh: {fm_pct(info['pct'])}")
            elif info['pct'] <= PCT_ALERT_DOWN:
                alerts.append(f"📉 {s} giảm sâu: {fm_pct(info['pct'])}")
            
            vol_ratio = (info['vol'] / info['avg5_vol']) if info.get('vol') and info.get('avg5_vol') else None
            if vol_ratio and vol_ratio >= VOL_SURGE_MULT:
                alerts.append(f"📊 {s} KLGD đột biến: {vol_ratio:.1f}× TB")

    if alerts:
        lines.extend(alerts)
    else:
        lines.append("(Không có cảnh báo)")

    lines.append("")
    lines.append(f"(Thời gian báo cáo: {now}) - Bot_fixed")
    return "\n".join(lines)

def chunk_text(s, limit=3500):
    s = s or ""
    if len(s) <= limit: return [s]
    chunks = []
    cur = s
    while len(cur) > limit:
        cut = cur.rfind("\n", 0, limit)
        if cut == -1: cut = limit
        chunks.append(cur[:cut])
        cur = cur[cut:]
    if cur: chunks.append(cur)
    return chunks

def send_to_telegram(text):
    if not BOT_TOKEN or not CHAT_ID:
        dbg("BOT_TOKEN or CHAT_ID missing - printing preview instead of sending")
        print(text)
        return False
    base = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    dbg(f"Sending to Telegram CHAT_ID={CHAT_ID}, token=***{BOT_TOKEN[-6:]}")
    parts = chunk_text(text, 3500)
    ok = True
    for i, part in enumerate(parts, 1):
        payload = {"chat_id": CHAT_ID, "text": part, "disable_web_page_preview": True}
        try:
            r = requests.post(base, data=payload, timeout=20)
            dbg(f"Telegram response {r.status_code}: {r.text[:200]}")
            if r.status_code != 200: ok = False
        except Exception as e:
            dbg(f"Telegram send error: {e}")
            ok = False
    return ok

def main():
    try:
        report = build_report()
        print("=== Report preview ===")
        print(report)
        sent = send_to_telegram(report)
        if sent:
            dbg("Report sent/attempted.")
        else:
            dbg("Report not sent.")
    except Exception as e:
        print("Fatal error:", e)
        traceback.print_exc()

if __name__ == '__main__':
    main()
