# src/bot.py
import os
import re
import time
import unicodedata
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

import requests
import pandas as pd

# ========= ENV =========
TELEGRAM_TOKEN         = os.getenv("TELEGRAM_TOKEN", "").strip()
CHAT_ID                = os.getenv("CHAT_ID", "").strip()
GITHUB_CSV_URL         = os.getenv("GITHUB_CSV_URL", "").strip()

AVG_GOALS_THRESHOLD    = float(os.getenv("AVG_GOALS_THRESHOLD", "2.5"))
CHECK_TIME_MINUTES     = int(os.getenv("CHECK_TIME_MINUTES", "50"))
CHECK_INTERVAL_SECONDS = int(os.getenv("CHECK_INTERVAL_SECONDS", "300"))

EXCLUDE_KEYWORDS       = [k.strip() for k in os.getenv("LEAGUE_EXCLUDE_KEYWORDS", "").split(",") if k.strip()]

RAPIDAPI_BASE          = os.getenv("RAPIDAPI_BASE", "https://bet365data.p.rapidapi.com").rstrip("/")
RAPIDAPI_HOST          = os.getenv("RAPIDAPI_HOST", "bet365data.p.rapidapi.com")
RAPIDAPI_KEY           = os.getenv("RAPIDAPI_KEY", "")
RAPIDAPI_EVENTS_PATH   = os.getenv("RAPIDAPI_EVENTS_PATH", "/live-events")
RAPIDAPI_EVENTS_PARAMS = os.getenv("RAPIDAPI_EVENTS_PARAMS", "sport=soccer")

SEND_STARTUP_MESSAGE   = os.getenv("SEND_STARTUP_MESSAGE", "1").strip() == "1"

HEADERS_RAPIDAPI = {
    "x-rapidapi-host": RAPIDAPI_HOST,
    "x-rapidapi-key": RAPIDAPI_KEY,
}

def log(msg: str) -> None:
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S} - INFO - {msg}", flush=True)

# ========= Utils =========
def normalize(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"\([^)]*\)", " ", s)    # rimuovi (F), (U19) ecc.
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def send_telegram(text: str) -> None:
    if not TELEGRAM_TOKEN or not CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url, json={
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }, timeout=20)

def read_csv(url: str) -> pd.DataFrame:
    df = pd.read_csv(url)
    # mappa colonne comuni
    rename = {}
    lower = {c.lower(): c for c in df.columns}
    def pick(opts):
        for o in opts:
            if o.lower() in lower:
                return lower[o.lower()]
        return None

    map_to = {
        "home": ["home", "home_team", "Home Team"],
        "away": ["away", "away_team", "Away Team"],
        "league": ["league", "competition", "Country & League"],
        "avg": ["avg", "avg_total_goals", "Avg Total Goals", "Average Total Goals"]
    }
    for k, opts in map_to.items():
        real = pick(opts)
        if real:
            rename[real] = k

    if rename:
        df = df.rename(columns=rename)

    for c in ["home", "away", "league"]:
        if c in df.columns:
            df[c] = df[c].astype(str).fillna("").map(str).map(lambda x: x.strip())
    if "avg" in df.columns:
        df["avg"] = pd.to_numeric(df["avg"], errors="coerce")
    return df

def league_allowed(league_name: str) -> bool:
    if not EXCLUDE_KEYWORDS:
        return True
    L = league_name.lower()
    return not any(k.lower() in L for k in EXCLUDE_KEYWORDS)

def fetch_live_events() -> List[Dict[str, Any]]:
    url = f"{RAPIDAPI_BASE}{RAPIDAPI_EVENTS_PATH}"
    params = {}
    if RAPIDAPI_EVENTS_PARAMS:
        for p in RAPIDAPI_EVENTS_PARAMS.split("&"):
            if "=" in p:
                k, v = p.split("=", 1)
                params[k.strip()] = v.strip()
    r = requests.get(url, headers=HEADERS_RAPIDAPI, params=params, timeout=25)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        arr = data.get("data") or data.get("events") or data.get("results") or []
        return [x for x in arr if isinstance(x, dict)]
    return []

def safe_get(d: Dict[str, Any], *path, default=None):
    cur = d
    for k in path:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return default
    return cur

def extract(ev: Dict[str, Any]):
    league = (safe_get(ev, "league", "name")
              or ev.get("league_name")
              or safe_get(ev, "tournament", "name")
              or ev.get("competition") or "")
    home = (safe_get(ev, "home", "name")
            or safe_get(ev, "teams", "home", "name")
            or ev.get("homeTeam") or ev.get("home_name") or ev.get("home") or "")
    away = (safe_get(ev, "away", "name")
            or safe_get(ev, "teams", "away", "name")
            or ev.get("awayTeam") or ev.get("away_name") or ev.get("away") or "")
    hs = (safe_get(ev, "state", "homeScore")
          or safe_get(ev, "score", "home")
          or ev.get("homeScore") or 0)
    as_ = (safe_get(ev, "state", "awayScore")
           or safe_get(ev, "score", "away")
           or ev.get("awayScore") or 0)
    try: hs = int(hs)
    except: hs = 0
    try: as_ = int(as_)
    except: as_ = 0

    kickoff = (ev.get("kickoff") or ev.get("startTime")
               or safe_get(ev, "time", "kickoff") or None)
    ko_epoch = None
    if isinstance(kickoff, (int, float)):
        ko_epoch = int(kickoff) if kickoff < 10**12 else int(kickoff // 1000)
    elif isinstance(kickoff, str) and kickoff.isdigit():
        val = int(kickoff); ko_epoch = val if val < 10**12 else val // 1000
    return league, str(home), str(away), hs, as_, ko_epoch

def get_minute(ev: Dict[str, Any], ko_epoch: Optional[int]) -> Optional[int]:
    cand = (safe_get(ev, "time", "minute")
            or safe_get(ev, "clock", "minute")
            or ev.get("minute"))
    if cand is not None:
        try: return int(cand)
        except: pass
    if ko_epoch:
        now = int(datetime.now(timezone.utc).timestamp())
        if now > ko_epoch:
            return (now - ko_epoch) // 60
    return None

def lookup_avg(df: pd.DataFrame, league: str, home: str, away: str) -> Optional[float]:
    if df is None or df.empty:
        return None
    n_home, n_away = normalize(home), normalize(away)
    def rows_match(r):
        h = normalize(str(r.get("home",""))); a = normalize(str(r.get("away","")))
        return (h==n_home and a==n_away) or (h==n_away and a==n_home)
    cand = df[df.apply(rows_match, axis=1)]
    if cand.empty:
        return None
    val = cand.iloc[0].get("avg", None)
    try:
        return float(val) if pd.notna(val) else None
    except:
        return None

def to_msg(league, home, away, minute, avg):
    return (
        "🚨 <b>SEGNALE OVER 1.5!</b>\n"
        f"⚽ <b>{home}</b> vs <b>{away}</b>\n"
        f"🏆 {league}\n"
        f"📊 AVG Goals: <b>{avg:.2f}</b>\n"
        f"🕒 {minute}' - Risultato: 0-0\n"
        "✅ Controlla Bet365 Live!\n\n"
        "🎯 <b>Punta Over 1.5 FT</b>"
    )

def main():
    if SEND_STARTUP_MESSAGE:
        send_telegram("🤖 FootyStats Bot avviato\nMonitoraggio partite in corso…")
    log(f"Soglia AVG: {AVG_GOALS_THRESHOLD:.2f} | Minuti check: {CHECK_TIME_MINUTES}")

    already = set()  # dedup semplice

    while True:
        try:
            log("================================================")
            log("INIZIO CONTROLLO")
            log("================================================")

            log(f"Scarico CSV: {GITHUB_CSV_URL}")
            df = read_csv(GITHUB_CSV_URL)
            log(f"CSV caricato ({len(df)} righe)")

            events = fetch_live_events()
            log(f"API live-events: {len(events)} match live")

            for ev in events:
                league, home, away, hs, as_, ko_epoch = extract(ev)
                if not league or not home or not away:
                    continue
                if not league_allowed(league):
                    continue
                if hs != 0 or as_ != 0:
                    continue

                minute = get_minute(ev, ko_epoch)
                if minute is None or minute < CHECK_TIME_MINUTES:
                    continue

                avg = lookup_avg(df, league, home, away)
                if avg is None or avg < AVG_GOALS_THRESHOLD:
                    continue

                key = f"{normalize(league)}|{normalize(home)}|{normalize(away)}"
                if key in already:
                    continue
                already.add(key)

                send_telegram(to_msg(league, home, away, minute, avg))
                log(f"Segnale inviato: {home} vs {away} | {league} | {minute}' | AVG {avg:.2f}")

        except Exception as e:
            log(f"Errore generico: {e}")

        log(f"Sleep {CHECK_INTERVAL_SECONDS}s…")
        time.sleep(CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()
