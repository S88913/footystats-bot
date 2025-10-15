import os
import time
import requests
import csv
from datetime import datetime, timezone
from io import StringIO
import logging
from urllib.parse import parse_qsl

# ===== Logging =====
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ===== ENV =====
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID        = os.getenv("CHAT_ID")
RAPIDAPI_KEY   = os.getenv("RAPIDAPI_KEY")

RAPIDAPI_HOST  = os.getenv("RAPIDAPI_HOST", "bet365data.p.rapidapi.com")
RAPIDAPI_BASE  = os.getenv("RAPIDAPI_BASE", f"https://{RAPIDAPI_HOST}")
RAPIDAPI_EVENTS_PATH   = os.getenv("RAPIDAPI_EVENTS_PATH", "/live-events")
RAPIDAPI_EVENTS_PARAMS = dict(parse_qsl(os.getenv("RAPIDAPI_EVENTS_PARAMS", "")))

GITHUB_CSV_URL      = os.getenv("GITHUB_CSV_URL", "https://raw.githubusercontent.com/<USERNAME>/footystats-bot/main/matches_today.csv")
AVG_GOALS_THRESHOLD = float(os.getenv("AVG_GOALS_THRESHOLD", "2.5"))
CHECK_TIME_MINUTES  = int(os.getenv("CHECK_TIME_MINUTES", "50"))
CHECK_INTERVAL      = int(os.getenv("CHECK_INTERVAL_SECONDS", "300"))
LEAGUE_EXCLUDE_KEYWORDS = [kw.strip().lower() for kw in os.getenv("LEAGUE_EXCLUDE_KEYWORDS", "Esoccer,Volta,8 mins play,H2H GG").split(",") if kw.strip()]

notified_matches = set()

# ===== Utils =====
def send_telegram_message(message: str) -> bool:
    if not TELEGRAM_TOKEN or not CHAT_ID:
        logger.error("TELEGRAM_TOKEN/CHAT_ID non impostati.")
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        r = requests.post(url, data={"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"}, timeout=15)
        if r.ok:
            logger.info("Telegram: messaggio inviato")
            return True
        logger.error("Telegram %s: %s", r.status_code, r.text)
    except Exception as e:
        logger.exception("Telegram exception: %s", e)
    return False

def http_get(url, headers=None, params=None, timeout=25):
    try:
        r = requests.get(url, headers=headers, params=params, timeout=timeout)
        if not r.ok:
            logger.error("HTTP %s %s | body: %s", r.status_code, url, r.text[:300])
        return r
    except Exception as e:
        logger.error("HTTP exception on %s: %s", url, e)
        return None

def load_csv_from_github():
    try:
        logger.info("Scarico CSV: %s", GITHUB_CSV_URL)
        r = requests.get(GITHUB_CSV_URL, timeout=30)
        r.raise_for_status()
        rows = list(csv.DictReader(StringIO(r.text)))
        logger.info("CSV caricato (%d righe)", len(rows))
        return rows
    except Exception as e:
        logger.exception("Errore caricamento CSV: %s", e)
        return []

def get_avg_goals(row) -> float:
    # prova nomi tipici; fallback a 0.0
    for k in ["Average Goals", "AVG Goals", "AvgGoals", "Avg Goals"]:
        if k in row and row[k]:
            try:
                return float(row[k])
            except Exception:
                pass
    return 0.0

def filter_matches_by_avg(matches):
    out = []
    for m in matches:
        try:
            if get_avg_goals(m) >= AVG_GOALS_THRESHOLD:
                out.append(m)
        except Exception:
            pass
    logger.info("Filtrati per AVG >= %.2f: %d", AVG_GOALS_THRESHOLD, len(out))
    return out

# ===== Live events (solo soccer) =====
def get_live_matches():
    url = f"{RAPIDAPI_BASE.rstrip('/')}/{RAPIDAPI_EVENTS_PATH.lstrip('/')}"
    headers = {"X-RapidAPI-Key": RAPIDAPI_KEY, "X-RapidAPI-Host": RAPIDAPI_HOST}
    r = http_get(url, headers=headers, params=RAPIDAPI_EVENTS_PARAMS, timeout=25)
    if not r or not r.ok:
        return []
    try:
        data = r.json() or {}
    except Exception:
        logger.error("Response non-JSON: %s", r.text[:300]); return []
    root = data.get("data") or {}
    raw = root.get("events") or []
    events = []
    for it in raw:
        league = (it.get("league") or it.get("CT") or "N/A").strip()
        if any(ex in league.lower() for ex in LEAGUE_EXCLUDE_KEYWORDS):
            continue
        events.append({
            "home": (it.get("home") or "").strip(),
            "away": (it.get("away") or "").strip(),
            "league": league,
            "SS": (it.get("SS") or "").strip(),  # es. "0-0"
        })
    logger.info("API live-events: %d match live", len(events))
    return events

# ===== Matching CSV vs Live =====
def _norm(s): return (s or "").lower().strip()

def _soft_team_match(a: str, b: str) -> bool:
    def clean(x: str) -> str:
        x = x.lower()
        for junk in [" (w)", "(w)", " (u19)", "(u19)", " u19", " u20", " fc", " cf", ".", ","]:
            x = x.replace(junk, "")
        return " ".join(x.split())
    A, B = clean(a), clean(b)
    return A == B or A in B or B in A

def match_teams(csv_match, live_match) -> bool:
    csv_home = _norm(csv_match.get("Home Team") or csv_match.get("Home") or csv_match.get("home"))
    csv_away = _norm(csv_match.get("Away Team") or csv_match.get("Away") or csv_match.get("away"))
    live_home = _norm(live_match.get("home"))
    live_away = _norm(live_match.get("away"))
    if not (csv_home and csv_away and live_home and live_away):
        return False
    return _soft_team_match(csv_home, live_home) and _soft_team_match(csv_away, live_away)

# ===== Calcolo minuto dal kickoff CSV =====
def kickoff_minute_from_csv(csv_match) -> int | None:
    """
    Il CSV ha il kickoff in UTC come epoch (prima colonna nel tuo file, p.es. 1760544000).
    Provo varie chiavi; se non trovo, provo la prima colonna.
    """
    candidate_keys = [
        "timestamp","epoch","unix","Date Unix","Kickoff Unix","start_time","start","time_unix"
    ]
    epoch_val = None

    for k in candidate_keys:
        if k in csv_match and csv_match[k]:
            try:
                n = int(float(str(csv_match[k]).strip()))
                if n >= 1_000_000_000:  # plausibile epoch
                    epoch_val = n
                    break
            except Exception:
                pass

    if epoch_val is None:
        # fallback: prima colonna se è numerica (nel tuo CSV lo è)
        try:
            first_key = next(iter(csv_match.keys()))
            n = int(float(csv_match[first_key]))
            if n >= 1_000_000_000:
                epoch_val = n
        except Exception:
            pass

    if epoch_val is None:
        return None

    now_utc = datetime.now(timezone.utc).timestamp()
    minute = int(max(0, (now_utc - epoch_val) // 60))
    if minute > 150:  # clamp prudenziale
        minute = 150
    return minute

# ===== Workflow =====
def check_matches():
    logger.info("=" * 60); logger.info("INIZIO CONTROLLO"); logger.info("=" * 60)

    csv_matches = load_csv_from_github()
    if not csv_matches:
        logger.warning("CSV vuoto"); return

    filtered = filter_matches_by_avg(csv_matches)
    if not filtered:
        logger.info("Nessun match con AVG >= soglia"); return

    live = get_live_matches()
    if not live:
        logger.info("Nessun live attualmente"); return

    opportunities = 0
    for cm in filtered:
        for lm in live:
            if not match_teams(cm, lm):
                continue

            score = lm.get("SS") or ""
            if score != "0-0":
                continue

            minute = kickoff_minute_from_csv(cm)
            if minute is None:
                continue

            logger.info("Match %s vs %s | %s | %d' | %s",
                        lm.get('home'), lm.get('away'), score, minute, lm.get('league'))

            if minute >= CHECK_TIME_MINUTES:
                key = f"{lm.get('home')}|{lm.get('away')}"
                if key in notified_matches:
                    continue
                opportunities += 1
                avg = get_avg_goals(cm)
                msg = (
                    "🚨 <b>SEGNALE OVER 1.5!</b>\n\n"
                    f"⚽ <b>{lm.get('home')} vs {lm.get('away')}</b>\n"
                    f"🏆 {lm.get('league', 'N/A')}\n"
                    f"📊 AVG Goals: <b>{avg:.2f}</b>\n"
                    f"⏱️ <b>{minute}'</b> - Risultato: <b>{score}</b>\n"
                    "✅ Controlla Bet365 Live!\n\n"
                    "🎯 <b>Punta Over 1.5 FT</b>"
                )
                if send_telegram_message(msg):
                    notified_matches.add(key)
                    logger.info("Segnalato: %s", key)

    logger.info("Opportunità trovate: %d", opportunities)
    logger.info("=" * 60)

def main():
    logger.info("Bot avviato")
    logger.info("Soglia AVG: %.2f | Minuti check: %d", AVG_GOALS_THRESHOLD, CHECK_TIME_MINUTES)
    send_telegram_message("🤖 <b>FootyStats Bot avviato</b>\nMonitoraggio partite in corso…")
    while True:
        try:
            check_matches()
            logger.info("Sleep %ds…", CHECK_INTERVAL)
            time.sleep(CHECK_INTERVAL)
        except KeyboardInterrupt:
            send_telegram_message("⛔ Bot arrestato"); break
        except Exception as e:
            logger.exception("Errore loop principale: %s", e)
            time.sleep(60)

if __name__ == "__main__":
    main()
