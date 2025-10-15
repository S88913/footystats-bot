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

# Endpoint (dal tuo screenshot)
RAPIDAPI_HOST        = os.getenv("RAPIDAPI_HOST", "bet365data.p.rapidapi.com")
RAPIDAPI_BASE        = os.getenv("RAPIDAPI_BASE", f"https://{RAPIDAPI_HOST}")
RAPIDAPI_EVENTS_PATH = os.getenv("RAPIDAPI_EVENTS_PATH", "/live-events")
RAPIDAPI_EVENTS_PARAMS = dict(parse_qsl(os.getenv("RAPIDAPI_EVENTS_PARAMS", "")))  # es: "sport=soccer"

# Bot config
GITHUB_CSV_URL      = os.getenv("GITHUB_CSV_URL", "https://raw.githubusercontent.com/<USERNAME>/footystats-bot/main/matches_today.csv")
AVG_GOALS_THRESHOLD = float(os.getenv("AVG_GOALS_THRESHOLD", "2.5"))
CHECK_TIME_MINUTES  = int(os.getenv("CHECK_TIME_MINUTES", "50"))
CHECK_INTERVAL      = int(os.getenv("CHECK_INTERVAL_SECONDS", "300"))
LEAGUE_EXCLUDE_KEYWORDS = [kw.strip().lower() for kw in os.getenv("LEAGUE_EXCLUDE_KEYWORDS", "Esoccer").split(",") if kw.strip()]

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

def filter_matches_by_avg(matches):
    out = []
    for m in matches:
        try:
            if float(m.get("Average Goals", 0)) >= AVG_GOALS_THRESHOLD:
                out.append(m)
        except Exception:
            pass
    logger.info("Filtrati per AVG >= %.2f: %d", AVG_GOALS_THRESHOLD, len(out))
    return out

def http_get(url, headers=None, params=None, timeout=25):
    try:
        r = requests.get(url, headers=headers, params=params, timeout=timeout)
        if not r.ok:
            logger.error("HTTP %s %s | body: %s", r.status_code, url, r.text[:300])
        return r
    except Exception as e:
        logger.error("HTTP exception on %s: %s", url, e)
        return None

def get_live_matches():
    """/live-events?sport=soccer (bet365data) → normalizza in id, home, away, SS, TU, league"""
    if not RAPIDAPI_KEY:
        logger.error("RAPIDAPI_KEY mancante.")
        return []

    url = f"{RAPIDAPI_BASE.rstrip('/')}/{RAPIDAPI_EVENTS_PATH.lstrip('/')}"
    headers = {"X-RapidAPI-Key": RAPIDAPI_KEY, "X-RapidAPI-Host": RAPIDAPI_HOST}
    r = http_get(url, headers=headers, params=RAPIDAPI_EVENTS_PARAMS, timeout=25)
    if not r or not r.ok:
        return []

    try:
        data = r.json() or {}
    except Exception:
        logger.error("Response non-JSON: %s", r.text[:300])
        return []

    root = data.get("data") or {}
    raw_events = root.get("events") or []

    events = []
    for it in raw_events:
        league = (it.get("league") or it.get("CT") or "N/A").strip()
        if any(ex in league.lower() for ex in LEAGUE_EXCLUDE_KEYWORDS):
            continue
        events.append({
            "id": str(it.get("id") or it.get("IID") or it.get("fi") or ""),
            "home": (it.get("home") or "").strip(),
            "away": (it.get("away") or "").strip(),
            "league": league,
            "SS": (it.get("SS") or "").strip(),
            "TU": (it.get("TU") or "").strip(),  # "YYYYMMDDHHMMSS"
        })

    logger.info("API live-events: %d match live", len(events))
    return events

def parse_timestamp(tu_string):
    try:
        return datetime.strptime(tu_string, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except Exception:
        return None

def get_elapsed_minutes(start_time) -> int:
    try:
        return int((datetime.now(timezone.utc) - start_time).total_seconds() // 60)
    except Exception:
        return 0

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
    csv_home = _norm(csv_match.get("Home Team"))
    csv_away = _norm(csv_match.get("Away Team"))
    live_home = _norm(live_match.get("home"))
    live_away = _norm(live_match.get("away"))
    if not (csv_home and csv_away and live_home and live_away):
        return False
    return _soft_team_match(csv_home, live_home) and _soft_team_match(csv_away, live_away)

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

            notified_key = f"{lm.get('id')}|{lm.get('home')}|{lm.get('away')}|{lm.get('TU')}"
            if notified_key in notified_matches:
                continue

            score = lm.get("SS") or ""
            tu_time = lm.get("TU") or ""
            start_time = parse_timestamp(tu_time)
            if not start_time:
                logger.debug("TU mancante/non valido per %s", lm.get("id")); continue

            elapsed = get_elapsed_minutes(start_time)
            logger.info("Match %s vs %s | %s | %d' | %s", lm.get('home'), lm.get('away'), score, elapsed, lm.get('league'))

            if elapsed >= CHECK_TIME_MINUTES and score == "0-0":
                opportunities += 1
                msg = (
                    "🚨 <b>SEGNALE OVER 1.5!</b>\n\n"
                    f"⚽ <b>{lm.get('home')} vs {lm.get('away')}</b>\n"
                    f"🏆 {lm.get('league', 'N/A')}\n"
                    f"📊 AVG Goals: <b>{cm.get('Average Goals','?')}</b>\n"
                    f"⏱️ <b>{elapsed}'</b> - Risultato: <b>{score}</b>\n"
                    "✅ Controlla Bet365 Live!\n\n"
                    "🎯 <b>Punta Over 1.5 FT</b>"
                )
                if send_telegram_message(msg):
                    notified_matches.add(notified_key)
                    logger.info("Segnalato: %s", notified_key)

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
