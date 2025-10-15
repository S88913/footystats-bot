import os
import time
import csv
import re
import unicodedata
from io import StringIO
from datetime import datetime, timezone
import logging
import requests
from urllib.parse import parse_qsl

# ===== Logging =====
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
DEBUG_LOG = os.getenv("DEBUG_LOG", "0") == "1"

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

# ===== Helpers =====
def send_telegram_message(message: str) -> bool:
    if not TELEGRAM_TOKEN or not CHAT_ID:
        logger.error("TELEGRAM_TOKEN/CHAT_ID mancanti.")
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
    keys = [
        "Average Goals", "AVG Goals", "AvgGoals", "Avg Goals",
        "Avg Total Goals", "Average Total Goals", "Avg_Total_Goals"
    ]
    for k in keys:
        v = row.get(k)
        if v is None or str(v).strip() == "":
            continue
        try:
            return float(v)
        except Exception:
            continue
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
    if DEBUG_LOG:
        logger.info("Esempio AVG: %s", [get_avg_goals(x) for x in out[:5]])
    return out

# ===== Live events =====
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

# ===== Team matching robusto =====
STOPWORDS = {
    "fc","cf","sc","ac","club","cd","de","del","da","do","d","u19","u20","u21","b","ii","iii",
    "women","w","reserves","team","sv","afc","youth"
}

def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

def team_tokens(name: str) -> set[str]:
    if not name:
        return set()
    s = strip_accents(name).lower()
    s = re.sub(r"[’'`]", " ", s)
    s = re.sub(r"\(.*?\)", " ", s)                 # rimuovi parentesi
    s = re.sub(r"[^a-z0-9]+", " ", s)              # solo alfanumerico
    toks = [t for t in s.split() if t and t not in STOPWORDS]
    # rimuovi token troppo corti
    toks = [t for t in toks if len(t) >= 3 or t.isdigit()]
    return set(toks)

def _soft_team_match(csv_name: str, live_name: str) -> bool:
    A, B = team_tokens(csv_name), team_tokens(live_name)
    if not A or not B:
        return False
    if A == B or A.issubset(B) or B.issubset(A):
        return True
    # almeno due token forti in comune (o uno se una delle due squadre è "mononimo", es. "Mouna")
    inter = A & B
    if len(A) == 1 or len(B) == 1:
        return len(inter) >= 1
    return len(inter) >= 2

def match_teams(csv_match, live_match) -> bool:
    csv_home = csv_match.get("Home Team") or csv_match.get("Home") or csv_match.get("home") or ""
    csv_away = csv_match.get("Away Team") or csv_match.get("Away") or csv_match.get("away") or ""
    return _soft_team_match(csv_home, live_match.get("home","")) and \
           _soft_team_match(csv_away, live_match.get("away",""))

# ===== Minuto dal kickoff (CSV epoch UTC) =====
def kickoff_minute_from_csv(csv_match) -> int | None:
    candidate_keys = ["timestamp","epoch","unix","Date Unix","Kickoff Unix","start_time","start","time_unix"]
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
        # fallback: prima colonna se è numerica
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
    if minute > 150:
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
    matched = 0

    for cm in filtered:
        for lm in live:
            if not match_teams(cm, lm):
                continue
            matched += 1

            score = lm.get("SS") or ""
            if score != "0-0":
                continue

            minute = kickoff_minute_from_csv(cm)
            if minute is None:
                continue

            if DEBUG_LOG:
                logger.info("Abbinato: %s vs %s | %s | %d' | %s",
                            lm.get('home'), lm.get('away'), score, minute, lm.get('league'))

            if minute >= CHECK_TIME_MINUTES:
                key = f"{lm.get('home')}|{lm.get('away')}"
                if key in notified_matches:
                    continue
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

    logger.info("Riepilogo: Abbinati CSV↔Live=%d | Opportunità=%d", matched, opportunities)
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
