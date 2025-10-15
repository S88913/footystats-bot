import os
import time
import requests
import csv
from datetime import datetime
from io import StringIO
import logging

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ====== ENV ======
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")  # nessun default qui
CHAT_ID        = os.getenv("CHAT_ID")
RAPIDAPI_KEY   = os.getenv("RAPIDAPI_KEY")
GITHUB_CSV_URL = os.getenv(
    "GITHUB_CSV_URL",
    # Sostituisci <USERNAME> con il tuo utente GitHub!
    "https://raw.githubusercontent.com/<USERNAME>/footystats-bot/main/matches_today.csv"
)

# Parametri
AVG_GOALS_THRESHOLD = float(os.getenv("AVG_GOALS_THRESHOLD", "2.5"))
CHECK_TIME_MINUTES  = int(os.getenv("CHECK_TIME_MINUTES", "50"))

# Stato
notified_matches = set()

def send_telegram_message(message: str) -> bool:
    """Invia un messaggio su Telegram."""
    if not TELEGRAM_TOKEN or not CHAT_ID:
        logger.error("Variabili TELEGRAM_TOKEN/CHAT_ID mancanti.")
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        data = {"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"}
        r = requests.post(url, data=data, timeout=15)
        if r.ok:
            logger.info("Telegram: messaggio inviato")
            return True
        logger.error("Telegram error: %s", r.text)
    except Exception as e:
        logger.exception("Telegram exception: %s", e)
    return False

def load_csv_from_github():
    """Scarica e legge il CSV da GitHub."""
    try:
        logger.info("Scarico CSV: %s", GITHUB_CSV_URL)
        r = requests.get(GITHUB_CSV_URL, timeout=30)
        r.raise_for_status()
        reader = csv.DictReader(StringIO(r.text))
        rows = list(reader)
        logger.info("CSV caricato (%d righe)", len(rows))
        return rows
    except Exception as e:
        logger.exception("Errore caricamento CSV: %s", e)
        return []

def filter_matches_by_avg(matches):
    """Keep Average Goals >= soglia."""
    out = []
    for m in matches:
        try:
            if float(m.get("Average Goals", 0)) >= AVG_GOALS_THRESHOLD:
                out.append(m)
        except Exception:
            pass
    logger.info("Filtrati per AVG >= %.2f: %d", AVG_GOALS_THRESHOLD, len(out))
    return out

def get_live_matches():
    """Legge eventi live da RapidAPI (Bet365)."""
    if not RAPIDAPI_KEY:
        logger.error("Variabile RAPIDAPI_KEY mancante.")
        return []
    try:
        url = "https://bet36528.p.rapidapi.com/events"
        headers = {
            "x-rapidapi-host": "bet36528.p.rapidapi.com",
            "x-rapidapi-key": RAPIDAPI_KEY
        }
        r = requests.get(url, headers=headers, timeout=20)
        r.raise_for_status()
        data = r.json() or {}
        events = (data.get("data") or {}).get("events") or []
        logger.info("API live: %d eventi", len(events))
        return events
    except Exception as e:
        logger.exception("Errore API live: %s", e)
        return []

def parse_timestamp(tu_string):
    """Converte TU (YYYYMMDDHHMMSS) -> datetime UTC."""
    try:
        return datetime.strptime(tu_string, "%Y%m%d%H%M%S")
    except Exception:
        return None

def get_elapsed_minutes(start_time: datetime) -> int:
    try:
        return int((datetime.utcnow() - start_time).total_seconds() // 60)
    except Exception:
        return 0

def match_teams(csv_match, live_match) -> bool:
    csv_home = (csv_match.get("Home Team") or "").lower().strip()
    csv_away = (csv_match.get("Away Team") or "").lower().strip()
    live_home = (live_match.get("home") or "").lower().strip()
    live_away = (live_match.get("away") or "").lower().strip()
    if not csv_home or not csv_away or not live_home or not live_away:
        return False
    if csv_home == live_home and csv_away == live_away:
        return True
    if csv_home in live_home and csv_away in live_away:
        return True
    if live_home in csv_home and live_away in csv_away:
        return True
    return False

def check_matches():
    logger.info("=" * 60)
    logger.info("INIZIO CONTROLLO")
    logger.info("=" * 60)

    csv_matches = load_csv_from_github()
    if not csv_matches:
        logger.warning("CSV vuoto")
        return

    filtered = filter_matches_by_avg(csv_matches)
    if not filtered:
        logger.info("Nessun match con AVG >= soglia")
        return

    live = get_live_matches()
    if not live:
        logger.info("Nessun live attualmente")
        return

    opportunities = 0
    for cm in filtered:
        for lm in live:
            if not match_teams(cm, lm):
                continue

            match_id = lm.get("id") or f"{lm.get('home')}-{lm.get('away')}"
            score = lm.get("SS") or ""
            tu_time = lm.get("TU") or ""

            if match_id in notified_matches:
                continue

            start_time = parse_timestamp(tu_time)
            if not start_time:
                # Se manca TU, non possiamo calcolare i minuti → skippa
                logger.debug("TU mancante per %s", match_id)
                continue

            elapsed = get_elapsed_minutes(start_time)
            logger.info("Match %s vs %s | %s | %d'",
                        lm.get('home'), lm.get('away'), score, elapsed)

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
                    notified_matches.add(match_id)
                    logger.info("Segnalato: %s", match_id)

    logger.info("Opportunità trovate: %d", opportunities)
    logger.info("=" * 60)

def main():
    logger.info("Bot avviato")
    logger.info("Soglia AVG: %.2f | Minuti check: %d", AVG_GOALS_THRESHOLD, CHECK_TIME_MINUTES)
    send_telegram_message("🤖 <b>FootyStats Bot avviato</b>\nMonitoraggio partite in corso…")
    interval = int(os.getenv("CHECK_INTERVAL_SECONDS", "300"))
    while True:
        try:
            check_matches()
            logger.info("Sleep %ds…", interval)
            time.sleep(interval)
        except KeyboardInterrupt:
            send_telegram_message("⛔ Bot arrestato")
            break
        except Exception as e:
            logger.exception("Errore loop principale: %s", e)
            time.sleep(60)

if __name__ == "__main__":
    main()
