# src/bot.py
import os
import re
import time
import csv
import unicodedata
import requests
from io import StringIO
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone

# =========================
# ENV
# =========================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
CHAT_ID = os.getenv("CHAT_ID", "").strip()

GITHUB_CSV_URL = os.getenv("GITHUB_CSV_URL", "").strip()

AVG_GOALS_THRESHOLD    = float(os.getenv("AVG_GOALS_THRESHOLD", "2.5"))
CHECK_TIME_MINUTES     = int(os.getenv("CHECK_TIME_MINUTES", "50"))   # tuo requisito: inviare a ~50'
CHECK_INTERVAL_SECONDS = int(os.getenv("CHECK_INTERVAL_SECONDS", "300"))

# RapidAPI (Bet365Data)
RAPIDAPI_BASE          = os.getenv("RAPIDAPI_BASE", "https://bet365data.p.rapidapi.com").rstrip("/")
RAPIDAPI_EVENTS_PATH   = os.getenv("RAPIDAPI_EVENTS_PATH", "/live-events")
RAPIDAPI_EVENTS_PARAMS = os.getenv("RAPIDAPI_EVENTS_PARAMS", "sport=soccer")
RAPIDAPI_HOST          = os.getenv("RAPIDAPI_HOST", "bet365data.p.rapidapi.com")
RAPIDAPI_KEY           = os.getenv("RAPIDAPI_KEY", "")

# Filtri leghe (facoltativi)
EXCLUDE_KEYWORDS = [k.strip() for k in os.getenv(
    "LEAGUE_EXCLUDE_KEYWORDS",
    "Esoccer,Volta,8 mins play,H2H GG,Futsal,Beach,Penalty,Esports"
).split(",") if k.strip()]

SEND_STARTUP_MESSAGE = os.getenv("SEND_STARTUP_MESSAGE", "1").strip() == "1"
DEBUG_LOG            = os.getenv("DEBUG_LOG", "0").strip() == "1"

# =========================
# Log helpers
# =========================
def log(msg: str) -> None:
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S} - INFO - {msg}", flush=True)

def dlog(msg: str) -> None:
    if DEBUG_LOG:
        print(f"{datetime.now():%Y-%m-%d %H:%M:%S} - DEBUG - {msg}", flush=True)

# =========================
# Utils
# =========================
def normalize(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"\([^)]*\)", " ", s)         # rimuovi (U19), (W), (Res), ecc.
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def send_telegram(text: str) -> None:
    if not TELEGRAM_TOKEN or not CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, json={
            "chat_id": CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }, timeout=20)
    except Exception as e:
        log(f"Telegram error: {e}")

def league_allowed(league: str) -> bool:
    if not league:
        return True
    l = league.lower()
    return not any(k.lower() in l for k in EXCLUDE_KEYWORDS)

# =========================
# CSV loader (senza pandas)
# Supporta sia CSV con header che "grezzi"
# =========================
def fetch_csv_rows(url: str) -> List[Dict[str, Any]]:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    text = r.text

    # 1) prova come CSV con header
    try:
        reader = csv.DictReader(StringIO(text))
        rows = list(reader)
        # Se ha header sensati, tienilo
        if reader.fieldnames and len(reader.fieldnames) >= 4:
            dlog(f"CSV header: {reader.fieldnames}")
            return rows
    except Exception:
        pass

    # 2) fallback: senza header -> parse manuale con indici
    rows_out = []
    simple_reader = csv.reader(StringIO(text))
    for fields in simple_reader:
        if not fields or len(fields) < 6:
            continue
        # euristiche sugli indici frequenti visti nei tuoi CSV:
        # 0=epoch, 3=league, 4=home, 5=away, avg: cerchiamo un valore numerico plausibile in range 0.3..7
        try:
            epoch = int(fields[0])
        except Exception:
            epoch = None

        league = str(fields[3]).strip() if len(fields) > 3 else ""
        home   = str(fields[4]).strip() if len(fields) > 4 else ""
        away   = str(fields[5]).strip() if len(fields) > 5 else ""

        avg = None
        # prova a scorrere le colonne alla ricerca di una media plausibile
        for v in fields:
            try:
                x = float(v)
                if 0.3 <= x <= 7.0:
                    avg = x
                    break
            except Exception:
                continue

        rows_out.append({
            "epoch": epoch,
            "league": league,
            "home": home,
            "away": away,
            "avg": avg
        })
    return rows_out

def extract_match_fields(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # supporta sia DictReader (header) che parse manuale
    epoch = None
    league = ""
    home = ""
    away = ""
    avg = None

    # epoch
    for k in ("epoch", "timestamp", "kickoff_ts", "ts"):
        if k in row and row[k] not in ("", None):
            try:
                epoch = int(row[k])
                break
            except Exception:
                pass
    if epoch is None:
        # a volte la prima colonna è '1761065100' ma senza chiave; prova _rowid/0
        for k in row.keys():
            # tenta su tutte le chiavi se il valore è int-like lungo
            try:
                v = int(row[k])
                if v > 1_600_000_000:
                    epoch = v
                    break
            except Exception:
                continue

    # league
    for k in ("league", "competition", "Country & League"):
        if k in row and row[k]:
            league = str(row[k]).strip()
            break

    # home/away
    for k in ("home", "home_team", "Home Team", "Home"):
        if k in row and row[k]:
            home = str(row[k]).strip()
            break
    for k in ("away", "away_team", "Away Team", "Away"):
        if k in row and row[k]:
            away = str(row[k]).strip()
            break

    # avg
    for k in ("avg", "Avg Total Goals", "Average Total Goals", "Average Goals"):
        if k in row and row[k] not in ("", None):
            try:
                avg = float(row[k])
                break
            except Exception:
                pass
    if avg is None:
        # euristica su tutti i valori
        for v in row.values():
            try:
                x = float(v)
                if 0.3 <= x <= 7.0:
                    avg = x
                    break
            except Exception:
                continue

    if not home or not away or avg is None:
        return None

    return {"epoch": epoch, "league": league, "home": home, "away": away, "avg": avg}

# =========================
# RapidAPI live events
# =========================
HEADERS_RAPIDAPI = {
    "x-rapidapi-host": RAPIDAPI_HOST,
    "x-rapidapi-key": RAPIDAPI_KEY,
}

def parse_params(qs: str) -> Dict[str, str]:
    params: Dict[str, str] = {}
    if not qs:
        return params
    for p in qs.split("&"):
        if "=" in p:
            k, v = p.split("=", 1)
            params[k.strip()] = v.strip()
    return params

def fetch_live_events() -> List[Dict[str, Any]]:
    url = f"{RAPIDAPI_BASE}{RAPIDAPI_EVENTS_PATH}"
    params = parse_params(RAPIDAPI_EVENTS_PARAMS)
    r = requests.get(url, headers=HEADERS_RAPIDAPI, params=params, timeout=25)
    r.raise_for_status()
    data = r.json()

    # l'API può restituire direttamente una lista o {"data":[...]}
    if isinstance(data, dict) and "data" in data:
        data = data["data"]
    if not isinstance(data, list):
        return []

    events: List[Dict[str, Any]] = []
    for ev in data:
        try:
            league = (
                ev.get("league")
                or ev.get("league_name")
                or (ev.get("tournament") or {}).get("name")
                or ev.get("competition")
                or ""
            )
            home = (ev.get("home") or ev.get("homeTeam") or ev.get("home_name") or "")
            away = (ev.get("away") or ev.get("awayTeam") or ev.get("away_name") or "")

            # punteggio
            home_goals = 0
            away_goals = 0
            score = ev.get("score")
            if isinstance(score, dict):
                home_goals = int(score.get("home", 0) or 0)
                away_goals = int(score.get("away", 0) or 0)
            elif isinstance(score, str) and "-" in score:
                try:
                    s1, s2 = score.split("-", 1)
                    home_goals = int(s1.strip()); away_goals = int(s2.strip())
                except Exception:
                    pass
            else:
                home_goals = int(ev.get("homeScore", ev.get("home_score", 0)) or 0)
                away_goals = int(ev.get("awayScore", ev.get("away_score", 0)) or 0)

            # minuto (se presente) e sanificazione 0..130
            minute = None
            for k in ("minute",):
                if k in ev and ev[k] not in (None, ""):
                    try:
                        minute = int(ev[k])
                    except Exception:
                        minute = None
            if minute is not None and not (0 <= minute <= 130):
                minute = None

            events.append({
                "league": str(league),
                "home": str(home),
                "away": str(away),
                "home_goals": int(home_goals),
                "away_goals": int(away_goals),
                "minute": minute
            })
        except Exception:
            continue

    return events

# =========================
# Live detection & minute calc (anti-180')
# =========================
def event_is_inplay(ev: Dict[str, Any]) -> bool:
    """
    Prova a capire se l'evento è davvero live.
    In questa versione "base" sfruttiamo solo il fatto che l'API 'live-events'
    dovrebbe già essere live; lasciamo qui una funzione estendibile.
    """
    # Se servisse, qui potresti controllare campi tipo status/inPlay ecc.
    return True

def compute_minute_from_kickoff(epoch: Optional[int]) -> Optional[int]:
    """
    Calcola minuto dal kickoff epoch. Ritorna None se:
      - kickoff nel futuro
      - valore fuori 0..130
    """
    if not epoch:
        return None
    try:
        kick = datetime.fromtimestamp(int(epoch), tz=timezone.utc)
    except Exception:
        return None
    now = datetime.now(timezone.utc)
    if now < kick:
        return None
    m = int((now - kick).total_seconds() // 60)
    if 0 <= m <= 130:
        return m
    return None

# =========================
# Matching CSV <-> Live
# =========================
def build_live_index(events: List[Dict[str, Any]]) -> Dict[Tuple[str, str], Dict[str, Any]]:
    idx: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for ev in events:
        key = (normalize(ev["home"]), normalize(ev["away"]))
        idx[key] = ev
    return idx

# =========================
# Notifiche
# =========================
notified: set[str] = set()

def notify(league: str, home: str, away: str, minute: int, avg: float):
    key = f"{normalize(league)}|{normalize(home)}|{normalize(away)}|{minute}"
    if key in notified:
        return
    notified.add(key)
    message = (
        "🚨 <b>SEGNALE OVER 1.5!</b>\n\n"
        f"⚽ <b>{home}</b> vs <b>{away}</b>\n"
        f"🏆 {league}\n"
        f"📊 AVG Goals: <b>{avg:.2f}</b>\n"
        f"⏱️ <b>{minute}'</b> - Risultato: <b>0-0</b>\n"
        "✅ Controlla Bet365 Live!\n\n"
        "🎯 <b>Punta Over 1.5 FT</b>"
    )
    send_telegram(message)
    log(f"Segnale inviato: {home} vs {away} | {league} | {minute}' | AVG {avg:.2f}")

# =========================
# MAIN
# =========================
def main():
    if SEND_STARTUP_MESSAGE:
        send_telegram("🤖 FootyStats Bot avviato\nMonitoraggio partite in corso…")
    log(f"Soglia AVG: {AVG_GOALS_THRESHOLD:.2f} | Minuti check: {CHECK_TIME_MINUTES}")

    while True:
        try:
            log("================================================")
            log("INIZIO CONTROLLO")
            log("================================================")

            # 1) CSV
            log(f"Scarico CSV: {GITHUB_CSV_URL}")
            rows_raw = fetch_csv_rows(GITHUB_CSV_URL)
            log(f"CSV caricato ({len(rows_raw)} righe)")

            # 2) live events
            events = fetch_live_events()
            log(f"API live-events: {len(events)} match live")
            live_idx = build_live_index(events)

            # 3) scan
            found = 0
            for r in rows_raw:
                row = extract_match_fields(r)
                if not row:
                    continue

                # filtra lega
                if not league_allowed(row["league"]):
                    continue

                # filtro AVG
                if row["avg"] is None or row["avg"] < AVG_GOALS_THRESHOLD:
                    continue

                # trova evento live per home/away
                ev = live_idx.get((normalize(row["home"]), normalize(row["away"])))
                if not ev:
                    continue

                # punteggio deve essere 0-0
                if ev["home_goals"] != 0 or ev["away_goals"] != 0:
                    continue

                # deve essere davvero live (funzione lasciata aperta per futuri stati)
                if not event_is_inplay(ev):
                    continue

                # minuto: preferisci quello dell'API se 0..130, altrimenti fallback dal kickoff epoch CSV
                minute = ev.get("minute", None)
                if minute is None or not (0 <= minute <= 130):
                    minute = compute_minute_from_kickoff(row.get("epoch"))

                # niente segnali pre-kickoff o minuti strani
                if minute is None:
                    dlog(f"Minuto non disponibile per {row['home']} - {row['away']}")
                    continue

                # condizione chiave
                if minute >= CHECK_TIME_MINUTES:
                    notify(row["league"] or ev.get("league", ""),
                           row["home"], row["away"], minute, float(row["avg"]))
                    found += 1

            log(f"Opportunità trovate: {found}")

        except Exception as e:
            log(f"Errore generico: {e}")

        log(f"Sleep {CHECK_INTERVAL_SECONDS}s…")
        time.sleep(CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()
