# src/bot.py
import csv
import io
import os
import re
import time
import unicodedata
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests


# -----------------------------
# Config da Environment
# -----------------------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
CHAT_ID = os.getenv("CHAT_ID", "").strip()

GITHUB_CSV_URL = os.getenv("GITHUB_CSV_URL", "").strip()

AVG_GOALS_THRESHOLD = float(os.getenv("LEAGUE_MIN_AVG", os.getenv("AVG_GOALS_THRESHOLD", "2.5")))
TEAM_FORM_MIN_AVG = float(os.getenv("TEAM_FORM_MIN_AVG", "0"))  # opzionale, se non presente nel CSV viene ignorato

MIN_MINUTE = int(os.getenv("MIN_MINUTE", "50"))
MAX_MINUTE = int(os.getenv("MAX_MINUTE", "56"))

LEAGUE_BLACKLIST = [k.strip().lower() for k in os.getenv("LEAGUE_BLACKLIST", "").split("|") if k.strip()]
LEAGUE_EXCLUDE_KEYWORDS = [
    k.strip().lower() for k in os.getenv(
        "LEAGUE_EXCLUDE_KEYWORDS",
        "Esoccer,Volta,8 mins play,H2H GG,Futsal,Beach,Penalty,Esports"
    ).split(",") if k.strip()
]
LEAGUE_WHITELIST = [k.strip().lower() for k in os.getenv("LEAGUE_WHITELIST", "").split("|") if k.strip()]

SEND_STARTUP_MSG = os.getenv("SEND_STARTUP_MSG", os.getenv("SEND_STARTUP_", "1")).strip() == "1"
DEBUG_LOG = os.getenv("DEBUG_LOG", "0").strip() == "1"

# RapidAPI (solo lista eventi live)
RAPIDAPI_BASE = os.getenv("RAPIDAPI_BASE", "https://bet365data.p.rapidapi.com").rstrip("/")
RAPIDAPI_HOST = os.getenv("RAPIDAPI_HOST", "bet365data.p.rapidapi.com")
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "").strip()
RAPIDAPI_EVENTS_PATH = os.getenv("RAPIDAPI_EVENTS_PATH", "/live-events")
RAPIDAPI_EVENTS_PARAMS = os.getenv("RAPIDAPI_EVENTS_PARAMS", "sport=soccer")

HEADERS_RAPIDAPI = {
    "x-rapidapi-host": RAPIDAPI_HOST,
    "x-rapidapi-key": RAPIDAPI_KEY,
    "accept": "application/json",
}

SLEEP_SECONDS = int(os.getenv("CHECK_INTERVAL_SECONDS", "300"))
CHECK_TIME_MINUTES = int(os.getenv("CHECK_TIME_MINUTES", "50"))  # per testo di riepilogo


# -----------------------------
# Utils
# -----------------------------
def log(level: str, msg: str):
    if level == "DEBUG" and not DEBUG_LOG:
        return
    print(f"{datetime.now(timezone.utc).astimezone().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} - {level} - {msg}")


def telegram_send(text: str):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        log("ERROR", "TELEGRAM_TOKEN/CHAT_ID mancanti. Messaggio non inviato.")
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        r = requests.post(url, json={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=20)
        r.raise_for_status()
        log("INFO", "Telegram: messaggio inviato")
    except Exception as e:
        log("ERROR", f"Telegram error: {e}")


def norm(s: str) -> str:
    """Normalizza nome squadra/lega per match più robusto."""
    s0 = s.lower()
    # rimuove tag come (w), (women), (f), u19/u21/u23, reserves ecc.
    s0 = re.sub(r"\((w|women|f)\)", "", s0)
    s0 = re.sub(r"\b(women|feminine|femmini?le|female|ladies)\b", "", s0)
    s0 = re.sub(r"\b(u19|u20|u21|u23|reserves?|ii|b)\b", "", s0)
    s0 = re.sub(r"[\.\-_/]", " ", s0)
    s0 = re.sub(r"\s+", " ", s0).strip()
    s0 = unicodedata.normalize("NFKD", s0)
    s0 = "".join(c for c in s0 if not unicodedata.combining(c))
    return s0


def get_field_case_insensitive(row: Dict, *keys: str) -> Optional[str]:
    lower_map = {k.lower(): v for k, v in row.items()}
    for k in keys:
        if k.lower() in lower_map:
            return lower_map[k.lower()]
    return None


def parse_float(val: Optional[str]) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(str(val).replace(",", "."))
    except Exception:
        return None


def get_avg_from_row(row: Dict) -> Optional[float]:
    # Aggiornato per FootyStats: colonna esatta "Average Goals"
    val = get_field_case_insensitive(
        row,
        "average goals", "avg_total_goals", "avg goals", "avg total goals", "goals_avg", "avg"
    )
    f = parse_float(val)
    return f


def get_team_form_avg(row: Dict) -> Optional[float]:
    """
    Se il CSV fornisce una stima di forma (ultime 5/10), prova a leggerla.
    Altrimenti restituisce None e il filtro non viene applicato.
    """
    candidates = [
        "team form avg", "form avg", "avg last 5", "avg last5", "avg last 10", "form_l5", "form_l10"
    ]
    val = None
    for c in candidates:
        val = get_field_case_insensitive(row, c)
        if val:
            break
    return parse_float(val)


def parse_kickoff_from_row(row: Dict) -> Optional[datetime]:
    """
    Prova a ricostruire l'orario di kickoff dal CSV:
    - prima colonna epoch (se presente)
    - oppure stringa tipo 'Oct 15 2025 - 4:00pm'
    """
    # 1) prima colonna se è un intero plausibile (epoch in secondi)
    try:
        first_key = list(row.keys())[0]
        first_val = row[first_key]
        if re.fullmatch(r"\d{9,11}", str(first_val).strip()):
            ts = int(str(first_val).strip())
            # Consideriamo UTC
            return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception:
        pass

    # 2) cerca una colonna con 'date'/'time'
    date_str = None
    for k in row.keys():
        if "date" in k.lower() or "time" in k.lower():
            date_str = row[k]
            if date_str:
                break
    if date_str:
        # Esempio: 'Oct 15 2025 - 4:00pm'
        s = str(date_str).replace(" - ", " ")
        for fmt in ["%b %d %Y %I:%M%p", "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M"]:
            try:
                dt_naive = datetime.strptime(s, fmt)
                # Assumiamo che sia orario locale della lega ≈ UTC (meglio di niente)
                return dt_naive.replace(tzinfo=timezone.utc)
            except Exception:
                continue
    return None


def league_allowed(league_name: str) -> bool:
    ln = league_name.lower()
    if any(k in ln for k in LEAGUE_EXCLUDE_KEYWORDS):
        return False
    if LEAGUE_BLACKLIST and any(k in ln for k in LEAGUE_BLACKLIST):
        return False
    if LEAGUE_WHITELIST:
        return any(k in ln for k in LEAGUE_WHITELIST)
    return True


# -----------------------------
# Dati live RapidAPI
# -----------------------------
def fetch_live_events() -> List[Dict]:
    url = f"{RAPIDAPI_BASE}{RAPIDAPI_EVENTS_PATH}"
    params = {}
    if RAPIDAPI_EVENTS_PARAMS:
        # es: sport=soccer&some=1
        for p in RAPIDAPI_EVENTS_PARAMS.split("&"):
            if "=" in p:
                k, v = p.split("=", 1)
                params[k] = v
    log("DEBUG", f"Chiamo live-events: {url} {params}")
    r = requests.get(url, headers=HEADERS_RAPIDAPI, params=params, timeout=20)
    r.raise_for_status()
    data = r.json() or {}
    events = data.get("data") or data.get("events") or []
    log("INFO", f"API live-events: {len(events)} match live")
    return events


def extract_event_fields(ev: Dict) -> Tuple[str, str, str, Optional[int], Optional[int], Optional[datetime]]:
    """
    Ritorna: (id, home, away, home_score, away_score, kickoff_dt)
    """
    eid = str(ev.get("id") or ev.get("event_id") or "")
    home = str(ev.get("home", {}).get("name") or ev.get("homeTeam") or ev.get("home_name") or ev.get("homeName") or "")
    away = str(ev.get("away", {}).get("name") or ev.get("awayTeam") or ev.get("away_name") or ev.get("awayName") or "")
    # punteggio
    hs = ev.get("scores", {}).get("home") if isinstance(ev.get("scores"), dict) else None
    as_ = ev.get("scores", {}).get("away") if isinstance(ev.get("scores"), dict) else None
    if hs is None:
        hs = ev.get("home_score") or ev.get("homeScore")
    if as_ is None:
        as_ = ev.get("away_score") or ev.get("awayScore")
    try:
        hs = int(hs) if hs is not None else None
        as_ = int(as_) if as_ is not None else None
    except Exception:
        hs, as_ = None, None

    # kickoff
    kts = ev.get("startTime") or ev.get("start_time") or ev.get("kickoff")
    kdt = None
    if kts:
        try:
            # spesso è epoch (s)
            if str(kts).isdigit():
                kdt = datetime.fromtimestamp(int(kts), tz=timezone.utc)
            else:
                # prova ISO
                kdt = datetime.fromisoformat(str(kts).replace("Z", "+00:00"))
        except Exception:
            kdt = None

    return eid, home, away, hs, as_, kdt


# -----------------------------
# CSV FootyStats
# -----------------------------
def load_csv_rows(url: str) -> List[Dict]:
    log("INFO", f"Scarico CSV: {url}")
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    content = r.content.decode("utf-8", errors="ignore")
    reader = csv.DictReader(io.StringIO(content))
    rows = list(reader)
    log("INFO", f"CSV caricato ({len(rows)} righe)")
    return rows


# -----------------------------
# Matching & regole segnale
# -----------------------------
def match_row_to_event(row: Dict, events: List[Dict]) -> Optional[Dict]:
    """
    Tenta un match robusto per home/away (normalizzati).
    Se disponibile, usa anche una finestra intorno al kickoff.
    """
    home = norm(get_field_case_insensitive(row, "home", "home team", "hometeam", "team1", "home_name") or "")
    away = norm(get_field_case_insensitive(row, "away", "away team", "awayteam", "team2", "away_name") or "")
    if not home or not away:
        return None

    csv_kickoff = parse_kickoff_from_row(row)

    best = None
    for ev in events:
        eid, eh, ea, _, _, ev_kick = extract_event_fields(ev)
        if not eh or not ea:
            continue
        eh_n = norm(eh)
        ea_n = norm(ea)

        names_ok = (home in eh_n or eh_n in home) and (away in ea_n or ea_n in away)
        if not names_ok:
            # prova swap (nel dubbio)
            names_ok = (home in ea_n or ea_n in home) and (away in eh_n or eh_n in away)

        if not names_ok:
            continue

        if csv_kickoff and ev_kick:
            # accetta se partenza entro 3 ore
            delta = abs((ev_kick - csv_kickoff).total_seconds())
            if delta > 3 * 3600:
                continue

        best = ev
        break

    return best


def minutes_since_kickoff(row: Dict, ev: Dict) -> Optional[int]:
    _, _, _, _, _, ev_kick = extract_event_fields(ev)
    csv_kick = parse_kickoff_from_row(row)

    base = ev_kick or csv_kick
    if not base:
        return None

    now = datetime.now(timezone.utc)
    mins = int((now - base).total_seconds() // 60)
    # Evita negativi (partita non iniziata)
    return max(mins, 0)


def league_name_from_row(row: Dict) -> str:
    league = get_field_case_insensitive(row, "league", "competition", "tournament", "country_league", "division") or ""
    country = get_field_case_insensitive(row, "country") or ""
    name = (country + " " + league).strip()
    return re.sub(r"\s+", " ", name)


def build_signal_text(row: Dict, ev: Dict, minute: int, avg_goals: float, score: Tuple[int, int]) -> str:
    home = get_field_case_insensitive(row, "home", "home team", "hometeam", "team1", "home_name") or ""
    away = get_field_case_insensitive(row, "away", "away team", "awayteam", "team2", "away_name") or ""
    league = league_name_from_row(row)
    hs, as_ = score
    lines = [
        "🚨 <b>SEGNALE OVER 1.5!</b>",
        f"⚽ <b>{home}</b> vs <b>{away}</b>",
        f"🏆 {league}" if league else "",
        f"📊 <b>AVG Goals:</b> {avg_goals:.2f}",
        f"⏱️ <b>{minute}'</b> - <b>Risultato:</b> {hs}-{as_}",
        "✅ Controlla Bet365 Live!",
        "",
        "🎯 <b>Punta Over 1.5 FT</b>",
    ]
    return "\n".join([l for l in lines if l])


# Per evitare duplicati durante il run corrente
SENT_CACHE = set()  # chiave: event_id o (home|away|kickoff_day)


def make_event_key(ev: Dict, row: Dict) -> str:
    eid, eh, ea, _, _, ev_kick = extract_event_fields(ev)
    if eid:
        return eid
    # fallback robusto
    home = norm(get_field_case_insensitive(row, "home", "home team", "hometeam", "team1", "home_name") or "")
    away = norm(get_field_case_insensitive(row, "away", "away team", "awayteam", "team2", "away_name") or "")
    kd = (ev_kick or parse_kickoff_from_row(row) or datetime.now(timezone.utc)).date().isoformat()
    return f"{home}|{away}|{kd}"


# -----------------------------
# Main loop
# -----------------------------
def main():
    if SEND_STARTUP_MSG:
        telegram_send("🤖 <b>FootyStats Bot avviato</b>\nMonitoraggio partite in corso…")

    while True:
        try:
            log("INFO", f"Soglia AVG: {AVG_GOALS_THRESHOLD:.2f} | Minuti check: {CHECK_TIME_MINUTES}")
            log("INFO", "============================================================")
            log("INFO", "INIZIO CONTROLLO")
            log("INFO", "============================================================")

            rows = load_csv_rows(GITHUB_CSV_URL)
            live = fetch_live_events()

            found = 0

            for row in rows:
                league = league_name_from_row(row)
                if league and not league_allowed(league):
                    log("DEBUG", f"Skip lega (blacklist/keyword): {league}")
                    continue

                avg_goals = get_avg_from_row(row)
                if avg_goals is None:
                    log("DEBUG", f"Skip: AVG mancante | lega={league} | teams={get_field_case_insensitive(row, 'home')}-{get_field_case_insensitive(row, 'away')}")
                    continue

                if avg_goals < AVG_GOALS_THRESHOLD:
                    continue

                # (opzionale) forma squadra se presente
                tf = get_team_form_avg(row)
                if tf is not None and tf < TEAM_FORM_MIN_AVG:
                    log("DEBUG", f"Skip: Team form {tf:.2f} < {TEAM_FORM_MIN_AVG:.2f}")
                    continue

                ev = match_row_to_event(row, live)
                if not ev:
                    log("DEBUG", "Nessun match live corrispondente (nomi/tempo)")
                    continue

                eid, eh, ea, hs, as_, _ = extract_event_fields(ev)
                if hs is None or as_ is None:
                    log("DEBUG", f"Nessun punteggio per evento {eid}")
                    continue

                # condizione risultato 0-0
                if hs != 0 or as_ != 0:
                    continue

                minute = minutes_since_kickoff(row, ev)
                if minute is None:
                    log("DEBUG", "Minuti non calcolabili (manca kickoff sia in CSV che API)")
                    continue

                if minute < MIN_MINUTE or minute > MAX_MINUTE:
                    log("DEBUG", f"Fuori finestra minuti [{MIN_MINUTE}-{MAX_MINUTE}]: {minute}'")
                    continue

                key = make_event_key(ev, row)
                if key in SENT_CACHE:
                    continue

                text = build_signal_text(row, ev, minute, avg_goals, (hs, as_))
                telegram_send(text)
                SENT_CACHE.add(key)
                found += 1

            log("INFO", f"Opportunità trovate: {found}")
            log("INFO", "============================================================")
            log("INFO", f"Sleep {SLEEP_SECONDS}s…")
            time.sleep(SLEEP_SECONDS)

        except requests.HTTPError as e:
            log("ERROR", f"HTTP error: {e}")
            time.sleep(10)
        except Exception as e:
            log("ERROR", f"Errore generico: {e}")
            time.sleep(10)


if __name__ == "__main__":
    log("INFO", "Bot avviato")
    main()
