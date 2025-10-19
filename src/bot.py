# src/bot.py
import os
import re
import time
import math
import json
import unicodedata
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

import requests
import pandas as pd

# =========================
# Lettura ENV / Parametri
# =========================
TELEGRAM_TOKEN            = os.getenv("TELEGRAM_TOKEN", "").strip()
CHAT_ID                   = os.getenv("CHAT_ID", "").strip()

GITHUB_CSV_URL            = os.getenv("GITHUB_CSV_URL", "").strip()

AVG_GOALS_THRESHOLD       = float(os.getenv("AVG_GOALS_THRESHOLD", "2.5"))
CHECK_TIME_MINUTES        = int(os.getenv("CHECK_TIME_MINUTES", "50"))   # tenuto per compatibilità, non usato direttamente
CHECK_INTERVAL_SECONDS    = int(os.getenv("CHECK_INTERVAL_SECONDS", "300"))

# Finestra trigger
MIN_MINUTE                = int(os.getenv("MIN_MINUTE", "50"))
MAX_MINUTE                = int(os.getenv("MAX_MINUTE", "56"))

# Filtri campionati / forma
LEAGUE_MIN_AVG            = float(os.getenv("LEAGUE_MIN_AVG", "0"))  # opzionale, se presente in CSV
TEAM_FORM_MIN_AVG         = float(os.getenv("TEAM_FORM_MIN_AVG", "0"))

LEAGUE_BLACKLIST          = os.getenv("LEAGUE_BLACKLIST", "")
LEAGUE_WHITELIST          = os.getenv("LEAGUE_WHITELIST", "").strip()  # opzionale

# RapidAPI
RAPIDAPI_BASE             = os.getenv("RAPIDAPI_BASE", "https://bet365data.p.rapidapi.com").rstrip("/")
RAPIDAPI_HOST             = os.getenv("RAPIDAPI_HOST", "bet365data.p.rapidapi.com")
RAPIDAPI_KEY              = os.getenv("RAPIDAPI_KEY", "")
RAPIDAPI_EVENTS_PATH      = os.getenv("RAPIDAPI_EVENTS_PATH", "/live-events")
RAPIDAPI_EVENTS_PARAMS    = os.getenv("RAPIDAPI_EVENTS_PARAMS", "sport=soccer")
RAPIDAPI_MARKETS_PATH     = os.getenv("RAPIDAPI_MARKETS_PATH", "/live-events/{id}")  # non necessario per il tuo segnale, lasciato per future estensioni
RAPIDAPI_MARKETS_ID_PARAM = os.getenv("RAPIDAPI_MARKETS_ID_PARAM", "id")

SEND_STARTUP_MESSAGE      = os.getenv("SEND_STARTUP_MESSAGE", "1").strip() == "1"
DEBUG_LOG                 = os.getenv("DEBUG_LOG", "0").strip() == "1"

HEADERS_RAPIDAPI = {
    "x-rapidapi-host": RAPIDAPI_HOST,
    "x-rapidapi-key": RAPIDAPI_KEY,
}

# Stato per evitare duplicati
already_notified: set = set()

# =========================
# Utilità
# =========================
def log(level: str, msg: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now} - {level.upper()} - {msg}", flush=True)

def normalize(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    # rimuovi tag tipo (F), U19, ecc.
    s = re.sub(r"\([^)]*\)", " ", s)
    s = re.sub(r"\b(u19|u20|u21|u23|w|women|reserves?)\b", " ", s)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def compile_pipe_regex(pipe_str: str) -> Optional[re.Pattern]:
    pipe_str = (pipe_str or "").strip()
    if not pipe_str:
        return None
    # Escapa ogni token e fai match case-insensitive in qualsiasi punto della stringa
    tokens = [t.strip() for t in pipe_str.split("|") if t.strip()]
    if not tokens:
        return None
    pattern = "|".join(re.escape(t) for t in tokens)
    return re.compile(pattern, flags=re.IGNORECASE)

RX_BLACK = compile_pipe_regex(LEAGUE_BLACKLIST)
RX_WHITE = compile_pipe_regex(LEAGUE_WHITELIST)

# =========================
# Telegram
# =========================
def send_telegram(text: str) -> None:
    if not TELEGRAM_TOKEN or not CHAT_ID:
        log("WARN", "Telegram non configurato: salta invio messaggio")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        r = requests.post(url, json=payload, timeout=20)
        if r.status_code // 100 == 2:
            if DEBUG_LOG:
                log("DEBUG", f"Telegram OK: {r.status_code}")
        else:
            log("ERROR", f"Telegram errore HTTP {r.status_code}: {r.text[:200]}")
    except Exception as e:
        log("ERROR", f"Telegram eccezione: {e}")

# =========================
# CSV Footystats
# =========================
def read_csv(url: str) -> pd.DataFrame:
    df = pd.read_csv(url)
    # uniforma nomi colonne più comuni
    cols = {c.lower(): c for c in df.columns}
    # cerca colonne team/league/avg più probabili
    # (il CSV FootyStats può avere molte varianti: gestiamo le più comuni)
    expected = {
        "home": ["home", "home_team", "team_home", "Home Team", "Home"],
        "away": ["away", "away_team", "team_away", "Away Team", "Away"],
        "league": ["league", "competition", "league_name", "Country & League"],
        "avg": ["avg", "avg_goals", "avg_total_goals", "Avg Total Goals", "Average Total Goals"],
        "kickoff": ["kickoff", "start_time", "date", "timestamp"],
        # opzionale: forme
        "home_form": ["home_form_avg", "home_form_last5", "home_last5_avg"],
        "away_form": ["away_form_avg", "away_form_last5", "away_last5_avg"],
        "league_avg": ["league_avg", "league_avg_goals"],
    }

    # mappa scelte reali -> alias standard
    rename_map = {}

    def pick(colnames: List[str]) -> Optional[str]:
        for name in colnames:
            key = name.lower()
            if key in cols:
                return cols[key]
        return None

    chosen = {k: pick(v) for k, v in expected.items()}

    # rinomina ciò che troviamo
    for std, real in chosen.items():
        if real:
            rename_map[real] = std

    if rename_map:
        df = df.rename(columns=rename_map)

    # normalizza stringhe chiave
    for col in ["home", "away", "league"]:
        if col in df.columns:
            df[col] = df[col].astype(str).fillna("").map(str).map(lambda x: x.strip())

    # forza numerici dove possibile
    for col in ["avg", "league_avg", "home_form", "away_form"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df

def lookup_match_avg(df: pd.DataFrame, league: str, home: str, away: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Ritorna (avg_tot_goals, home_form, away_form) se matchato, altrimenti (None,None,None)."""
    if df is None or df.empty:
        return (None, None, None)

    # Filtra per lega se presente
    sub = df
    if "league" in df.columns and league:
        # match soft sulla lega
        n_lg = normalize(league)
        sub = df[df["league"].map(lambda x: normalize(str(x)) == n_lg)] or df

    # match per nomi squadre normalizzati (ordine irrilevante, ma proviamo diretto)
    n_home = normalize(home)
    n_away = normalize(away)

    def rows_match(r) -> bool:
        h = normalize(str(r.get("home", "")))
        a = normalize(str(r.get("away", "")))
        return (h == n_home and a == n_away) or (h == n_away and a == n_home)

    cand = sub[sub.apply(rows_match, axis=1)]
    if cand.empty and "league" in df.columns:
        # se non ha matchato, prova su tutto df (magari la lega diverge di stringa)
        cand = df[df.apply(rows_match, axis=1)]

    if cand.empty:
        return (None, None, None)

    row = cand.iloc[0]
    avg = float(row["avg"]) if "avg" in row and pd.notna(row["avg"]) else None
    hf  = float(row["home_form"]) if "home_form" in row and pd.notna(row["home_form"]) else None
    af  = float(row["away_form"]) if "away_form" in row and pd.notna(row["away_form"]) else None
    return (avg, hf, af)

# =========================
# RapidAPI: Live Events
# =========================
def fetch_live_events() -> List[Dict[str, Any]]:
    """Chiama /live-events, gestendo formati eterogenei, e ritorna solo eventi in forma dict."""
    url = f"{RAPIDAPI_BASE}{RAPIDAPI_EVENTS_PATH}"
    params: Dict[str, str] = {}
    if RAPIDAPI_EVENTS_PARAMS:
        for p in RAPIDAPI_EVENTS_PARAMS.split("&"):
            if "=" in p:
                k, v = p.split("=", 1)
                params[k.strip()] = v.strip()

    if DEBUG_LOG:
        log("DEBUG", f"Chiamo live-events: {url} {params}")

    r = requests.get(url, headers=HEADERS_RAPIDAPI, params=params, timeout=25)
    r.raise_for_status()
    data = r.json()

    # Normalizza: l'API può restituire dict o list
    if isinstance(data, list):
        events = data
    elif isinstance(data, dict):
        events = data.get("data") or data.get("events") or data.get("results") or []
    else:
        events = []

    # Tieni solo dict validi
    events = [ev for ev in events if isinstance(ev, dict)]
    log("INFO", f"API live-events: {len(events)} match live")
    return events

def safe_get(d: Dict[str, Any], *path, default=None):
    cur = d
    for key in path:
        if isinstance(cur, dict) and key in cur:
            cur = cur[key]
        else:
            return default
    return cur

def extract_event_fields(ev: Dict[str, Any]) -> Tuple[str, str, str, int, int, Optional[int], str]:
    """
    Estrae: (event_id, home_name, away_name, home_score, away_score, kickoff_epoch_utc, league_name)
    Gestisce varie strutture comuni in RapidAPI/Bet365.
    """
    eid = str(ev.get("id") or ev.get("event_id") or ev.get("uid") or "")
    league = (
        safe_get(ev, "league", "name")
        or ev.get("league_name")
        or safe_get(ev, "tournament", "name")
        or ev.get("competition")
        or ""
    )
    # nomi squadre
    home = (
        safe_get(ev, "home", "name")
        or safe_get(ev, "teams", "home", "name")
        or ev.get("homeTeam")
        or ev.get("home_name")
        or ev.get("home")
        or ""
    )
    away = (
        safe_get(ev, "away", "name")
        or safe_get(ev, "teams", "away", "name")
        or ev.get("awayTeam")
        or ev.get("away_name")
        or ev.get("away")
        or ""
    )

    # punteggio
    hs = (
        safe_get(ev, "state", "homeScore")
        or safe_get(ev, "score", "home")
        or safe_get(ev, "scores", "home")
        or ev.get("homeScore")
        or 0
    )
    as_ = (
        safe_get(ev, "state", "awayScore")
        or safe_get(ev, "score", "away")
        or safe_get(ev, "scores", "away")
        or ev.get("awayScore")
        or 0
    )
    try:
        hs = int(hs)
    except Exception:
        hs = 0
    try:
        as_ = int(as_)
    except Exception:
        as_ = 0

    # kickoff epoch (UTC)
    kickoff = (
        ev.get("kickoff")
        or ev.get("startTime")
        or safe_get(ev, "timer", "start")     # in secondi
        or safe_get(ev, "time", "kickoff")
        or None
    )
    # normalizza a int epoch secondi se possibile
    ko_epoch = None
    if isinstance(kickoff, (int, float)):
        # se è ragionevole (< 10^12), consideralo già in secondi
        ko_epoch = int(kickoff) if kickoff < 10**12 else int(kickoff // 1000)
    elif isinstance(kickoff, str):
        # prova a parse unix string
        if kickoff.isdigit():
            val = int(kickoff)
            ko_epoch = val if val < 10**12 else val // 1000
        else:
            # ISO8601
            try:
                dt = datetime.fromisoformat(kickoff.replace("Z", "+00:00"))
                ko_epoch = int(dt.timestamp())
            except Exception:
                ko_epoch = None

    return eid, str(home), str(away), hs, as_, ko_epoch, str(league)

def get_elapsed_minutes(ev: Dict[str, Any], ko_epoch: Optional[int]) -> Optional[int]:
    """Ritorna i minuti trascorsi: prima prova dai campi 'timer', altrimenti da kickoff epoch."""
    # 1) minute esplicito
    cand = (
        safe_get(ev, "time", "minute")
        or safe_get(ev, "clock", "minute")
        or safe_get(ev, "timer", "minute")
        or ev.get("minute")
    )
    if cand is not None:
        try:
            return int(cand)
        except Exception:
            pass

    # 2) elapsed dal kickoff
    if ko_epoch:
        now = int(datetime.now(timezone.utc).timestamp())
        if now > ko_epoch:
            return (now - ko_epoch) // 60
    return None

# =========================
# Logica filtro e segnale
# =========================
def league_allowed(league_name: str) -> bool:
    if RX_WHITE and not RX_WHITE.search(league_name):
        return False
    if RX_BLACK and RX_BLACK.search(league_name):
        return False
    return True

def event_to_signal_text(league: str, home: str, away: str, minute: int, avg: float) -> str:
    # Messaggio coerente con il tuo formato
    lines = [
        "🚨 <b>SEGNALE OVER 1.5!</b>",
        f"⚽ <b>{home}</b> vs <b>{away}</b>",
        f"🏆 {league}",
        f"📊 AVG Goals: <b>{avg:.2f}</b>",
        f"🕒 {minute}' - Risultato: 0-0",
        "✅ Controlla Bet365 Live!",
        "",
        "🎯 <b>Punta Over 1.5 FT</b>",
    ]
    return "\n".join(lines)

def process_once(df: pd.DataFrame) -> None:
    # Scarica eventi live
    events = fetch_live_events()
    if not events:
        return

    for ev in events:
        try:
            eid, home, away, hs, as_, ko_epoch, league = extract_event_fields(ev)

            if not home or not away or not league:
                if DEBUG_LOG:
                    log("DEBUG", f"Skip evento incompleto: {ev}")
                continue

            if not league_allowed(league):
                if DEBUG_LOG:
                    log("DEBUG", f"Skip lega (blacklist/whitelist): {league} | teams={home}-{away}")
                continue

            # punteggio deve essere 0-0
            if hs != 0 or as_ != 0:
                if DEBUG_LOG:
                    log("DEBUG", f"Skip punteggio {home}-{away} {hs}-{as_}")
                continue

            # minuti
            minute = get_elapsed_minutes(ev, ko_epoch)
            if minute is None:
                if DEBUG_LOG:
                    log("DEBUG", f"Nessun minuto disponibile per {home} vs {away} (ID {eid})")
                continue

            if minute < MIN_MINUTE or minute > MAX_MINUTE:
                if DEBUG_LOG:
                    log("DEBUG", f"Fuori finestra minuto {minute} per {home}-{away}")
                continue

            # lookup AVG dal CSV
            avg, hf, af = lookup_match_avg(df, league, home, away)
            if avg is None:
                if DEBUG_LOG:
                    log("DEBUG", f"AVG non trovato su CSV per {home} vs {away} | {league}")
                continue

            if avg < AVG_GOALS_THRESHOLD:
                if DEBUG_LOG:
                    log("DEBUG", f"AVG {avg:.2f} < soglia {AVG_GOALS_THRESHOLD:.2f} per {home}-{away}")
                continue

            # opzionale: forma squadre se disponibile nel CSV
            if TEAM_FORM_MIN_AVG > 0:
                ok_form = True
                if hf is not None and hf < TEAM_FORM_MIN_AVG:
                    ok_form = False
                if af is not None and af < TEAM_FORM_MIN_AVG:
                    ok_form = False
                if not ok_form:
                    if DEBUG_LOG:
                        log("DEBUG", f"Forma insufficiente ({hf}, {af}) per {home}-{away}")
                    continue

            # evita doppioni
            dedup_key = f"{normalize(league)}|{normalize(home)}|{normalize(away)}"
            if dedup_key in already_notified:
                continue
            already_notified.add(dedup_key)

            # invia segnale
            text = event_to_signal_text(league, home, away, minute, avg)
            send_telegram(text)
            log("INFO", f"Segnale inviato: {home} vs {away} | {league} | {minute}' | AVG {avg:.2f}")

        except Exception as e:
            log("ERROR", f"Errore su evento: {e}")

# =========================
# Main loop
# =========================
def main():
    if SEND_STARTUP_MESSAGE:
        try:
            send_telegram("🤖 FootyStats Bot avviato\nMonitoraggio partite in corso…")
        except Exception:
            pass

    log("INFO", f"Soglia AVG: {AVG_GOALS_THRESHOLD:.2f} | Minuti check: {MIN_MINUTE}-{MAX_MINUTE}")

    while True:
        try:
            log("INFO", "==============================")
            log("INFO", "INIZIO CONTROLLO")
            log("INFO", "==============================")

            # carica CSV
            log("INFO", f"Scarico CSV: {GITHUB_CSV_URL}")
            df = read_csv(GITHUB_CSV_URL)
            log("INFO", f"CSV caricato ({len(df)} righe)")

            process_once(df)

        except requests.HTTPError as e:
            log("ERROR", f"Errore HTTP: {e}")
        except Exception as e:
            log("ERROR", f"Errore generico: {e}")

        log("INFO", f"Sleep {CHECK_INTERVAL_SECONDS}s…")
        time.sleep(CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()
