# src/bot.py
import os
import time
import math
import re
import requests
import pandas as pd
from datetime import datetime, timezone
from typing import Optional, Dict, Any

# -----------------------------
# Lettura ENV (usa valori sicuri di default)
# -----------------------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
CHAT_ID = os.getenv("CHAT_ID", "").strip()

GITHUB_CSV_URL = os.getenv("GITHUB_CSV_URL", "").strip()

AVG_GOALS_THRESHOLD = float(os.getenv("AVG_GOALS_THRESHOLD", os.getenv("LEAGUE_MIN_AVG", "2.5")))
MIN_MINUTE = int(os.getenv("MIN_MINUTE", "50"))
MAX_MINUTE = int(os.getenv("MAX_MINUTE", "56"))

CHECK_INTERVAL_SECONDS = int(os.getenv("CHECK_INTERVAL_SECONDS", "300"))
SEND_STARTUP_MESSAGE = os.getenv("SEND_STARTUP_MESSAGE", "1").strip() == "1"
DEBUG_LOG = os.getenv("DEBUG_LOG", "0").strip() == "1"

# RapidAPI (Bet365)
RAPIDAPI_BASE = os.getenv("RAPIDAPI_BASE", "https://bet365data.p.rapidapi.com").rstrip("/")
RAPIDAPI_EVENTS_PATH = os.getenv("RAPIDAPI_EVENTS_PATH", "/live-events")
RAPIDAPI_EVENTS_PARAMS = os.getenv("RAPIDAPI_EVENTS_PARAMS", "sport=soccer")
RAPIDAPI_HOST = os.getenv("RAPIDAPI_HOST", "bet365data.p.rapidapi.com")
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "")

# Filtri leghe
LEAGUE_BLACKLIST = os.getenv("LEAGUE_BLACKLIST", "")
LEAGUE_WHITELIST = os.getenv("LEAGUE_WHITELIST", "").strip()
EXCLUDE_KEYWORDS = os.getenv("LEAGUE_EXCLUDE_KEYWORDS", "Esoccer,Volta,8 mins play,H2H GG,Futsal,Beach,Penalty,Esports").strip()

# -----------------------------
# Utils
# -----------------------------
def log(level: str, msg: str):
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S} - {level} - {msg}")

def dlog(msg: str):
    if DEBUG_LOG:
        log("DEBUG", msg)

def normalize_name(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.lower()
    s = re.sub(r"\(w\)|\(f\)|\(u\d{2}\)|\(res\)|\(women\)|\(men\)", "", s)
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def telegram_send(text: str):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        log("WARN", "TELEGRAM_TOKEN/CHAT_ID mancanti—salto invio.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=15)
        r.raise_for_status()
        dlog("Telegram: messaggio inviato")
    except Exception as e:
        log("ERROR", f"Telegram error: {e}")

def compute_match_minute(start_epoch: Optional[int]) -> Optional[int]:
    """
    Calcola il minuto dal kickoff (epoch in secondi, UTC).
    Ritorna None se kickoff nel futuro o minuto fuori range plausibile 0..130.
    """
    if not start_epoch:
        return None
    try:
        kick = datetime.fromtimestamp(int(start_epoch), tz=timezone.utc)
    except Exception:
        return None
    now = datetime.now(timezone.utc)
    if now < kick:
        return None
    m = int((now - kick).total_seconds() // 60)
    if m < 0 or m > 130:
        return None
    return m

# -----------------------------
# CSV handling (tollerante)
# -----------------------------
CSV_COL_CANDIDATES = {
    "epoch": ["epoch", "timestamp", "kickoff_ts", "ts", 0],
    "league": ["league", "competition", "tournament", 3],
    "home": ["home", "home_team", "home_name", "Home", 4],
    "away": ["away", "away_team", "away_name", "Away", 5],
    "avg":  ["Avg Total Goals", "avg_total_goals", "avg", "AVG", "Avg", -1],  # -1 = fallback euristica
}

def _pick_col(df: pd.DataFrame, keys):
    # se è un indice numerico
    for k in keys:
        if isinstance(k, int):
            if k in df.columns:
                return k
    # altrimenti prova i nomi
    lower_map = {str(c).strip().lower(): c for c in df.columns}
    for k in keys:
        if isinstance(k, str):
            kk = k.strip().lower()
            if kk in lower_map:
                return lower_map[kk]
    return None

def load_matches_csv(url: str) -> pd.DataFrame:
    df = pd.read_csv(url, header=0)
    # Se la prima riga sembra dati veri ma i nomi non aiutano, prova anche senza header
    if not {"home", "away", "league"}.intersection({str(c).lower() for c in df.columns}):
        try:
            df2 = pd.read_csv(url, header=None)
            # usa df2 se ha più colonne o se il primo campo è chiaramente epoch
            first = df2.iloc[0, 0]
            if (df2.shape[1] >= df.shape[1]) or (isinstance(first, (int, float)) and int(first) > 1_600_000_000):
                df = df2
        except Exception:
            pass
    return df

def extract_row_fields(row: pd.Series) -> Optional[Dict[str, Any]]:
    # epoch
    epoch_col = _pick_col(row.to_frame().T, CSV_COL_CANDIDATES["epoch"])
    epoch = None
    if epoch_col is not None:
        try:
            epoch = int(row[epoch_col])
        except Exception:
            epoch = None

    # league
    league_col = _pick_col(row.to_frame().T, CSV_COL_CANDIDATES["league"])
    league = str(row[league_col]).strip() if league_col is not None else ""

    # home/away
    home_col = _pick_col(row.to_frame().T, CSV_COL_CANDIDATES["home"])
    away_col = _pick_col(row.to_frame().T, CSV_COL_CANDIDATES["away"])
    if home_col is None or away_col is None:
        return None
    home = str(row[home_col]).strip()
    away = str(row[away_col]).strip()

    # avg (tenta varie colonne; se -1 usa euristica: cerca una colonna che contenga 'avg' e sia numerica)
    avg_col = _pick_col(row.to_frame().T, CSV_COL_CANDIDATES["avg"])
    avg = None
    if avg_col is not None and avg_col != -1:
        try:
            avg = float(row[avg_col])
        except Exception:
            avg = None
    if avg is None:
        # euristica: prendi la prima colonna che nel nome contenga 'avg' e sia numerica
        for c in row.index:
            name = str(c).lower()
            if "avg" in name and isinstance(row[c], (int, float)) and not math.isnan(float(row[c])):
                avg = float(row[c])
                break
        # se ancora None, prova a scorrere tutti i valori e prendere un float “ragionevole” 0.5..6
        if avg is None:
            for v in row.values:
                try:
                    x = float(v)
                    if 0.3 <= x <= 7.0:
                        avg = x
                        break
                except Exception:
                    continue

    if avg is None:
        return None

    return {
        "epoch": epoch,
        "league": league,
        "home": home,
        "away": away,
        "avg": avg,
    }

# -----------------------------
# RapidAPI: lista eventi live
# -----------------------------
def fetch_live_events() -> list[dict]:
    url = f"{RAPIDAPI_BASE}{RAPIDAPI_EVENTS_PATH}"
    params = {}
    # supporta "sport=soccer&..." passato in stringa
    if RAPIDAPI_EVENTS_PARAMS:
        for chunk in RAPIDAPI_EVENTS_PARAMS.split("&"):
            if "=" in chunk:
                k, v = chunk.split("=", 1)
                params[k] = v

    headers = {
        "x-rapidapi-host": RAPIDAPI_HOST,
        "x-rapidapi-key": RAPIDAPI_KEY,
    }
    r = requests.get(url, params=params, headers=headers, timeout=20)
    r.raise_for_status()
    data = r.json()

    # La API a volte restituisce {"data":[...]} oppure direttamente [...]
    if isinstance(data, dict) and "data" in data:
        data = data["data"]
    if not isinstance(data, list):
        return []

    events = []
    for it in data:
        try:
            # diversi payload possibili: prova a leggere home/away/score/minuto
            home = it.get("home", it.get("homeTeam", it.get("home_name", "")))
            away = it.get("away", it.get("awayTeam", it.get("away_name", "")))
            league = it.get("league", it.get("leagueName", it.get("competition", "")))
            match_id = it.get("id", it.get("event_id", it.get("eventId", "")))

            # score
            score = it.get("score")
            if isinstance(score, dict):
                h = score.get("home", 0)
                a = score.get("away", 0)
            elif isinstance(score, str) and "-" in score:
                parts = score.split("-")
                h = int(parts[0].strip())
                a = int(parts[1].strip())
            else:
                # a volte le chiavi sono separate
                h = int(it.get("homeScore", it.get("home_score", 0)) or 0)
                a = int(it.get("awayScore", it.get("away_score", 0)) or 0)

            # minute (se presente) con sanificazione
            minute = it.get("minute", it.get("time", it.get("clock", None)))
            try:
                minute = int(minute)
            except Exception:
                minute = None
            if minute is not None and not (0 <= minute <= 130):
                minute = None

            events.append({
                "id": str(match_id),
                "home": str(home or ""),
                "away": str(away or ""),
                "league": str(league or ""),
                "home_goals": int(h),
                "away_goals": int(a),
                "minute": minute,
            })
        except Exception as e:
            dlog(f"Evento scartato (parse): {e}")
            continue

    return events

# -----------------------------
# Filtri leghe
# -----------------------------
def league_is_blocked(league: str) -> bool:
    L = (league or "")
    # keywords
    for kw in (EXCLUDE_KEYWORDS.split(",") if EXCLUDE_KEYWORDS else []):
        if kw.strip() and kw.strip().lower() in L.lower():
            return True
    # blacklist pipe-delimited
    for kw in (LEAGUE_BLACKLIST.split("|") if LEAGUE_BLACKLIST else []):
        if kw.strip() and kw.strip().lower() in L.lower():
            return True
    # whitelist (se impostata, passa solo se presente)
    if LEAGUE_WHITELIST:
        ok = False
        for kw in LEAGUE_WHITELIST.split("|"):
            if kw.strip() and kw.strip().lower() in L.lower():
                ok = True
                break
        return not ok
    return False

# -----------------------------
# Matching CSV <-> Live events
# -----------------------------
def build_event_index(events: list[dict]) -> Dict[tuple, dict]:
    idx = {}
    for e in events:
        key = (normalize_name(e["home"]), normalize_name(e["away"]))
        idx[key] = e
    return idx

def minute_for_match(csv_row: Dict[str, Any], maybe_event: Optional[dict]) -> Optional[int]:
    """
    Restituisce il minuto “pulito”:
      1) usa il minuto dell'API se valido,
      2) altrimenti stima dal kickoff epoch (senza pre-kickoff, range 0..130)
    """
    if maybe_event and maybe_event.get("minute") is not None:
        m = int(maybe_event["minute"])
        if 0 <= m <= 130:
            return m
    return compute_match_minute(csv_row.get("epoch"))

# -----------------------------
# Notifiche (dedup in memoria)
# -----------------------------
notified_keys: set[str] = set()

def notify_signal(row: Dict[str, Any], minute: int, league: str):
    key = f"{row['home']}|{row['away']}|{minute}"
    if key in notified_keys:
        return
    notified_keys.add(key)

    text = (
        "🚨 <b>SEGNALE OVER 1.5!</b>\n\n"
        f"⚽ <b>{row['home']}</b> vs <b>{row['away']}</b>\n"
        f"🏆 {league}\n"
        f"📊 <b>AVG Goals:</b> {row['avg']:.2f}\n"
        f"⏱️ <b>{minute}'</b> - <b>Risultato:</b> 0-0\n"
        "✅ Controlla Bet365 Live!\n\n"
        "🎯 <b>Punta Over 1.5 FT</b>"
    )
    telegram_send(text)

# -----------------------------
# MAIN LOOP
# -----------------------------
def main():
    if SEND_STARTUP_MESSAGE:
        telegram_send("🤖 FootyStats Bot avviato\nMonitoraggio partite in corso…")

    log("INFO", f"Soglia AVG: {AVG_GOALS_THRESHOLD:.2f} | Minuti check: {MIN_MINUTE}-{MAX_MINUTE}")

    while True:
        log("INFO", "============================================================")
        log("INFO", "INIZIO CONTROLLO")
        log("INFO", "============================================================")
        try:
            # 1) CSV
            log("INFO", f"Scarico CSV: {GITHUB_CSV_URL}")
            df = load_matches_csv(GITHUB_CSV_URL)
            log("INFO", f"CSV caricato ({len(df)} righe)")

            # 2) Live events API
            events = fetch_live_events()
            log("INFO", f"API live-events: {len(events)} match live")
            ev_index = build_event_index(events)

            # 3) Scansione CSV
            found = 0
            for _, raw in df.iterrows():
                fields = extract_row_fields(raw)
                if not fields:
                    dlog("Skip riga: campi essenziali mancanti (home/away/avg/epoch).")
                    continue

                if fields["avg"] < AVG_GOALS_THRESHOLD:
                    dlog(f"Skip: AVG {fields['avg']:.2f} < soglia")
                    continue

                if league_is_blocked(fields["league"]):
                    dlog(f"Skip lega (blacklist/keyword): {fields['league']}")
                    continue

                # match con evento live
                e = ev_index.get((normalize_name(fields["home"]), normalize_name(fields["away"])))
                if not e:
                    # nessun evento live per questa coppia
                    continue

                # stato 0-0?
                if e["home_goals"] != 0 or e["away_goals"] != 0:
                    continue

                # minuto pulito (API valida o fallback kickoff)
                minute = minute_for_match(fields, e)
                if minute is None:
                    dlog(f"Minuto indisponibile o pre-kickoff per {fields['home']} vs {fields['away']}")
                    continue

                # Finestra di allerta
                if MIN_MINUTE <= minute <= MAX_MINUTE:
                    found += 1
                    notify_signal(fields, minute, e.get("league") or fields["league"])

            log("INFO", f"Opportunità trovate: {found}")

        except Exception as e:
            log("ERROR", f"Errore generico: {e}")

        log("INFO", f"Sleep {CHECK_INTERVAL_SECONDS}s…")
        time.sleep(CHECK_INTERVAL_SECONDS)

# -----------------------------
if __name__ == "__main__":
    main()
