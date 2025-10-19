import os
import time
import csv
import logging
from io import StringIO
from datetime import datetime, timedelta, timezone
import re
import requests

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger("footystats-bot")

# -----------------------------------------------------------------------------
# ENV & Config (tutto compatibile con la tua versione)
# -----------------------------------------------------------------------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
CHAT_ID        = os.getenv("CHAT_ID", "")

# CSV FootyStats (raw)
GITHUB_CSV_URL = os.getenv("GITHUB_CSV_URL", "")

# Soglie base già usate
AVG_GOALS_THRESHOLD    = float(os.getenv("AVG_GOALS_THRESHOLD", "2.50"))
MIN_MINUTE             = int(os.getenv("MIN_MINUTE", "50"))
MAX_MINUTE             = int(os.getenv("MAX_MINUTE", "56"))
CHECK_INTERVAL_SECONDS = int(os.getenv("CHECK_INTERVAL_SECONDS", "300"))

# RapidAPI config (già in uso)
RAPIDAPI_BASE   = os.getenv("RAPIDAPI_BASE", "https://bet365data.p.rapidapi.com")
RAPIDAPI_HOST   = os.getenv("RAPIDAPI_HOST", "bet365data.p.rapidapi.com")
RAPIDAPI_KEY    = os.getenv("RAPIDAPI_KEY", "")
RAPIDAPI_EVENTS_PATH  = os.getenv("RAPIDAPI_EVENTS_PATH", "/live-events")
RAPIDAPI_EVENTS_PARAMS= os.getenv("RAPIDAPI_EVENTS_PARAMS", "sport=soccer")
RAPIDAPI_MARKETS_PATH = os.getenv("RAPIDAPI_MARKETS_PATH", "/live-events/{id}")
RAPIDAPI_MARKET_ID_PARAM = os.getenv("RAPIDAPI_MARKETS_ID_PARAM", "id")

# Esclusioni leghe “virtual/esoccer” (già avevi qualcosa di simile)
LEAGUE_EXCLUDE_KEYWORDS = [
    w.strip().lower() for w in os.getenv(
        "LEAGUE_EXCLUDE_KEYWORDS",
        "Esoccer,Volta,8 mins play,H2H GG"
    ).split(",") if w.strip()
]

# 🔥 NUOVI PARAMETRI DI FILTRO (facoltativi)
# Soglia minima di prolificità per LEGA, calcolata dal CSV (media “Average Goals” per quella lega nel giorno)
LEAGUE_MIN_AVG = float(os.getenv("LEAGUE_MIN_AVG", "2.60"))

# Blacklist/Whitelist leghe (match per substring case-insensitive)
LEAGUE_BLACKLIST = [s.strip().lower() for s in os.getenv("LEAGUE_BLACKLIST", "").split("|") if s.strip()]
LEAGUE_WHITELIST = [s.strip().lower() for s in os.getenv("LEAGUE_WHITELIST", "").split("|") if s.strip()]

# Forma recente minima (se il CSV ha colonne utili: last5/last10 goals ecc.)
TEAM_FORM_MIN_AVG = float(os.getenv("TEAM_FORM_MIN_AVG", "1.40"))

# Per leggere “ora locale” quando serve
UTC = timezone.utc

# Per evitare doppie notifiche
notified_ids = set()

# -----------------------------------------------------------------------------
# Utils
# -----------------------------------------------------------------------------
def send_telegram(msg: str) -> bool:
    if not TELEGRAM_TOKEN or not CHAT_ID:
        log.warning("Telegram non configurato; salto invio.")
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        data = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True}
        r = requests.post(url, data=data, timeout=10)
        if r.status_code == 200:
            log.info("Telegram: messaggio inviato")
            return True
        log.error("Telegram: errore %s - %s", r.status_code, r.text)
    except Exception as e:
        log.error("Telegram: eccezione %s", e)
    return False

def normalize_name(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[\.\-_/]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def similar(a: str, b: str) -> bool:
    """Matching permissivo: es. 'milan u20' ~ 'ac milan u20'."""
    A = set(normalize_name(a).split())
    B = set(normalize_name(b).split())
    if not A or not B:
        return False
    inter = len(A & B)
    return inter >= max(1, min(len(A), len(B)) - 1)

def parse_float(x, default=None):
    try:
        return float(str(x).replace(",", "."))
    except Exception:
        return default

# -----------------------------------------------------------------------------
# CSV loader + statistiche di LEGA e FORMA (best-effort)
# -----------------------------------------------------------------------------
def load_csv_matches():
    log.info("Scarico CSV: %s", GITHUB_CSV_URL)
    r = requests.get(GITHUB_CSV_URL, timeout=30)
    r.raise_for_status()
    reader = csv.DictReader(StringIO(r.text))
    rows = list(reader)
    log.info("CSV caricato (%d righe)", len(rows))
    return rows

def league_avg_from_csv(rows):
    """Media 'Average Goals' per lega (calcolata sul CSV del giorno)."""
    by_league = {}
    for m in rows:
        lg = (m.get("League") or m.get("league") or "").strip()
        avg = parse_float(m.get("Average Goals") or m.get("Avg Total Goals") or m.get("avg"), None)
        if not lg or avg is None:
            continue
        by_league.setdefault(lg, []).append(avg)
    out = {}
    for lg, arr in by_league.items():
        if arr:
            out[lg] = sum(arr) / len(arr)
    return out

# pattern di possibili colonne “forma” (best-effort).
FORM_HINTS = [
    r"last\s*5.*goals", r"last\s*10.*goals", r"form.*5", r"form.*10",
    r"avg.*last\s*5", r"avg.*last\s*10", r"L5.*goals", r"L10.*goals"
]
FORM_PATTERNS = [re.compile(p, re.I) for p in FORM_HINTS]

def extract_form_avg(row: dict, is_home: bool) -> float | None:
    """Tenta di leggere una metrica di forma recente (goals) per home/away."""
    # 1) colonne esplicite più comuni
    keys_try = []
    if is_home:
        keys_try += [
            "Home Last 5 Goals", "Home Last 10 Goals",
            "Home Form Last 5", "Home Form Last 10",
            "Home L5 Goals", "Home L10 Goals",
        ]
    else:
        keys_try += [
            "Away Last 5 Goals", "Away Last 10 Goals",
            "Away Form Last 5", "Away Form Last 10",
            "Away L5 Goals", "Away L10 Goals",
        ]

    for k in keys_try:
        if k in row:
            val = parse_float(row.get(k), None)
            if val is not None:
                return val

    # 2) fallback: cerca pattern generici che contengano "home"/"away" nel nome
    for col, val in row.items():
        cname = col.lower()
        if is_home and "home" not in cname:
            continue
        if (not is_home) and "away" not in cname:
            continue
        if any(p.search(cname) for p in FORM_PATTERNS):
            num = parse_float(val, None)
            if num is not None:
                return num

    # 3) ultimo fallback: se non troviamo nulla, None
    return None

# -----------------------------------------------------------------------------
# RapidAPI
# -----------------------------------------------------------------------------
def get_live_events():
    url = f"{RAPIDAPI_BASE}{RAPIDAPI_EVENTS_PATH}"
    params = {}
    # supporto "sport=soccer&foo=bar" (stringa già pronta in ENV)
    if RAPIDAPI_EVENTS_PARAMS:
        for kv in RAPIDAPI_EVENTS_PARAMS.split("&"):
            if "=" in kv:
                k, v = kv.split("=", 1)
                params[k] = v

    headers = {
        "x-rapidapi-host": RAPIDAPI_HOST,
        "x-rapidapi-key": RAPIDAPI_KEY
    }
    r = requests.get(url, headers=headers, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    events = (data or {}).get("data", {}).get("events", []) or []
    # normalizzo struttura minima che ci serve
    cleaned = []
    for e in events:
        cleaned.append({
            "id": e.get("id") or e.get("IID"),
            "home": e.get("home", ""),
            "away": e.get("away", ""),
            "league": e.get("league") or e.get("CT") or "",
            "score": e.get("SS") or "",
            "TU": e.get("TU") or "",  # timestamp “time updated” del provider
        })
    log.info("API live-events: %d match live", len(cleaned))
    return cleaned

# -----------------------------------------------------------------------------
# Minuto stimato dal CSV (stessa logica che già usi: partenza UTC dal CSV)
# CSV: campo “Date”/“Kickoff” opzionale oppure “TU” dal feed live.
# Qui usiamo TU come “orario start” stimato: TU è last update, quindi
# in alcune leghe può non rappresentare l’inizio — per questo usiamo la
# finestra 50–56 come compromesso pratico.
# -----------------------------------------------------------------------------
def estimate_minute_from_tu(tu_str: str) -> int | None:
    """
    TU formati più frequenti: 'YYYYMMDDHHMMSS' oppure vuoto.
    Se manca/è strano, restituisce None.
    """
    tu_str = (tu_str or "").strip()
    if not tu_str or not re.match(r"^\d{14}$", tu_str):
        return None
    try:
        dt = datetime.strptime(tu_str, "%Y%m%d%H%M%S").replace(tzinfo=UTC)
        # ATTENZIONE: è un'approssimazione — non è l'ora di calcio d'inizio.
        # Noi la usiamo solo per far scattare la finestra 50–56 in modo robusto.
        # Confronto con "adesso":
        minutes = int((datetime.now(UTC) - dt).total_seconds() // 60)
        return minutes
    except Exception:
        return None

# -----------------------------------------------------------------------------
# Filtri
# -----------------------------------------------------------------------------
def league_blocked(league_name: str) -> bool:
    ln = (league_name or "").lower()
    if LEAGUE_BLACKLIST and any(b in ln for b in LEAGUE_BLACKLIST):
        return True
    if LEAGUE_EXCLUDE_KEYWORDS and any(k in ln for k in LEAGUE_EXCLUDE_KEYWORDS):
        return True
    return False

def league_allowed_by_avg(league_name: str, league_avg_map: dict) -> bool:
    if LEAGUE_WHITELIST and not any(w in (league_name or "").lower() for w in LEAGUE_WHITELIST):
        # se usi whitelist, passa solo ciò che è whitelisted
        return False
    league_avg = league_avg_map.get(league_name, None)
    if league_avg is None:
        # se mancano dati di lega nel csv del giorno, non penalizzo
        return True
    return league_avg >= LEAGUE_MIN_AVG

def team_form_ok(row: dict) -> bool:
    """Usa forma recente se disponibile nel CSV; altrimenti True."""
    # home
    h_form = extract_form_avg(row, is_home=True)
    a_form = extract_form_avg(row, is_home=False)

    vals = []
    if h_form is not None:
        vals.append(h_form)
    if a_form is not None:
        vals.append(a_form)
    if not vals:
        # CSV non fornisce form utili → non filtro
        return True

    # criterio semplice: media delle due forme deve superare soglia
    return (sum(vals) / len(vals)) >= TEAM_FORM_MIN_AVG

# -----------------------------------------------------------------------------
# Match CSV ↔ Live matching
# -----------------------------------------------------------------------------
def csv_row_matches_event(row: dict, event: dict) -> bool:
    h1 = row.get("Home Team") or row.get("home team") or ""
    a1 = row.get("Away Team") or row.get("away team") or ""
    h2 = event.get("home", "")
    a2 = event.get("away", "")
    if not h1 or not a1 or not h2 or not a2:
        return False

    # exact
    if normalize_name(h1) == normalize_name(h2) and normalize_name(a1) == normalize_name(a2):
        return True
    # permissivo
    if similar(h1, h2) and similar(a1, a2):
        return True
    # cross (a volte invertito)
    if similar(h1, a2) and similar(a1, h2):
        return True
    return False

def score_is_00(score: str) -> bool:
    return score.strip() in {"0-0", "0 – 0", "0 : 0", "0–0", "0:0"}

# -----------------------------------------------------------------------------
# Messaggio
# -----------------------------------------------------------------------------
def build_signal_message(ev: dict, row: dict, minute: int) -> str:
    league = ev.get("league", "N/A")
    home = ev.get("home", "?")
    away = ev.get("away", "?")
    avg  = row.get("Average Goals") or row.get("Avg Total Goals") or "N/A"
    return (
        "🚨 <b>SEGNALE OVER 1.5!</b>\n\n"
        f"⚽ <b>{home} vs {away}</b>\n"
        f"🏆 {league}\n"
        f"📊 AVG Goals: <b>{avg}</b>\n"
        f"⏱️ <b>{minute}'</b> - Risultato: <b>0-0</b>\n"
        "✅ Controlla Bet365 Live!\n\n"
        "🎯 <b>Punta Over 1.5 FT</b>"
    )

# -----------------------------------------------------------------------------
# Main check
# -----------------------------------------------------------------------------
def run_check():
    log.info("=" * 60)
    log.info("INIZIO CONTROLLO")
    log.info("=" * 60)

    # 1) CSV
    rows = load_csv_matches()
    if not rows:
        log.warning("CSV vuoto")
        return

    # 2) mappa AVG di LEGA
    league_avg_map = league_avg_from_csv(rows)

    # 3
