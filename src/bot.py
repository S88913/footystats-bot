import os
import re
import csv
import json
import time
import math
import unicodedata
import logging
from datetime import datetime, timezone
from urllib.parse import urlencode

import requests

# -------------------------
# Env
# -------------------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
CHAT_ID = os.getenv("CHAT_ID", "").strip()

GITHUB_CSV_URL = os.getenv("GITHUB_CSV_URL", "").strip()

# Parametri filtro
LEAGUE_MIN_AVG = float(os.getenv("LEAGUE_MIN_AVG", "2.6"))
TEAM_FORM_MIN_AVG = float(os.getenv("TEAM_FORM_MIN_AVG", "0"))  # 0 = disattivato
MIN_MINUTE = int(os.getenv("MIN_MINUTE", "50"))
MAX_MINUTE = int(os.getenv("MAX_MINUTE", "56"))

# Black/White list (separatore | o , o ;)
LEAGUE_BLACKLIST = os.getenv("LEAGUE_BLACKLIST", "")
LEAGUE_WHITELIST = os.getenv("LEAGUE_WHITELIST", "").strip()

# RapidAPI
RAPIDAPI_BASE = os.getenv("RAPIDAPI_BASE", "https://bet365data.p.rapidapi.com").rstrip("/")
RAPIDAPI_HOST = os.getenv("RAPIDAPI_HOST", "bet365data.p.rapidapi.com")
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "")
RAPIDAPI_EVENTS_PATH = os.getenv("RAPIDAPI_EVENTS_PATH", "/live-events")
RAPIDAPI_EVENTS_PARAMS = os.getenv("RAPIDAPI_EVENTS_PARAMS", "sport=soccer")
RAPIDAPI_MARKETS_PATH = os.getenv("RAPIDAPI_MARKETS_PATH", "/live-events/{id}")
RAPIDAPI_MARKETS_ID_PARAM = os.getenv("RAPIDAPI_MARKETS_ID_PARAM", "id")

CHECK_INTERVAL_SECONDS = int(os.getenv("CHECK_INTERVAL_SECONDS", "300"))
SEND_STARTUP_MSG = os.getenv("SEND_STARTUP_MSG", "1").strip() == "1"
DEBUG_LOG = os.getenv("DEBUG_LOG", "0").strip() == "1"

# -------------------------
# Logging
# -------------------------
logging.basicConfig(
    level=logging.DEBUG if DEBUG_LOG else logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("bot")
log.info("Bot avviato")
log.info("Soglia AVG: %.2f | Minuti check: %d..%d", LEAGUE_MIN_AVG, MIN_MINUTE, MAX_MINUTE)

# -------------------------
# Util
# -------------------------
def now_utc():
    return datetime.now(timezone.utc)

def to_int_safe(x, default=None):
    try:
        return int(x)
    except Exception:
        return default

def normalize_name(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"[^\w]+", " ", s)
    s = re.sub(r"\b(f|w)\b", " ", s)  # rimuovi tag (F) / (W)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def split_list(s: str):
    if not s:
        return []
    # supporta | , ;
    parts = re.split(r"[|,;]\s*", s.strip())
    return [p.strip() for p in parts if p.strip()]

BLACKLIST = [normalize_name(x) for x in split_list(LEAGUE_BLACKLIST)]
WHITELIST = [normalize_name(x) for x in split_list(LEAGUE_WHITELIST)]

# -------------------------
# Telegram
# -------------------------
def tg_send(msg: str):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        log.warning("Telegram non configurato: salto invio")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"}, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log.error("Errore Telegram: %s", e)

# -------------------------
# CSV loader
# -------------------------
def fetch_csv_rows(url: str):
    if not url:
        raise ValueError("GITHUB_CSV_URL mancante")
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    content = r.text.strip().splitlines()
    reader = csv.DictReader(content)
    rows = list(reader)
    log.info("CSV caricato (%d righe)", len(rows))
    return rows

def get_field_case_insensitive(row: dict, *candidates):
    for k in row.keys():
        kl = k.lower().strip()
        for c in candidates:
            if kl == c.lower():
                return row[k]
    # fallback parziale
    for k in row.keys():
        kl = k.lower()
        for c in candidates:
            if c.lower() in kl:
                return row[k]
    return None

def get_avg_from_row(row: dict) -> float:
    # prova varie chiavi comuni
    val = get_field_case_insensitive(
        row,
        "avg_total_goals", "avg goals", "avg total goals", "goals_avg", "avg",
    )
    if val is None or val == "":
        return float("nan")
    try:
        return float(str(val).replace(",", "."))
    except Exception:
        return float("nan")

def get_forms_from_row(row: dict):
    # restituisce (home_form, away_form) se presenti, altrimenti (None, None)
    hf = get_field_case_insensitive(row, "home_form_avg", "home form", "home_form")
    af = get_field_case_insensitive(row, "away_form_avg", "away form", "away_form")
    def conv(x):
        if x is None or x == "": 
            return None
        try:
            return float(str(x).replace(",", "."))
        except Exception:
            return None
    return conv(hf), conv(af)

def get_kickoff_epoch(row: dict):
    # 1) prima colonna spesso è epoch
    #    se DictReader ha header strano, prova a prendere il primo value numerico a 10 cifre
    for k, v in row.items():
        vstr = str(v).strip()
        if vstr.isdigit() and len(vstr) in (10, 13):
            try:
                ts = int(vstr[:10])
                return ts
            except Exception:
                pass
        break
    # 2) proviamo a colonne note
    v = get_field_case_insensitive(row, "timestamp", "kickoff_ts", "epoch")
    if v and str(v).isdigit():
        return int(str(v)[:10])
    # 3) ultima spiaggia: usa data testuale (rischioso). Es: "Oct 15 2025 - 4:00pm"
    dt = get_field_case_insensitive(row, "date", "kickoff", "time")
    if dt:
        try:
            # Parsing “Oct 15 2025 - 4:00pm” come *naive* => assumiamo UTC.
            dt = re.sub(r"\s*-\s*", " ", dt)
            parsed = datetime.strptime(dt, "%b %d %Y %I:%M%p")
            return int(parsed.replace(tzinfo=timezone.utc).timestamp())
        except Exception:
            pass
    return None

def teams_from_row(row: dict):
    h = get_field_case_insensitive(row, "home", "home_team", "home team", "home_team_name")
    a = get_field_case_insensitive(row, "away", "away_team", "away team", "away_team_name")
    return (h or "").strip(), (a or "").strip()

def league_from_row(row: dict):
    lg = get_field_case_insensitive(row, "league", "competition", "tournament")
    return (lg or "").strip()

# -------------------------
# RapidAPI
# -------------------------
SESSION = requests.Session()
SESSION.headers.update({
    "x-rapidapi-host": RAPIDAPI_HOST,
    "x-rapidapi-key": RAPIDAPI_KEY,
})

def rapid_get(path: str, params: dict = None):
    url = f"{RAPIDAPI_BASE}{path}"
    if params:
        url = f"{url}?{urlencode(params)}"
    r = SESSION.get(url, timeout=30)
    r.raise_for_status()
    return r.json()

def fetch_live_soccer_events():
    # events
    params = dict([p.split("=", 1) for p in RAPIDAPI_EVENTS_PARAMS.split("&") if "=" in p]) if RAPIDAPI_EVENTS_PARAMS else {}
    data = rapid_get(RAPIDAPI_EVENTS_PATH, params)
    events = (data or {}).get("data", {}).get("events", [])
    log.info("API live-events: %d match live", len(events))
    return events

def parse_event_basic(ev: dict):
    # ritorna dict con chiavi: id, home, away, league, score, updatedAtUTC
    return {
        "id": ev.get("id") or ev.get("IID"),
        "home": ev.get("home", ""),
        "away": ev.get("away", ""),
        "league": ev.get("league") or ev.get("CT") or "",
        "score": ev.get("SS", ""),  # es "0-0"
        "updatedAtUTC": ev.get("updatedAtUTC"),
    }

# -------------------------
# Match logic
# -------------------------
sent_keys = set()  # anti-duplicato per sessione

def league_filtered(league_name: str) -> (bool, str):
    n = normalize_name(league_name)
    if WHITELIST:
        ok = any(w in n for w in WHITELIST)
        return (not ok, "not_in_whitelist")
    if BLACKLIST:
        if any(b in n for b in BLACKLIST):
            return (True, "in_blacklist")
    return (False, "")

def minute_from_kickoff(kick_ts: int) -> int | None:
    if not kick_ts:
        return None
    m = int((now_utc().timestamp() - kick_ts) // 60)
    return m

def score_is_00(score: str) -> bool:
    m = re.match(r"^\s*(\d+)\s*[-:]\s*(\d+)\s*$", str(score))
    if not m:
        return False
    return (int(m.group(1)) == 0 and int(m.group(2)) == 0)

def fuzzy_match(csv_home, csv_away, ev_home, ev_away) -> bool:
    # normalizza e verifica sovrapposizione sufficiente
    ch, ca = normalize_name(csv_home), normalize_name(csv_away)
    eh, ea = normalize_name(ev_home), normalize_name(ev_away)
    if not ch or not ca or not eh or not ea:
        return False
    # regola: ogni nome deve essere contenuto almeno in parte nell’altro
    def soft_in(a, b):
        return a in b or b in a
    ok = soft_in(ch, eh) and soft_in(ca, ea)
    # tollera scambi home/away (rarissimo, ma sicuro)
    if not ok:
        ok = soft_in(ch, ea) and soft_in(ca, eh)
    return ok

def format_alarm(row, ev, minute, avg):
    league = league_from_row(row) or ev.get("league") or "Unknown League"
    home, away = teams_from_row(row)
    if not home or not away:
        home, away = ev.get("home", ""), ev.get("away", "")
    score = ev.get("score", "0-0")
    msg = (
        "🚨 <b>SEGNALE OVER 1.5!</b>\n\n"
        f"⚽ {home} vs {away}\n"
        f"🏆 {league}\n"
        f"📊 AVG Goals: <b>{avg:.2f}</b>\n"
        f"🕒 {minute}' - Risultato: {score}\n"
        "✅ Controlla Bet365 Live!\n\n"
        "🎯 Punta Over 1.5 FT"
    )
    return msg

# -------------------------
# Main scan
# -------------------------
def scan_once():
    try:
        rows = fetch_csv_rows(GITHUB_CSV_URL)
    except Exception as e:
        log.error("Errore caricamento CSV: %s", e)
        return

    # Pre-filtra per AVG e lega
    playable = []
    for r in rows:
        avg = get_avg_from_row(r)
        league = league_from_row(r)
        flt, reason = league_filtered(league)
        if math.isnan(avg):
            if DEBUG_LOG:
                log.debug("Skip: AVG mancante | lega=%s | teams=%s-%s", league, *teams_from_row(r))
            continue
        if flt:
            if DEBUG_LOG:
                log.debug("Skip: lega filtrata (%s) | %s", reason, league)
            continue
        if avg < LEAGUE_MIN_AVG:
            if DEBUG_LOG:
                log.debug("Skip: AVG %.2f < %.2f | %s", avg, LEAGUE_MIN_AVG, league)
            continue
        playable.append((r, avg))

    log.info("Filtrati per AVG >= %.2f: %d", LEAGUE_MIN_AVG, len(playable))

    # Live events
    try:
        live_events = fetch_live_soccer_events()
    except Exception as e:
        log.error("Errore API live: %s", e)
        return

    # Indicizza eventi normalizzati per matching veloce
    norm_events = []
    for ev in live_events:
        b = parse_event_basic(ev)
        b["nh"] = normalize_name(b["home"])
        b["na"] = normalize_name(b["away"])
        b["nl"] = normalize_name(b["league"])
        norm_events.append(b)

    found = 0

    for row, avg in playable:
        home, away = teams_from_row(row)
        league = league_from_row(row)
        nh, na, nl = normalize_name(home), normalize_name(away), normalize_name(league)

        # match veloce su squadra/e
        candidates = [e for e in norm_events if (nh in e["nh"] or e["nh"] in nh) and (na in e["na"] or e["na"] in na)]
        if not candidates:
            # fallback: stessa lega + una squadra coincide
            candidates = [e for e in norm_events if (nl and nl in e["nl"]) and (nh in e["nh"] or na in e["na"] or e["nh"] in nh or e["na"] in na)]

        if not candidates:
            if DEBUG_LOG:
                log.debug("No match live: %s vs %s | %s", home, away, league)
            continue

        # scegli il candidato con score presente
        ev = None
        for c in candidates:
            if c.get("score"):
                ev = c
                break
        if not ev:
            ev = candidates[0]

        # condizioni sul risultato
        if not score_is_00(ev.get("score", "")):
            if DEBUG_LOG:
                log.debug("Skip: score non 0-0 (%s) per %s vs %s", ev.get("score"), home, away)
            continue

        # minuto da kickoff
        kick_ts = get_kickoff_epoch(row)
        minute = minute_from_kickoff(kick_ts) if kick_ts else None
        if minute is None:
            if DEBUG_LOG:
                log.debug("Kickoff non disponibile: %s vs %s", home, away)
            continue

        if minute < MIN_MINUTE or minute > MAX_MINUTE:
            if DEBUG_LOG:
                log.debug("Fuori finestra minuto (%d) %s vs %s", minute, home, away)
            continue

        # forma squadre (se presente e richiesta)
        if TEAM_FORM_MIN_AVG > 0:
            hf, af = get_forms_from_row(row)
            # se non c'è info, non blocchiamo; blocchiamo solo se presenti e < soglia
            cond_ok = True
            if hf is not None and hf < TEAM_FORM_MIN_AVG:
                cond_ok = False
            if af is not None and af < TEAM_FORM_MIN_AVG:
                cond_ok = False
            if not cond_ok:
                if DEBUG_LOG:
                    log.debug("Skip: forma bassa (home=%s, away=%s) per %s vs %s",
                              hf, af, home, away)
                continue

        # anti-duplicato (chiave: data+home+away arrotondata al minuto)
        key = f"{normalize_name(league)}|{normalize_name(home)}|{normalize_name(away)}|{minute//1}"
        if key in sent_keys:
            if DEBUG_LOG:
                log.debug("Già notificato: %s", key)
            continue
        sent_keys.add(key)

        msg = format_alarm(row, ev, minute, avg)
        tg_send(msg)
        found += 1
        log.info("Segnale inviato: %s vs %s | %d' | AVG=%.2f", home, away, minute, avg)

    log.info("Opportunità trovate: %d", found)
    log.info("============================================================")

# -------------------------
# Avvio
# -------------------------
if SEND_STARTUP_MSG:
    tg_send("🤖 FootyStats Bot avviato\nMonitoraggio partite in corso…")

while True:
    log.info("============================================================")
    log.info("INIZIO CONTROLLO")
    log.info("============================================================")
    scan_once()
    time.sleep(CHECK_INTERVAL_SECONDS)
