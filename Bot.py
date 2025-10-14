import os
import time
import requests
import csv
from datetime import datetime, timedelta
from io import StringIO
import logging

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configurazioni
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN', '7912248885:AAFwOdg0rX3weVr6NXzW1adcUorvlRY8LyI')
CHAT_ID = os.getenv('CHAT_ID', '6146221712')
RAPIDAPI_KEY = os.getenv('RAPIDAPI_KEY', '686f007a59c2156eb6c1efd2')
GITHUB_CSV_URL = os.getenv('GITHUB_CSV_URL', 'https://raw.githubusercontent.com/S88913/footystats-bot/main/matches_today.csv')
AVG_GOALS_THRESHOLD = 2.50
CHECK_TIME_MINUTES = 50

# Dizionario per tracciare i match già notificati
notified_matches = set()

def send_telegram_message(message):
    """Invia messaggio su Telegram"""
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        data = {
            'chat_id': CHAT_ID,
            'text': message,
            'parse_mode': 'HTML'
        }
        response = requests.post(url, data=data, timeout=10)
        if response.status_code == 200:
            logger.info("✅ Messaggio Telegram inviato con successo")
            return True
        else:
            logger.error(f"❌ Errore invio Telegram: {response.text}")
            return False
    except Exception as e:
        logger.error(f"❌ Errore invio Telegram: {e}")
        return False

def load_csv_from_github():
    """Carica il CSV da GitHub"""
    try:
        logger.info("📥 Scarico CSV da GitHub...")
        response = requests.get(GITHUB_CSV_URL, timeout=30)
        response.raise_for_status()
        
        csv_content = StringIO(response.text)
        reader = csv.DictReader(csv_content)
        matches = list(reader)
        
        logger.info(f"✅ CSV caricato: {len(matches)} partite totali")
        return matches
    except Exception as e:
        logger.error(f"❌ Errore caricamento CSV: {e}")
        return []

def filter_matches_by_avg(matches):
    """Filtra match con AVG Goals >= 2.50"""
    filtered = []
    for match in matches:
        try:
            avg_goals = float(match.get('Average Goals', 0))
            if avg_goals >= AVG_GOALS_THRESHOLD:
                filtered.append(match)
        except (ValueError, TypeError):
            continue
    
    logger.info(f"🎯 Match filtrati con AVG >= {AVG_GOALS_THRESHOLD}: {len(filtered)}")
    return filtered

def get_live_matches():
    """Ottiene i match live da API Bet365"""
    try:
        url = "https://bet36528.p.rapidapi.com/events"
        headers = {
            'x-rapidapi-host': 'bet36528.p.rapidapi.com',
            'x-rapidapi-key': RAPIDAPI_KEY
        }
        
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        data = response.json()
        events = data.get('data', {}).get('events', [])
        
        logger.info(f"📡 API Bet365: {len(events)} match live")
        return events
    except Exception as e:
        logger.error(f"❌ Errore API Bet365: {e}")
        return []

def parse_timestamp(tu_string):
    """Converte timestamp TU in datetime"""
    try:
        # TU format: "20251014154712" = YYYYMMDDHHMMSS
        dt = datetime.strptime(tu_string, "%Y%m%d%H%M%S")
        return dt
    except Exception as e:
        logger.error(f"❌ Errore parsing timestamp {tu_string}: {e}")
        return None

def get_elapsed_minutes(start_time):
    """Calcola minuti trascorsi dall'inizio"""
    try:
        now = datetime.utcnow()
        elapsed = (now - start_time).total_seconds() / 60
        return int(elapsed)
    except Exception as e:
        logger.error(f"❌ Errore calcolo tempo: {e}")
        return 0

def match_teams(csv_match, live_match):
    """Verifica se il match CSV corrisponde al match live"""
    csv_home = csv_match.get('Home Team', '').lower().strip()
    csv_away = csv_match.get('Away Team', '').lower().strip()
    
    live_home = live_match.get('home', '').lower().strip()
    live_away = live_match.get('away', '').lower().strip()
    
    # Match esatto
    if csv_home == live_home and csv_away == live_away:
        return True
    
    # Match parziale (contiene)
    if csv_home in live_home and csv_away in live_away:
        return True
    if live_home in csv_home and live_away in csv_away:
        return True
    
    return False

def check_matches():
    """Controlla i match e invia notifiche"""
    logger.info("=" * 60)
    logger.info("🔍 INIZIO CONTROLLO MATCH")
    logger.info("=" * 60)
    
    # 1. Carica CSV da GitHub
    csv_matches = load_csv_from_github()
    if not csv_matches:
        logger.warning("⚠️ Nessun match nel CSV")
        return
    
    # 2. Filtra per AVG Goals
    filtered_matches = filter_matches_by_avg(csv_matches)
    if not filtered_matches:
        logger.info("ℹ️ Nessun match con AVG >= 2.50 oggi")
        return
    
    # 3. Ottieni match live da API
    live_matches = get_live_matches()
    if not live_matches:
        logger.warning("⚠️ Nessun match live al momento")
        return
    
    # 4. Controlla ogni match filtrato
    opportunities_found = 0
    
    for csv_match in filtered_matches:
        for live_match in live_matches:
            if not match_teams(csv_match, live_match):
                continue
            
            # Match trovato!
            match_id = live_match.get('id', '')
            score = live_match.get('SS', '')
            tu_time = live_match.get('TU', '')
            
            # Salta se già notificato
            if match_id in notified_matches:
                continue
            
            # Parse timestamp
            start_time = parse_timestamp(tu_time)
            if not start_time:
                continue
            
            # Calcola minuti trascorsi
            elapsed = get_elapsed_minutes(start_time)
            
            logger.info(f"⚽ {live_match['home']} vs {live_match['away']}")
            logger.info(f"   Score: {score} | Minuti: {elapsed} | AVG: {csv_match['Average Goals']}")
            
            # Verifica condizioni: >= 50 minuti E 0-0
            if elapsed >= CHECK_TIME_MINUTES and score == "0-0":
                opportunities_found += 1
                
                # Prepara messaggio
                message = f"""🚨 <b>SEGNALE OVER 1.5!</b>

⚽ <b>{live_match['home']} vs {live_match['away']}</b>
🏆 {live_match.get('league', 'N/A')}
📊 AVG Goals: <b>{csv_match['Average Goals']}</b>
⏱️ <b>{elapsed}'</b> - Risultato: <b>{score}</b>
✅ Controlla Bet365 Live!

🎯 <b>Punta Over 1.5 FT</b>"""
                
                # Invia notifica
                if send_telegram_message(message):
                    notified_matches.add(match_id)
                    logger.info(f"✅ OPPORTUNITÀ SEGNALATA: {live_match['home']} vs {live_match['away']}")
    
    logger.info(f"📊 Opportunità trovate: {opportunities_found}")
    logger.info("=" * 60)

def main():
    """Funzione principale con loop continuo"""
    logger.info("🚀 Bot FootyStats avviato!")
    logger.info(f"📊 Soglia AVG Goals: >= {AVG_GOALS_THRESHOLD}")
    logger.info(f"⏱️ Controllo al minuto: {CHECK_TIME_MINUTES}")
    logger.info(f"🔄 Controllo ogni 5 minuti")
    
    # Messaggio di avvio
    send_telegram_message("🤖 <b>Bot FootyStats avviato!</b>\n\nMonitoraggio partite in corso...")
    
    check_interval = 300  # 5 minuti
    
    while True:
        try:
            check_matches()
            logger.info(f"😴 Attendo {check_interval}s prima del prossimo controllo...")
            time.sleep(check_interval)
        except KeyboardInterrupt:
            logger.info("⛔ Bot fermato dall'utente")
            send_telegram_message("⛔ Bot FootyStats arrestato")
            break
        except Exception as e:
            logger.error(f"❌ Errore nel loop principale: {e}")
            time.sleep(60)  # Attendi 1 minuto in caso di errore

if __name__ == "__main__":
    main()
