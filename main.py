import asyncio
import time
import aiohttp
import os
import json
import re
import tempfile
from aiohttp import web
from datetime import datetime
import pytz
from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession
from telethon.tl.types import User, MessageMediaPhoto, MessageMediaDocument
API_ID = int(os.environ.get("TELEGRAM_API_ID", "0"))
API_HASH = os.environ.get("TELEGRAM_API_HASH", "")
N8N_WEBHOOK_URL = os.environ.get("N8N_WEBHOOK_URL", "")
CONTROL_CHAT_ID = int(os.environ.get("CONTROL_CHAT_ID", "-1003808377504"))
# Topic del gruppo di controllo: il flusso normale va in "chat", le cose che
# richiedono un'azione di Jack vanno in "alert" (che lui tiene con le notifiche accese).
TOPIC_CHAT = int(os.environ.get("TOPIC_CHAT", "2"))
TOPIC_ALERT = int(os.environ.get("TOPIC_ALERT", "4"))
# Topic "VIP": Jack inoltra qui il messaggio di un cliente e riceve la bozza di risposta.
TOPIC_VIP = int(os.environ.get("TOPIC_VIP", "200"))

# Bozze in attesa di invio: {message_id_bozza: {chat_id, nome, testo, chat_text, messaggio}}
bozze_vip = {}
SESSION_STRING = os.environ.get("TELEGRAM_SESSION_STRING", "")
TEST_MODE = os.environ.get("TEST_MODE", "false").lower() == "true"
TEST_SENDER_ID = os.environ.get("TEST_SENDER_ID", "")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
TELEGRAM_BOT_TOKEN = "8502735249:AAHkiAgn25Lck0jUXuCiQUDS2oUGJyP9gbo"
# La chiave NON va scritta qui: il repo e' pubblico (serve per gli audio).
# Va impostata come variabile d'ambiente ANTHROPIC_API_KEY su Railway.
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
PORT = int(os.environ.get("FOLLOWUP_SERVER_PORT", "8080"))
DEBOUNCE_TEXT = 60
DEBOUNCE_EXTRA_AUDIO = 15
MAX_HISTORY_MESSAGES = 150
ITALY_TZ = pytz.timezone("Europe/Rome")
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
pending_messages = {}
import random


def debounce_dinamico(testo_totale: str) -> int:
    """
    Attesa prima di rispondere, in base a quanto ha scritto il lead (tutti i messaggi accumulati).
    Un "ok" non merita un minuto di silenzio; un messaggio lungo con domande merita di piu'.
    A questi secondi si sommano ~20-40s di elaborazione n8n: il lead vede la risposta dopo debounce + quella latenza.
    Sempre con una componente casuale: mai lo stesso tempo due volte.
    """
    t = (testo_totale or "").strip()
    n = len(t)
    domande = t.count("?")
    if n <= 80:                      # "ok", "si", una frase
        base = random.randint(20, 30)
    elif n <= 250:                   # un paragrafo
        base = random.randint(40, 60)
    else:                            # un papiro
        base = random.randint(50, 70)
    base += min(15, domande * 5)     # piu' domande, un po' piu' di "tempo per pensare"
    return max(15, min(80, base))    # mai oltre 80s: sopra si aggiungono gia' i 20-40s di n8n


# ultimo istante in cui il lead risultava "sta scrivendo" (da UpdateUserTyping). Serve al debounce:
# se il lead sta ancora digitando dopo i 60s, aspettiamo che smetta prima di rispondere a tutto insieme.
last_typing = {}
TYPING_GRACE = 8          # secondi di silenzio dalla tastiera prima di considerare finito il messaggio
TYPING_MAX_EXTRA = 120    # tetto all'attesa extra, per non restare appesi
pending_tasks = {}

# --- paused_leads persistente su file ---
# Prima viveva solo in RAM: ogni redeploy di Railway azzerava il set e i lead
# in pausa tornavano a essere gestiti dall'agent senza che nessuno se ne accorgesse.
# Cartella dati persistente: il volume Railway (RAILWAY_VOLUME_MOUNT_PATH, es. /data) o DATA_DIR.
# Senza volume si torna alla cartella del codice, come prima (i file spariscono a ogni deploy).
_CODE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR") or os.environ.get("RAILWAY_VOLUME_MOUNT_PATH") or _CODE_DIR
try:
    os.makedirs(DATA_DIR, exist_ok=True)
except Exception as _e:
    print(f"[DATA_DIR] impossibile creare {DATA_DIR}: {_e}, uso la cartella del codice"); DATA_DIR = _CODE_DIR


def _migra_file(nome: str):
    """Se un file di stato esiste ancora accanto al codice e non nel volume, lo copia nel volume (una volta sola)."""
    vecchio = os.path.join(_CODE_DIR, nome); nuovo = os.path.join(DATA_DIR, nome)
    try:
        if DATA_DIR != _CODE_DIR and os.path.exists(vecchio) and not os.path.exists(nuovo):
            import shutil; shutil.copy2(vecchio, nuovo); print(f"[DATA_DIR] migrato {nome} in {DATA_DIR}")
    except Exception as e:
        print(f"[DATA_DIR] migrazione {nome} fallita: {e}")


_migra_file("paused_leads.json"); _migra_file("lead_state.json")
PAUSED_LEADS_FILE = os.path.join(DATA_DIR, "paused_leads.json")


def _carica_paused_leads() -> set:
    try:
        with open(PAUSED_LEADS_FILE, "r") as fh:
            return set(json.load(fh))
    except Exception:
        return set()


def _salva_paused_leads():
    try:
        with open(PAUSED_LEADS_FILE, "w") as fh:
            json.dump(list(paused_leads), fh)
    except Exception as e:
        print(f"[PAUSED_LEADS] Errore salvataggio: {e}")


paused_leads = _carica_paused_leads()

# ═══════════════════════════════════════════════════════════════════════
# BLOCCO E1 — REGISTRAZIONE ACCOMPAGNATA (attivo SOLO per gli id in E1_TEST_IDS, o per tutti con E1_ALL=true)
# ═══════════════════════════════════════════════════════════════════════
E1_ALL = os.environ.get("E1_ALL", "false").lower() == "true"
E1_TEST_IDS = {int(x) for x in os.environ.get("E1_TEST_IDS", "").replace(";", ",").split(",") if x.strip().isdigit()}
STATI_REG = ("link_inviato", "in_registrazione", "registrato", "deposito_dichiarato", "whitelist", "ripensamento")
TEMPLATE_KEYS = ("link_registrazione", "tutorial_registrazione", "tutorial_mt5", "chiusura", "benvenuto")
templates_salvati = {}   # key -> telethon Message dai "Messaggi salvati"


def e1_attivo(chat_id) -> bool:
    try:
        return E1_ALL or int(chat_id) in E1_TEST_IDS
    except Exception:
        return False


def reg_get(chat_id) -> dict:
    return lead_state.setdefault("reg", {}).get(str(chat_id), {})


def reg_set(chat_id, **campi):
    r = lead_state.setdefault("reg", {}).setdefault(str(chat_id), {})
    r.update(campi); r["ts"] = datetime.now(pytz.UTC).isoformat()
    _salva_lead_state()
    print(f"[STATO] {chat_id}: {r.get('stato')} {campi}")
    return r


# --- stato lead persistente (v10) ---
# rientri: {chat_id: timestamp_iso} lead usciti da Perso perche' hanno riscritto.
# Il flusso follow-up n8n lo legge da /folder-status (campo `rientro`) e manda UN solo messaggio.
LEAD_STATE_FILE = os.path.join(DATA_DIR, "lead_state.json")


def _carica_lead_state() -> dict:
    try:
        with open(LEAD_STATE_FILE, "r") as fh:
            d = json.load(fh)
            if not isinstance(d, dict): d = {}
            for k in ("rientri", "cache", "fu", "reg"): d.setdefault(k, {})
            return d
    except Exception:
        return {"rientri": {}, "cache": {}, "fu": {}, "reg": {}}


def _salva_lead_state():
    try:
        with open(LEAD_STATE_FILE, "w") as fh:
            json.dump(lead_state, fh)
    except Exception as e:
        print(f"[LEAD_STATE] Errore salvataggio: {e}")


lead_state = _carica_lead_state()
agent_messages   = {}
folder_lock = asyncio.Lock()  # previene race condition tra chiamate /move-to-folder simultanee

# Traccia i vocali inviati per chat: {chat_id: {message_id: audio_key}}
# Serve perche' i vocali non hanno testo e senza questo non comparirebbero nello storico
# letto dall'agent, che finirebbe per rimandare lo stesso audio due volte.
sent_audios = {}

# Libreria audio — nome chiave (usato nei flag [AUDIO_1]..[AUDIO_3]) -> URL raw GitHub
AUDIO_LIBRARY = {
    "audio1_benvenuto": "https://raw.githubusercontent.com/virtusfxclub-ui/jack-supporto-agent/main/audio/audio1_benvenuto.ogg",
    "audio2_spiegazione": "https://raw.githubusercontent.com/virtusfxclub-ui/jack-supporto-agent/main/audio/audio2_spiegazione.ogg",
    "audio3_scettico": "https://raw.githubusercontent.com/virtusfxclub-ui/jack-supporto-agent/main/audio/audio3_scettico.ogg",
}


async def carica_audio_bytes(audio_key: str):
    """Scarica l'audio dal repo GitHub. Il repo deve essere pubblico."""
    url = AUDIO_LIBRARY.get(audio_key)
    if not url:
        print(f"[AUDIO] Chiave sconosciuta: {audio_key}")
        return None
    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.get(url) as resp:
                if resp.status == 200:
                    return await resp.read()
                if resp.status == 404:
                    print(f"[AUDIO] 404 su {audio_key}: verifica che il repo GitHub sia PUBBLICO "
                          f"e che il file esista in audio/")
                else:
                    print(f"[AUDIO] Download fallito ({resp.status}) per {audio_key}")
    except Exception as e:
        print(f"[AUDIO] Errore download {audio_key}: {e}")
    return None


NIGHT_START = int(os.environ.get("NIGHT_START", "23"))  # ora italiana di inizio notte (inclusa)
NIGHT_END = int(os.environ.get("NIGHT_END", "9"))       # ora italiana di fine notte (esclusa)


def is_night_time():
    """Notte in Italia: da NIGHT_START a NIGHT_END (default 23:00 - 09:00). Modificabile da env senza toccare il codice."""
    h = datetime.now(ITALY_TZ).hour
    if NIGHT_START > NIGHT_END:          # finestra a cavallo di mezzanotte (es. 23 -> 9)
        return h >= NIGHT_START or h < NIGHT_END
    return NIGHT_START <= h < NIGHT_END
def get_night_bridge_message(first_name: str, context_hint: str = "") -> str:
    """Restituisce il messaggio bridge notturno appropriato in base all'ora"""
    name = first_name or ""
    context_lower = context_hint.lower()
    now = datetime.now(ITALY_TZ)
    hour = now.hour
    if any(k in context_lower for k in ['registr', 'link', 'iniziare', 'partire', 'procedo']):
        action = "ti giro il link per iniziare"
    elif any(k in context_lower for k in ['mail', 'referral', 'puprime', 'cambio', 'conto']):
        action = "ti mando la mail da inviare"
    elif any(k in context_lower for k in ['chiamata', 'audio', 'telefono', 'videochiamata']):
        action = "ti scrivo"
    else:
        action = "ti rispondo"
    # NIENTE nome qui: il main conosce solo il nome del profilo Telegram (spesso un
    # nickname o un brand tipo "Trading LM"), non quello dichiarato dal lead in chat.
    if hour >= 23 or hour < 3:
        return f"Guarda, sto andando a letto adesso. Ti scrivo direttamente quando sono in ufficio e {action}, ok?"
    else:
        return f"Guarda, appena sono in ufficio ti scrivo io personalmente e {action}, ok?"
async def get_chat_history(sender_id: int) -> str:
    """Carica la cronologia della chat esistente da Telethon"""
    try:
        me = await client.get_me()
        messages = []
        async for msg in client.iter_messages(sender_id, limit=MAX_HISTORY_MESSAGES):
            if msg.text:
                sender = "Jack" if msg.out else "Lead"
                messages.append(f"{sender}: {msg.text}")
        if not messages:
            return ""
        # Inverti per avere ordine cronologico
        messages.reverse()
        history = " | ".join(messages)
        print(f"[HISTORY] Caricati {len(messages)} messaggi dalla chat")
        return history
    except Exception as e:
        print(f"[HISTORY ERROR] {e}")
        return ""
PROMPT_BOZZA_VIP = """Sei Jack di Virtus FX Club (Giacomo Pozzi) e stai rispondendo in prima persona a un CLIENTE che ha già depositato ed è dentro la community VIP. Non è un lead da convincere: è un cliente da assistere, seguire, e con cui costruire un rapporto che dura.

═══ IL TUO TONO — ESATTAMENTE COSÌ, NON UN TONO GENERICO ═══

Calmo, diretto, sicuro di te senza essere freddo. Frasi brevi, spesso spezzate su più righe invece di un unico blocco. Zero formalismi, zero "gentile cliente". Usi spesso: "Figurati", "Perfetto", "Top", "Ok ottimo", "Ci pensiamo noi", emoji misurate (🫡💪🤝), mai a raffica.

Rispondi SEMPRE in modo concreto e specifico, mai vago. Se sai la risposta tecnica, la dai con precisione (numeri, step, tempi). Se non sai qualcosa o serve verificare, lo dici chiaramente invece di inventare — meglio "fammi verificare" che una risposta a caso.

PATTERN CHIAREZZA POI FERMEZZA: quando il cliente ha dubbi o scetticismo, prima rispondi punto per punto in modo diretto e onesto, senza girarci intorno. POI, se serve, chiudi con una frase ferma che rimette la palla nelle sue mani — mai supplicando. Esempio reale tuo: "Ti dico chiaramente: non sono qui a far perdere tempo a nessuno, ma a costruire una community seria e duratura. L'opportunità c'è, sta a te decidere se coglierla."

═══ REGOLE FERREE — MAI VIOLARLE ═══

- MAI promettere numeri di guadagno specifici o percentuali garantite. Se chiedono rendimento, rispondi che è variabile e dipende dal mercato, dal capitale e dal profilo di rischio.
- MAI dire che esiste un rimborso in caso di perdita del capitale. Se lo chiedono: "No, non esiste un rimborso, stiamo tutti rischiando. La differenza la fa la strategia: puntiamo alla costanza nel lungo periodo, non al colpo grosso."
- MAI proporre un cambio di broker diverso da AXI. Lavoriamo solo con AXI.
- MAI concedere da solo un cambio referral. Se il cliente lo chiede (secondo conto, conto di un parente, ecc.), rispondi che va verificato col team, chiedi la mail di registrazione, e scrivi [ALERT_VERIFICA_REFERRAL] in fondo — NON dire mai "sì si può fare" prima della verifica.
- Se il cliente ha un conto AXI con deposito da chiudere per un secondo conto o problema simile: si va sempre con chiusura e riapertura (48-72 ore), MAI cambio referral. Scrivi [ALERT_CHIUSURA] in fondo se emerge questo caso.
- Se il cliente dice di aver depositato su un NUOVO conto (non quello collegato): rispondi con una conferma breve tipo "Perfetto, controllo subito e ti aggiungo" e scrivi [ALERT_DEPOSITO] in fondo — l'accesso VIP e la verifica del deposito li fa sempre Jack di persona, mai promettere l'accesso da solo.

═══ SE IL CLIENTE HA BRUCIATO IL CAPITALE O È IN FORTE DRAWDOWN ═══

Segui ESATTAMENTE questa sequenza, non saltare passaggi:
1. Valida l'emozione senza giudicare: "Capisco, dispiace anche a me"
2. Se pertinente, normalizza l'errore condividendo che può capitare a chiunque (mai in modo che sembri sminuire la sua perdita)
3. Sposta il discorso sulla gestione emotiva/del rischio, non sui soldi in sé: "La gestione delle emozioni è una delle cose più difficili nel trading, ci sono passato anche io"
4. Se è stato un errore in manuale (lot size, entry sbagliata), proponi con calma di valutare il copy per il futuro, che toglie il fattore umano: "Se vuoi, per il futuro possiamo valutare il copy automatico così togliamo la componente emotiva"
5. Non promettere mai un recupero garantito. Puoi dire che con capitale nuovo si può ripartire, ma sempre con onestà sul rischio

═══ SEQUENZA TECNICA — COLLEGAMENTO AL COPY (segui esattamente questi step se il cliente sta collegando il copy) ═══

1. "Allora per collegarti al copy è molto semplice. Basta condividerci le chiavi solo del conto mt5 standard. E dovremo aprire delle operazioni per superare il pending ma molto semplice, ci pensiamo noi, così il collegamento avviene con successo e replichiamo solo le operazioni del copy. Ma prima di farlo ti faccio mettere una password temporanea al conto ok?"
2. Chiedi uno screenshot della dashboard PRIMA di dire dove cliccare (sicurezza): "Mi fai uno screen della tua dashboard così ti dico esattamente dove cliccare?"
3. Fai scegliere la lot size in base al capitale, dai sempre un consiglio ma lascia decidere: "Che lot size preferisci, [X] o [Y]? Per il tuo capitale consiglierei [X]"
4. AVVISO OBBLIGATORIO prima del collegamento: "Ti sto facendo collegare, vedrai delle operazioni aprirsi non ti preoccupare è normale per il collegamento nel pending. Potresti vedere 1/2 euro di perdita ma che poi rifacciamo con le operazioni reali, serve appunto per la conferma del copy!"
5. Conferma quando è collegato

═══ DATI TECNICI DI RIFERIMENTO (usali se richiesti, sempre con precisione) ═══

- Segnali: 6-7 al giorno in manuale, 1-2 al giorno con il copy (selettivo e conservativo)
- Struttura segnale: 2 TP + 1 operazione "open" (senza TP fisso, solo stop loss) per prendere il parziale, poi le altre due vanno a breakeven
- Winrate medio: superiore all'83% generale, il copy specificamente può arrivare al 90% essendo più selettivo
- Drawdown massimo storico: 15%
- Sessioni: Tokyo, Londra, New York — principalmente Londra e New York (10-17 circa orario italiano)
- Commissione: 10% sui profitti, solo quando il cliente preleva, su base mensile
- MyFxBook: in sviluppo, serve un anno di operatività pubblica prima di pubblicarlo. Nel frattempo: report nel gruppo pubblico + ogni operazione nel VIP in tempo reale
- Bonus AXI: 50% da 200€, 100% da 500€
- L'obiettivo del progetto non è il "colpo grosso", è costruire qualcosa che duri e cresca nel tempo — questa frase o varianti si possono usare quando il cliente parla di aspettative alte o risultati brevi

═══ MESSAGGIO DI BENVENUTO STANDARD (se il cliente sta appena entrando nel VIP) ═══

"Benvenuto in Virtus FX! 🏛
Ti lascio i link per i due canali a cui accedere da ora in poi:
🔗 Community VIP — qui trovi l'operatività quotidiana, gli aggiornamenti, i risultati.
🔗 Virtus Accademy — canale con i tutorial pratici (come collegare il conto, come gestire un'operazione) e contenuti educativi su rischio-rendimento e mercato dell'oro."

E poco dopo, se è un cliente nuovo, questo messaggio di rapporto (usalo una volta, non ripeterlo sempre):
"Ci tengo a dirti una cosa semplice, ma a cui tengo davvero: siamo tanti nella community. Quindi io scrivo a tutti ogni due o tre giorni per sapere come state, come sta andando. Ma se hai bisogno di qualcosa, una domanda, un dubbio, qualsiasi cosa, scrivimi tu direttamente senza aspettare che ti contatti io."

═══ COSA FARE ORA ═══

CLIENTE: {nome_cliente}

ULTIMI MESSAGGI DELLA CHAT:
{chat_text}

MESSAGGIO A CUI RISPONDERE:
{messaggio}
{extra}

Scrivi SOLO il testo della risposta da mandare al cliente, pronto da inviare, nel tono di Jack descritto sopra. Se il contesto richiede uno dei flag (ALERT_VERIFICA_REFERRAL, ALERT_CHIUSURA, ALERT_DEPOSITO), scrivilo in fondo al messaggio. Niente premesse, niente virgolette, niente spiegazioni su cosa hai scritto."""


def ripulisci_flag_bozza(testo: str, nome_cliente: str, chat_id):
    """Toglie gli alert flag dalla bozza VIP e manda la notifica corrispondente, se presente."""
    flags = {
        '[ALERT_VERIFICA_REFERRAL]': ("🔵 VERIFICA CAMBIO REFERRAL", "Ha chiesto un cambio referral: verifica con il manager."),
        '[ALERT_CHIUSURA]': ("🟠 CHIUSURA CONTO AXI", "Serve la procedura di chiusura e riapertura."),
        '[ALERT_DEPOSITO]': ("🟢 DEPOSITO FATTO", "Dice di aver depositato: verifica e dagli l'accesso."),
    }
    for flag, (titolo, desc) in flags.items():
        if flag in testo:
            testo = testo.replace(flag, '').strip()
            _l = link_chat({}, chat_id)
            asyncio.create_task(notify_jack(
                f"{titolo}\n\n👤 {nome_cliente}\n\n{desc}\n\n👉 Apri la chat: {_l}",
                topic="alert"
            ))
    return testo.strip()


async def genera_bozza_vip(nome_cliente: str, chat_text: str, messaggio: str, istruzione: str = "") -> str:
    """Chiede a Claude una bozza di risposta per un cliente VIP, nel tono di Jack."""
    if not ANTHROPIC_API_KEY:
        print("[BOZZA] Manca la variabile d'ambiente ANTHROPIC_API_KEY su Railway")
        return ""
    extra = f"\n\nISTRUZIONE DI JACK PER QUESTA RISCRITTURA: {istruzione}" if istruzione else ""
    prompt = PROMPT_BOZZA_VIP.format(nome_cliente=nome_cliente, chat_text=chat_text, messaggio=messaggio, extra=extra)
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-sonnet-5",
                    "max_tokens": 600,
                    "messages": [{"role": "user", "content": prompt}],
                },
            ) as r:
                d = await r.json()
                return (d.get("content", [{}])[0].get("text") or "").strip()
    except Exception as e:
        print(f"[BOZZA ERROR] {e}")
        return ""


def link_chat(sender_info: dict, sender_id) -> str:
    """Link diretto alla chat: username se disponibile, altrimenti per ID."""
    u = (sender_info or {}).get("username") or ""
    return f"https://t.me/{u}" if u else f"tg://user?id={sender_id}"


def _norm_folder(nome: str) -> str:
    """Normalizza un nome cartella per il confronto: toglie emoji, spazi e maiuscole."""
    if not nome:
        return ""
    return "".join(ch for ch in nome.lower() if ch.isalnum())


async def notify_jack(text: str, topic: str = "alert", buttons=None):
    """
    Manda una notifica nel gruppo di controllo.
    topic="chat"  -> flusso normale (messaggi in arrivo), si puo' silenziare
    topic="alert" -> richiede un'azione di Jack, notifiche accese
    """
    thread_id = TOPIC_CHAT if topic == "chat" else TOPIC_ALERT
    payload = {"chat_id": CONTROL_CHAT_ID, "text": text, "disable_web_page_preview": True}
    if thread_id:
        payload["message_thread_id"] = thread_id
    if buttons:
        payload["reply_markup"] = {"inline_keyboard": buttons}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json=payload
            ) as resp:
                # Telegram risponde 200 con ok:false quando la chat non esiste o il topic
                # e' sbagliato: senza questo controllo l'errore passava inosservato.
                body = await resp.json()
                if not body.get("ok"):
                    print(f"[NOTIFY FAIL] topic={topic} chat_id={CONTROL_CHAT_ID} "
                          f"thread={thread_id} -> {body.get('description')}")
                    # Ritenta senza topic: se il gruppo non ha i Topic attivi
                    # o l'id del thread e' sbagliato, almeno il messaggio arriva.
                    if thread_id:
                        payload.pop("message_thread_id", None)
                        async with session.post(
                            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                            json=payload
                        ) as resp2:
                            body2 = await resp2.json()
                            if not body2.get("ok"):
                                print(f"[NOTIFY FAIL 2] {body2.get('description')}")
    except Exception as e:
        print(f"[NOTIFY ERROR] {e}")
async def transcribe_audio(file_path: str, content_type: str = "audio/ogg", filename: str = "audio.ogg") -> str:
    try:
        with open(file_path, "rb") as audio_file:
            async with aiohttp.ClientSession() as session:
                data = aiohttp.FormData()
                data.add_field("file", audio_file, filename=filename, content_type=content_type)
                data.add_field("model", "whisper-1")
                data.add_field("language", "it")
                async with session.post(
                    "https://api.openai.com/v1/audio/transcriptions",
                    headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
                    data=data,
                    timeout=aiohttp.ClientTimeout(total=60)
                ) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        return result.get("text", "")
                    else:
                        print(f"[WHISPER ERROR] Status: {resp.status}")
                        return ""
    except Exception as e:
        print(f"[WHISPER EXCEPTION] {e}")
        return ""
async def extract_audio_from_video(video_path: str) -> str:
    try:
        audio_path = video_path + "_audio.mp3"
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-i", video_path, "-vn", "-acodec", "mp3", "-y", audio_path,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL
        )
        await proc.wait()
        if proc.returncode == 0 and os.path.exists(audio_path):
            return audio_path
        return ""
    except Exception as e:
        print(f"[FFMPEG ERROR] {e}")
        return ""
def clean_dashes(text: str) -> str:
    """Rimuove tutti i tipi di trattino usati come separatori e li converte in virgola"""
    import re
    # Em dash e en dash con spazi attorno
    text = re.sub(r'\s*—\s*', ', ', text)
    text = re.sub(r'\s*–\s*', ', ', text)
    # Trattino normale con spazi attorno (es. "ciao - come stai")
    text = re.sub(r' - ', ', ', text)
    # Trattino a inizio riga (elenchi puntati tipo "- cosa")
    text = re.sub(r'(?m)^- ', '', text)
    # Pulizia doppia virgola o virgola a inizio frase
    text = re.sub(r',\s*,', ',', text)
    text = re.sub(r'^\s*,\s*', '', text)
    return text.strip()
def normalizza_flag(text: str) -> str:
    """"[AUDIO_ 1]", "[NOTIFICA_\nJACK]", "[AGENT 2]" -> "[AUDIO_1]", "[NOTIFICA_JACK]", "[AGENT2]".
    Lo streaming di n8n spezza i flag; senza questa normalizzazione finiscono scritti al lead."""
    if not text or '[' not in text:
        return text
    def _fix(m):
        compact = re.sub(r'\s+', '', m.group(1)).upper()
        return f'[{compact}]' if re.fullmatch(r'[A-Z0-9_]+', compact) else m.group(0)
    return re.sub(r'\[\s*([A-Za-z0-9_\s]{1,40}?)\s*\]', _fix, text)


_RE_META = [
    re.compile(r'\bstep\s*\d', re.I),
    re.compile(r"^\s*(deve|devo|dovrei|dovrebbe|bisogna|quindi si procede|analisi|ragionamento|nota|note interne|ok,? (il|la) lead)\b", re.I),
    re.compile(r"\b(il lead|la lead|del lead|al lead|l'agent|l’agent|il prompt|la regola|le regole|il flag|il flusso)\b", re.I),
    re.compile(r'\[NOME\]', re.I),
    re.compile(r'\b(ESCALATION|NOTIFICA_?JACK|AGENT_?2|AUDIO_?\d)\b(?![^\[]*\])', re.I),
]


def scarta_meta(text: str) -> str:
    """Toglie i paragrafi 'meta': ragionamenti interni del modello finiti nell'output
    ("Deve rispondere allo step 6: ..."). Seconda rete dopo n8n. Mai svuotare tutto."""
    if not text or '\n' not in text:
        # un solo paragrafo: se e' tutto meta, lo lascio (meglio di un silenzio) ma lo segnalo
        if text and any(r.search(text) for r in _RE_META):
            print(f"[META] paragrafo unico sospetto: {text[:80]}")
        return text
    paragrafi = re.split(r'\n\s*\n', text)
    tenuti = [p for p in paragrafi if not any(r.search(p) for r in _RE_META)]
    if not tenuti:
        return text
    if len(tenuti) != len(paragrafi):
        print(f"[META] scartati {len(paragrafi) - len(tenuti)} paragrafi interni")
    return "\n\n".join(tenuti)


def ricuci_testo(text: str) -> str:
    """
    Ripara gli a capo spuri introdotti da una risposta in streaming.
    n8n puo' spedire la risposta a pezzi: i confini dei pezzi diventano '\n'
    in mezzo alle parole ("segn\nali", "qu\nindi").
    I doppi a capo veri vanno preservati: servono a separare i messaggi.
    """
    if not text or '\n' not in text:
        return text

    # Doppio a capo SPURIO (chunk vuoto dello streaming n8n) che taglia una frase a meta': prima una lettera/virgola,
    # dopo una minuscola o punteggiatura -> e' uno spazio, non un separatore di messaggio. Va fatto PRIMA di proteggere i separatori.
    t = re.sub(r"(?<=[\wàèéìòùÀÈÉÌÒÙ,;:'’])\n[ \t]*\n+(?=[ \t]*[a-zàèéìòù0-9,;:.!?)])", ' ', text)

    # Protegge i doppi a capo VERI (separatori di messaggio) con un segnaposto
    SEP = '\x00SEP\x00'
    t = re.sub(r'\n\s*\n+', SEP, t)
    # A capo singolo tra due caratteri di parola (lettere, cifre, _, apostrofi) = parola spezzata -> ricuci senza spazio
    t = re.sub(r"(?<=[\wàèéìòùÀÈÉÌÒÙ'’])\n(?=[\wàèéìòùÀÈÉÌÒÙ'’])", '', t)
    # A capo davanti a punteggiatura di chiusura / dopo apertura -> via
    t = re.sub(r'\n(?=[,.;:!?)\]»”%€$])', '', t)
    t = re.sub(r'(?<=[(\[«“€$])\n', '', t)

    # Tutti gli altri a capo singoli rimasti diventano spazi
    t = t.replace('\n', ' ')

    # Ripristina i separatori e normalizza gli spazi doppi
    t = t.replace(SEP, '\n\n')
    t = re.sub(r'[ \t]{2,}', ' ', t)
    return t.strip()


async def send_split_messages(chat_id, text):
    text = ricuci_testo(normalizza_flag(text))
    text = clean_dashes(text)
    parts = [p.strip() for p in text.split("\n\n") if p.strip()]
    if not parts:
        return
    # "letto" appena prima di iniziare a scrivere
    try:
        await client.send_read_acknowledge(chat_id)
    except Exception as e:
        print(f"[READ] Impossibile segnare come letto {chat_id}: {e}")
    for i, part in enumerate(parts):
        # indicatore "sta scrivendo": proporzionale alla lunghezza, mai oltre 8s
        typing_time = max(2.0, min(8.0, len(part) / 25.0))
        try:
            async with client.action(chat_id, 'typing'):
                await asyncio.sleep(typing_time)
        except Exception:
            await asyncio.sleep(typing_time)
        await client.send_message(chat_id, part)
        # Traccia questo messaggio come inviato dall'agent
        if chat_id not in agent_messages:
            agent_messages[chat_id] = []
        agent_messages[chat_id].append(part.strip())
        # Mantieni solo gli ultimi 200 messaggi per chat per non occupare troppa memoria
        if len(agent_messages[chat_id]) > 200:
            agent_messages[chat_id] = agent_messages[chat_id][-200:]
        if i < len(parts) - 1:
            # pausa "di lettura" tra un messaggio e l'altro (il typing del prossimo si aggiunge)
            await asyncio.sleep(random.uniform(1.5, 4.0))
async def process_messages(sender_id, sender_info, debounce):
    await asyncio.sleep(debounce)
    # Se il lead sta ancora scrivendo, aspetto che smetta (max TYPING_MAX_EXTRA): cosi' rispondo a tutto in una volta
    # invece di dare due risposte separate a un messaggio spezzato in due.
    extra = 0
    while extra < TYPING_MAX_EXTRA and (time.time() - last_typing.get(sender_id, 0)) < TYPING_GRACE:
        await asyncio.sleep(3); extra += 3
    if extra:
        print(f"[DEBOUNCE] {sender_info.get('full_name')} stava scrivendo: atteso {extra}s in piu'")
    if e1_attivo(sender_id) and reg_get(sender_id).get("stato") == "link_inviato":
        reg_set(sender_id, stato="in_registrazione")
    if sender_id in paused_leads:
        print(f"[PAUSED] {sender_info['full_name']} è in pausa — ignoro")
        pending_messages.pop(sender_id, None)
        pending_tasks.pop(sender_id, None)
        return
    if sender_id not in pending_messages or not pending_messages[sender_id]:
        return
    messages = pending_messages.pop(sender_id, [])
    pending_tasks.pop(sender_id, None)
    combined_text = "\n".join([m["text"] for m in messages if m.get("text")])
    media_type = messages[-1].get("media_type", "text")
    print(f"[MSG IN] {sender_info['full_name']}: {combined_text[:100]}")
    # Carica storico chat Telethon se disponibile
    chat_history = await get_chat_history(sender_id)
    payload = {
        "sender_id": str(sender_id),
        "sender_username": sender_info["username"],
        "sender_full_name": sender_info["full_name"],
        "sender_name": sender_info["first_name"],
        "chat_id": str(sender_id),
        "message_text": combined_text,
        "e1": e1_attivo(sender_id),
        "stato_reg": reg_get(sender_id).get("stato") or "",
        "scelta": reg_get(sender_id).get("scelta") or "",
        "notte": is_night_time(),
        "media_type": media_type,
        "telegram_history": chat_history
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                N8N_WEBHOOK_URL,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=180)
            ) as resp:
                if resp.status == 200:
                    try:
                        reply_text = await resp.text()
                        # Ricuci SUBITO: se la risposta arriva in streaming i flag possono
                        # essere spezzati ("[NOTIFICA_JAC\nK]") e i controlli sotto fallirebbero.
                        reply_text = scarta_meta(ricuci_testo(normalizza_flag(reply_text))).strip()
                    except Exception:
                        reply_text = ""
                    if not reply_text:
                        print(f"[WARN] Nessuna reply ricevuta da n8n")
                        return
                    # Gestione BLOCK — proposta commerciale
                    if reply_text.startswith("[BLOCK]"):
                        paused_leads.add(sender_id)
                        _salva_paused_leads()
                        print(f"[BLOCKED] {sender_info['full_name']} bloccato")
                        return
                    # E1 — INNESCO: Agent 1 chiude con "ti giro il link" + ESCALATION. Se E1 e' attivo per questa chat
                    # e non e' gia' in registrazione, invece della pausa: link + tutorial + "Dimmi pure quando parti!"
                    # Innesco = ESCALATION di Agent 1 quando la chat e' allo step del capitale/registrazione.
                    # NON dipende dalle parole del modello: il bridge lo manda il main con il testo fisso di Jack,
                    # il testo del modello in questo turno viene scartato (cosi' non puo' dire "ti registro io").
                    _rt_l = reply_text.lower()
                    # In E1 ogni ESCALATION di Agent 1/2 su una chat non ancora in registrazione e' l'innesco,
                    # tranne: richiesta di chiamata/vocale, frasi di attesa "verifico", lead perso dopo i 2 tentativi.
                    _innesco = (reply_text.startswith("[PAUSE]") and e1_attivo(sender_id)
                                and reg_get(sender_id).get("stato") not in STATI_REG
                                and not re.search(r'chiamata|videochiamata|vocale|verific|manager|ufficio|domattina|nessun problema|se in futuro|buona giornata|scrivimi', _rt_l))
                    if _innesco:
                        scelta = "copy" if re.search(r'\bcopy\b', chat_history or "", re.I) and not re.search(r'manual', (combined_text or ""), re.I) else reg_get(sender_id).get("scelta") or ""
                        await send_split_messages(sender_id, "Ti giro subito il link per registrarti su AXI e partire. Hai 10 minuti adesso? Ti seguo io passo passo 💪")
                        ok1 = await invia_template(sender_id, "link_registrazione")
                        ok2 = await invia_template(sender_id, "tutorial_registrazione")
                        if ok1:
                            await send_split_messages(sender_id, "Dimmi pure quando parti! 💪")
                            reg_set(sender_id, stato="link_inviato", scelta=scelta, orario_dichiarato="", tocchi=0)
                            asyncio.create_task(notify_jack(
                                f"🔗 LINK INVIATO (E1)\n\n👤 {sender_info['full_name']}\n\nL'agent lo segue nella registrazione. Tu entri a deposito fatto.\n👉 {link_chat(sender_info, sender_id)}",
                                topic="alert"))
                        else:
                            # template mancante: comportamento classico, pausa + escalation
                            paused_leads.add(sender_id); _salva_paused_leads()
                            asyncio.create_task(notify_jack(
                                f"⚫ ESCALATION (E1: template link non trovato nei messaggi salvati)\n\n👤 {sender_info['full_name']}\n👉 {link_chat(sender_info, sender_id)}",
                                topic="alert", buttons=[[{"text": "▶️ Riprendi agent", "callback_data": f"resume:{sender_id}"}]]))
                        return

                    # UN SOLO FLAG: se il modello ne ha scritti due (es. ALERT_CHIUSURA + ESCALATION),
                    # vince l'ALERT specifico: l'escalation generica viene ignorata.
                    if reply_text.startswith("[PAUSE]") and re.search(r'\[\s*ALERT[_\s]*(CHIUSURA|DEPOSITO|VERIFICA[_\s]*REFERRAL|REGISTRATO|RIPENSAMENTO)\s*\]', reply_text, re.I):
                        print(f"[FLAG] due flag nello stesso messaggio: tengo l'ALERT, ignoro ESCALATION")
                        reply_text = reply_text[7:]

                    # Gestione PAUSE — escalation
                    if reply_text.startswith("[PAUSE]"):
                        clean_reply = reply_text[7:].strip()
                        # Se è notte sostituisci con messaggio notturno
                        if is_night_time():
                            clean_reply = get_night_bridge_message(
                                sender_info["first_name"],
                                combined_text
                            )
                        await send_split_messages(sender_id, clean_reply)
                        paused_leads.add(sender_id)
                        _salva_paused_leads()
                        print(f"[PAUSED] {sender_info['full_name']} messo in pausa dopo escalation")
                        # NOTIFICA DAL MAIN, nel punto esatto in cui il lead viene messo in pausa.
                        # Non dipende da n8n: se l'agent si ferma, Jack lo sa. Sempre.
                        try:
                            _l = link_chat(sender_info, sender_id)
                            await notify_jack(
                                f"⚫ ESCALATION - CHIUDI TU\n\n"
                                f"👤 {sender_info['full_name']}\n\n"
                                f"Ultimo messaggio:\n{combined_text[:400]}\n\n"
                                f"Risposta inviata:\n{clean_reply[:600] or '(nessuna)'}\n\n"
                                f"⏸ Agent in pausa su questa chat.\n"
                                f"👉 Apri la chat: {_l}",
                                topic="alert",
                                buttons=[[{"text": "▶️ Riprendi agent", "callback_data": f"resume:{sender_id}"}]]
                            )
                        except Exception as _e:
                            print(f"[NOTIFY ERROR] escalation {sender_id}: {_e}")
                        return
                    # STORICO PDF — DISATTIVATO. Il documento non va mai inviato a nessuno.
                    # Il marker viene comunque ripulito dal testo come rete di sicurezza,
                    # cosi' se il modello lo scrivesse per errore non finisce visibile al lead.
                    if '[STORICO_LEAD]' in reply_text:
                        print(f"[STORICO] Flag ignorato (invio PDF disattivato) — chat {sender_id}")
                    clean_reply = reply_text.replace('[STORICO_LEAD]', '').strip()

                    _link = link_chat(sender_info, sender_id)

                    # ---- E1: documenti richiesti dall'agent ([DOC:chiave]) ----
                    doc_da_inviare = []
                    for m_doc in re.finditer(r'\[\s*DOC\s*:\s*([a-z_]+)\s*\]', clean_reply, re.I):
                        k = m_doc.group(1).lower()
                        if k in TEMPLATE_KEYS and k != "benvenuto" and k not in doc_da_inviare:
                            doc_da_inviare.append(k)
                    clean_reply = re.sub(r'\[\s*DOC\s*:\s*[a-z_]+\s*\]', '', clean_reply, flags=re.I).strip()
                    clean_reply = re.sub(r'\[\s*ORARIO\s*:[^\]]*\]', '', clean_reply, flags=re.I).strip()
                    if not e1_attivo(sender_id):
                        doc_da_inviare = []
                    if "link_registrazione" in doc_da_inviare and reg_get(sender_id).get("stato") == "whitelist":
                        doc_da_inviare.remove("link_registrazione")   # regola dura: mai il link a chi e' in whitelist

                    # ---- E1: registrato (nessuna pausa, l'agent continua ad accompagnare) ----
                    if re.search(r'\[\s*ALERT[_\s]*REGISTRATO\s*\]', clean_reply, re.I):
                        clean_reply = re.sub(r'\[\s*ALERT[_\s]*REGISTRATO\s*\]', '', clean_reply, flags=re.I).strip()
                        reg_set(sender_id, stato="registrato")
                        asyncio.create_task(notify_jack(
                            f"📝 REGISTRATO (E1)\n\n👤 {sender_info['full_name']}\n\nDice di essersi registrato: verifica in dashboard con nome/mail che trovi in chat. L'agent continua a seguirlo fino al deposito.\n👉 {link_chat(sender_info, sender_id)}",
                            topic="alert"))
                    if re.search(r'\[\s*ALERT[_\s]*RIPENSAMENTO\s*\]', clean_reply, re.I):
                        clean_reply = re.sub(r'\[\s*ALERT[_\s]*RIPENSAMENTO\s*\]', '', clean_reply, flags=re.I).strip()
                        reg_set(sender_id, stato="ripensamento")
                        paused_leads.add(sender_id); _salva_paused_leads()
                        asyncio.create_task(notify_jack(
                            f"🤔 RIPENSAMENTO (E1)\n\n👤 {sender_info['full_name']}\n\nDopo il link ha frenato. L'agent ha fatto una domanda leggera e si e' fermato.\n👉 {link_chat(sender_info, sender_id)}",
                            topic="alert", buttons=[[{"text": "▶️ Riprendi agent", "callback_data": f"resume:{sender_id}"}]]))

                    # Alert dedicato: verifica cambio referral su conto AXI mai depositato
                    if '[ALERT_VERIFICA_REFERRAL]' in clean_reply:
                        clean_reply = clean_reply.replace('[ALERT_VERIFICA_REFERRAL]', '').strip()
                        asyncio.create_task(notify_jack(
                            f"🔵 VERIFICA CAMBIO REFERRAL\n\n"
                            f"👤 {sender_info['full_name']}\n\n"
                            f"Ha un conto AXI mai depositato, ha dato la mail di registrazione: "
                            f"verifica con il manager se il cambio referral è possibile.\n\n"
                            f"👉 Apri la chat: {_link}",
                            topic="alert"
                        ))

                    # Alert dedicato: serve la procedura di chiusura conto AXI
                    if '[ALERT_CHIUSURA]' in clean_reply:
                        clean_reply = clean_reply.replace('[ALERT_CHIUSURA]', '').strip()
                        if e1_attivo(sender_id):
                            reg_set(sender_id, stato="whitelist"); paused_leads.add(sender_id); _salva_paused_leads()
                        asyncio.create_task(notify_jack(
                            f"🟠 CHIUSURA CONTO AXI\n\n"
                            f"👤 {sender_info['full_name']}\n\n"
                            f"Ha già un conto AXI con deposito: serve mandargli la mail "
                            f"e il documento con la procedura di chiusura e riapertura.\n\n"
                            f"👉 Apri la chat: {_link}",
                            topic="alert"
                        ))

                    # Alert dedicato: il lead dice di aver depositato -> serve accesso VIP
                    if '[ALERT_DEPOSITO]' in clean_reply:
                        clean_reply = clean_reply.replace('[ALERT_DEPOSITO]', '').strip()
                        if e1_attivo(sender_id):
                            reg_set(sender_id, stato="deposito_dichiarato"); paused_leads.add(sender_id); _salva_paused_leads()
                            if "benvenuto" not in reg_get(sender_id).get("docs", []):
                                doc_da_inviare.append("benvenuto")   # unico caso in cui l'agent manda il link VIP
                        asyncio.create_task(notify_jack(
                            f"🟢 DEPOSITO FATTO\n\n"
                            f"👤 {sender_info['full_name']}\n\n"
                            f"Dice di aver depositato: verifica e dagli l'accesso al VIP.\n\n"
                            f"👉 Apri la chat: {_link}",
                            topic="alert"
                        ))

                    # Gestione AUDIO — Claude decide nel testo quale audio mandare, se serve
                    AUDIO_KEY_MAP = {
                        "AUDIO_1": "audio1_benvenuto",
                        "AUDIO_2": "audio2_spiegazione",
                        "AUDIO_3": "audio3_scettico",
                    }
                    audio_key_to_send = None
                    for key in ["AUDIO_1", "AUDIO_2", "AUDIO_3"]:
                        marker = f"[{key}]"
                        if marker in clean_reply and audio_key_to_send is None:
                            # Manda solo il primo audio trovato (un solo audio per messaggio)
                            audio_key_to_send = AUDIO_KEY_MAP[key]
                        # Rimuove SEMPRE il marker dal testo visibile, anche se non e' il primo,
                        # cosi' non resta mai scritto un flag letterale tipo "[AUDIO_2]" nel messaggio
                        clean_reply = clean_reply.replace(marker, "").strip()

                    # RETE DI SICUREZZA: se un flag e' sopravvissuto alla pulizia di n8n
                    # (streaming che lo spezza, o il modello che scrive una variante tipo
                    # "[AGENT 2]" con spazio invece di "[AGENT2]"), lo intercettiamo qui.
                    # Regex tollerante a spazi/underscore/maiuscole: un match esatto su
                    # stringa lascerebbe passare qualsiasi piccola variazione.
                    FLAG_PATTERNS = {
                        'NOTIFICA_JACK': re.compile(r'\[\s*NOTIFICA[_\s]*JACK\s*\]', re.I),
                        'ESCALATION': re.compile(r'\[\s*ESCALATION\s*\]', re.I),
                        'AGENT2': re.compile(r'\[\s*AGENT[_\s]*2\s*\]', re.I),
                        'STORICO_LEAD': re.compile(r'\[\s*STORICO[_\s]*LEAD\s*\]', re.I),
                        'ALERT_CHIUSURA': re.compile(r'\[\s*ALERT[_\s]*CHIUSURA\s*\]', re.I),
                        'ALERT_DEPOSITO': re.compile(r'\[\s*ALERT[_\s]*DEPOSITO\s*\]', re.I),
                        'ALERT_VERIFICA_REFERRAL': re.compile(r'\[\s*ALERT[_\s]*VERIFICA[_\s]*REFERRAL\s*\]', re.I),
                        'AUDIO_1': re.compile(r'\[\s*AUDIO[_\s]*1\s*\]', re.I),
                        'AUDIO_2': re.compile(r'\[\s*AUDIO[_\s]*2\s*\]', re.I),
                        'AUDIO_3': re.compile(r'\[\s*AUDIO[_\s]*3\s*\]', re.I),
                    }
                    flag_trovati = [nome for nome, pat in FLAG_PATTERNS.items() if pat.search(clean_reply)]
                    if flag_trovati:
                        for nome in flag_trovati:
                            clean_reply = FLAG_PATTERNS[nome].sub('', clean_reply)
                        clean_reply = re.sub(r'\n{3,}', '\n\n', clean_reply).strip()
                        print(f"[FLAG RESIDUI] Ripuliti {flag_trovati} dalla risposta a {sender_id}")

                        # Se il flag chiedeva l'intervento di Jack, la notifica va mandata comunque
                        if 'NOTIFICA_JACK' in flag_trovati or 'ESCALATION' in flag_trovati:
                            _l = link_chat(sender_info, sender_id)
                            asyncio.create_task(notify_jack(
                                f"⚫ SERVE IL TUO INTERVENTO\n\n"
                                f"👤 {sender_info['full_name']}\n\n"
                                f"L'agent ha chiesto di passare a te.\n\n"
                                f"👉 Apri la chat: {_l}",
                                topic="alert",
                                buttons=[[{"text": "▶️ Riprendi agent", "callback_data": f"resume:{sender_id}"}]]
                            ))

                    if not clean_reply.strip() and not audio_key_to_send and not doc_da_inviare:
                        print(f"[MSG OUT] Nessun testo da inviare a {sender_id}")
                    elif clean_reply.strip():
                        await send_split_messages(sender_id, clean_reply)
                        print(f"[MSG OUT] → {sender_info['full_name']}: {clean_reply[:80]}")

                    # E1: documenti dai messaggi salvati, dopo il testo
                    for k in doc_da_inviare:
                        await asyncio.sleep(random.uniform(1.5, 3.0))
                        ok = await invia_template(sender_id, k)
                        if ok and k == "link_registrazione" and reg_get(sender_id).get("stato") not in STATI_REG:
                            reg_set(sender_id, stato="link_inviato", tocchi=0)
                    # E1: orario dichiarato dal lead (l'agent lo scrive come [ORARIO: ...])
                    m_or = re.search(r'\[\s*ORARIO\s*:\s*([^\]]{2,40})\]', reply_text, re.I)
                    if m_or and e1_attivo(sender_id):
                        reg_set(sender_id, orario_dichiarato=m_or.group(1).strip())

                    if audio_key_to_send:
                        try:
                            audio_bytes = await carica_audio_bytes(audio_key_to_send)
                            if not audio_bytes:
                                print(f"[AUDIO ERROR] {audio_key_to_send} non disponibile, messaggio inviato senza vocale")
                            else:
                                # Stima durata audio dai byte (Opus ~64kbps = ~8000 byte/sec)
                                # e calcola un delay di "registrazione" proporzionale ma contenuto.
                                est_seconds = len(audio_bytes) / 8000.0
                                # delay tra 3 e 8 secondi: circa un terzo della durata stimata, con tetti
                                record_delay = max(3.0, min(8.0, est_seconds * 0.33))

                                # Mostra "sta registrando un messaggio vocale..." durante l'attesa
                                try:
                                    async with client.action(sender_id, 'record-audio'):
                                        await asyncio.sleep(record_delay)
                                except Exception:
                                    # Se l'indicatore non è disponibile, aspetta comunque
                                    await asyncio.sleep(record_delay)

                                with tempfile.NamedTemporaryFile(suffix=".ogg", delete=False) as tmp_file:
                                    tmp_file.write(audio_bytes)
                                    tmp_path = tmp_file.name
                                try:
                                    sent_msg = await client.send_file(sender_id, tmp_path, voice_note=True)
                                    # registra quale audio e' stato mandato, per renderlo visibile nello storico
                                    try:
                                        if sent_msg is not None:
                                            sent_audios.setdefault(sender_id, {})[sent_msg.id] = audio_key_to_send
                                    except Exception:
                                        pass
                                    print(f"[AUDIO] {audio_key_to_send} inviato a {sender_info['full_name']} (delay {record_delay:.1f}s)")
                                finally:
                                    os.remove(tmp_path)
                        except Exception as e:
                            print(f"[AUDIO ERROR] {e}")

                else:
                    print(f"[ERROR] n8n status: {resp.status}")
    except Exception as e:
        print(f"[EXCEPTION] {e}")
@client.on(events.UserUpdate)
async def on_user_typing(event):
    """Traccia lo stato 'sta scrivendo' dei lead (chat private) per il debounce."""
    try:
        if event.typing and getattr(event, 'user_id', None):
            last_typing[event.user_id] = time.time()
    except Exception:
        pass


@client.on(events.NewMessage(incoming=True))
async def handle_incoming(event):
    try:
        if not event.is_private:
            return
        sender = await event.get_sender()
        if not isinstance(sender, User):
            return
        if sender.bot:
            return
        me = await client.get_me()
        if sender.id == me.id:
            return
        first_name = sender.first_name or ""
        last_name = sender.last_name or ""
        full_name = f"{first_name} {last_name}".strip()
        sender_username = sender.username or ""
        sender_id = sender.id
        if TEST_MODE:
            allowed_ids = [x.strip() for x in TEST_SENDER_ID.split(",")]
            if str(sender_id) not in allowed_ids:
                print(f"[TEST MODE] Ignoro {full_name} — non è il tester")
                return
            print(f"[TEST MODE] Messaggio da tester: {full_name}")
        if "VIP" in full_name.upper():
            # I clienti VIP li gestisce Jack a mano: l'agent non risponde.
            # NIENTE notifica di gruppo qui: Jack riceve gia' la notifica nativa
            # di Telegram sulla chat privata (l'agent non segna il messaggio come
            # letto, quindi la notifica non viene soppressa). Un duplicato nel
            # gruppo era solo rumore. Se in futuro l'agent rispondesse ai VIP,
            # qui e' il punto giusto per reintrodurre l'alert.
            print(f"[SKIP VIP] {full_name} — non rispondo, nessuna notifica di gruppo")
            return
        if sender_id in paused_leads:
            print(f"[PAUSED] {full_name} ha scritto ma è in pausa")
            _link = f"https://t.me/{sender_username}" if sender_username else f"tg://user?id={sender_id}"
            await notify_jack(
                f"⏸ LEAD IN PAUSA HA SCRITTO\n\n"
                f"👤 {full_name}\n"
                f"💬 {event.message.message or '[media]'}\n\n"
                f"👉 Apri la chat: {_link}",
                topic="alert",
                buttons=[[{"text": "▶️ Riprendi agent", "callback_data": f"resume:{sender_id}"}]]
            )
            return
        message_text = event.message.message or ""
        media_type = "text"
        debounce = DEBOUNCE_TEXT

        # NOTA: il "letto" NON si segna qui. Segnarlo subito e rispondere dopo un minuto e' il segnale
        # "online, ha letto, non risponde" che raffredda il lead. Si segna in send_split_messages,
        # un istante prima di "sta scrivendo": il lead vede letto -> sta scrivendo -> risposta, tutto insieme.

        # Notifica sul topic chat SOLO per il primo messaggio di un lead nuovo:
        # vedere ogni singola riga di ogni conversazione era pura saturazione.
        # Finche' e' l'agent a gestire lo scambio non serve seguirlo messaggio
        # per messaggio; quello che conta arriva comunque nel topic alert.
        try:
            e_gia_in_storico = sender_id in agent_messages and len(agent_messages.get(sender_id, [])) > 0
            if not e_gia_in_storico:
                anteprima = message_text.strip() if message_text.strip() else "[media]"
                if len(anteprima) > 200:
                    anteprima = anteprima[:200] + "..."
                _link = link_chat({"username": sender_username}, sender_id)
                asyncio.create_task(notify_jack(
                    f"🆕 NUOVO LEAD\n\n"
                    f"👤 {full_name}\n"
                    f"💬 {anteprima}\n\n"
                    f"👉 Apri la chat: {_link}",
                    topic="chat"
                ))
        except Exception as e:
            print(f"[NOTIFY] Errore notifica nuovo messaggio: {e}")
        if event.message.media:
            if isinstance(event.message.media, MessageMediaPhoto) and e1_attivo(sender_id) and reg_get(sender_id).get("stato") in STATI_REG:
                # E1: lo screenshot viene descritto e passato all'agent come testo. L'agent risponde solo ai casi noti.
                desc = await descrivi_screenshot(sender_id, event.message)
                if not desc:
                    desc = "caso 9: screenshot non leggibile"
                message_text = f"[SCREENSHOT: {desc}]" + (f" Didascalia: {message_text}" if message_text else "")
                media_type = "screenshot"   # debounce corto: chi manda uno screen aspetta una risposta, non un minuto
                if reg_get(sender_id).get("stato") == "link_inviato":
                    reg_set(sender_id, stato="in_registrazione")
            elif isinstance(event.message.media, MessageMediaPhoto):
                print(f"[IMAGE] Immagine da {full_name} — metto in pausa e notifico Jack")
                paused_leads.add(sender_id)
                caption = f" — didascalia: \"{message_text}\"" if message_text else ""
                _link = f"https://t.me/{sender_username}" if sender_username else f"tg://user?id={sender_id}"
                await notify_jack(
                    f"🖼 IMMAGINE RICEVUTA{caption}\n\n"
                    f"👤 {full_name}\n\n"
                    f"Vai nella chat e rispondi tu direttamente.\n\n"
                    f"👉 Apri la chat: {_link}",
                    topic="alert",
                    buttons=[[{"text": "▶️ Riprendi agent", "callback_data": f"resume:{sender_id}"}]]
                )
                return
            elif isinstance(event.message.media, MessageMediaDocument):
                doc = event.message.media.document
                mime = doc.mime_type if hasattr(doc, "mime_type") else ""
                if "audio" in mime or "ogg" in mime or "voice" in mime:
                    media_type = "audio"
                    audio_duration = 0
                    try:
                        for attr in doc.attributes:
                            if hasattr(attr, "duration"):
                                audio_duration = int(attr.duration)
                                break
                    except Exception:
                        audio_duration = 30
                    debounce = audio_duration + DEBOUNCE_EXTRA_AUDIO
                    print(f"[AUDIO] Durata: {audio_duration}s — trascrivo con Whisper...")
                    if OPENAI_API_KEY:
                        try:
                            with tempfile.NamedTemporaryFile(suffix=".ogg", delete=False) as tmp:
                                tmp_path = tmp.name
                            await client.download_media(event.message, file=tmp_path)
                            transcription = await transcribe_audio(tmp_path)
                            os.unlink(tmp_path)
                            if transcription:
                                message_text = f"[MESSAGGIO VOCALE TRASCRITTO]: {transcription}"
                                print(f"[AUDIO] Trascrizione: {transcription[:100]}")
                            else:
                                message_text = "[Messaggio vocale non trascritto — chiedi di ripetere per iscritto]"
                        except Exception as e:
                            print(f"[AUDIO ERROR] {e}")
                            message_text = "[Messaggio vocale — chiedi di ripetere per iscritto]"
                    else:
                        message_text = "[Messaggio vocale — chiedi di ripetere per iscritto]"
                elif "video" in mime or mime == "video/mp4":
                    media_type = "video"
                    video_duration = 0
                    try:
                        for attr in doc.attributes:
                            if hasattr(attr, "duration"):
                                video_duration = int(attr.duration)
                                break
                    except Exception:
                        video_duration = 30
                    debounce = video_duration + DEBOUNCE_EXTRA_AUDIO
                    print(f"[VIDEO] Durata: {video_duration}s — estraggo audio e trascrivo...")
                    if OPENAI_API_KEY:
                        try:
                            with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp:
                                tmp_path = tmp.name
                            await client.download_media(event.message, file=tmp_path)
                            audio_path = await extract_audio_from_video(tmp_path)
                            os.unlink(tmp_path)
                            if audio_path:
                                transcription = await transcribe_audio(audio_path, "audio/mpeg", "audio.mp3")
                                os.unlink(audio_path)
                                if transcription:
                                    message_text = f"[VIDEO MESSAGGIO TRASCRITTO]: {transcription}"
                                    print(f"[VIDEO] Trascrizione: {transcription[:100]}")
                                else:
                                    message_text = "[Video messaggio non trascritto — chiedi di ripetere per iscritto]"
                            else:
                                message_text = "[Video messaggio — chiedi di ripetere per iscritto]"
                        except Exception as e:
                            print(f"[VIDEO ERROR] {e}")
                            message_text = "[Video messaggio — chiedi di ripetere per iscritto]"
                    else:
                        message_text = "[Video messaggio — chiedi di ripetere per iscritto]"
                else:
                    media_type = "documento"
                    message_text = "[L'utente ha inviato un file]"
                    debounce = DEBOUNCE_TEXT
        if not message_text.strip():
            return
        if sender_id not in pending_messages:
            pending_messages[sender_id] = []
        pending_messages[sender_id].append({
            "text": message_text,
            "media_type": media_type
        })
        if media_type == "screenshot":
            debounce = random.randint(6, 10)
        elif media_type == "text":
            # tutti i messaggi accumulati finora: piu' scrive, piu' aspettiamo (entro i limiti)
            testo_totale = " ".join(m["text"] for m in pending_messages[sender_id] if m.get("media_type") == "text")
            debounce = debounce_dinamico(testo_totale)
        sender_info = {
            "full_name": full_name,
            "username": sender_username,
            "first_name": first_name,
            "media_type": media_type
        }
        if sender_id in pending_tasks and not pending_tasks[sender_id].done():
            pending_tasks[sender_id].cancel()
        pending_tasks[sender_id] = asyncio.create_task(
            process_messages(sender_id, sender_info, debounce)
        )
        print(f"[DEBOUNCE] {full_name} — tipo: {media_type} — attendo {debounce}s")
    except Exception as e:
        print(f"[EXCEPTION] {e}")
def _topic_id(event):
    """Restituisce l'id del topic in cui e' stato scritto il messaggio (None se nessuno)."""
    r = getattr(event.message, 'reply_to', None)
    if not r:
        return None
    return getattr(r, 'reply_to_top_id', None) or getattr(r, 'reply_to_msg_id', None)


@client.on(events.NewMessage(chats=CONTROL_CHAT_ID))
async def handle_topic_vip(event):
    """
    Topic VIP: Jack inoltra qui il messaggio di un cliente e riceve la bozza di risposta.
    Rispondendo alla bozza con un'istruzione ("piu' corto", "digli che...") la rigenera.
    """
    if _topic_id(event) != TOPIC_VIP:
        return

    msg = event.message

    # --- Caso 1: e' una risposta a una bozza -> rigenera con l'istruzione ---
    reply_id = getattr(getattr(msg, 'reply_to', None), 'reply_to_msg_id', None)
    if reply_id and reply_id in bozze_vip and not msg.forward:
        istruzione = (msg.message or "").strip()
        if not istruzione:
            return
        b = bozze_vip[reply_id]
        await event.reply("✏️ Riscrivo...")
        nuova = await genera_bozza_vip(b["nome"], b["chat_text"], b["messaggio"], istruzione)
        if not nuova:
            await event.reply("❌ Non sono riuscito a rigenerare la bozza.")
            return
        nuova = ripulisci_flag_bozza(nuova, b["nome"], b["chat_id"])
        sent = await client.send_message(
            CONTROL_CHAT_ID, nuova,
            reply_to=TOPIC_VIP,
            buttons=[[Button.inline("✅ Invia al cliente", f"vipsend:{b['chat_id']}".encode())]]
        )
        bozze_vip[sent.id] = {**b, "testo": nuova}
        return

    # --- Caso 2: messaggio inoltrato da un cliente -> genera la bozza ---
    if not msg.forward:
        return

    fwd = msg.forward
    cliente_id = getattr(getattr(fwd, 'sender_id', None), 'user_id', None) or getattr(fwd, 'sender_id', None)
    nome_cliente = (getattr(fwd, 'from_name', None) or "").strip()

    if not cliente_id:
        # Privacy attiva sui forward: senza id non sappiamo a chi rispondere
        await event.reply(
            "⚠️ Non riesco a risalire al mittente (ha la privacy attiva sui messaggi inoltrati).\n"
            "Scrivimi l'ID o l'username del cliente e ti preparo la bozza."
        )
        return

    testo_cliente = (msg.message or "").strip()
    if not testo_cliente:
        await event.reply("⚠️ Il messaggio inoltrato non contiene testo.")
        return

    try:
        ent = await client.get_entity(int(cliente_id))
        nome_cliente = f"{getattr(ent, 'first_name', '') or ''} {getattr(ent, 'last_name', '') or ''}".strip() or nome_cliente
    except Exception:
        pass

    await event.reply("✍️ Preparo la risposta...")

    # Ultimi messaggi della chat col cliente, per dare contesto
    chat_text = ""
    try:
        righe = []
        async for m in client.iter_messages(int(cliente_id), limit=12):
            if m.text:
                chi = "Jack" if m.out else (nome_cliente or "Cliente")
                righe.append(f"{chi}: {m.text}")
        chat_text = "\n".join(reversed(righe))
    except Exception as e:
        print(f"[VIP] Storico non leggibile per {cliente_id}: {e}")

    bozza = await genera_bozza_vip(nome_cliente or "Cliente", chat_text, testo_cliente)
    if not bozza:
        await event.reply("❌ Non sono riuscito a generare la bozza.")
        return
    bozza = ripulisci_flag_bozza(bozza, nome_cliente or "Cliente", int(cliente_id))

    sent = await client.send_message(
        CONTROL_CHAT_ID, bozza,
        reply_to=TOPIC_VIP,
        buttons=[[Button.inline("✅ Invia al cliente", f"vipsend:{cliente_id}".encode())]]
    )
    bozze_vip[sent.id] = {
        "chat_id": int(cliente_id),
        "nome": nome_cliente or "Cliente",
        "testo": bozza,
        "chat_text": chat_text,
        "messaggio": testo_cliente,
    }
    print(f"[VIP] Bozza pronta per {nome_cliente} ({cliente_id})")


@client.on(events.CallbackQuery(pattern=b"vipsend:"))
async def handle_vip_send(event):
    """Bottone 'Invia al cliente': manda la bozza come @jacksupporto."""
    try:
        cliente_id = int(event.data.decode().split(":")[1])
        msg_id = event.message_id
        b = bozze_vip.get(msg_id)
        if not b:
            await event.answer("Bozza non trovata (server riavviato). Copia e incolla a mano.", alert=True)
            return
        await client.send_message(cliente_id, b["testo"])
        await event.edit(f"✅ INVIATO a {b['nome']}\n\n{b['testo']}")
        await event.answer("Inviato")
        print(f"[VIP] Risposta inviata a {b['nome']} ({cliente_id})")
    except Exception as e:
        print(f"[VIP SEND ERROR] {e}")
        await event.answer("Errore nell'invio", alert=True)


@client.on(events.NewMessage(chats=CONTROL_CHAT_ID))
async def handle_control(event):
    if _topic_id(event) == TOPIC_VIP:
        return
    text = event.message.message or ""
    if text.lower().startswith("riprendi"):
        parts = text.split()
        if len(parts) >= 2:
            try:
                sender_id = int(parts[1])
                if sender_id in paused_leads:
                    paused_leads.discard(sender_id)
                    _salva_paused_leads()
                    await event.reply(f"✅ Agent riattivato per ID {sender_id} — risponderà al prossimo messaggio")
                    print(f"[RESUME] Lead {sender_id} riattivato")
                else:
                    await event.reply(f"Il lead {sender_id} non era in pausa")
            except ValueError:
                await event.reply("Formato: riprendi [sender_id]")
    elif text.startswith("/stato"):
        me = await client.get_me()
        paused_list = ", ".join(str(x) for x in paused_leads) if paused_leads else "nessuno"
        night = "sì" if is_night_time() else "no"
        await event.reply(
            f"🤖 Jack Agent attivo\n"
            f"📱 @{me.username}\n"
            f"🔧 Test mode: {TEST_MODE}\n"
            f"🎤 Whisper: {'attivo' if OPENAI_API_KEY else 'non configurato'}\n"
            f"🌙 Modalità notte: {night}\n"
            f"⏸ Lead in pausa: {paused_list}\n"
            f"✅ Tutto operativo"
        )
PROMPT_FOLLOWUP = """Sei Jack (Giacomo Pozzi) di Virtus FX Club. Stai scrivendo su Telegram a un lead che non risponde da {ore} ore.
Scrivi UN messaggio di follow-up, massimo 2 righe, che riprende ESATTAMENTE dal punto in cui la conversazione si era fermata e chiude con una domanda concreta che fa avanzare.

NUMERO DEL FOLLOW-UP: {n} di 3
- 1: leggero, un promemoria naturale sul punto lasciato aperto
- 2: riprendi il punto e proponi il passo concreto (se aveva detto un capitale: "ce li hai i [cifra]? ti giro il link")
- 3: domanda secca e diretta: c'e' qualcosa che non ti ha convinto?
LEAD CALDO: {caldo}. Se caldo (aveva detto il capitale o che si registrava), vai dritto: chiedi se procede e proponi di mandargli il link.

REGOLE FERREE:
- Usa il nome SOLO se il lead lo ha dichiarato lui nella chat (mai il nome del profilo Telegram). Se non c'e', nessun nome.
- Niente numeri di rendimento, pips, percentuali, promesse di guadagno. Niente link. Niente riferimenti a broker diversi da AXI.
- VIETATO: "fammi sapere", "dimmi pure", "resto a disposizione", "quando vuoi", "senza fretta", "in bocca al lupo".
- Non ripetere una frase gia' scritta nella chat. Non ricominciare da capo, non ripresentarti.
- Tono: calmo, diretto, umano, come un messaggio scritto al volo. Zero elenchi, zero markdown, al massimo un'emoji.
- Se nello storico il lead ha detto "non mi interessa" in modo netto, scrivi solo: SKIP

ULTIMI MESSAGGI DELLA CHAT:
{chat}

Rispondi SOLO con il testo del messaggio (o SKIP). Niente virgolette, niente spiegazioni."""


async def genera_followup(chat_id: int, n: int, caldo: bool, ore: float) -> str:
    """Genera il testo del follow-up dallo storico reale della chat. Ritorna "" se non va inviato."""
    if not ANTHROPIC_API_KEY:
        return ""
    _, msgs = await read_chat_messages(chat_id, hours=720, limit=40)
    if not msgs:
        return ""
    chat_text = "\n".join(f"[{m['time']}] {m['sender']}: {m['text']}" for m in msgs[-16:])
    prompt = PROMPT_FOLLOWUP.format(ore=int(ore), n=n, caldo="SI" if caldo else "NO", chat=chat_text)
    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.post(
                "https://api.anthropic.com/v1/messages",
                headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                json={"model": "claude-sonnet-5", "max_tokens": 200, "messages": [{"role": "user", "content": prompt}]},
                timeout=aiohttp.ClientTimeout(total=60),
            ) as r:
                d = await r.json()
                out = (d.get("content", [{}])[0].get("text") or "").strip().strip('"')
    except Exception as e:
        print(f"[FOLLOWUP] generazione fallita: {e}")
        return ""
    if not out or out.upper().startswith("SKIP"):
        return ""
    out = ricuci_testo(normalizza_flag(out))
    out = re.sub(r'\[\s*[A-Z0-9_]+\s*\]', '', out).strip()   # nessun flag nei follow-up
    return out[:500]


async def handle_send_followup(request: web.Request) -> web.Response:
    """
    POST /send-followup
    Body A (testo pronto): {"chat_id": "123", "message": "..."}
    Body B (generato dallo storico): {"chat_id": "123", "fu_n": 1|2|3, "caldo": true|false, "ore": 30}
    In entrambi i casi registra il follow-up in lead_state["fu"] (usato da /folder-status e dalla riconciliazione).
    """
    try:
        body = await request.json()
        chat_id = body.get("chat_id") or body.get("chatId")
        message = body.get("message") or body.get("followupMsg") or body.get("text")
        fu_n = int(body.get("fu_n") or 0)
        if not chat_id:
            return web.json_response({"ok": False, "error": "chat_id obbligatorio"}, status=400)
        chat_id = int(chat_id)
        if not message and fu_n:
            message = await genera_followup(chat_id, fu_n, bool(body.get("caldo")), float(body.get("ore") or 24))
            if not message:
                return web.json_response({"ok": True, "sent": False, "reason": "skip"})
        if not message:
            return web.json_response({"ok": False, "error": "message o fu_n obbligatori"}, status=400)
        print(f"[FOLLOWUP] Invio FU{fu_n or '?'} a {chat_id}: {message[:80]}")
        await send_split_messages(chat_id, message)
        prev = lead_state.setdefault("fu", {}).get(str(chat_id), {})
        lead_state["fu"][str(chat_id)] = {"n": fu_n or (prev.get("n", 0) + 1), "ts": datetime.now(pytz.UTC).isoformat(), "testo": message}
        _salva_lead_state()
        print(f"[FOLLOWUP] Inviato a {chat_id}")
        return web.json_response({"ok": True, "sent": True, "message": message})
    except Exception as e:
        print(f"[HTTP ERROR] {e}")
        return web.json_response({"ok": False, "error": str(e)}, status=500)

PALLINI = ["\U0001F7E1", "\U0001F7E2", "\U0001F7E0", "\U0001F534"]  # giallo, verde, arancione, rosso

async def handle_resume_lead(request: web.Request) -> web.Response:
    """
    POST /resume-lead
    Body: {"chat_id": "123"}
    Riattiva l'agent su un lead in pausa. Chiamato dal bottone "Riprendi agent".
    """
    try:
        data = await request.json()
        chat_id = int(data.get("chat_id"))
        era_in_pausa = chat_id in paused_leads
        paused_leads.discard(chat_id)
        _salva_paused_leads()
        print(f"[RESUME] Lead {chat_id} riattivato (era in pausa: {era_in_pausa})")

        # Notifica visibile nel topic alert: il popup di Telegram sul bottone
        # e' troppo discreto e facile da perdere, questa resta come messaggio.
        try:
            nome = ""
            try:
                ent = await client.get_entity(chat_id)
                nome = f"{getattr(ent, 'first_name', '') or ''} {getattr(ent, 'last_name', '') or ''}".strip()
            except Exception:
                pass
            _l = f"tg://user?id={chat_id}"
            if era_in_pausa:
                testo = f"🟢 ATTIVO\n\n{nome or chat_id} — agent riattivato, risponderà al prossimo messaggio.\n\n👉 Apri la chat: {_l}"
            else:
                testo = f"ℹ️ {nome or chat_id} non risultava in pausa (probabile riavvio del sistema nel frattempo). L'agent è già attivo su questo lead."
            asyncio.create_task(notify_jack(testo, topic="alert"))
        except Exception as e:
            print(f"[RESUME NOTIFY ERROR] {e}")

        return web.json_response({"ok": True, "era_in_pausa": era_in_pausa})
    except Exception as e:
        print(f"[RESUME ERROR] {e}")
        return web.json_response({"ok": False, "error": str(e)}, status=500)


async def handle_rename_contact(request: web.Request) -> web.Response:
    """
    POST /rename-contact
    Body: {"chat_id": "123", "pallino": "🟠"}
    Cambia SOLO il pallino nel nome del contatto, lasciando intatto il resto.
    "Giorgia D. VIP 🟡" -> "Giorgia D. VIP 🟠"
    Se il contatto non ha pallino, lo aggiunge in coda.
    Passare pallino vuoto rimuove il pallino senza aggiungerne uno.
    """
    try:
        from telethon.tl.functions.contacts import AddContactRequest

        data = await request.json()
        chat_id = int(data.get("chat_id"))
        pallino = (data.get("pallino") or "").strip()

        if pallino and pallino not in PALLINI:
            return web.json_response(
                {"ok": False, "error": f"Pallino non valido. Ammessi: {PALLINI}"}, status=400)

        entity = await client.get_entity(chat_id)
        first = getattr(entity, 'first_name', '') or ''
        last = getattr(entity, 'last_name', '') or ''

        # Rimuove eventuali pallini esistenti, preservando tutto il resto del nome
        def pulisci(s):
            for p in PALLINI:
                s = s.replace(p, '')
            return ' '.join(s.split())

        nuovo_first = pulisci(first)
        nuovo_last = pulisci(last)

        # Il pallino va messo PRIMA della parola "VIP" se c'e' (cosi' resta ben visibile
        # anche quando il nome e' lungo), altrimenti in coda.
        if pallino:
            import re
            pattern = re.compile(r'\bvip\b', re.IGNORECASE)

            if pattern.search(nuovo_last):
                nuovo_last = pattern.sub(lambda m: f"{pallino} {m.group(0)}", nuovo_last, count=1)
            elif pattern.search(nuovo_first):
                nuovo_first = pattern.sub(lambda m: f"{pallino} {m.group(0)}", nuovo_first, count=1)
            elif nuovo_last:
                nuovo_last = f"{nuovo_last} {pallino}"
            else:
                nuovo_first = f"{nuovo_first} {pallino}"

            nuovo_first = ' '.join(nuovo_first.split())
            nuovo_last = ' '.join(nuovo_last.split())

        await client(AddContactRequest(
            id=entity,
            first_name=nuovo_first[:64],
            last_name=nuovo_last[:64],
            phone="",
            add_phone_privacy_exception=False
        ))

        nome_finale = f"{nuovo_first} {nuovo_last}".strip()
        print(f"[RENAME] {chat_id}: '{first} {last}'.strip() -> '{nome_finale}'")
        return web.json_response({"ok": True, "nome_nuovo": nome_finale})

    except Exception as e:
        print(f"[RENAME ERROR] {e}")
        return web.json_response({"ok": False, "error": str(e)}, status=500)


async def handle_send_audio(request: web.Request) -> web.Response:
    """
    POST /send-audio
    Body: {"chat_id": "123456", "audio_key": "audio1_benvenuto"}
    Scarica l'audio da GitHub (raw) e lo invia come vocale nativo Telegram (voice_note=True).
    audio_key deve essere una delle chiavi in AUDIO_LIBRARY.
    """
    try:
        body = await request.json()
        chat_id = body.get("chat_id") or body.get("chatId")
        audio_key = body.get("audio_key") or body.get("audioKey")

        if not chat_id or not audio_key:
            return web.json_response({"ok": False, "error": "chat_id e audio_key obbligatori"}, status=400)

        if audio_key not in AUDIO_LIBRARY:
            return web.json_response({"ok": False, "error": f"audio_key '{audio_key}' non riconosciuto. Validi: {list(AUDIO_LIBRARY.keys())}"}, status=400)

        chat_id = int(chat_id)

        print(f"[AUDIO] Preparo {audio_key} per invio a {chat_id}")

        audio_bytes = await carica_audio_bytes(audio_key)
        if not audio_bytes:
            return web.json_response({"ok": False, "error": f"Audio '{audio_key}' non trovato ne' su disco ne' su GitHub"}, status=502)

        with tempfile.NamedTemporaryFile(suffix=".ogg", delete=False) as tmp_file:
            tmp_file.write(audio_bytes)
            tmp_path = tmp_file.name

        try:
            sent_msg = await client.send_file(chat_id, tmp_path, voice_note=True)
            try:
                if sent_msg is not None:
                    sent_audios.setdefault(chat_id, {})[sent_msg.id] = audio_key
            except Exception:
                pass
            print(f"[AUDIO] Inviato {audio_key} a {chat_id}")
        finally:
            os.remove(tmp_path)

        return web.json_response({"ok": True, "audio_key": audio_key})
    except Exception as e:
        print(f"[HTTP ERROR] {e}")
        return web.json_response({"ok": False, "error": str(e)}, status=500)

async def handle_healthcheck(request: web.Request) -> web.Response:
    return web.json_response({"ok": True, "service": "jack-supporto-agent"})
async def get_dialog_filters():
    """Recupera tutte le chat folders (dialog filters) dell'account"""
    from telethon.tl.functions.messages import GetDialogFiltersRequest
    result = await client(GetDialogFiltersRequest())
    return result.filters


async def _leggi_stato_cartelle(giorni_attivita: int = 45):
    """
    Ritorna (chats, folder_ids). Per ogni chat privata attiva negli ultimi N giorni:
    nome, chat_id, username, cartelle (LISTA di tutte le cartelle in cui sta),
    cartella_attuale (una sola, scelta per priorita' fissa: retrocompatibilita' con n8n).
    v10: prima /folder-status restituiva UNA cartella a caso (l'ultima letta) e leggeva
    solo 200 dialoghi: oltre il 200esimo i lead sparivano dalla gestione.
    """
    filters = await get_dialog_filters()
    folder_map = {}   # chat_id -> [folder_title, ...]
    folder_ids = {}
    for f in filters:
        if hasattr(f, 'title') and hasattr(f, 'id') and hasattr(f, 'include_peers'):
            folder_title = f.title.text if hasattr(f.title, 'text') else str(f.title)
            folder_title = folder_title.strip()
            folder_ids[folder_title] = f.id
            for peer in f.include_peers:
                peer_id = getattr(peer, 'user_id', None) or getattr(peer, 'channel_id', None) or getattr(peer, 'chat_id', None)
                if peer_id:
                    folder_map.setdefault(str(peer_id), []).append(folder_title)

    me = await client.get_me()
    cutoff = datetime.now(ITALY_TZ).timestamp() - (giorni_attivita * 24 * 3600)
    chats_data = []
    async for dialog in client.iter_dialogs():
        if not dialog.is_user or dialog.entity.bot or dialog.entity.id == me.id:
            continue
        if dialog.entity.id in (777000, 42777, 1087968824):  # chat di servizio Telegram / notifiche
            continue
        if dialog.date and dialog.date.timestamp() < cutoff:
            # iter_dialogs e' ordinato per data: da qui in poi sono tutti piu' vecchi
            break
        chat_id = str(dialog.entity.id)
        full_name = f"{dialog.entity.first_name or ''} {dialog.entity.last_name or ''}".strip()
        cartelle = folder_map.get(chat_id, [])
        chats_data.append({
            "username": getattr(dialog.entity, 'username', None) or "",
            "chat_id": chat_id,
            "nome": full_name,
            "cartelle": cartelle,
            "cartella_attuale": _cartella_prioritaria(cartelle),
            "rientro": chat_id in lead_state.get("rientri", {}),
            "stato_reg": lead_state.get("reg", {}).get(chat_id, {}).get("stato") or "",
            "fu_count": lead_state.get("fu", {}).get(chat_id, {}).get("n", 0),
            "fu_last_iso": lead_state.get("fu", {}).get(chat_id, {}).get("ts", ""),
            "ultimo_msg": dialog.date.isoformat() if dialog.date else "",
        })
    return chats_data, folder_ids


# Ordine di priorita' quando una chat sta in piu' cartelle: la prima che matcha vince.
# Serve solo per il campo legacy `cartella_attuale`; la logica v10 usa la lista completa.
PRIORITA_CARTELLE = ["attesa", "vip", "contattare", "bruciati", "transfer", "registraz", "followup", "perso", "trattativa"]


def _cartella_prioritaria(cartelle):
    if not cartelle:
        return "Nessuna cartella"
    normalizzate = {_norm_folder(c): c for c in cartelle}
    for p in PRIORITA_CARTELLE:
        if p in normalizzate:
            return normalizzate[p]
    return cartelle[0]


async def handle_get_folder_status(request: web.Request) -> web.Response:
    """GET /folder-status — vedi _leggi_stato_cartelle. Risposta retrocompatibile + campo `cartelle` e `rientro`."""
    try:
        chats_data, folder_ids = await _leggi_stato_cartelle()
        return web.json_response({"ok": True, "chats": chats_data, "folder_ids": folder_ids})
    except Exception as e:
        print(f"[FOLDER-STATUS ERROR] {e}")
        return web.json_response({"ok": False, "error": str(e)}, status=500)


async def move_chat_to_folder(chat_id: int, target_folder_name: str, exclusive: bool = False) -> dict:
    """
    Sposta una chat nella cartella indicata.
    exclusive=True: rimuove da TUTTE le altre cartelle (clienti con pallino, VIP senza pallino).
    exclusive=False: rimuove solo dalle cartelle auto-gestite dei lead (Trattativa/Followup/Perso).
    folder_name vuoto + exclusive=True: rimuovi da tutte.
    """
    from telethon.tl.functions.messages import UpdateDialogFilterRequest

    target_folder_name = (target_folder_name or "").strip()
    solo_rimozione = (target_folder_name == "")
    if solo_rimozione and not exclusive:
        return {"ok": False, "error": "folder_name mancante"}

    async with folder_lock:
        entity = await _get_entity_robusto(chat_id)
        input_peer = await client.get_input_entity(entity)
        filters = await get_dialog_filters()
        AUTO_MANAGED_FOLDERS = ["Trattativa", "Followup", "Perso", "Registraz"]

        target_filter = None
        for f in filters:
            if not (hasattr(f, 'title') and hasattr(f, 'id') and hasattr(f, 'include_peers')):
                continue
            folder_title = f.title.text if hasattr(f.title, 'text') else str(f.title)
            folder_title = folder_title.strip()
            is_target = _norm_folder(folder_title) == _norm_folder(target_folder_name)
            is_auto = any(_norm_folder(folder_title) == _norm_folder(x) for x in AUTO_MANAGED_FOLDERS)

            should_remove = (not is_target) and (exclusive or is_auto)
            if should_remove:
                new_peers = [p for p in f.include_peers if getattr(p, 'user_id', None) != chat_id]
                if len(new_peers) != len(f.include_peers):
                    f.include_peers = new_peers
                    await client(UpdateDialogFilterRequest(id=f.id, filter=f))
            if is_target:
                target_filter = f

        if solo_rimozione:
            print(f"[FOLDER] Chat {chat_id} rimossa da tutte le cartelle")
            return {"ok": True, "rimosso_da_tutte": True}
        if target_filter is None:
            return {"ok": False, "error": f"Cartella '{target_folder_name}' non trovata"}

        already_in = any(getattr(p, 'user_id', None) == chat_id for p in target_filter.include_peers)
        if not already_in:
            target_filter.include_peers.append(input_peer)
            await client(UpdateDialogFilterRequest(id=target_filter.id, filter=target_filter))
        print(f"[FOLDER] Chat {chat_id} spostata in '{target_folder_name}' (exclusive={exclusive})")
        return {"ok": True, "moved_to": target_folder_name}


async def handle_move_to_folder(request: web.Request) -> web.Response:
    """POST /move-to-folder  Body: {"chat_id": "123", "folder_name": "Trattativa", "exclusive": false}"""
    try:
        data = await request.json()
        res = await move_chat_to_folder(int(data.get("chat_id")), data.get("folder_name", ""), bool(data.get("exclusive", False)))
        status = 200 if res.get("ok") else (404 if "non trovata" in str(res.get("error", "")) else 400)
        return web.json_response(res, status=status)
    except Exception as e:
        print(f"[MOVE-FOLDER ERROR] {e}")
        return web.json_response({"ok": False, "error": str(e)}, status=500)


async def _get_entity_robusto(chat_id: int):
    """get_entity con fallback: dopo un redeploy la cache entita' e' vuota e get_entity(int) fallisce
    finche' il contatto non viene 'visto'. Il fallback scorre i dialoghi (che popolano la cache)."""
    try:
        return await client.get_entity(chat_id)
    except Exception:
        trovato = None
        async for d in client.iter_dialogs():
            if getattr(d, 'id', None) == chat_id or (d.entity is not None and getattr(d.entity, 'id', None) == chat_id):
                trovato = d.entity
                break
        if trovato is not None:
            return trovato
        # il giro sui dialoghi ha popolato la cache: ultimo tentativo
        return await client.get_entity(chat_id)


async def carica_template_salvati() -> dict:
    """Legge i 'Messaggi salvati' (chat con se stessi). Ogni messaggio che finisce con una riga '#chiave'
    diventa un template inviabile con [DOC:chiave]. L'etichetta viene tolta prima dell'invio."""
    trovati = {}
    try:
        async for msg in client.iter_messages('me', limit=300):
            testo = (msg.message or "").rstrip()
            righe = testo.splitlines()
            if not righe:
                continue
            m = re.fullmatch(r'#\s*([a-z0-9_]+)\s*', righe[-1].strip().lower())
            if m and m.group(1) in TEMPLATE_KEYS and m.group(1) not in trovati:
                trovati[m.group(1)] = msg
    except Exception as e:
        print(f"[TEMPLATE] lettura messaggi salvati fallita: {e}")
    templates_salvati.clear(); templates_salvati.update(trovati)
    print(f"[TEMPLATE] caricati: {sorted(trovati)} (mancanti: {sorted(set(TEMPLATE_KEYS) - set(trovati))})")
    return {k: (v.message or "")[:60] for k, v in trovati.items()}


async def invia_template(chat_id: int, key: str) -> bool:
    """Invia al lead una COPIA (non un inoltro) del messaggio salvato: testo, link cliccabili, PDF."""
    msg = templates_salvati.get(key)
    if msg is None:
        await carica_template_salvati(); msg = templates_salvati.get(key)
    if msg is None:
        print(f"[TEMPLATE] '{key}' non trovato nei messaggi salvati"); return False
    testo = (msg.message or "").rstrip()
    righe = testo.splitlines()
    if righe and righe[-1].strip().startswith("#"):
        testo = "\n".join(righe[:-1]).rstrip()
    # Solo documenti e foto si riallegano. L'anteprima di un link (MessageMediaWebPage) NON e' un file:
    # si lascia che Telegram la ricrei dal link nel testo.
    allegato = msg.media if isinstance(msg.media, (MessageMediaDocument, MessageMediaPhoto)) else None
    try:
        async with client.action(chat_id, 'document' if allegato else 'typing'):
            await asyncio.sleep(random.uniform(2, 4))
        await client.send_message(chat_id, testo, formatting_entities=msg.entities, file=allegato, link_preview=True)
        agent_messages.setdefault(chat_id, []).append(testo.strip())
        r = reg_get(chat_id); docs = list(r.get("docs", []))
        if key not in docs: docs.append(key)
        reg_set(chat_id, docs=docs)
        print(f"[TEMPLATE] inviato '{key}' a {chat_id}")
        return True
    except Exception as e:
        print(f"[TEMPLATE] invio '{key}' a {chat_id} fallito: {e}"); return False


PROMPT_FU_REG = """Sei Jack di Virtus FX Club. Stai accompagnando un lead nella registrazione su AXI: ha gia' detto si', ha ricevuto il link e il tutorial, e non scrive da {ore} ore.
STATO: {stato} (link_inviato = ha il link ma non ha ancora iniziato; in_registrazione = sta facendo la procedura; registrato = conto aperto, manca il deposito)
TOCCO NUMERO: {n} di 5
ORARIO CHE IL LEAD AVEVA DICHIARATO: {orario} (se ha detto un momento preciso e quel momento non e' ancora passato rispetto a ORA, rispondi SKIP)
ORA ATTUALE (Italia): {ora}

COME SCRIVE JACK IN QUESTA FASE (esempi reali, usali come modello, non copiarli a caso):
tocco 1, poco dopo: "Sei riuscito a registrarti?" oppure "Se ti blocchi da qualche parte mandami uno screen, ci sono io!"
tocco 2, stessa giornata: "Sei riuscito [nome]?" / "Dimmi pure quando parti che ti seguo"
tocco 3, giorno dopo, con aggancio al calendario: "Giorno [nome]! Oggi hai tempo di farla cosi' partiamo con l'inizio della settimana?" / "Ciao [nome], ti aspetto! Dimmi pure quando torni che ti seguo"
tocco 4, FOMO vera e sobria: "Oggi il team ha gia' operato, ti aspettavo dentro" / "Con il copy abbiamo gia' operato oggi, cosi' non perdi l'operativita'"
tocco 5, ultimo: "Dimmi se ti devo tenere il posto in community [nome], non mi hai piu' fatto sapere" (i posti nel VIP sono limitati e si aprono a scaglioni: e' vero e si puo' dire)

REGOLE: massimo 2 righe. Nome solo se dichiarato dal lead, e non in tutti i messaggi. Un'emoji ogni tanto (💪 🤝 😊), non sempre. Niente punto finale. Riprendi dal punto esatto in cui si era fermato (se aveva un problema con la carta chiedi di quello, se stava attivando il bonus chiedi di quello).
VIETATO: numeri di guadagno, "siamo a profitto", percentuali, promesse, link, capitale, scadenze inventate, "fammi sapere" da solo, "senza fretta". Non ripetere una frase gia' scritta nella chat. Se il lead ha detto chiaramente di non voler procedere, o e' in mezzo a un impegno serio che ha dichiarato (lavoro, emergenza, ferie): SKIP.

ULTIMI MESSAGGI:
{chat}

Rispondi SOLO con il testo (o SKIP)."""

# Ore dall'ultimo NOSTRO messaggio, per tocco 1..5. In registrazione il ritmo e' piu' fitto che in vendita:
# chi ha il link in mano e' caldo, e un promemoria lo sblocca. Dopo il quinto si passa a Jack, mai Perso.
SOGLIE_FU_REG = {
    "link_inviato": [2, 6, 20, 30, 48], "in_registrazione": [2, 6, 20, 30, 48], "registrato": [3, 8, 20, 30, 48],
}


async def followup_registrazione(dry_run: bool = False) -> dict:
    """Un giro di follow-up di accompagnamento per i lead E1. Chiamato ogni ora da n8n (9-23)."""
    now_it = datetime.now(ITALY_TZ); h = now_it.hour
    report = {"inviati": [], "skip": {}, "alert": []}
    if h < 9 or h >= 23:
        report["skip"]["fuori_orario"] = True; return report
    finestra_piena = h in (9, 10, 18, 19)
    for cid, r in list(lead_state.get("reg", {}).items()):
        stato = r.get("stato")
        if stato not in SOGLIE_FU_REG or int(cid) in paused_leads or not e1_attivo(cid):
            continue
        tocchi = int(r.get("tocchi", 0))
        if tocchi >= 5:
            continue
        if tocchi > 0 and not finestra_piena:
            report["skip"][cid] = "fuori finestra"; continue
        try:
            _, msgs = await read_chat_messages(int(cid), hours=24 * 14, limit=40)
        except Exception as e:
            report["skip"][cid] = f"lettura fallita {e}"; continue
        if not msgs or not _is_our_sender(msgs[-1]["sender"]):
            continue   # il lead ha scritto per ultimo: risponde l'agent, non il follow-up
        ore = _ore_da(msgs[-1]["timestamp_iso"]) or 0
        if ore < SOGLIE_FU_REG[stato][tocchi]:
            continue
        if dry_run:
            report["inviati"].append({"chat_id": cid, "stato": stato, "tocco": tocchi + 1, "dry_run": True}); continue
        chat_text = "\n".join(f"[{m['time']}] {m['sender']}: {m['text']}" for m in msgs[-14:])
        prompt = PROMPT_FU_REG.format(ore=int(ore), stato=stato, n=tocchi + 1, orario=r.get("orario_dichiarato") or "nessuno",
                                      ora=now_it.strftime("%A %H:%M"), chat=chat_text)
        testo = ""
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.post("https://api.anthropic.com/v1/messages",
                    headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                    json={"model": "claude-sonnet-5", "max_tokens": 150, "messages": [{"role": "user", "content": prompt}]},
                    timeout=aiohttp.ClientTimeout(total=60)) as resp:
                    d = await resp.json(); testo = (d.get("content", [{}])[0].get("text") or "").strip().strip('"')
        except Exception as e:
            report["skip"][cid] = f"generazione fallita {e}"; continue
        if not testo or testo.upper().startswith("SKIP"):
            report["skip"][cid] = "SKIP dal modello"; continue
        testo = scarta_meta(ricuci_testo(normalizza_flag(testo)))
        testo = re.sub(r'\[\s*[A-Z0-9_:]+\s*\]', '', testo).strip()[:400]
        await send_split_messages(int(cid), testo)
        reg_set(cid, tocchi=tocchi + 1)
        report["inviati"].append({"chat_id": cid, "stato": stato, "tocco": tocchi + 1, "testo": testo})
        if tocchi + 1 >= 5:
            try:
                await notify_jack(f"⏳ REGISTRAZIONE FERMA (E1)\n\nChat {cid}: stato {stato}, 5 tocchi senza risposta. Non lo mando in Perso: decidi tu.\n👉 tg://user?id={cid}", topic="alert")
                report["alert"].append(cid)
            except Exception:
                pass
    print(f"[FU-REG] inviati={len(report['inviati'])} skip={len(report['skip'])}")
    return report


async def handle_followup_registrazione(request: web.Request) -> web.Response:
    """POST/GET /registrazione-followup?dry_run=1"""
    dry = str(request.rel_url.query.get("dry_run", "0")).lower() in ("1", "true")
    try:
        return web.json_response({"ok": True, **(await followup_registrazione(dry_run=dry))})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)}, status=500)


async def handle_reload_templates(request: web.Request) -> web.Response:
    """GET /reload-templates — rilegge i messaggi salvati (dopo che Jack li ha modificati)."""
    return web.json_response({"ok": True, "templates": await carica_template_salvati()})


def _is_our_sender(sender: str) -> bool:
    return sender in ("Agent", "Jack (manuale)")


async def read_chat_messages(chat_id: int, hours: int = 72, limit: int = 5):
    """Legge i messaggi di una chat. Ritorna (entity, messaggi in ordine cronologico)."""
    cutoff = datetime.now(ITALY_TZ).timestamp() - (hours * 3600)
    entity = await _get_entity_robusto(chat_id)
    chat_agent_msgs = agent_messages.get(chat_id, [])
    chat_sent_audios = sent_audios.get(chat_id, {})
    messages = []
    async for msg in client.iter_messages(entity, limit=limit):
        if not msg.date or msg.date.timestamp() < cutoff:
            break
        if msg.text:
            if msg.out:
                sender = "Agent" if msg.text.strip() in chat_agent_msgs else "Jack (manuale)"
            else:
                sender = getattr(entity, 'first_name', None) or "Lead"
            messages.append({"sender": sender, "text": msg.text, "time": msg.date.strftime("%H:%M"), "timestamp_iso": msg.date.isoformat()})
            continue
        # Vocali: senza testo, ma DEVONO comparire nello storico o l'agent li rimanda.
        if getattr(msg, 'voice', None) and msg.out:
            audio_key = chat_sent_audios.get(msg.id)
            etichetta = f"[VOCALE GIA' INVIATO: {audio_key}]" if audio_key else "[VOCALE GIA' INVIATO]"
            messages.append({"sender": "Agent", "text": etichetta, "time": msg.date.strftime("%H:%M"), "timestamp_iso": msg.date.isoformat()})
    messages.reverse()
    return entity, messages


async def handle_get_single_chat(request: web.Request) -> web.Response:
    """GET /get-single-chat?chat_id=123456&hours=72&limit=5 (default invariati per retrocompatibilita')."""
    try:
        chat_id_str = request.rel_url.query.get("chat_id", "")
        if not chat_id_str:
            return web.json_response({"ok": False, "error": "chat_id mancante"}, status=400)
        chat_id = int(chat_id_str)
        hours = int(request.rel_url.query.get("hours", "72"))
        limit = int(request.rel_url.query.get("limit", "5"))
        entity, messages = await read_chat_messages(chat_id, hours, limit)
        full_name = f"{getattr(entity, 'first_name', '') or ''} {getattr(entity, 'last_name', '') or ''}".strip()
        return web.json_response({"ok": True, "chat_id": str(chat_id), "nome": full_name, "messaggi": messages,
                                  "rientro": str(chat_id) in lead_state.get("rientri", {}),
                                  "e1": e1_attivo(chat_id), "stato_reg": reg_get(chat_id).get("stato") or ""})
    except Exception as e:
        print(f"[GET-SINGLE-CHAT ERROR] {e}")
        return web.json_response({"ok": False, "error": str(e)}, status=500)


async def handle_get_chats(request: web.Request) -> web.Response:
    """
    GET /get-chats?hours=8
    Restituisce tutte le conversazioni private degli ultimi X ore
    """
    try:
        hours = int(request.rel_url.query.get("hours", "8"))
        cutoff = datetime.now(ITALY_TZ).timestamp() - (hours * 3600)
        
        chats_data = []
        
        async for dialog in client.iter_dialogs(limit=100):
            if not dialog.is_user:
                continue
            if dialog.entity.bot:
                continue
                
            me = await client.get_me()
            if dialog.entity.id == me.id:
                continue
            
            # Controlla se ci sono messaggi recenti
            if dialog.date and dialog.date.timestamp() < cutoff:
                continue
            
            messages = []
            chat_agent_msgs = agent_messages.get(dialog.entity.id, [])
            async for msg in client.iter_messages(dialog.entity, limit=50):
                if not msg.date or msg.date.timestamp() < cutoff:
                    break
                if msg.text:
                    if msg.out:
                        # Controlla se è un messaggio dell'agent o di Jack
                        is_agent = msg.text.strip() in chat_agent_msgs
                        sender = "Agent" if is_agent else "Jack (manuale)"
                    else:
                        sender = dialog.entity.first_name or "Lead"
                    messages.append({
                        "sender": sender,
                        "text": msg.text,
                        "time": msg.date.strftime("%H:%M")
                    })
            
            if messages:
                messages.reverse()
                chats_data.append({
                    "nome": f"{dialog.entity.first_name or ''} {dialog.entity.last_name or ''}".strip(),
                    "messaggi": messages
                })
        
        return web.json_response({"ok": True, "chats": chats_data, "hours": hours})
    
    except Exception as e:
        print(f"[GET-CHATS ERROR] {e}")
        return web.json_response({"ok": False, "error": str(e)}, status=500)

# ═══════════════════════════════════════════════════════════════════════
# RICONCILIAZIONE CARTELLE (v10) — sostituisce il sotto-flusso n8n "Gestione Cartelle"
# ═══════════════════════════════════════════════════════════════════════
# Una sola macchina a stati, in un posto solo, con la lista COMPLETA delle cartelle per chat.
#
# CLIENTI (pallino nel nome): pallino -> cartella cliente, esclusiva. VIP senza pallino -> fuori da tutto.
# TRANSFER / ATTESA / whitelist: non si toccano (li gestisce Lorenzo a mano).
# LEAD (tutto il resto):
#   - nessun messaggio                     -> skip
#   - ultimo nostro, < 12h                 -> Trattativa
#   - ultimo nostro, FU3 inviato e >= 7gg  -> Perso (allineato al flusso follow-up n8n: 24h / 3gg / 7gg / 7gg)
#   - ultimo nostro, >= 12h                -> Followup
#   - ultimo del lead, < 12h               -> Trattativa (l'agent sta rispondendo)
#   - ultimo del lead, >= 12h              -> Claude decide TRATTATIVA / RINVIO / PERSO
#   - lead in Perso che ha riscritto       -> Trattativa + flag `rientro` (follow-up ridotto a 1 msg)
# Claude viene chiamato SOLO nell'ultimo caso ambiguo, con un tetto per esecuzione.

# Testi follow-up: devono restare IDENTICI a FU_SEND / FU_MATCH nel nodo n8n "Decidi Azione Follow-Up".
FU_TESTI = [
    ["Fammi sapere se hai domande, sarei felice di averti in community", "Ciao, fammi sapere se hai domande"],
    ["Nel gruppo continuiamo a condividere le operazioni ogni giorno, ci stai dando un'occhiata?",
     "I posti in community stanno per finire, fammi sapere se sei ancora interessato", "Ei."],
    ["Ti faccio una domanda secca: c'è qualcosa che non ti ha convinto?",
     "Sarei contento di tenerti il posto e di averti in community con me",
     "Non mi hai fatto sapere più nulla, aspettavo la tua risposta",
     "Ho visto che sei tornato a scrivere"],   # follow-up unico post-rientro: conta come FU3 (poi -> Perso)
]
FU_RIENTRO_TESTO = "Ho visto che sei tornato a scrivere"  # prefisso del messaggio unico post-rientro (n8n)
# Classificazione = una parola: Haiku basta e costa ~10x meno di Sonnet. Override con env MODELLO_CLASSIFICA.
MODELLO_CLASSIFICA = os.environ.get("MODELLO_CLASSIFICA", "claude-haiku-4-5-20251001")

CARTELLE_CLIENTI = {"attesa", "vip", "contattare", "bruciati"}
CARTELLE_LEAD_AUTO = {"trattativa", "followup", "perso", "registraz"}
PALLINO_CARTELLA = {"\U0001F7E1": "Attesa", "\U0001F7E2": "VIP", "\U0001F7E0": "Contattare", "\U0001F534": "Bruciati"}

PROMPT_CLASSIFICA_LEAD = """Analizza questa conversazione Telegram tra Jack (venditore, community copy trading) e un lead. L'ultimo messaggio e' del lead e non ha ancora ricevuto risposta da ore.
Classifica lo stato in UNA sola parola:
TRATTATIVA: il lead fa una domanda, porta avanti il discorso, chiede come procedere, mostra interesse attivo.
RINVIO: il lead chiude con un rinvio anche cortese ("ok grazie", "va bene", "ti faccio sapere", "ci sentiamo", "lunedi'", "dopo le ferie") o con un messaggio breve e generico che non porta avanti nulla.
PERSO: rifiuto netto e definitivo ("non mi interessa", "lasciamo perdere", "non voglio procedere", insulti, richiesta di non essere piu' contattato).

CHAT:
{chat}

Rispondi SOLO con: TRATTATIVA oppure RINVIO oppure PERSO."""


def _link_lead(c: dict) -> str:
    """Link apribile alla chat: username se c'e', altrimenti tg://user?id=."""
    u = (c.get("username") or "").lstrip("@")
    return f"https://t.me/{u}" if u else f"tg://user?id={c.get('chat_id')}"


def _norm_txt(s: str) -> str:
    return " ".join((s or "").split()).strip().lower()


def _fu_index(testo: str):
    """Ritorna 1..3 se il testo e' uno dei follow-up (nuovi o vecchi), altrimenti 0."""
    t = _norm_txt(testo)
    for i, varianti in enumerate(FU_TESTI, start=1):
        for v in varianti:
            v = _norm_txt(v)
            if len(v) < 12:
                if t == v:
                    return i
            elif v in t:
                return i
    return 0


PROMPT_SCREENSHOT = """Questo screenshot arriva da una persona che si sta registrando o sta depositando sul broker AXI (portale my.axiconnect.online, app AXI o MetaTrader 5).
Descrivi in UNA riga, in italiano, cosa mostra, scegliendo SOLO tra questi casi:
1) "scelta tipo conto" (indica il tipo selezionato: Standard/Pro/altro)
2) "valuta e leva" (indica valuta e leva selezionate)
3) "codice promozionale / premi" (indica se e' visibile AXI50 o AXI100)
4) "deposito / metodo di pagamento" (indica metodo e cifra se visibili)
5) "errore carta o pagamento rifiutato" (riporta il testo dell'errore)
6) "conto creato / dashboard" (indica numero conto o stato 'in attesa di revisione' se visibili)
7) "menu laterale / impostazioni" (elenca le voci visibili)
8) "mail di chiusura inviata" (screenshot di una mail a success@axi.com)
9) "altro" (descrivi in poche parole; se e' un sito diverso da AXI dillo)
10) "deposito completato" (ricevuta, "pagamento riuscito", saldo accreditato, cifra depositata visibile)
11) "form di registrazione" (indica QUALE pagina: prima pagina con le domande "cosa ti interessa", piattaforma/tipo conto, indirizzo, situazione lavorativa, finanze/risparmi, elenco documenti del contratto, consensi/captcha, mail e password, verifica ID). Riporta le opzioni visibili in poche parole.
Formato: "caso N: descrizione". Non aggiungere consigli. Se vedi dati sensibili (numero carta, documento, password) NON riportarli, scrivi solo "dati sensibili in vista"."""


async def descrivi_screenshot(chat_id: int, message) -> str:
    """Scarica la foto e la fa descrivere a Claude. Ritorna il testo da dare all'agent, o "" se fallisce."""
    if not ANTHROPIC_API_KEY:
        return ""
    try:
        import base64
        raw = await client.download_media(message, bytes)
        if not raw:
            return ""
        b64 = base64.b64encode(raw).decode()
        mime = "image/jpeg"
        async with aiohttp.ClientSession() as sess:
            async with sess.post("https://api.anthropic.com/v1/messages",
                headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                json={"model": "claude-sonnet-5", "max_tokens": 150, "messages": [{"role": "user", "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": mime, "data": b64}},
                    {"type": "text", "text": PROMPT_SCREENSHOT}]}]},
                timeout=aiohttp.ClientTimeout(total=60)) as r:
                d = await r.json()
                out = (d.get("content", [{}])[0].get("text") or "").strip()
        print(f"[SCREENSHOT] {chat_id}: {out[:120]}")
        return out
    except Exception as e:
        print(f"[SCREENSHOT] errore {chat_id}: {e}"); return ""


async def _claude_classifica_lead(chat_text: str) -> str:
    if not ANTHROPIC_API_KEY:
        return "TRATTATIVA"
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(
                "https://api.anthropic.com/v1/messages",
                headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                json={"model": MODELLO_CLASSIFICA, "max_tokens": 10,
                      "messages": [{"role": "user", "content": PROMPT_CLASSIFICA_LEAD.format(chat=chat_text)}]},
                timeout=aiohttp.ClientTimeout(total=40),
            ) as r:
                d = await r.json()
                out = (d.get("content", [{}])[0].get("text") or "").strip().upper()
                for k in ("PERSO", "RINVIO", "TRATTATIVA"):
                    if k in out:
                        return k
    except Exception as e:
        print(f"[RECONCILE] Claude error: {e}")
    return "TRATTATIVA"


def _ore_da(ts_iso: str):
    try:
        dt = datetime.fromisoformat(ts_iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=pytz.UTC)
        return (datetime.now(pytz.UTC) - dt).total_seconds() / 3600
    except Exception:
        return None


async def reconcile_folders(dry_run: bool = True, max_claude: int = 40, giorni: int = 45) -> dict:
    chats, folder_ids = await _leggi_stato_cartelle(giorni)
    folder_ids_norm = {_norm_folder(k) for k in folder_ids}
    report = {"dry_run": dry_run, "totale_chat": len(chats), "movimenti": [], "anomalie": [], "claude_chiamate": 0,
              "skip": {"cliente": 0, "manuale": 0, "senza_messaggi": 0, "budget_claude": 0, "gia_ok": 0, "cache": 0}}
    now_iso = datetime.now(ITALY_TZ).isoformat()

    for c in chats:
        chat_id = int(c["chat_id"]); nome = c["nome"] or ""; nome_l = nome.lower()
        cartelle = c["cartelle"]; cart_norm = {_norm_folder(x) for x in cartelle}
        target = None; exclusive = False; motivo = ""

        # ---- 1. clienti: decide il pallino ----
        pallino = next((p for p in PALLINO_CARTELLA if p in nome), None)
        if pallino:
            target, exclusive, motivo = PALLINO_CARTELLA[pallino], True, f"pallino {pallino}"
            if _norm_folder(target) in cart_norm and len(cartelle) == 1:
                report["skip"]["gia_ok"] += 1; continue
        elif "vip" in nome_l:
            # Cliente reale senza pallino = invisibile in tutte le cartelle. Lo segnalo ogni giro finche' non ha il pallino.
            spam = any(k in nome_l for k in ("agency", "supporto", "support", "test", "limited"))
            if not spam:
                report["anomalie"].append({"chat_id": c["chat_id"], "nome": nome, "link": _link_lead(c), "cartelle": cartelle, "nota": "VIP SENZA PALLINO: aggiungi 🟢/🟡/🟠/🔴 nel nome o resta fuori da tutte le cartelle"})
            if not cartelle:
                report["skip"]["gia_ok"] += 1; continue
            target, exclusive, motivo = "", True, "VIP senza pallino: fuori da tutte le cartelle"
        # ---- 2. gestiti a mano: mai toccare ----
        elif any(k in nome_l for k in ("transfer", "whitelist", "white list")) or cart_norm & {"transfer", "attesa"}:
            report["skip"]["manuale"] += 1; continue
        elif cart_norm & (CARTELLE_CLIENTI - {"contattare"}):
            report["skip"]["cliente"] += 1
            report["anomalie"].append({"chat_id": c["chat_id"], "nome": nome, "link": _link_lead(c), "cartelle": cartelle, "nota": "in cartella clienti ma senza pallino nel nome"})
            continue
        # ---- 2b. E1: lead in registrazione -> cartella Registrazione (se esiste), whitelist -> Attesa la gestisce Jack ----
        elif lead_state.get("reg", {}).get(c["chat_id"], {}).get("stato") in ("link_inviato", "in_registrazione", "registrato", "deposito_dichiarato", "ripensamento"):
            if "registraz" in folder_ids_norm:
                target, exclusive, motivo = "Registraz", True, f"E1: {lead_state['reg'][c['chat_id']]['stato']}"
                if "registraz" in cart_norm and len(cartelle) == 1:
                    report["skip"]["gia_ok"] += 1; continue
            else:
                report["skip"]["gia_ok"] += 1; continue
        # ---- 3. lead (compresi i vecchi lead rimasti in Contattare senza pallino: vanno tolti da li') ----
        else:
            exclusive = "contattare" in cart_norm
            # CACHE: nessun messaggio nuovo dall'ultimo giro e cartelle invariate -> non rileggo nemmeno la chat.
            # Vale solo se l'ultima decisione era "gia' a posto" E la situazione non dipende dal tempo che passa
            # (Followup/Perso maturano con le ore: quelle si ricontrollano sempre).
            cache = lead_state.setdefault("cache", {}).get(c["chat_id"])
            if cache and cache.get("ultimo_msg") == c.get("ultimo_msg") and sorted(cache.get("cartelle", [])) == sorted(cartelle) \
               and cache.get("stabile"):
                report["skip"]["cache"] += 1; continue
            try:
                _, msgs = await read_chat_messages(chat_id, hours=giorni * 24, limit=60)
            except Exception as e:
                report["anomalie"].append({"chat_id": c["chat_id"], "nome": nome, "link": _link_lead(c), "nota": f"lettura chat fallita: {e}"}); continue
            if not msgs:
                report["skip"]["senza_messaggi"] += 1; continue

            last = msgs[-1]; last_ours = _is_our_sender(last["sender"]); ore = _ore_da(last["timestamp_iso"]) or 0
            fu = 0; lead_dopo_fu = False
            stato_fu = lead_state.get("fu", {}).get(c["chat_id"], {})
            testo_fu_salvato = _norm_txt(stato_fu.get("testo", ""))
            for m in msgs:
                if _is_our_sender(m["sender"]):
                    k = _fu_index(m["text"])
                    if not k and testo_fu_salvato and _norm_txt(m["text"]) == testo_fu_salvato:
                        k = stato_fu.get("n", 0)
                    if k: fu, lead_dopo_fu = k, False
                elif fu:
                    lead_dopo_fu = True
            # se il lead ha risposto dopo l'ultimo FU, il ciclo riparte da zero
            if lead_dopo_fu and c["chat_id"] in lead_state.get("fu", {}) and not dry_run:
                lead_state["fu"].pop(c["chat_id"], None); _salva_lead_state()
            in_perso = "perso" in cart_norm

            ultimo_e_fu = last_ours and (_fu_index(last["text"]) > 0 or (testo_fu_salvato and _norm_txt(last["text"]) == testo_fu_salvato))
            if last_ours:
                if ore < 12 and not ultimo_e_fu:
                    target, motivo = "Trattativa", f"ultimo nostro {ore:.0f}h fa"
                elif fu >= 3 and ore >= 168 and not lead_dopo_fu:
                    target, motivo = "Perso", "FU3 inviato, nessuna risposta da 7 giorni"
                elif in_perso and fu >= 3:
                    report["skip"]["gia_ok"] += 1; continue
                else:
                    target, motivo = "Followup", f"ultimo nostro {ore:.0f}h fa, FU inviati: {fu}"
            else:
                in_pausa = chat_id in paused_leads
                if in_pausa:
                    report["anomalie"].append({"chat_id": c["chat_id"], "nome": nome, "link": _link_lead(c), "cartelle": cartelle, "nota": f"AGENT IN PAUSA: il lead ha scritto {ore:.0f}h fa e aspetta (Riprendi agent o rispondi tu)"})
                if ore >= 168:
                    # Oltre 7 giorni senza nostra risposta: rimetterlo in Trattativa non serve (nessuno rispondera').
                    # Resta dove sta, segnalato come anomalia, senza spendere una chiamata Claude.
                    report["skip"]["gia_ok"] += 1; continue
                if ore < 12:
                    target, motivo = "Trattativa", f"lead ha scritto {ore:.0f}h fa"
                else:
                    if report["claude_chiamate"] >= max_claude:
                        report["skip"]["budget_claude"] += 1; continue
                    chat_text = "\n".join(f"[{m['time']}] {m['sender']}: {m['text']}" for m in msgs[-15:])
                    esito = await _claude_classifica_lead(chat_text); report["claude_chiamate"] += 1
                    target = {"PERSO": "Perso", "RINVIO": "Followup"}.get(esito, "Trattativa")
                    motivo = f"Claude: {esito}"
                if in_perso and target != "Perso":
                    motivo += " · RIENTRO da Perso"
                    if not dry_run:
                        lead_state.setdefault("rientri", {})[c["chat_id"]] = now_iso; _salva_lead_state()

            # rientro chiuso: se torna in Perso o e' passato troppo tempo, pulisci il flag
            if target == "Perso" and c["chat_id"] in lead_state.get("rientri", {}) and not dry_run:
                lead_state["rientri"].pop(c["chat_id"], None); _salva_lead_state()

            # gia' a posto? (sta SOLO nella cartella target tra quelle auto)
            if _norm_folder(target) in cart_norm and not (cart_norm & CARTELLE_LEAD_AUTO - {_norm_folder(target)}) and not exclusive:
                report["skip"]["gia_ok"] += 1
                # Stabile = non cambia col solo passare del tempo: Perso definitivo, oppure Followup con FU3 gia' partito
                # ma non ancora maturo lo escludo (matura). Trattativa "ultimo nostro <12h" matura in Followup: NON stabile.
                stabile = (target == "Perso") or (target == "Followup" and fu < 3 and last_ours)
                # Followup con fu<3 e ultimo nostro: il passaggio FU1->FU2->FU3 lo fa n8n scrivendo un messaggio nuovo,
                # quindi `ultimo_msg` cambia e la cache si invalida da sola.
                if not dry_run:
                    lead_state["cache"][c["chat_id"]] = {"ultimo_msg": c.get("ultimo_msg"), "cartelle": cartelle, "stabile": stabile}
                continue
            if not dry_run:
                lead_state["cache"].pop(c["chat_id"], None)

        mov = {"chat_id": c["chat_id"], "nome": nome, "link": _link_lead(c), "da": cartelle, "a": target or "(nessuna)", "exclusive": exclusive, "motivo": motivo, "eseguito": False}
        if not dry_run:
            try:
                res = await move_chat_to_folder(chat_id, target, exclusive)
                mov["eseguito"] = bool(res.get("ok")); mov["esito"] = res
            except Exception as e:
                mov["esito"] = {"ok": False, "error": str(e)}
        report["movimenti"].append(mov)

    if not dry_run:
        # pulizia: cache solo per chat ancora attive
        attivi = {c["chat_id"] for c in chats}
        lead_state["cache"] = {k: v for k, v in lead_state.get("cache", {}).items() if k in attivi}
        _salva_lead_state()
    print(f"[RECONCILE] dry_run={dry_run} chat={len(chats)} movimenti={len(report['movimenti'])} claude={report['claude_chiamate']} cache={report['skip']['cache']} anomalie={len(report['anomalie'])}")
    return report


async def handle_reconcile_folders(request: web.Request) -> web.Response:
    """
    GET  /reconcile-folders?dry_run=1&max_claude=40&notify=0
    POST /reconcile-folders  {"dry_run": true, "max_claude": 40, "notify": false}
    dry_run=true (DEFAULT): restituisce i movimenti proposti senza eseguirli.
    """
    try:
        q = request.rel_url.query
        body = {}
        if request.method == "POST":
            try: body = await request.json()
            except Exception: body = {}
        def _flag(k, default):
            v = body.get(k, q.get(k, default))
            return str(v).lower() in ("1", "true", "yes", "si") if not isinstance(v, bool) else v
        dry_run = _flag("dry_run", True)
        notify = _flag("notify", False)
        max_claude = int(body.get("max_claude", q.get("max_claude", 40)))
        report = await reconcile_folders(dry_run=dry_run, max_claude=max_claude)
        if notify and (report["movimenti"] or report["anomalie"]):
            righe = [f"📁 Cartelle{' (DRY RUN)' if dry_run else ''}: {len(report['movimenti'])} spostamenti, {len(report['anomalie'])} da sistemare"]
            if report["movimenti"]:
                righe.append("")
                for m in report["movimenti"][:15]:
                    righe.append(f"• {m['nome']} — {', '.join(m['da']) or 'nessuna'} → {m['a']} ({m['motivo']})\n  {m['link']}")
                if len(report["movimenti"]) > 15:
                    righe.append(f"  … e altri {len(report['movimenti']) - 15}")
            anomalie = sorted(report["anomalie"], key=lambda a: 0 if "PAUSA" in a["nota"] else 1)
            if anomalie:
                righe.append("\n🔧 DA SISTEMARE (spariscono al giro dopo):")
                for a in anomalie[:20]:
                    righe.append(f"⚠️ {a['nome']}: {a['nota']}\n  {a['link']}")
            await notify_jack("\n".join(righe), topic="alert")
        return web.json_response({"ok": True, **report})
    except Exception as e:
        print(f"[RECONCILE ERROR] {e}")
        return web.json_response({"ok": False, "error": str(e)}, status=500)


async def start_http_server():
    app = web.Application()
    app.router.add_post("/send-followup", handle_send_followup)
    app.router.add_post("/send-audio",    handle_send_audio)
    app.router.add_post("/rename-contact", handle_rename_contact)
    app.router.add_post("/resume-lead",    handle_resume_lead)
    app.router.add_get("/health",         handle_healthcheck)
    app.router.add_get("/get-chats",      handle_get_chats)
    app.router.add_get("/get-single-chat", handle_get_single_chat)
    app.router.add_get("/folder-status",  handle_get_folder_status)
    app.router.add_post("/move-to-folder", handle_move_to_folder)
    app.router.add_get("/reload-templates", handle_reload_templates)
    app.router.add_get("/registrazione-followup", handle_followup_registrazione)
    app.router.add_post("/registrazione-followup", handle_followup_registrazione)
    app.router.add_get("/reconcile-folders",  handle_reconcile_folders)
    app.router.add_post("/reconcile-folders", handle_reconcile_folders)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"[HTTP] Server avviato su 0.0.0.0:{PORT}")
    return runner
async def main():
    print("🚀 Jack Supporto Agent avviato")
    await client.connect()
    authorized = await client.is_user_authorized()
    if not authorized:
        print("[ERROR] Sessione non autorizzata — rigenera la session string")
        return
    me = await client.get_me()
    print(f"✅ Connesso come {me.first_name} (@{me.username})")
    # WARM-UP cache entita': dopo un redeploy Telethon non conosce piu' nessun contatto e
    # /get-single-chat risponde 500 ("Could not find the input entity") finche' il lead non riscrive.
    # Scorrere i dialoghi una volta popola la cache per tutti (follow-up e riconciliazione inclusi).
    try:
        n_dial = 0
        async for _d in client.iter_dialogs():
            n_dial += 1
        print(f"📇 Cache entita' pronta: {n_dial} dialoghi")
    except Exception as e:
        print(f"[WARN] warm-up dialoghi fallito: {e}")
    print(f"💾 Dati persistenti in: {DATA_DIR}{'' if DATA_DIR != _CODE_DIR else ' (NESSUN VOLUME: si perdono al deploy)'}")
    await carica_template_salvati()
    print(f"🧩 E1: {'TUTTI' if E1_ALL else ('test ids ' + str(sorted(E1_TEST_IDS)) if E1_TEST_IDS else 'SPENTO')}")
    print(f"🔧 Test mode: {TEST_MODE}")
    print(f"🎤 Whisper: {'attivo' if OPENAI_API_KEY else 'non configurato'}")
    print(f"🌙 Modalità notte attiva: {is_night_time()}")
    print(f"📖 Max messaggi storia: {MAX_HISTORY_MESSAGES}")
    print(f"🌐 HTTP follow-up porta: {PORT}")
    try:
        await client.send_message(
            CONTROL_CHAT_ID,
            f"🟢 Jack Agent online\n"
            f"📱 @{me.username}\n"
            f"🔧 Test mode: {TEST_MODE}\n"
            f"🎤 Whisper + Video: {'attivo' if OPENAI_API_KEY else 'non configurato'}\n"
            f"📖 Storico chat: {MAX_HISTORY_MESSAGES} messaggi\n"
            f"🌐 HTTP follow-up: porta {PORT}\n"
            f"Pronto."
        )
    except Exception as e:
        print(f"[WARN] {e}")
    http_runner = await start_http_server()
    try:
        await client.run_until_disconnected()
    finally:
        await http_runner.cleanup()
if __name__ == "__main__":
    asyncio.run(main())
