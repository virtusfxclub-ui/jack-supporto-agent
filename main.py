import asyncio
import aiohttp
import os
import re
import tempfile
from aiohttp import web
from datetime import datetime
import pytz
from telethon import TelegramClient, events
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
SESSION_STRING = os.environ.get("TELEGRAM_SESSION_STRING", "")
TEST_MODE = os.environ.get("TEST_MODE", "false").lower() == "true"
TEST_SENDER_ID = os.environ.get("TEST_SENDER_ID", "")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
TELEGRAM_BOT_TOKEN = "8502735249:AAHkiAgn25Lck0jUXuCiQUDS2oUGJyP9gbo"
PORT = int(os.environ.get("FOLLOWUP_SERVER_PORT", "8080"))
DEBOUNCE_TEXT = 60
DEBOUNCE_EXTRA_AUDIO = 15
MAX_HISTORY_MESSAGES = 150
ITALY_TZ = pytz.timezone("Europe/Rome")
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
pending_messages = {}
pending_tasks = {}
paused_leads = set()
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


def is_night_time():
    """Controlla se è notte in Italia (00:00 - 08:00)"""
    now = datetime.now(ITALY_TZ)
    return 0 <= now.hour < 8
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
    if 0 <= hour < 3:
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
async def update_airtable_vip(chat_id: str):
    """Aggiorna lo stato del lead VIP su Airtable a cliente"""
    try:
        search_url = f"https://api.airtable.com/v0/appxEMWaNLn7X9a31/Leads"
        headers = {"Authorization": f"Bearer {os.environ.get('AIRTABLE_TOKEN', '')}"}
        params = {"filterByFormula": f"{{chat_id}}='{chat_id}'"}
        async with aiohttp.ClientSession() as session:
            async with session.get(search_url, headers=headers, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    records = data.get("records", [])
                    if records:
                        record_id = records[0]["id"]
                        patch_url = f"{search_url}/{record_id}"
                        patch_data = {"fields": {"stato": "cliente"}}
                        async with session.patch(patch_url, headers={**headers, "Content-Type": "application/json"}, json=patch_data) as patch_resp:
                            if patch_resp.status == 200:
                                print(f"[AIRTABLE] Lead {chat_id} aggiornato a cliente")
                            else:
                                print(f"[AIRTABLE ERROR] Status: {patch_resp.status}")
    except Exception as e:
        print(f"[AIRTABLE VIP ERROR] {e}")
def _norm_folder(nome: str) -> str:
    """
    Normalizza un nome cartella per il confronto: toglie emoji, spazi e maiuscole.
    Serve perche' le cartelle su Telegram hanno il pallino nel nome ("VIP 🟢")
    ma nel codice le referenziamo come "VIP".
    """
    if not nome:
        return ""
    return "".join(ch for ch in nome.lower() if ch.isalnum())


async def notify_jack(text: str, topic: str = "alert"):
    """
    Manda una notifica nel gruppo di controllo.
    topic="chat"  -> flusso normale (messaggi in arrivo), si puo' silenziare
    topic="alert" -> richiede un'azione di Jack, notifiche accese
    """
    thread_id = TOPIC_CHAT if topic == "chat" else TOPIC_ALERT
    payload = {"chat_id": CONTROL_CHAT_ID, "text": text}
    if thread_id:
        payload["message_thread_id"] = thread_id
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
def ricuci_testo(text: str) -> str:
    """
    Ripara gli a capo spuri introdotti da una risposta in streaming.
    n8n puo' spedire la risposta a pezzi: i confini dei pezzi diventano '\n'
    in mezzo alle parole ("segn\nali", "qu\nindi").
    I doppi a capo veri vanno preservati: servono a separare i messaggi.
    """
    if not text or '\n' not in text:
        return text

    # Protegge i doppi a capo (separatori di messaggio) con un segnaposto
    SEP = '\x00SEP\x00'
    t = re.sub(r'\n\s*\n+', SEP, text)

    # A capo singolo tra due caratteri di parola = parola spezzata -> ricuci senza spazio
    t = re.sub(r'(?<=[\wàèéìòùÀÈÉÌÒÙ\'])\n(?=[\wàèéìòùÀÈÉÌÒÙ])', '', t)

    # Tutti gli altri a capo singoli rimasti diventano spazi
    t = t.replace('\n', ' ')

    # Ripristina i separatori e normalizza gli spazi doppi
    t = t.replace(SEP, '\n\n')
    t = re.sub(r'[ \t]{2,}', ' ', t)
    return t.strip()


async def send_split_messages(chat_id, text):
    text = ricuci_testo(text)
    text = clean_dashes(text)
    parts = [p.strip() for p in text.split("\n\n") if p.strip()]
    if not parts:
        return
    for i, part in enumerate(parts):
        await client.send_message(chat_id, part)
        # Traccia questo messaggio come inviato dall'agent
        if chat_id not in agent_messages:
            agent_messages[chat_id] = []
        agent_messages[chat_id].append(part.strip())
        # Mantieni solo gli ultimi 200 messaggi per chat per non occupare troppa memoria
        if len(agent_messages[chat_id]) > 200:
            agent_messages[chat_id] = agent_messages[chat_id][-200:]
        if i < len(parts) - 1:
            if len(part) > 120:
                delay = 7.0
            elif len(part) > 60:
                delay = 5.0
            else:
                delay = 3.0
            await asyncio.sleep(delay)
async def process_messages(sender_id, sender_info, debounce):
    await asyncio.sleep(debounce)
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
                        reply_text = reply_text.strip()
                    except Exception:
                        reply_text = ""
                    if not reply_text:
                        print(f"[WARN] Nessuna reply ricevuta da n8n")
                        return
                    # Gestione BLOCK — proposta commerciale
                    if reply_text.startswith("[BLOCK]"):
                        paused_leads.add(sender_id)
                        print(f"[BLOCKED] {sender_info['full_name']} bloccato")
                        return
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
                        print(f"[PAUSED] {sender_info['full_name']} messo in pausa dopo escalation")
                        return
                    # STORICO PDF — DISATTIVATO. Il documento non va mai inviato a nessuno.
                    # Il marker viene comunque ripulito dal testo come rete di sicurezza,
                    # cosi' se il modello lo scrivesse per errore non finisce visibile al lead.
                    if '[STORICO_LEAD]' in reply_text:
                        print(f"[STORICO] Flag ignorato (invio PDF disattivato) — chat {sender_id}")
                    clean_reply = reply_text.replace('[STORICO_LEAD]', '').strip()

                    # Alert dedicato: serve la procedura di chiusura conto AXI
                    if '[ALERT_CHIUSURA]' in clean_reply:
                        clean_reply = clean_reply.replace('[ALERT_CHIUSURA]', '').strip()
                        asyncio.create_task(notify_jack(
                            f"🟠 CHIUSURA CONTO AXI\n\n"
                            f"👤 {sender_info['full_name']}\n"
                            f"📱 ID: {sender_id}\n\n"
                            f"Ha già un conto AXI con deposito: serve mandargli la mail "
                            f"e il documento con la procedura di chiusura e riapertura.",
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

                    await send_split_messages(sender_id, clean_reply)
                    print(f"[MSG OUT] → {sender_info['full_name']}: {clean_reply[:80]}")

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
            print(f"[SKIP VIP] {full_name} — aggiorno Airtable a cliente")
            # Aggiorna automaticamente lo stato su Airtable a cliente
            asyncio.create_task(update_airtable_vip(str(sender_id)))
            return
        if sender_id in paused_leads:
            print(f"[PAUSED] {full_name} ha scritto ma è in pausa")
            await notify_jack(
                f"⏸ LEAD IN PAUSA HA SCRITTO\n\n"
                f"👤 {full_name} (@{sender_username})\n"
                f"💬 Ha scritto: {event.message.message or '[media]'}\n\n"
                f"Per riattivare l'agent copia e invia questo comando:\n"
                f"riprendi {sender_id}",
                topic="alert"
            )
            return
        message_text = event.message.message or ""
        media_type = "text"
        debounce = DEBOUNCE_TEXT

        # Segna il messaggio del lead come letto → il lead vede le spunte blu (comportamento umano)
        try:
            await client.send_read_acknowledge(sender_id, event.message)
        except Exception as e:
            print(f"[READ] Impossibile segnare come letto {sender_id}: {e}")

        # Notifica sul bot di controllo: serve perché il read receipt sopra sopprime
        # la notifica push nativa di Telegram sugli altri dispositivi di Jack.
        try:
            anteprima = message_text.strip() if message_text.strip() else "[media]"
            if len(anteprima) > 200:
                anteprima = anteprima[:200] + "..."
            username_txt = f" (@{sender_username})" if sender_username else ""
            asyncio.create_task(notify_jack(
                f"💬 {full_name}{username_txt}\n"
                f"{anteprima}",
                topic="chat"
            ))
        except Exception as e:
            print(f"[NOTIFY] Errore notifica nuovo messaggio: {e}")
        if event.message.media:
            if isinstance(event.message.media, MessageMediaPhoto):
                print(f"[IMAGE] Immagine da {full_name} — metto in pausa e notifico Jack")
                paused_leads.add(sender_id)
                caption = f" — didascalia: \"{message_text}\"" if message_text else ""
                await notify_jack(
                    f"🖼 IMMAGINE RICEVUTA{caption}\n\n"
                    f"👤 {full_name} (@{sender_username})\n"
                    f"📱 ID: {sender_id}\n\n"
                    f"Vai nella chat e rispondi tu direttamente.\n"
                    f"Per riattivare l'agent copia e invia:\n"
                    f"riprendi {sender_id}",
                    topic="alert"
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
@client.on(events.NewMessage(chats=CONTROL_CHAT_ID))
async def handle_control(event):
    text = event.message.message or ""
    if text.lower().startswith("riprendi"):
        parts = text.split()
        if len(parts) >= 2:
            try:
                sender_id = int(parts[1])
                if sender_id in paused_leads:
                    paused_leads.discard(sender_id)
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
async def handle_send_followup(request: web.Request) -> web.Response:
    """POST /send-followup — manda messaggio follow-up come @jacksupporto"""
    try:
        body = await request.json()
        chat_id = body.get("chat_id") or body.get("chatId")
        message = body.get("message") or body.get("followupMsg") or body.get("text")
        if not chat_id or not message:
            return web.json_response({"ok": False, "error": "chat_id e message obbligatori"}, status=400)
        chat_id = int(chat_id)
        print(f"[FOLLOWUP] Invio a {chat_id}: {message[:80]}")
        await client.send_message(chat_id, message)
        print(f"[FOLLOWUP] Inviato a {chat_id}")
        return web.json_response({"ok": True})
    except Exception as e:
        print(f"[HTTP ERROR] {e}")
        return web.json_response({"ok": False, "error": str(e)}, status=500)

PALLINI = ["\U0001F7E1", "\U0001F7E2", "\U0001F7E0", "\U0001F534"]  # giallo, verde, arancione, rosso

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


async def handle_get_folder_status(request: web.Request) -> web.Response:
    """
    GET /folder-status
    Restituisce per ogni chat privata: nome contatto, chat_id, e in quale cartella si trova
    """
    try:
        filters = await get_dialog_filters()
        folder_map = {}  # chat_id -> folder_title
        folder_ids = {}  # folder_title -> folder_id

        for f in filters:
            if hasattr(f, 'title') and hasattr(f, 'id') and hasattr(f, 'include_peers'):
                folder_title = f.title.text if hasattr(f.title, 'text') else str(f.title)
                folder_title = folder_title.strip()
                folder_ids[folder_title] = f.id
                for peer in f.include_peers:
                    peer_id = getattr(peer, 'user_id', None) or getattr(peer, 'channel_id', None) or getattr(peer, 'chat_id', None)
                    if peer_id:
                        folder_map[str(peer_id)] = folder_title

        chats_data = []
        cutoff_30d = datetime.now(ITALY_TZ).timestamp() - (30 * 24 * 3600)
        async for dialog in client.iter_dialogs(limit=200):
            if not dialog.is_user:
                continue
            if dialog.entity.bot:
                continue
            me = await client.get_me()
            if dialog.entity.id == me.id:
                continue
            # Salta dialoghi senza attività negli ultimi 30 giorni (riduce carico)
            if dialog.date and dialog.date.timestamp() < cutoff_30d:
                continue

            chat_id = str(dialog.entity.id)
            full_name = f"{dialog.entity.first_name or ''} {dialog.entity.last_name or ''}".strip()
            current_folder = folder_map.get(chat_id, "Nessuna cartella")

            chats_data.append({
                "username": getattr(dialog.entity, 'username', None) or "",
                "chat_id": chat_id,
                "nome": full_name,
                "cartella_attuale": current_folder
            })

        return web.json_response({"ok": True, "chats": chats_data, "folder_ids": folder_ids})

    except Exception as e:
        print(f"[FOLDER-STATUS ERROR] {e}")
        return web.json_response({"ok": False, "error": str(e)}, status=500)


async def handle_move_to_folder(request: web.Request) -> web.Response:
    """
    POST /move-to-folder
    Body: {"chat_id": "123456", "folder_name": "Trattativa", "exclusive": false}
    Sposta una chat nella cartella specificata.
    Se exclusive=true, rimuove la chat da TUTTE le altre cartelle gestite (usato per VIP).
    Se exclusive=false (default), rimuove solo dalle cartelle auto-gestite (Trattativa/Contattare/Perso),
    permettendo a una chat di stare in più cartelle manuali contemporaneamente (es. Attesa + Transfer).
    """
    try:
        from telethon.tl.functions.messages import UpdateDialogFilterRequest

        data = await request.json()
        chat_id = int(data.get("chat_id"))
        target_folder_name = data.get("folder_name", "").strip()
        exclusive = bool(data.get("exclusive", False))

        # folder_name vuoto + exclusive=true significa: rimuovi da TUTTE le cartelle
        # (usato per i VIP senza pallino, che non devono stare da nessuna parte)
        solo_rimozione = (target_folder_name == "")
        if not target_folder_name and not exclusive:
            return web.json_response({"ok": False, "error": "folder_name mancante"}, status=400)

        # Lock per evitare che chiamate simultanee leggano filtri non aggiornati (race condition)
        async with folder_lock:
            entity = await client.get_entity(chat_id)
            input_peer = await client.get_input_entity(entity)

            filters = await get_dialog_filters()

            # Cartelle gestite automaticamente dal bot (escludiamo VIP, Transfer, Attesa, Support che sono manuali)
            # NB: la cartella dei lead da ricontattare ora si chiama "Followup".
            # "Contattare" e' passata al mondo clienti (arancione) e NON va toccata qui.
            AUTO_MANAGED_FOLDERS = ["Trattativa", "Followup", "Perso"]

            target_filter = None
            for f in filters:
                if not (hasattr(f, 'title') and hasattr(f, 'id') and hasattr(f, 'include_peers')):
                    continue

                folder_title = f.title.text if hasattr(f.title, 'text') else str(f.title)
                folder_title = folder_title.strip()

                # Confronto tollerante: "VIP 🟢" deve corrispondere a "VIP"
                is_target = _norm_folder(folder_title) == _norm_folder(target_folder_name)
                is_auto = any(_norm_folder(folder_title) == _norm_folder(x) for x in AUTO_MANAGED_FOLDERS)

                should_remove = False
                if not is_target:
                    if exclusive:
                        # Modalita' esclusiva: rimuovi da QUALSIASI altra cartella
                        should_remove = True
                    elif is_auto:
                        # Modalita' normale: rimuovi solo dalle cartelle auto-gestite
                        should_remove = True

                if should_remove:
                    new_peers = [p for p in f.include_peers if getattr(p, 'user_id', None) != chat_id]
                    if len(new_peers) != len(f.include_peers):
                        f.include_peers = new_peers
                        await client(UpdateDialogFilterRequest(id=f.id, filter=f))

                if is_target:
                    target_filter = f

            if solo_rimozione:
                print(f"[FOLDER] Chat {chat_id} rimossa da tutte le cartelle")
                return web.json_response({"ok": True, "rimosso_da_tutte": True})

            if target_filter is None:
                return web.json_response({"ok": False, "error": f"Cartella '{target_folder_name}' non trovata"}, status=404)

            already_in = any(getattr(p, 'user_id', None) == chat_id for p in target_filter.include_peers)
            if not already_in:
                target_filter.include_peers.append(input_peer)
                await client(UpdateDialogFilterRequest(id=target_filter.id, filter=target_filter))

            print(f"[FOLDER] Chat {chat_id} spostata in '{target_folder_name}' (exclusive={exclusive})")
            return web.json_response({"ok": True, "moved_to": target_folder_name})

    except Exception as e:
        print(f"[MOVE-FOLDER ERROR] {e}")
        return web.json_response({"ok": False, "error": str(e)}, status=500)


async def handle_get_single_chat(request: web.Request) -> web.Response:
    """
    GET /get-single-chat?chat_id=123456&hours=72&limit=5
    Restituisce SOLO la chat specificata, senza scansionare tutti i dialoghi.
    Molto più veloce di /get-chats quando serve una sola chat.
    Parametro 'limit' opzionale (default 5, invariato per retrocompatibilità col sistema cartelle/follow-up).
    Il workflow principale (routing Agent1/Agent2) passa un limit più alto (es. 200) per avere lo storico completo.
    """
    try:
        chat_id_str = request.rel_url.query.get("chat_id", "")
        if not chat_id_str:
            return web.json_response({"ok": False, "error": "chat_id mancante"}, status=400)

        chat_id = int(chat_id_str)
        hours = int(request.rel_url.query.get("hours", "72"))
        limit = int(request.rel_url.query.get("limit", "5"))
        cutoff = datetime.now(ITALY_TZ).timestamp() - (hours * 3600)

        entity = await client.get_entity(chat_id)
        chat_agent_msgs = agent_messages.get(chat_id, [])

        chat_sent_audios = sent_audios.get(chat_id, {})

        messages = []
        async for msg in client.iter_messages(entity, limit=limit):
            if not msg.date or msg.date.timestamp() < cutoff:
                break

            if msg.text:
                if msg.out:
                    is_agent = msg.text.strip() in chat_agent_msgs
                    sender = "Agent" if is_agent else "Jack (manuale)"
                else:
                    sender = getattr(entity, 'first_name', None) or "Lead"
                messages.append({
                    "sender": sender,
                    "text": msg.text,
                    "time": msg.date.strftime("%H:%M"),
                    "timestamp_iso": msg.date.isoformat()
                })
                continue

            # Vocali: non hanno testo, ma DEVONO comparire nello storico.
            # Senza questo l'agent non sa di aver gia' mandato un audio e lo rimanda.
            if getattr(msg, 'voice', None) and msg.out:
                audio_key = chat_sent_audios.get(msg.id)
                etichetta = f"[VOCALE GIA' INVIATO: {audio_key}]" if audio_key else "[VOCALE GIA' INVIATO]"
                messages.append({
                    "sender": "Agent",
                    "text": etichetta,
                    "time": msg.date.strftime("%H:%M"),
                    "timestamp_iso": msg.date.isoformat()
                })

        messages.reverse()
        full_name = f"{getattr(entity, 'first_name', '') or ''} {getattr(entity, 'last_name', '') or ''}".strip()

        return web.json_response({
            "ok": True,
            "chat_id": str(chat_id),
            "nome": full_name,
            "messaggi": messages
        })

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
async def start_http_server():
    app = web.Application()
    app.router.add_post("/send-followup", handle_send_followup)
    app.router.add_post("/send-audio",    handle_send_audio)
    app.router.add_post("/rename-contact", handle_rename_contact)
    app.router.add_get("/health",         handle_healthcheck)
    app.router.add_get("/get-chats",      handle_get_chats)
    app.router.add_get("/get-single-chat", handle_get_single_chat)
    app.router.add_get("/folder-status",  handle_get_folder_status)
    app.router.add_post("/move-to-folder", handle_move_to_folder)
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
