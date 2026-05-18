import asyncio
import aiohttp
import os
import tempfile
from datetime import datetime
import pytz
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import User, MessageMediaPhoto, MessageMediaDocument

API_ID = int(os.environ.get("TELEGRAM_API_ID", "0"))
API_HASH = os.environ.get("TELEGRAM_API_HASH", "")
N8N_WEBHOOK_URL = os.environ.get("N8N_WEBHOOK_URL", "")
CONTROL_CHAT_ID = int(os.environ.get("CONTROL_CHAT_ID", "-5137754911"))
SESSION_STRING = os.environ.get("TELEGRAM_SESSION_STRING", "")
TEST_MODE = os.environ.get("TEST_MODE", "false").lower() == "true"
TEST_SENDER_ID = os.environ.get("TEST_SENDER_ID", "")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
TELEGRAM_BOT_TOKEN = "8502735249:AAHkiAgn25Lck0jUXuCiQUDS2oUGJyP9gbo"

DEBOUNCE_TEXT = 30
DEBOUNCE_EXTRA_AUDIO = 15
MAX_HISTORY_MESSAGES = 50
ITALY_TZ = pytz.timezone("Europe/Rome")

client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

pending_messages = {}
pending_tasks = {}
paused_leads = set()


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

    if 0 <= hour < 3:
        return f"Guarda {name}, sto andando a letto adesso. Domattina ti scrivo io personalmente e {action}, ok?"
    else:
        return f"Guarda {name}, domattina appena sono in ufficio ti scrivo io personalmente e {action}, ok?"


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


async def notify_jack(text: str):
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": CONTROL_CHAT_ID, "text": text}
            )
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


async def send_split_messages(chat_id, text):
    parts = [p.strip() for p in text.split("\n\n") if p.strip()]
    if not parts:
        return
    for i, part in enumerate(parts):
        await client.send_message(chat_id, part)
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

                    # Gestione STORICO_LEAD — manda risposta rapida /STORICO
                    should_send_storico = '[STORICO_LEAD]' in reply_text
                    clean_reply = reply_text.replace('[STORICO_LEAD]', '').strip()

                    await send_split_messages(sender_id, clean_reply)
                    print(f"[MSG OUT] → {sender_info['full_name']}: {clean_reply[:80]}")

                    if should_send_storico:
                        await asyncio.sleep(2)
                        try:
                            # Carica tutti i messaggi salvati e forwardali in sequenza
                            saved_messages = []
                            async for msg in client.iter_messages('me', limit=50):
                                saved_messages.append(msg)
                            
                            if saved_messages:
                                # Inverti per inviare in ordine cronologico
                                saved_messages.reverse()
                                for msg in saved_messages:
                                    await client.forward_messages(sender_id, msg, 'me')
                                    await asyncio.sleep(1)
                                print(f"[STORICO] Inviati {len(saved_messages)} messaggi storico a {sender_info['full_name']}")
                            else:
                                print(f"[STORICO] Nessun messaggio trovato nei messaggi salvati")
                        except Exception as e:
                            print(f"[STORICO ERROR] {e}")

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
            print(f"[SKIP VIP] {full_name}")
            return

        if sender_id in paused_leads:
            print(f"[PAUSED] {full_name} ha scritto ma è in pausa")
            await notify_jack(
                f"⏸ LEAD IN PAUSA HA SCRITTO\n\n"
                f"👤 {full_name} (@{sender_username})\n"
                f"💬 Ha scritto: {event.message.message or '[media]'}\n\n"
                f"Scrivi 'riprendi {sender_id}' per riattivare l'agent."
            )
            return

        message_text = event.message.message or ""
        media_type = "text"
        debounce = DEBOUNCE_TEXT

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
                    f"Scrivi 'riprendi {sender_id}' quando vuoi che riprenda l'agent."
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

    try:
        await client.send_message(
            CONTROL_CHAT_ID,
            f"🟢 Jack Agent online\n"
            f"📱 @{me.username}\n"
            f"🔧 Test mode: {TEST_MODE}\n"
            f"🎤 Whisper + Video: {'attivo' if OPENAI_API_KEY else 'non configurato'}\n"
            f"📖 Storico chat: {MAX_HISTORY_MESSAGES} messaggi\n"
            f"Pronto."
        )
    except Exception as e:
        print(f"[WARN] {e}")

    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
