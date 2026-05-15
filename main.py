import asyncio
import aiohttp
import os
import tempfile
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

client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

pending_messages = {}
pending_tasks = {}


async def notify_jack(text: str):
    """Manda notifica nel gruppo Jack Agent Control via bot Telegram"""
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": CONTROL_CHAT_ID, "text": text}
            )
    except Exception as e:
        print(f"[NOTIFY ERROR] {e}")


async def transcribe_audio(file_path: str) -> str:
    """Trascrive audio con Whisper API OpenAI"""
    try:
        with open(file_path, "rb") as audio_file:
            async with aiohttp.ClientSession() as session:
                data = aiohttp.FormData()
                data.add_field("file", audio_file, filename="audio.ogg", content_type="audio/ogg")
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


async def send_split_messages(chat_id, text):
    """Spezza il testo su doppio newline e manda messaggi separati con delay umano"""
    parts = [p.strip() for p in text.split("\n\n") if p.strip()]
    if not parts:
        return
    for i, part in enumerate(parts):
        await client.send_message(chat_id, part)
        if i < len(parts) - 1:
            # Delay basato sulla lunghezza del messaggio appena inviato
            if len(part) > 120:
                delay = 5.0
            elif len(part) > 60:
                delay = 3.5
            else:
                delay = 2.0
            await asyncio.sleep(delay)


async def process_messages(sender_id, sender_info, debounce):
    """Aspetta debounce secondi poi processa tutti i messaggi accumulati"""
    await asyncio.sleep(debounce)

    if sender_id not in pending_messages or not pending_messages[sender_id]:
        return

    messages = pending_messages.pop(sender_id, [])
    pending_tasks.pop(sender_id, None)

    combined_text = "\n".join([m["text"] for m in messages if m.get("text")])
    media_type = messages[-1].get("media_type", "text")

    print(f"[MSG IN] {sender_info['full_name']}: {combined_text[:100]}")

    payload = {
        "sender_id": str(sender_id),
        "sender_username": sender_info["username"],
        "sender_full_name": sender_info["full_name"],
        "sender_name": sender_info["first_name"],
        "chat_id": str(sender_id),
        "message_text": combined_text,
        "media_type": media_type
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
                    if reply_text:
                        await send_split_messages(sender_id, reply_text)
                        print(f"[MSG OUT] → {sender_info['full_name']}: {reply_text[:80]}")
                    else:
                        print(f"[WARN] Nessuna reply ricevuta da n8n")
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

        message_text = event.message.message or ""
        media_type = "text"
        debounce = DEBOUNCE_TEXT

        if event.message.media:

            # IMMAGINE → notifica Jack, non rispondere al lead
            if isinstance(event.message.media, MessageMediaPhoto):
                print(f"[IMAGE] Immagine da {full_name} — notifico Jack")
                caption = f" — didascalia: \"{message_text}\"" if message_text else ""
                await notify_jack(
                    f"🖼 IMMAGINE RICEVUTA{caption}\n\n"
                    f"👤 {full_name} (@{sender_username})\n"
                    f"📱 ID: {sender_id}\n\n"
                    f"Vai nella chat e rispondi tu direttamente.\n"
                    f"Scrivi qui 'ok ripreso' quando vuoi che riprenda l'agent."
                )
                return

            # AUDIO → trascrivi con Whisper
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
    if text.startswith("/stato"):
        me = await client.get_me()
        await event.reply(
            f"🤖 Jack Agent attivo\n"
            f"📱 @{me.username}\n"
            f"🔧 Test mode: {TEST_MODE}\n"
            f"🎤 Whisper: {'attivo' if OPENAI_API_KEY else 'non configurato'}\n"
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
    print(f"⏱ Debounce testo: {DEBOUNCE_TEXT}s | audio: durata+{DEBOUNCE_EXTRA_AUDIO}s")

    try:
        await client.send_message(
            CONTROL_CHAT_ID,
            f"🟢 Jack Agent online\n"
            f"📱 @{me.username}\n"
            f"🔧 Test mode: {TEST_MODE}\n"
            f"🎤 Whisper: {'attivo' if OPENAI_API_KEY else 'non configurato'}\n"
            f"Pronto."
        )
    except Exception as e:
        print(f"[WARN] {e}")

    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
