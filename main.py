import asyncio
import aiohttp
import os
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

DEBOUNCE_TEXT = 30
DEBOUNCE_IMAGE = 20
DEBOUNCE_EXTRA_AUDIO = 15

client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

pending_messages = {}
pending_tasks = {}

async def send_split_messages(chat_id, text):
    parts = [p.strip() for p in text.split("\n\n") if p.strip()]
    if not parts:
        return
    for i, part in enumerate(parts):
        await client.send_message(chat_id, part)
        if i < len(parts) - 1:
            await asyncio.sleep(1.5)

async def process_messages(sender_id, sender_info, debounce):
    await asyncio.sleep(debounce)
    if sender_id not in pending_messages or not pending_messages[sender_id]:
        return
    messages = pending_messages.pop(sender_id, [])
    pending_tasks.pop(sender_id, None)
    combined_text = "\n".join(messages)
    print(f"[MSG IN] {sender_info['full_name']}: {combined_text[:100]}")
    payload = {
        "sender_id": str(sender_id),
        "sender_username": sender_info["username"],
        "sender_full_name": sender_info["full_name"],
        "sender_name": sender_info["first_name"],
        "chat_id": str(sender_id),
        "message_text": combined_text,
        "media_type": sender_info.get("media_type", "text")
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
            if isinstance(event.message.media, MessageMediaPhoto):
                media_type = "immagine"
                debounce = DEBOUNCE_IMAGE
                message_text = f"[Immagine{': ' + message_text if message_text else ''}]"

            elif isinstance(event.message.media, MessageMediaDocument):
                doc = event.message.media.document
                mime = doc.mime_type if hasattr(doc, 'mime_type') else ""

                if "audio" in mime or "ogg" in mime:
                    media_type = "audio"
                    # Leggi durata audio dal metadata
                    audio_duration = 0
                    try:
                        for attr in doc.attributes:
                            if hasattr(attr, 'duration'):
                                audio_duration = int(attr.duration)
                                break
                    except Exception:
                        audio_duration = 30
                    debounce = audio_duration + DEBOUNCE_EXTRA_AUDIO
                    message_text = f"[L'utente ha inviato un messaggio vocale di {audio_duration} secondi — rispondi chiedendo di scrivere in testo perché non puoi ascoltare gli audio]"
                    print(f"[AUDIO] Durata: {audio_duration}s — debounce: {debounce}s")
                else:
                    media_type = "documento"
                    message_text = "[L'utente ha inviato un file]"
                    debounce = DEBOUNCE_TEXT

        if not message_text.strip():
            return

        if sender_id not in pending_messages:
            pending_messages[sender_id] = []
        pending_messages[sender_id].append(message_text)

        sender_info = {
            "full_name": full_name,
            "username": sender_username,
            "first_name": first_name,
            "media_type": media_type
        }

        # Cancella task precedente e crea nuovo con debounce aggiornato
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
        await event.reply(f"🤖 Jack Agent attivo\n📱 @{me.username}\n✅ Tutto operativo")

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
    print(f"⏱ Debounce testo: {DEBOUNCE_TEXT}s | audio: durata+{DEBOUNCE_EXTRA_AUDIO}s | immagine: {DEBOUNCE_IMAGE}s")
    try:
        await client.send_message(
            CONTROL_CHAT_ID,
            f"🟢 Jack Agent online\n📱 @{me.username}\n🔧 Test mode: {TEST_MODE}\nPronto."
        )
    except Exception as e:
        print(f"[WARN] {e}")
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
