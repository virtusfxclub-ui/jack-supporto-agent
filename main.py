import asyncio
import aiohttp
import os
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import User

API_ID = int(os.environ.get("TELEGRAM_API_ID", "0"))
API_HASH = os.environ.get("TELEGRAM_API_HASH", "")
N8N_WEBHOOK_URL = os.environ.get("N8N_WEBHOOK_URL", "")
CONTROL_CHAT_ID = int(os.environ.get("CONTROL_CHAT_ID", "-5137754911"))
SESSION_STRING = os.environ.get("TELEGRAM_SESSION_STRING", "")
TEST_MODE = os.environ.get("TEST_MODE", "false").lower() == "true"
TEST_SENDER_ID = os.environ.get("TEST_SENDER_ID", "")

client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

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
        if not message_text.strip():
            return
        print(f"[MSG IN] {full_name} (@{sender_username}): {message_text[:80]}")
        payload = {
            "sender_id": str(sender_id),
            "sender_username": sender_username,
            "sender_full_name": full_name,
            "sender_name": first_name,
            "chat_id": str(sender_id),
            "message_text": message_text
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(
                N8N_WEBHOOK_URL,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=180)
            ) as resp:
                if resp.status == 200:
                    response_data = await resp.json()
                    reply_text = response_data.get("reply", "")
                    if reply_text:
                        await client.send_message(sender_id, reply_text)
                        print(f"[MSG OUT] → {full_name}: {reply_text[:80]}")
                else:
                    print(f"[ERROR] n8n status: {resp.status}")
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
    print(f"📡 Webhook: {N8N_WEBHOOK_URL[:50]}...")
    try:
        await client.send_message(CONTROL_CHAT_ID, f"🟢 Jack Agent online\n📱 @{me.username}\n🔧 Test mode: {TEST_MODE}\nPronto.")
    except Exception as e:
        print(f"[WARN] {e}")
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
