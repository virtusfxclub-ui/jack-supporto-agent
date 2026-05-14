import asyncio
import aiohttp
import os
import json
import random
from telethon import TelegramClient, events
from telethon.tl.types import User

# ─── CONFIG DA VARIABILI AMBIENTE ───────────────────────────────────────────
API_ID = int(os.environ.get("TELEGRAM_API_ID", "0"))
API_HASH = os.environ.get("TELEGRAM_API_HASH", "")
PHONE = os.environ.get("TELEGRAM_PHONE", "")
N8N_WEBHOOK_URL = os.environ.get("N8N_WEBHOOK_URL", "")
CONTROL_CHAT_ID = int(os.environ.get("CONTROL_CHAT_ID", "-5137754911"))

# Session string per Railway (non serve file locale)
SESSION_STRING = os.environ.get("TELEGRAM_SESSION_STRING", "")

# ─── CLIENT TELEGRAM ────────────────────────────────────────────────────────
if SESSION_STRING:
    client = TelegramClient.from_string(SESSION_STRING, api_id=API_ID, api_hash=API_HASH)
else:
    client = TelegramClient("jack_supporto", api_id=API_ID, api_hash=API_HASH)

# ─── HANDLER MESSAGGI IN ARRIVO ─────────────────────────────────────────────
@client.on(events.NewMessage(incoming=True))
async def handle_incoming(event):
    try:
        # Ignora messaggi dai gruppi e canali
        if not event.is_private:
            return

        # Prendi info mittente
        sender = await event.get_sender()
        if not isinstance(sender, User):
            return

        # Ignora bot
        if sender.bot:
            return

        sender_id = sender.id
        sender_username = sender.username or ""
        first_name = sender.first_name or ""
        last_name = sender.last_name or ""
        full_name = f"{first_name} {last_name}".strip()

        # Ignora messaggi da te stesso
        me = await client.get_me()
        if sender_id == me.id:
            return

        # Controlla se è VIP (nome salvato in rubrica contiene VIP)
        if "VIP" in full_name.upper():
            print(f"[SKIP VIP] {full_name}")
            return

        message_text = event.message.message or ""

        # Ignora messaggi vuoti (foto, video, audio senza testo)
        if not message_text.strip():
            return

        print(f"[MSG IN] {full_name} (@{sender_username}): {message_text[:50]}")

        # Manda al webhook n8n
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
                        # Delay umano già gestito da n8n, qui aspettiamo solo la risposta
                        await client.send_message(sender_id, reply_text)
                        print(f"[MSG OUT] → {full_name}: {reply_text[:50]}")
                else:
                    print(f"[ERROR] n8n risposta: {resp.status}")

    except Exception as e:
        print(f"[EXCEPTION] {e}")

# ─── HANDLER PER COMANDI DAL PANNELLO DI CONTROLLO ──────────────────────────
@client.on(events.NewMessage(chats=CONTROL_CHAT_ID))
async def handle_control(event):
    """
    Comandi dal gruppo Jack Agent Control:
    - /rispondi @username testo → attiva agent su quel contatto
    - /stato → mostra status agent
    """
    text = event.message.message or ""

    if text.startswith("/rispondi"):
        parts = text.split(" ", 2)
        if len(parts) >= 3:
            target_username = parts[1].replace("@", "")
            custom_msg = parts[2] if len(parts) > 2 else ""
            await event.reply(f"✅ Agent attivato su @{target_username}")
            print(f"[CONTROL] Attivazione manuale su @{target_username}")

    elif text.startswith("/stato"):
        me = await client.get_me()
        await event.reply(
            f"🤖 Jack Agent attivo\n"
            f"📱 Account: @{me.username}\n"
            f"🔗 n8n: {N8N_WEBHOOK_URL[:40]}...\n"
            f"✅ Tutto operativo"
        )

# ─── AVVIO ──────────────────────────────────────────────────────────────────
async def main():
    print("🚀 Jack Supporto Agent avviato")
    print(f"📡 Webhook n8n: {N8N_WEBHOOK_URL[:40]}...")

    if SESSION_STRING:
        await client.connect()
    else:
        await client.start(phone=PHONE)

    me = await client.get_me()
    print(f"✅ Connesso come @{me.username} ({me.first_name})")

    # Notifica sul pannello di controllo
    try:
        await client.send_message(
            CONTROL_CHAT_ID,
            "🟢 Jack Agent avviato e operativo\n"
            f"📱 Account: @{me.username}\n"
            "Pronto a ricevere messaggi."
        )
    except Exception as e:
        print(f"[WARN] Notifica controllo fallita: {e}")

    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
