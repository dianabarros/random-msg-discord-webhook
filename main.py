import os
import random
import asyncio
import logging
import aiohttp

from database import Database
from repository import TweetRepository

logging.basicConfig(level=logging.INFO)

WEBHOOK_URL = os.environ["DISCORD_WEBHOOK_URL"]
DB_URL = os.environ["DB_URL"]

async def send_webhook_message(content: str):
    async with aiohttp.ClientSession() as session:
        async with session.post(
            WEBHOOK_URL,
            json={"content": content},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status >= 300:
                text = await resp.text()
                raise RuntimeError(f"Webhook failed: {resp.status} {text}")

async def main():
    db = Database(DB_URL)
    await db.connect()
    repo = TweetRepository(db)

    try:
        tweets = await repo.get_all()
        if not tweets:
            logging.info("No tweets found.")
            return

        tweet = random.choice(tweets).tweet
        await send_webhook_message(tweet)
        logging.info("Message sent via webhook")

    finally:
        await db.disconnect()

asyncio.run(main())
