from collections.abc import Mapping
from datetime import date, datetime
from typing import List

import asyncpg
from pydantic import BaseModel

Mapping.register(asyncpg.Record)

class TweetModel(BaseModel):
    id: int
    username: str
    tweet: str
    tweet_dt: datetime
    ingestion_dt: date

class TweetRepository:
    def __init__(self, db):
        self.db = db

    async def get_latest_tweet_id(self):
        query = "SELECT id from public.user_tweets ORDER BY tweet_dt DESC LIMIT 1"
        async with self.db.pool.acquire() as conn:
            return await conn.fetchrow(query)

    async def get_tweet(self, tweet_id: str):
        query = "SELECT * FROM public.user_tweets WHERE id = $1"
        async with self.db.pool.acquire() as conn:
            return await conn.fetchrow(query, tweet_id)

    async def get_all(self):
        query = "SELECT * FROM public.user_tweets ORDER BY tweet_dt DESC"
        async with self.db.pool.acquire() as conn:
            records = await conn.fetch(query)
            result = [TweetModel.model_validate(record) for record in records]
            return result
