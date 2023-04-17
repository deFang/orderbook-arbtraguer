from pydantic import BaseModel


class RedisConfig(BaseModel):
    url: str
    orderbook_stream: str
