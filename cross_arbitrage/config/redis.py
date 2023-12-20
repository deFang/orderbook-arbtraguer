from pydantic import BaseModel


class RedisConfig(BaseModel):
    url: str
    orderbook_stream: str
    orderbook_stream_size: int = 2000000
