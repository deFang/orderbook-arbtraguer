from typing import Optional

from pydantic import BaseModel, validator

from cross_arbitrage.config.constant import EXCHANGES


class AccountConfig(BaseModel):
    exchange_name: Optional[str] = None
    api_key: str
    secret: str
    password: Optional[str] = None
    description: Optional[str] = None

    @validator("exchange_name")
    def exchange_name_must_in_list(cls, value):
        if value not in EXCHANGES:
            raise ValueError(f"exchange_name must in {','.join(EXCHANGES)}")
        return value
