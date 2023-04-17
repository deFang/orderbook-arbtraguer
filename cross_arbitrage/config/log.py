from pydantic import BaseModel, validator

from cross_arbitrage.config.constant import EXCHANGES, LOG_LEVELS


class LogConfig(BaseModel):
    level: str
    dir: str = "logs"

    @validator("level")
    def log_level_must_in_list(cls, value):
        if value not in LOG_LEVELS:
            raise ValueError(f"log level must in {','.join(LOG_LEVELS)}")
        return value
