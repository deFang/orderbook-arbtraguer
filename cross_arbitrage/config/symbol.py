from typing import List, Optional

from pydantic import BaseModel, validator


class ThresholdConfig(BaseModel):
    increase_position_threshold: Optional[float] = None
    decrease_position_threshold: Optional[float] = None
    cancel_increase_position_threshold: Optional[float] = None
    cancel_decrease_position_threshold: Optional[float] = None
    cancel_position_timeout: Optional[float] = None


class SymbolConfig(BaseModel):
    symbol_name: str
    makeonly_exchange_name: Optional[str] = None
    long_threshold_data: ThresholdConfig = ThresholdConfig()
    short_threshold_data: ThresholdConfig = ThresholdConfig()
    max_notional_per_order: Optional[str] = None
    max_notional_per_symbol: Optional[str] = None

    @validator("makeonly_exchange_name")
    def makeonly_exchange_name_must_in_list(cls, value, values):
        if (
            values.get("exchange_pair_names")
            and value not in values["exchange_pair_names"]
        ):
            raise ValueError(
                f"exchange_name must in {','.join(values['exchange_pair_names'])}"
            )
        return value
