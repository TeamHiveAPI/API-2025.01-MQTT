from pydantic import BaseModel, validator, Extra
from typing import Optional
from datetime import datetime
import pytz

class DadosEstacao(BaseModel):
    uid: str
    unix_time: int
    
    class Config:
        extra = Extra.allow

    @validator('unix_time')
    def convert_to_br_time(cls, v):
        # Converte Unix timestamp para datetime
        utc_time = datetime.fromtimestamp(v, tz=pytz.UTC)
        # Converte para Brazil timezone
        br_tz = pytz.timezone('America/Sao_Paulo')
        br_time = utc_time.astimezone(br_tz)
        return br_time