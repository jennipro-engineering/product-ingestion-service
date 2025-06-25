from pydantic import BaseModel,Field
from typing import List, Dict, Any

class IngestionPayload(BaseModel):
    source: str
    timestamp:str
    payload: List[Dict[str, Any]]=Field(..., description="gtin/upc, price and location are mandatory fields")