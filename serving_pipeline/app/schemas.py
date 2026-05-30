"""Pydantic schemas for request/response."""
from typing import Optional

from pydantic import BaseModel


class PredictRequest(BaseModel):
    msno: str
    event_time: Optional[str] = None  # "2017-03-15" from Kafka; None = use server time


class BatchPredictRequest(BaseModel):
    msno_list: list[str]
    event_time: Optional[str] = None


class PredictResponse(BaseModel):
    msno: str
    churn_probability: float
    is_churn: int
    member_found: bool = True
    reasons: list[str] = []
