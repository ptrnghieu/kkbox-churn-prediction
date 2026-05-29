"""Pydantic schemas for request/response."""
from pydantic import BaseModel


class PredictRequest(BaseModel):
    msno: str


class BatchPredictRequest(BaseModel):
    msno_list: list[str]


class PredictResponse(BaseModel):
    msno: str
    churn_probability: float
    is_churn: int
    member_found: bool = True
