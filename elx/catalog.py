from typing import List, Optional
from pydantic import BaseModel, Field


class Schema(BaseModel):
    properties: dict


class Stream(BaseModel):
    tap_stream_id: str
    replication_method: Optional[str] = "FULL_REFRESH"
    key_properties: List[str]
    stream_schema: Schema = Field(alias="schema")

    @property
    def name(self) -> str:
        return self.tap_stream_id

    @property
    def safe_name(self) -> str:
        return self.name.replace("-", "_")


class CatalogSelector:
    def __init__(self, catalog: dict):
        self.catalog = catalog

    def filter(
        self,
        selected: List[str] = ["*.*"],
        deselected: List[str] = [],
    ) -> dict:
        return self.catalog
