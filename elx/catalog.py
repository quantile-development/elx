from typing import List
from pydantic import BaseModel, Field


class Schema(BaseModel):
    properties: dict


class Stream(BaseModel):
    tap_stream_id: str
    replication_method: str
    key_properties: List[str]
    stream_schema: Schema = Field(alias="schema")

    @property
    def name(self) -> str:
        return self.tap_stream_id

    @property
    def safe_name(self) -> str:
        return self.name.replace("-", "_")


class Catalog(BaseModel):
    streams: List[Stream]
