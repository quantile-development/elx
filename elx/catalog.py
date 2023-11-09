from typing import List, Optional, Tuple
from pydantic import BaseModel, Field


class Stream(BaseModel):
    tap_stream_id: str
    stream: str = Field(alias="tap_stream_id")
    table_name: Optional[str] = None
    replication_method: Optional[str] = "FULL_TABLE"
    key_properties: List[str]
    stream_schema: dict = Field(alias="schema")
    is_view: Optional[bool] = False
    metadata: List[dict] = Field(default_factory=list)

    @property
    def name(self) -> str:
        return self.tap_stream_id

    @property
    def safe_name(self) -> str:
        return self.name.replace("-", "_")

    def find_by_breadcrumb(self, breadcrumb: List[str]) -> Optional[dict]:
        """
        Find metadata by breadcrumb.
        """
        for metadata in self.metadata:
            if metadata["breadcrumb"] == breadcrumb:
                return metadata

        return None


class Catalog(BaseModel):
    streams: List[Stream] = Field(default_factory=list)

    def select(self, streams: List[str]) -> "Catalog":
        catalog = self.copy()

        for stream in catalog.streams:
            metadata = stream.find_by_breadcrumb([])

            if metadata:
                metadata["metadata"]["selected"] = stream.tap_stream_id in streams

            else:
                stream.metadata.append(
                    {
                        "breadcrumb": [],
                        "metadata": {
                            "selected": stream.tap_stream_id in streams,
                        },
                    }
                )

        return catalog


# class Schema(BaseModel):
#     properties: dict


# class CatalogSelector:
#     def __init__(self, catalog: dict):
#         self.catalog = catalog

#     def update(self, update: dict):
#         self.catalog = catalog

#     def filter(
#         self,
#         selected: List[str] = ["*.*"],
#         deselected: List[str] = [],
#     ) -> dict:
#         return self.catalog
