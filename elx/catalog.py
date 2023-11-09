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

    def select(self, streams: Optional[List[str]]) -> "Catalog":
        # Make a copy of the existing catalog.
        catalog = self.copy(deep=True)

        # Simply return the catalog if no streams are selected.
        if streams is None:
            return catalog

        # Loop through the streams in the catalog.
        for stream in catalog.streams:
            # Find the stream metadata.
            metadata = stream.find_by_breadcrumb([])

            # Update the metadata if it exists.
            if metadata:
                metadata["metadata"]["selected"] = (
                    stream.tap_stream_id in streams
                ) or (stream.safe_name in streams)

            # Otherwise, create the metadata.
            else:
                stream.metadata.append(
                    {
                        "breadcrumb": [],
                        "metadata": {
                            "selected": (stream.tap_stream_id in streams)
                            or (stream.safe_name in streams),
                        },
                    }
                )

        return catalog
