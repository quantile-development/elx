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

    @property
    def stream_properties(self) -> List[str]:
        """
        List with all property names found in the stream_schema.
        """
        return list(self.stream_schema.get("properties", {}).keys())

    def find_by_breadcrumb(self, breadcrumb: List[str]) -> Optional[dict]:
        """
        Find metadata by breadcrumb.
        """
        for metadata in self.metadata:
            if metadata["breadcrumb"] == breadcrumb:
                return metadata

        return None

    def upsert_metadata(
        self,
        metadata: dict | None,
        breadcrumb: List[str],
        is_selected: bool,
    ) -> None:
        """
        Updates or creates metadata for a given breadcrumb.
        """
        # Update metadata if it exists
        if metadata:
            metadata["metadata"]["selected"] = is_selected
        # Otherwise create the metadata
        else:
            self.metadata.append(
                {"breadcrumb": breadcrumb, "metadata": {"selected": is_selected}}
            )


class Catalog(BaseModel):
    streams: List[Stream] = Field(default_factory=list)

    def deselect(self, patterns: Optional[List[str]]) -> "Catalog":
        # Make a copy of the existing catalog.
        catalog = self.copy(deep=True)

        # Return catalog if no patterns to deselect.
        if patterns is None:
            return catalog

        # Transform patterns to set for quick look-up.
        patterns = set(patterns)

        # Loop through the streams.
        for stream in catalog.streams:
            # Check if the stream name found in deselection patterns.
            is_deselected = (stream.tap_stream_id in patterns) or (
                stream.safe_name in patterns
            )

            # Find the stream metadata.
            metadata = stream.find_by_breadcrumb(breadcrumb=[])

            # Upsert the metadata.
            stream.upsert_metadata(
                metadata=metadata,
                breadcrumb=[],
                is_selected=not is_deselected,
            )

            # Loop over properties of stream.
            for stream_property in stream.stream_properties:
                # Check if stream property found in deselection patterns.
                is_deselected = (
                    f"{stream.safe_name}.{stream_property}" in patterns
                ) or (f"{stream.name}.{stream_property}" in patterns)

                # Find the stream metadata.
                metadata = stream.find_by_breadcrumb(
                    breadcrumb=["properties", stream_property]
                )

                # Upsert the metadata.
                stream.upsert_metadata(
                    metadata=metadata,
                    breadcrumb=["properties", stream_property],
                    is_selected=not is_deselected,
                )

        return catalog

    def select(self, streams: Optional[List[str]]) -> "Catalog":
        # Make a copy of the existing catalog.
        catalog = self.copy(deep=True)

        # Simply return the catalog if no streams are selected.
        if streams is None:
            return catalog

        # Loop through the streams in the catalog.
        for stream in catalog.streams:
            # Find the stream metadata.
            metadata = stream.find_by_breadcrumb(breadcrumb=[])

            # Check if stream is selected
            is_selected = (stream.tap_stream_id in streams) or (
                stream.safe_name in streams
            )

            # Upsert the metadata.
            stream.upsert_metadata(
                metadata=metadata,
                breadcrumb=[],
                is_selected=is_selected,
            )

        return catalog
