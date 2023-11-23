from typing import List, Optional, Tuple
from pydantic import BaseModel, Field


class Stream(BaseModel):
    tap_stream_id: str
    stream: str = Field(alias="tap_stream_id")
    table_name: Optional[str] = None
    replication_method: Optional[str] = "FULL_TABLE"
    replication_key: Optional[str] = None
    key_properties: List[str]
    stream_schema: dict = Field(alias="schema")
    is_view: Optional[bool] = False
    metadata: List[dict] = Field(default_factory=list)

    @property
    def name(self) -> str:
        """The name of the stream is the stream_id"""
        return self.tap_stream_id

    @property
    def safe_name(self) -> str:
        """"""
        return self.name.replace("-", "_")

    @property
    def is_selected(self) -> bool:
        """Returns boolean flag indicating whether stream has been selected or not."""
        # Find the stream metadata by breadcrumb
        metadata = self.find_metadata_by_breadcrumb(breadcrumb=[])

        # If metadata does not exist, stream should be considered selected
        if not metadata:
            return True

        # If metadata does exists, return value of `selected` property
        return metadata.get("selected", True)

    def find_metadata_by_breadcrumb(self, breadcrumb: List[str]) -> Optional[dict]:
        """
        Find metadata by breadcrumb.
        """
        for metadata in self.metadata:
            if metadata["breadcrumb"] == breadcrumb:
                return metadata["metadata"]

        return None

    def upsert_metadata(
        self,
        breadcrumb: List[str],
        metadata: dict,
    ) -> None:
        """
        Updates or creates metadata for a given breadcrumb.
        """
        # Find metadata by breadcrumb.
        metadata_record = self.find_metadata_by_breadcrumb(breadcrumb=breadcrumb)

        # Update metadata if it exists
        if metadata_record:
            metadata_record.update(metadata)

        # Otherwise add the metadata
        else:
            self.metadata.append(
                {
                    "breadcrumb": breadcrumb,
                    "metadata": metadata,
                }
            )


class Catalog(BaseModel):
    streams: List[Stream] = Field(default_factory=list)

    def find_stream(self, stream_id: str) -> Optional[Stream]:
        """
        Find a stream by stream_id.

        Args:
            stream_id (str): The stream_id to find.

        Returns:
            Optional[Stream]: The stream if found, otherwise None.
        """
        for stream in self.streams:
            if stream.tap_stream_id == stream_id:
                return stream

        return None

    def deselect(
        self,
        patterns: Optional[List[str]] = None,
    ) -> "Catalog":
        """
        Deselect streams and properties from the catalog.

        Args:
            patterns (Optional[List[str]]): List of patterns to deselect. E.g. ["users", "users.email"]

        Returns:
            Catalog: A new catalog with deselected streams and properties.
        """
        # Make a copy of the existing catalog.
        catalog = self.copy(deep=True)

        # Return catalog if no patterns to deselect.
        if patterns is None:
            return catalog

        for pattern in patterns:
            # Split the pattern into nodes.
            nodes = pattern.split(".")

            # Find the stream.
            stream = catalog.find_stream(nodes[0])

            # If an invalid stream is found, skip it.
            if stream is None:
                continue

            # Wether to deselect the stream or a property.
            breadcrumb = ["properties"] + nodes[1:] if len(nodes) > 1 else []

            # Update or create metadata.
            stream.upsert_metadata(
                breadcrumb=breadcrumb,
                metadata={
                    "selected": False,
                },
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
            # Check if stream is selected
            is_selected = (stream.tap_stream_id in streams) or (
                stream.safe_name in streams
            )

            # Upsert the metadata.
            stream.upsert_metadata(
                breadcrumb=[],
                metadata={
                    "selected": is_selected,
                },
            )

        return catalog

    def set_replication_keys(self, replication_keys: Optional[dict]) -> "Catalog":
        """
        Set the replication key for streams and updates the catalog.

        Args:
            keys (Optional[dict]): Dictionary stream replication_key value-pairs.
            E.g. {"stream_one": "updated_at", "stream_two": "modified_at"}

        Returns:
            Catalog: A new catalog with updated replication settings.
        """
        # Make a copy of the existing catalog.
        catalog = self.copy(deep=True)

        # Loop over the streams
        for stream in catalog.streams:
            # If the stream is specified in `replication_keys` dictionary
            if stream.tap_stream_id in replication_keys:
                # Set the replication method to INCREMENTAL
                stream.replication_method = "INCREMENTAL"

                # Update the replication key value
                stream.replication_key = replication_keys[stream.tap_stream_id]

                # Add the replication key to stream metadata
                stream.upsert_metadata(
                    breadcrumb=[],
                    metadata={
                        "valid-replication-keys": [
                            replication_keys[stream.tap_stream_id]
                        ]
                    },
                )

                # Set inclusion of replication property metadata to `automatic`
                stream.upsert_metadata(
                    breadcrumb=["properties", replication_keys[stream.tap_stream_id]],
                    metadata={"inclusion": "automatic"},
                )

        return catalog
