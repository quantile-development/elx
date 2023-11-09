import pytest
from elx import Tap
from elx.catalog import Catalog, Stream

DEFAULT_CATALOG = {
    "streams": [
        {
            "tap_stream_id": "animals",
            "replication_method": "FULL_TABLE",
            "table_name": None,
            "is_view": False,
            "key_properties": [],
            "schema": {
                "properties": {
                    "id": {"type": "integer"},
                    "description": {"type": "string"},
                    "verified": {"type": "boolean"},
                    "views": {"type": "integer"},
                    "created_at": {"type": "string"},
                },
                "type": "object",
                "required": ["created_at", "description", "id", "verified", "views"],
            },
            "metadata": [
                {
                    "breadcrumb": ["properties", "id"],
                    "metadata": {"inclusion": "available"},
                },
                {
                    "breadcrumb": ["properties", "description"],
                    "metadata": {"inclusion": "available"},
                },
                {
                    "breadcrumb": ["properties", "verified"],
                    "metadata": {"inclusion": "available"},
                },
                {
                    "breadcrumb": ["properties", "views"],
                    "metadata": {"inclusion": "available"},
                },
                {
                    "breadcrumb": ["properties", "created_at"],
                    "metadata": {"inclusion": "available"},
                },
                {
                    "breadcrumb": [],
                    "metadata": {
                        "inclusion": "available",
                        "selected": True,
                        "selected-by-default": True,
                        "table-key-properties": [],
                    },
                },
            ],
        }
    ]
}


def test_catalog(tap: Tap):
    """
    Make sure that the catalog selector returns the same catalog if no streams are selected.
    """
    assert tap.catalog.dict(by_alias=True) == DEFAULT_CATALOG


def test_catalog_update(tap: Tap):
    catalog = tap.catalog.select(["animals"])

    assert (
        catalog.dict(by_alias=True)["streams"][0]["metadata"][-1]["metadata"][
            "selected"
        ]
        == True
    )

    catalog = tap.catalog.select([])

    assert (
        catalog.dict(by_alias=True)["streams"][0]["metadata"][-1]["metadata"][
            "selected"
        ]
        == False
    )

    print(catalog.dict(by_alias=True))
