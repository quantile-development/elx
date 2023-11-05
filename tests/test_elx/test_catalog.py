import pytest
from elx.catalog import CatalogSelector
from elx import Tap


def test_catalog_selector_no_select(tap: Tap):
    """
    Make sure that the catalog selector returns the same catalog if no streams are selected.
    """
    catalog_selector = CatalogSelector(tap.catalog)
    catalog = catalog_selector.filter()
    assert catalog == tap.catalog


def test_catalog_selector_select(tap: Tap):
    """
    Make sure the right properties are selected.
    """
    catalog_selector = CatalogSelector(tap.catalog)
    catalog = catalog_selector.filter(selected=["animals.id"])
    assert catalog == tap.catalog
