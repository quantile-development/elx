from pytest import MonkeyPatch
from elx.state import state_client_factory, StateManager
from azure.storage.blob import BlobServiceClient


def test_transport_parameters(monkeypatch: MonkeyPatch):
    monkeypatch.setenv(
        "AZURE_STORAGE_CONNECTION_STRING",
        "DefaultEndpointsProtocol=https;AccountName=xyz;AccountKey=xyz;EndpointSuffix=core.windows.net",
    )
    azure_state_client = state_client_factory("azure://bucket/path")
    assert "client" in azure_state_client.params
    assert type(azure_state_client.params["client"]) == BlobServiceClient
