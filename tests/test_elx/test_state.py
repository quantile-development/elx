from anyio import Path
from click import File
import pytest
from pytest import MonkeyPatch
from elx.state import state_client_factory, StateManager
from azure.storage.blob import BlobServiceClient
from google.cloud.storage import Client


def test_transport_parameters_s3(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "abc123")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "abc123")
    s3_state_client = state_client_factory("s3://bucket/path")
    assert "client" in s3_state_client.params


def test_transport_parameters_azure(monkeypatch: MonkeyPatch):
    monkeypatch.setenv(
        "AZURE_STORAGE_CONNECTION_STRING",
        "DefaultEndpointsProtocol=https;AccountName=xyz;AccountKey=xyz;EndpointSuffix=core.windows.net",
    )
    azure_state_client = state_client_factory("azure://bucket/path")
    assert "client" in azure_state_client.params
    assert type(azure_state_client.params["client"]) == BlobServiceClient


def test_transport_parameters_gcs_credentials(monkeypatch: MonkeyPatch):
    with pytest.raises(FileNotFoundError):
        monkeypatch.setenv(
            "GOOGLE_APPLICATION_CREDENTIALS", "/path/to/credentials.json"
        )
        gcs_state_client = state_client_factory("gs://bucket/path")
        assert "client" in gcs_state_client.params
        assert type(gcs_state_client.params["client"]) == Client


def test_transport_parameters_gcs_token(monkeypatch: MonkeyPatch):
    with pytest.raises(TypeError):
        monkeypatch.setenv("GOOGLE_API_TOKEN", "abc123")
        gcs_state_client = state_client_factory("gs://bucket/path")
        assert "client" in gcs_state_client.params
        assert type(gcs_state_client.params["client"]) == Client


@pytest.mark.asyncio
async def test_state_save(state_manager: StateManager):
    """
    Test that state is saved to the correct path.
    """
    state_manager.save("test.json", {"foo": "bar"})
    assert await Path(state_manager.base_path, "test.json").exists()


def test_state_merge(state_manager: StateManager):
    state_manager.save("test.json", {"foo": "bar"})
    state_manager.save("test.json", {"bar": "foo"})
    assert state_manager.load("test.json") == {"foo": "bar", "bar": "foo"}
