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
