from abc import ABC, abstractproperty
from functools import cache
from pathlib import Path
import json
from sre_parse import State
from typing import Any, Dict
from smart_open import open
import os
from functools import cached_property


class StateClient(ABC):
    def __init__(self, base_path: str):
        self.base_path = base_path

    @property
    def client(self) -> Any:
        """
        Returns:
            Any: The client to use for interacting with the state store.
        """
        raise NotImplementedError

    @property
    def params(self) -> Dict[str, Any]:
        """
        Returns:
            Dict[str, Any]: The parameters to pass to smart_open.open.
        """
        return {
            "client": self.client,
        }


class S3StateClient(StateClient):
    """
    A state client for S3 state stores.
    """

    @cached_property
    def client(self):
        import boto3

        session = boto3.Session(
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        )
        return session.client("s3")


class AzureStateClient(StateClient):
    """
    A state client for Azure Blob Storage state stores.
    """

    @cached_property
    def client(self):
        from azure.storage.blob import BlobServiceClient

        return BlobServiceClient.from_connection_string(
            os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        )


class GCSStateClient(StateClient):
    """
    A state client for Google Cloud Storage state stores.
    """

    @cached_property
    def client(self):
        from google.cloud.storage import Client
        from google.auth.credentials import Credentials

        if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
            service_account_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
            return Client.from_service_account_json(service_account_path)
        elif "GOOGLE_API_TOKEN" in os.environ:
            token = os.environ["GOOGLE_API_TOKEN"]
            credentials = Credentials(token=token)
            return Client(credentials=credentials)
        else:
            raise Exception("No credentials found for Google Cloud Storage")


class LocalStateClient(StateClient):
    """
    A state client for local (and all other) state stores.
    """

    @property
    def params(self) -> dict:
        return {}


def state_client_factory(base_path: str) -> StateClient:
    if base_path.startswith("s3://"):
        return S3StateClient(base_path)
    elif base_path.startswith("azure://"):
        return AzureStateClient(base_path)
    elif base_path.startswith("gs://"):
        return GCSStateClient(base_path)
    else:
        return LocalStateClient(base_path)


class StateManager:
    def __init__(self, base_path: str = ".") -> None:
        """
        Args:
            base_path (str): The base path to store state files in. Defaults to "./state".
        """
        self.base_path = base_path
        self.state_client = state_client_factory(base_path)

    def load(self, state_file_name: str) -> dict:
        """
        Load a state file.

        Args:
            state_file_name (str): The name of the state file to load.

        Returns:
            dict: The contents of the state file.
        """
        if not Path(f"{self.base_path}/{state_file_name}").exists():
            return {}

        with open(
            f"{self.base_path}/{state_file_name}",
            "r",
            transport_params=self.state_client.params,
        ) as state_file:
            return json.loads(state_file.read())

    def save(self, state_file_name: str, state: dict = {}) -> None:
        """
        Save a state file.

        Args:
            state_file_name (str): The name of the state file to save.
        """
        # We first merge with any existing state to ensure that we don't overwrite
        existing_state = self.load(state_file_name)
        merged_state = {**existing_state, **state}

        # Then we write the merged state to the state file
        with open(
            f"{self.base_path}/{state_file_name}",
            "wb",
            transport_params=self.state_client.params,
        ) as state_file:
            state_file.write(json.dumps(merged_state).encode("utf-8"))
