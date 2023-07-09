from pathlib import Path
import json
from smart_open import open
import os


def transport_parameters(base_path) -> dict:
    if base_path.startswith("s3://"):
        return {}
    elif base_path.startswith("azure://"):
        from azure.storage.blob import BlobServiceClient

        return {
            "client": BlobServiceClient.from_connection_string(
                os.environ["AZURE_STORAGE_CONNECTION_STRING"]
            ),
        }
    else:
        return {}


class StateManager:
    def __init__(self, base_path: str = ".") -> None:
        """
        Args:
            base_path (str): The base path to store state files in. Defaults to "./state".
        """
        self.base_path = base_path

    def load(self, state_file_name: str) -> dict:
        """
        Load a state file.

        Args:
            state_file_name (str): The name of the state file to load.

        Returns:
            dict: The contents of the state file.
        """
        with open(
            f"{self.base_path}/{state_file_name}",
            "r",
            transport_params=transport_parameters(self.base_path),
        ) as state_file:
            return json.loads(state_file.read())

    def save(self, state_file_name: str, state: dict = {}) -> None:
        """
        Save a state file.

        Args:
            state_file_name (str): The name of the state file to save.
        """
        with open(
            f"{self.base_path}/{state_file_name}",
            "wb",
            transport_params=transport_parameters(self.base_path),
        ) as state_file:
            state_file.write(json.dumps(state).encode("utf-8"))


if __name__ == "__main__":
    state_manager = StateManager()
    state_manager.save("state.json", {"foo": "bar"})
    print(state_manager.load("state.json"))
