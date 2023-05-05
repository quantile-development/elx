from pathlib import Path
import json
from smart_open import open


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
        with open(Path(self.base_path) / Path(state_file_name), "r") as state_file:
            return json.load(state_file)

    def save(self, state_file_name: str, state: dict = {}) -> None:
        """
        Save a state file.

        Args:
            state_file_name (str): The name of the state file to save.
        """
        with open(Path(self.base_path) / Path(state_file_name), "w+") as state_file:
            json.dump(state, state_file)


if __name__ == "__main__":
    state_manager = StateManager()
    state_manager.save("state.json")
    state_manager.load("state.json")
