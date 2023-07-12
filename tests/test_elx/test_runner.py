from elx import Runner
from pathlib import Path


def test_extract_load(runner: Runner):
    """
    Test that the extract-load pipeline runs successfully.
    """
    # Run the extract-load pipeline
    runner.run()

    # Assert that the target created at least one json file
    json_files = Path(runner.target.config["destination_path"]).glob("*.jsonl")
    assert len(list(json_files)) > 0

    # Assert that the state file was created
    state_file = Path(runner.state_manager.base_path).glob("*.json")
    assert len(list(state_file)) == 1
