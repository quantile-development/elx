from elx import Runner, Target, Tap
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


def test_config_interpolation_values(runner: Runner):
    """
    Make sure the tap and target names are correct.
    """
    assert runner.interpolation_values["TAP_EXECUTABLE"] == "tap-mock-fixture"
    assert runner.interpolation_values["TARGET_EXECUTABLE"] == "target-jsonl"


def test_config_interpolation_target_values(tap: Tap):
    """
    Make sure the config gets interpolated correctly on the singer side.
    """
    target = Target(
        "target-jsonl",
        config={
            "tap_name": "{TAP_NAME}",
        },
    )

    runner = Runner(tap, target)

    assert runner.target.config["tap_name"] == "tap_mock_fixture"


def test_record_counts(runner: Runner):
    """
    Test that record counts are tracked per stream after a run.
    """
    # Run the extract-load pipeline
    runner.run()

    # Assert that record_counts is populated
    assert runner.record_counts is not None
    assert isinstance(runner.record_counts, dict)

    # Assert that at least one stream has records
    assert len(runner.record_counts) > 0

    # Assert all counts are positive integers
    for stream_name, count in runner.record_counts.items():
        assert isinstance(count, int)
        assert count > 0
