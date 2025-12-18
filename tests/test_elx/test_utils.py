from elx import RecordCounter
from elx.utils import interpolate_in_config


def test_interpolate_in_config():
    """
    Test the config interpolation function.
    """
    # Define the config
    config = {
        "key1": "value1",
        "key2": "Hello {WORLD}",
    }
    # Define the expected config
    expected_config = {
        "key1": "value1",
        "key2": "Hello world",
    }
    # Interpolate the config
    interpolated_config = interpolate_in_config(
        config=config,
        interpolation={"WORLD": "world"},
    )
    # Assert that the interpolated config is correct
    assert interpolated_config == expected_config


def test_record_counter_counts_records():
    """
    Test that RecordCounter correctly counts RECORD messages per stream.
    """
    counter = RecordCounter()

    # Simulate Singer RECORD messages
    counter.writelines('{"type": "RECORD", "stream": "users", "record": {"id": 1}}')
    counter.writelines('{"type": "RECORD", "stream": "users", "record": {"id": 2}}')
    counter.writelines('{"type": "RECORD", "stream": "orders", "record": {"id": 1}}')

    assert counter.counts == {"users": 2, "orders": 1}


def test_record_counter_ignores_non_record_messages():
    """
    Test that RecordCounter ignores non-RECORD messages.
    """
    counter = RecordCounter()

    # Simulate different Singer message types
    counter.writelines('{"type": "SCHEMA", "stream": "users", "schema": {}}')
    counter.writelines('{"type": "STATE", "value": {"bookmarks": {}}}')
    counter.writelines('{"type": "RECORD", "stream": "users", "record": {"id": 1}}')

    assert counter.counts == {"users": 1}


def test_record_counter_handles_invalid_json():
    """
    Test that RecordCounter gracefully handles invalid JSON.
    """
    counter = RecordCounter()

    counter.writelines("not valid json")
    counter.writelines('{"type": "RECORD", "stream": "users", "record": {"id": 1}}')

    assert counter.counts == {"users": 1}


def test_record_counter_reset():
    """
    Test that RecordCounter.reset() clears all counts.
    """
    counter = RecordCounter()

    counter.writelines('{"type": "RECORD", "stream": "users", "record": {"id": 1}}')
    assert counter.counts == {"users": 1}

    counter.reset()
    assert counter.counts == {}
