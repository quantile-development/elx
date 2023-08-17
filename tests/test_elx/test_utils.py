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
