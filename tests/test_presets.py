"""
Tests for cytotable.presets
"""

from cytotable.presets import config


def test_config():
    """
    Tests config to ensure proper values
    """

    # check that we have relevant keys for each preset
    for config_preset in config.values():
        assert sorted(
            [
                "CONFIG_NAMES_COMPARTMENTS",
                "CONFIG_NAMES_METADATA",
                "CONFIG_IDENTIFYING_COLUMNS",
                "CONFIG_CHUNK_SIZE",
                "CONFIG_CHUNK_COLUMNS",
                "CONFIG_JOINS",
                "CONFIG_SOURCE_VERSION",
            ]
        ) == sorted(config_preset.keys())
