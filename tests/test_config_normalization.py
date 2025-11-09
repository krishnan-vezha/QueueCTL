import pytest
import json
import queuectl


class TestConfigurationNormalization:
    """Test configuration key normalization between hyphenated and underscore formats."""

    def test_normalize_config_key_hyphen_to_underscore(self):
        """Test normalize_config_key() converts hyphens to underscores."""
        assert queuectl.normalize_config_key("max-retries") == "max_retries"
        assert queuectl.normalize_config_key("backoff-base") == "backoff_base"

    def test_normalize_config_key_already_underscore(self):
        """Test normalize_config_key() handles already-underscore keys."""
        assert queuectl.normalize_config_key("max_retries") == "max_retries"
        assert queuectl.normalize_config_key("backoff_base") == "backoff_base"

    def test_normalize_config_key_no_separator(self):
        """Test normalize_config_key() handles keys without separators."""
        assert queuectl.normalize_config_key("timeout") == "timeout"
        assert queuectl.normalize_config_key("workers") == "workers"

    def test_display_config_key_underscore_to_hyphen(self):
        """Test display_config_key() converts underscores to hyphens."""
        assert queuectl.display_config_key("max_retries") == "max-retries"
        assert queuectl.display_config_key("backoff_base") == "backoff-base"

    def test_display_config_key_already_hyphen(self):
        """Test display_config_key() handles already-hyphenated keys."""
        assert queuectl.display_config_key("max-retries") == "max-retries"
        assert queuectl.display_config_key("backoff-base") == "backoff-base"

    def test_display_config_key_no_separator(self):
        """Test display_config_key() handles keys without separators."""
        assert queuectl.display_config_key("timeout") == "timeout"
        assert queuectl.display_config_key("workers") == "workers"

    def test_round_trip_conversion_hyphen_to_underscore_to_hyphen(self):
        """Test round-trip conversion: hyphen → underscore → hyphen."""
        original = "max-retries"
        normalized = queuectl.normalize_config_key(original)
        displayed = queuectl.display_config_key(normalized)

        assert normalized == "max_retries"
        assert displayed == "max-retries"
        assert displayed == original

    def test_round_trip_conversion_underscore_to_hyphen_to_underscore(self):
        """Test round-trip conversion: underscore → hyphen → underscore."""
        original = "backoff_base"
        displayed = queuectl.display_config_key(original)
        normalized = queuectl.normalize_config_key(displayed)

        assert displayed == "backoff-base"
        assert normalized == "backoff_base"
        assert normalized == original

    def test_config_set_with_hyphenated_keys(self, temp_queuectl_dir):
        """Test config set with hyphenated keys stores as underscore format."""
        queuectl.init_db()

        # Set config using hyphenated key
        hyphenated_key = "max-retries"
        normalized_key = queuectl.normalize_config_key(hyphenated_key)
        queuectl.set_config(normalized_key, 5)

        # Verify it's stored with underscore format
        with open(queuectl.CONFIG_PATH, "r") as f:
            config = json.load(f)

        assert "max_retries" in config
        assert config["max_retries"] == 5
        assert "max-retries" not in config

    def test_config_get_displays_hyphenated_keys(self, temp_queuectl_dir):
        """Test config get displays hyphenated keys."""
        queuectl.init_db()

        # Set config with underscore format (internal storage)
        queuectl.set_config("max_retries", 7)
        queuectl.set_config("backoff_base", 3)

        # Get config and convert to display format
        config = queuectl.get_config()
        display_config = {queuectl.display_config_key(k): v for k, v in config.items()}

        assert "max-retries" in display_config
        assert "backoff-base" in display_config
        assert display_config["max-retries"] == 7
        assert display_config["backoff-base"] == 3

    def test_config_normalization_with_multiple_hyphens(self):
        """Test normalization with multiple hyphens/underscores."""
        assert (
            queuectl.normalize_config_key("some-long-key-name") == "some_long_key_name"
        )
        assert queuectl.display_config_key("some_long_key_name") == "some-long-key-name"

    def test_config_set_and_get_integration(self, temp_queuectl_dir):
        """Test full integration of config set and get with normalization."""
        queuectl.init_db()

        # Set config using hyphenated key (as user would input)
        user_key = "max-retries"
        normalized_key = queuectl.normalize_config_key(user_key)
        queuectl.set_config(normalized_key, 10)

        # Get config value using normalized key
        value = queuectl.get_config(normalized_key)
        assert value == 10

        # Verify display format
        display_key = queuectl.display_config_key(normalized_key)
        assert display_key == "max-retries"
