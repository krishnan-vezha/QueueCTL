import pytest
import json
import queuectl


class TestJobSpecificationParser:
    """Test job specification parser for JSON and string formats."""

    def test_valid_json_with_id(self):
        """Test parsing valid JSON with explicit ID."""
        input_str = '{"id": "job123", "command": "echo test"}'
        job_id, command = queuectl.parse_job_spec(input_str)

        assert job_id == "job123"
        assert command == "echo test"

    def test_valid_json_without_id(self):
        """Test parsing valid JSON without ID (should return None for auto-generation)."""
        input_str = '{"command": "sleep 2"}'
        job_id, command = queuectl.parse_job_spec(input_str)

        assert job_id is None
        assert command == "sleep 2"

    def test_invalid_json_fallback_to_string(self):
        """Test that invalid JSON falls back to simple string format."""
        input_str = "echo 'this is not json'"
        job_id, command = queuectl.parse_job_spec(input_str)

        assert job_id is None
        assert command == "echo 'this is not json'"

    def test_simple_string_format(self):
        """Test parsing simple command string format."""
        input_str = "ls -la"
        job_id, command = queuectl.parse_job_spec(input_str)

        assert job_id is None
        assert command == "ls -la"

    def test_json_missing_command_field(self):
        """Test that JSON with missing 'command' field raises ValueError."""
        input_str = '{"id": "job123", "other": "value"}'

        with pytest.raises(ValueError) as exc_info:
            queuectl.parse_job_spec(input_str)

        assert "command" in str(exc_info.value).lower()

    def test_json_with_extra_fields(self):
        """Test that JSON with extra fields still works (only id and command are used)."""
        input_str = '{"id": "job456", "command": "echo hello", "extra": "ignored"}'
        job_id, command = queuectl.parse_job_spec(input_str)

        assert job_id == "job456"
        assert command == "echo hello"

    def test_json_with_complex_command(self):
        """Test parsing JSON with complex command containing special characters."""
        input_str = '{"command": "bash -c \\"echo test && sleep 1\\""}'
        job_id, command = queuectl.parse_job_spec(input_str)

        assert job_id is None
        assert "bash -c" in command

    def test_empty_string_fallback(self):
        """Test that empty string falls back to string format."""
        input_str = ""
        job_id, command = queuectl.parse_job_spec(input_str)

        assert job_id is None
        assert command == ""

    def test_json_with_numeric_id(self):
        """Test parsing JSON with numeric ID (should be preserved as-is)."""
        input_str = '{"id": 12345, "command": "echo test"}'
        job_id, command = queuectl.parse_job_spec(input_str)

        assert job_id == 12345
        assert command == "echo test"
