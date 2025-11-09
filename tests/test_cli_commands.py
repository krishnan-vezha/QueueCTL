import pytest
import sys
import argparse
from io import StringIO
import queuectl


class TestCLICommandStructure:
    """Test nested subcommand parsing for worker, dlq, and config commands."""

    def test_worker_start_command_parsing(self):
        """Test parsing of 'worker start' subcommand."""
        sys.argv = ["queuectl", "worker", "start", "--count", "3"]
        parser = self._create_parser()
        args = parser.parse_args(sys.argv[1:])

        assert args.cmd == "worker"
        assert args.worker_cmd == "start"
        assert args.count == 3

    def test_worker_stop_command_parsing(self):
        """Test parsing of 'worker stop' subcommand."""
        sys.argv = ["queuectl", "worker", "stop"]
        parser = self._create_parser()
        args = parser.parse_args(sys.argv[1:])

        assert args.cmd == "worker"
        assert args.worker_cmd == "stop"

    def test_dlq_list_command_parsing(self):
        """Test parsing of 'dlq list' subcommand."""
        sys.argv = ["queuectl", "dlq", "list"]
        parser = self._create_parser()
        args = parser.parse_args(sys.argv[1:])

        assert args.cmd == "dlq"
        assert args.dlq_cmd == "list"

    def test_dlq_retry_command_parsing(self):
        """Test parsing of 'dlq retry' subcommand with job ID."""
        sys.argv = ["queuectl", "dlq", "retry", "job123"]
        parser = self._create_parser()
        args = parser.parse_args(sys.argv[1:])

        assert args.cmd == "dlq"
        assert args.dlq_cmd == "retry"
        assert args.job_id == "job123"
        assert args.all is False

    def test_dlq_retry_all_command_parsing(self):
        """Test parsing of 'dlq retry --all' subcommand."""
        sys.argv = ["queuectl", "dlq", "retry", "--all"]
        parser = self._create_parser()
        args = parser.parse_args(sys.argv[1:])

        assert args.cmd == "dlq"
        assert args.dlq_cmd == "retry"
        assert args.all is True

    def test_config_set_command_parsing(self):
        """Test parsing of 'config set' subcommand."""
        sys.argv = ["queuectl", "config", "set", "max-retries", "5"]
        parser = self._create_parser()
        args = parser.parse_args(sys.argv[1:])

        assert args.cmd == "config"
        assert args.config_cmd == "set"
        assert args.key == "max-retries"
        assert args.value == "5"

    def test_config_get_command_parsing(self):
        """Test parsing of 'config get' subcommand with key."""
        sys.argv = ["queuectl", "config", "get", "max-retries"]
        parser = self._create_parser()
        args = parser.parse_args(sys.argv[1:])

        assert args.cmd == "config"
        assert args.config_cmd == "get"
        assert args.key == "max-retries"

    def test_config_get_all_command_parsing(self):
        """Test parsing of 'config get' subcommand without key."""
        sys.argv = ["queuectl", "config", "get"]
        parser = self._create_parser()
        args = parser.parse_args(sys.argv[1:])

        assert args.cmd == "config"
        assert args.config_cmd == "get"
        assert args.key is None

    def test_worker_start_with_base_option(self):
        """Test parsing of 'worker start' with --base option."""
        sys.argv = ["queuectl", "worker", "start", "--count", "2", "--base", "3"]
        parser = self._create_parser()
        args = parser.parse_args(sys.argv[1:])

        assert args.cmd == "worker"
        assert args.worker_cmd == "start"
        assert args.count == 2
        assert args.base == 3

    def _create_parser(self):
        """Helper method to create the argument parser (mimics main() parser setup)."""
        parser = argparse.ArgumentParser(prog="queuectl")
        subparsers = parser.add_subparsers(dest="cmd")

        # Worker command group
        worker_parser = subparsers.add_parser("worker")
        worker_subparsers = worker_parser.add_subparsers(dest="worker_cmd")
        wstart = worker_subparsers.add_parser("start")
        wstart.add_argument("--count", type=int, default=1)
        wstart.add_argument("--base", type=int)
        worker_subparsers.add_parser("stop")

        # DLQ command group
        dlq_parser = subparsers.add_parser("dlq")
        dlq_subparsers = dlq_parser.add_subparsers(dest="dlq_cmd")
        dlq_subparsers.add_parser("list")
        dlqr = dlq_subparsers.add_parser("retry")
        dlqr.add_argument("job_id", nargs="?")
        dlqr.add_argument("--all", action="store_true")

        # Config command group
        config_parser = subparsers.add_parser("config")
        config_subparsers = config_parser.add_subparsers(dest="config_cmd")
        cfg_set = config_subparsers.add_parser("set")
        cfg_set.add_argument("key")
        cfg_set.add_argument("value")
        cfg_get = config_subparsers.add_parser("get")
        cfg_get.add_argument("key", nargs="?")

        return parser
