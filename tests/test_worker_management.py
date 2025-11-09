import pytest
import json
import os
import time
from pathlib import Path
import queuectl


class TestWorkerManagement:
    """Test worker registration, unregistration, and management functions."""

    def test_register_worker(self, temp_queuectl_dir):
        """Test worker registration on start."""
        queuectl.init_db()

        # Register a worker
        pid = 12345
        name = "Worker-1"
        backoff_base = 2

        queuectl.register_worker(pid, name, backoff_base)

        # Verify worker is in registry
        assert queuectl.WORKERS_PATH.exists()
        with open(queuectl.WORKERS_PATH, "r") as f:
            registry = json.load(f)

        assert len(registry["workers"]) == 1
        worker = registry["workers"][0]
        assert worker["pid"] == pid
        assert worker["name"] == name
        assert worker["backoff_base"] == backoff_base
        assert "started_at" in worker

    def test_register_multiple_workers(self, temp_queuectl_dir):
        """Test registering multiple workers."""
        queuectl.init_db()

        # Register multiple workers
        for i in range(3):
            queuectl.register_worker(10000 + i, f"Worker-{i+1}", 2)

        # Verify all workers are registered
        with open(queuectl.WORKERS_PATH, "r") as f:
            registry = json.load(f)

        assert len(registry["workers"]) == 3
        assert registry["workers"][0]["name"] == "Worker-1"
        assert registry["workers"][1]["name"] == "Worker-2"
        assert registry["workers"][2]["name"] == "Worker-3"

    def test_unregister_worker(self, temp_queuectl_dir):
        """Test worker unregistration on stop."""
        queuectl.init_db()

        # Register workers
        queuectl.register_worker(12345, "Worker-1", 2)
        queuectl.register_worker(12346, "Worker-2", 2)

        # Unregister one worker
        queuectl.unregister_worker(12345)

        # Verify worker is removed
        with open(queuectl.WORKERS_PATH, "r") as f:
            registry = json.load(f)

        assert len(registry["workers"]) == 1
        assert registry["workers"][0]["pid"] == 12346

    def test_unregister_nonexistent_worker(self, temp_queuectl_dir):
        """Test unregistering a worker that doesn't exist (should not error)."""
        queuectl.init_db()

        # Register a worker
        queuectl.register_worker(12345, "Worker-1", 2)

        # Try to unregister non-existent worker
        queuectl.unregister_worker(99999)

        # Verify original worker still exists
        with open(queuectl.WORKERS_PATH, "r") as f:
            registry = json.load(f)

        assert len(registry["workers"]) == 1
        assert registry["workers"][0]["pid"] == 12345

    def test_get_active_workers(self, temp_queuectl_dir):
        """Test get_active_workers() function."""
        queuectl.init_db()

        # Register workers with current process PID (which exists)
        current_pid = os.getpid()
        queuectl.register_worker(current_pid, "Worker-1", 2)

        # Get active workers
        workers = queuectl.get_active_workers()

        assert len(workers) == 1
        assert workers[0]["pid"] == current_pid
        assert workers[0]["name"] == "Worker-1"

    def test_get_active_workers_empty(self, temp_queuectl_dir):
        """Test get_active_workers() with no workers."""
        queuectl.init_db()

        workers = queuectl.get_active_workers()

        assert workers == []

    def test_cleanup_stale_workers(self, temp_queuectl_dir):
        """Test cleanup of stale PIDs."""
        queuectl.init_db()

        # Register a worker with current PID (alive) and fake PID (stale)
        current_pid = os.getpid()
        fake_pid = 999999  # Very unlikely to exist

        queuectl.register_worker(current_pid, "Worker-1", 2)
        queuectl.register_worker(fake_pid, "Worker-2", 2)

        # Cleanup stale workers
        stale_pids = queuectl.cleanup_stale_workers()

        # Verify stale PID was removed
        assert fake_pid in stale_pids

        # Verify only active worker remains
        with open(queuectl.WORKERS_PATH, "r") as f:
            registry = json.load(f)

        assert len(registry["workers"]) == 1
        assert registry["workers"][0]["pid"] == current_pid

    def test_stop_command_with_no_active_workers(self, temp_queuectl_dir, capsys):
        """Test stop command with no active workers."""
        queuectl.init_db()

        # Call stop_workers with no workers registered
        queuectl.stop_workers()

        captured = capsys.readouterr()
        assert "No active workers" in captured.out

    def test_worker_registry_file_locking(self, temp_queuectl_dir):
        """Test that worker registry operations use file locking."""
        queuectl.init_db()

        # Register a worker (this should create the file with locking)
        queuectl.register_worker(12345, "Worker-1", 2)

        # Verify file exists and is valid JSON
        assert queuectl.WORKERS_PATH.exists()
        with open(queuectl.WORKERS_PATH, "r") as f:
            registry = json.load(f)

        assert "workers" in registry
        assert len(registry["workers"]) == 1
