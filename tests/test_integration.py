import pytest
import json
import os
import time
import signal
from multiprocessing import Process
import queuectl


class TestIntegration:
    """Integration tests for complete workflows and end-to-end scenarios."""

    def test_worker_lifecycle_complete(self, temp_queuectl_dir):
        """Test complete worker lifecycle: start → register → stop → cleanup."""
        queuectl.init_db()

        # Start a worker process
        def worker_process():
            """Simple worker that runs for a short time."""
            queuectl.worker_loop(2)

        # Create and start worker
        worker = Process(target=worker_process, name="TestWorker-1")
        worker.start()

        # Register the worker
        queuectl.register_worker(worker.pid, worker.name, 2)

        # Verify worker is registered
        workers = queuectl.get_active_workers()
        assert len(workers) == 1
        assert workers[0]["pid"] == worker.pid
        assert workers[0]["name"] == worker.name

        # Stop the worker
        try:
            os.kill(worker.pid, signal.SIGTERM)
            worker.join(timeout=2)
            if worker.is_alive():
                worker.kill()
                worker.join()
        except:
            pass

        # Unregister the worker
        queuectl.unregister_worker(worker.pid)

        # Verify worker is unregistered
        workers = queuectl.get_active_workers()
        assert len(workers) == 0

        # Verify registry is cleaned up
        with open(queuectl.WORKERS_PATH, "r") as f:
            registry = json.load(f)
        assert len(registry["workers"]) == 0

    def test_end_to_end_job_processing_json_format(self, temp_queuectl_dir):
        """Test end-to-end job processing with JSON format."""
        queuectl.init_db()

        # Enqueue a job using JSON format with explicit ID
        json_spec = '{"id": "test-job-1", "command": "echo integration_test"}'
        job_id, command = queuectl.parse_job_spec(json_spec)
        queuectl.enqueue_job(command, job_id)

        # Verify job is in database
        conn = queuectl.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM jobs WHERE id=?", (job_id,))
        job = cur.fetchone()
        conn.close()

        assert job is not None
        assert job[0] == "test-job-1"
        assert job[1] == "echo integration_test"
        assert job[2] == "pending"

        # Process the job
        queuectl.process_job(job, 2)

        # Verify job completed
        conn = queuectl.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM jobs WHERE id=?", (job_id,))
        completed_job = cur.fetchone()
        conn.close()

        assert completed_job[2] == "completed"
        assert completed_job[7] is None  # No error message

    def test_end_to_end_job_processing_json_without_id(self, temp_queuectl_dir):
        """Test end-to-end job processing with JSON format without explicit ID."""
        queuectl.init_db()

        # Enqueue a job using JSON format without ID (auto-generate)
        json_spec = '{"command": "echo test_auto_id"}'
        job_id, command = queuectl.parse_job_spec(json_spec)
        actual_job_id = queuectl.enqueue_job(command, job_id)

        # Verify job is in database with auto-generated ID
        conn = queuectl.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM jobs WHERE id=?", (actual_job_id,))
        job = cur.fetchone()
        conn.close()

        assert job is not None
        assert job[0] == actual_job_id
        assert job[1] == "echo test_auto_id"
        assert job[2] == "pending"

    def test_configuration_persistence(self, temp_queuectl_dir):
        """Test that configuration changes persist correctly."""
        queuectl.init_db()

        # Set configuration values
        queuectl.set_config("max_retries", 5)
        queuectl.set_config("backoff_base", 3)

        # Verify values are persisted to file
        with open(queuectl.CONFIG_PATH, "r") as f:
            config = json.load(f)

        assert config["max_retries"] == 5
        assert config["backoff_base"] == 3

        # Retrieve values using get_config
        max_retries = queuectl.get_config("max_retries")
        backoff_base = queuectl.get_config("backoff_base")

        assert max_retries == 5
        assert backoff_base == 3

        # Update a value
        queuectl.set_config("max_retries", 7)

        # Verify update persisted
        updated_value = queuectl.get_config("max_retries")
        assert updated_value == 7

        # Verify file was updated
        with open(queuectl.CONFIG_PATH, "r") as f:
            updated_config = json.load(f)

        assert updated_config["max_retries"] == 7
        assert updated_config["backoff_base"] == 3  # Unchanged

    def test_job_retry_with_config_max_retries(self, temp_queuectl_dir):
        """Test that job retry respects max_retries from config."""
        queuectl.init_db()

        # Set max_retries in config
        queuectl.set_config("max_retries", 2)

        # Enqueue a job that will fail
        job_id = queuectl.enqueue_job("exit 1", "failing-job")

        # Get the job
        conn = queuectl.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM jobs WHERE id=?", (job_id,))
        job = cur.fetchone()
        conn.close()

        # Verify job has correct max_retries from config
        assert job[4] == 2  # max_retries field

    def test_multiple_workers_registration(self, temp_queuectl_dir):
        """Test registering and managing multiple workers."""
        queuectl.init_db()

        # Register multiple workers using current PID (which exists)
        current_pid = os.getpid()
        queuectl.register_worker(current_pid, "Worker-1", 2)

        # Verify worker is registered by checking file directly
        with open(queuectl.WORKERS_PATH, "r") as f:
            registry = json.load(f)

        assert len(registry["workers"]) == 1
        assert registry["workers"][0]["pid"] == current_pid

        # Add more workers to registry (for testing unregister)
        # We'll add them directly to test unregister functionality
        registry["workers"].append(
            {
                "pid": current_pid + 1000,
                "name": "Worker-2",
                "started_at": "2025-11-09T10:00:00Z",
                "backoff_base": 2,
            }
        )
        registry["workers"].append(
            {
                "pid": current_pid + 2000,
                "name": "Worker-3",
                "started_at": "2025-11-09T10:00:00Z",
                "backoff_base": 2,
            }
        )

        with open(queuectl.WORKERS_PATH, "w") as f:
            json.dump(registry, f, indent=2)

        # Verify all workers are in registry
        with open(queuectl.WORKERS_PATH, "r") as f:
            registry = json.load(f)
        assert len(registry["workers"]) == 3

        # Unregister one worker
        queuectl.unregister_worker(current_pid + 1000)

        # Verify worker was removed
        with open(queuectl.WORKERS_PATH, "r") as f:
            registry = json.load(f)

        assert len(registry["workers"]) == 2
        remaining_pids = [w["pid"] for w in registry["workers"]]
        assert current_pid + 1000 not in remaining_pids
        assert current_pid in remaining_pids
        assert current_pid + 2000 in remaining_pids

    def test_dlq_retry_integration(self, temp_queuectl_dir):
        """Test moving job to DLQ and retrying it."""
        queuectl.init_db()

        # Set max_retries to 1 for quick failure
        queuectl.set_config("max_retries", 1)

        # Enqueue a failing job
        job_id = queuectl.enqueue_job("exit 1", "dlq-test-job")

        # Get the job
        conn = queuectl.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM jobs WHERE id=?", (job_id,))
        job = cur.fetchone()

        # Process job until it moves to DLQ
        queuectl.process_job(job, 2)

        # Verify job is in DLQ
        cur.execute("SELECT * FROM dlq WHERE id=?", (job_id,))
        dlq_job = cur.fetchone()
        assert dlq_job is not None
        assert dlq_job[0] == job_id

        # Verify job is not in main queue
        cur.execute("SELECT * FROM jobs WHERE id=?", (job_id,))
        main_job = cur.fetchone()
        assert main_job is None

        conn.close()

        # Retry the job from DLQ
        queuectl.retry_dlq(job_id, retry_all=False)

        # Verify job is back in main queue
        conn = queuectl.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM jobs WHERE id=?", (job_id,))
        retried_job = cur.fetchone()
        assert retried_job is not None
        assert retried_job[2] == "pending"
        assert retried_job[3] == 0  # Attempts reset to 0

        # Verify job is not in DLQ
        cur.execute("SELECT * FROM dlq WHERE id=?", (job_id,))
        dlq_job_after = cur.fetchone()
        assert dlq_job_after is None

        conn.close()

    def test_config_normalization_integration(self, temp_queuectl_dir):
        """Test configuration normalization in full workflow."""
        queuectl.init_db()

        # User sets config with hyphenated key
        user_key = "max-retries"
        normalized_key = queuectl.normalize_config_key(user_key)
        queuectl.set_config(normalized_key, 8)

        # Verify stored with underscore format
        with open(queuectl.CONFIG_PATH, "r") as f:
            config = json.load(f)
        assert "max_retries" in config
        assert config["max_retries"] == 8

        # Retrieve and display with hyphenated format
        retrieved_value = queuectl.get_config(normalized_key)
        display_key = queuectl.display_config_key(normalized_key)

        assert retrieved_value == 8
        assert display_key == "max-retries"

        # Enqueue a job and verify it uses the config value
        job_id = queuectl.enqueue_job("echo test", "config-test-job")

        conn = queuectl.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM jobs WHERE id=?", (job_id,))
        job = cur.fetchone()
        conn.close()

        assert job[4] == 8  # max_retries from config
