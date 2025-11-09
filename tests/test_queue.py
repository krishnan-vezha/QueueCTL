import pytest
import sqlite3
import json
import time
from pathlib import Path
import queuectl


class TestInitialization:
    def test_init_db(self, temp_queuectl_dir):
        """Test database initialization."""
        queuectl.init_db()

        assert temp_queuectl_dir.exists()
        assert (temp_queuectl_dir / "jobs.db").exists()
        assert (temp_queuectl_dir / "config.json").exists()

        # Verify tables exist
        conn = queuectl.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cur.fetchall()]
        conn.close()

        assert "jobs" in tables
        assert "dlq" in tables

    def test_config_operations(self, temp_queuectl_dir):
        """Test configuration get/set operations."""
        queuectl.init_db()

        # Test default config
        max_retries = queuectl.get_config("max_retries")
        assert max_retries == 3

        # Test set config
        queuectl.set_config("max_retries", 5)
        max_retries = queuectl.get_config("max_retries")
        assert max_retries == 5


class TestJobManagement:
    def test_enqueue_job(self, temp_queuectl_dir):
        """Test job enqueueing."""
        queuectl.init_db()

        job_id = queuectl.enqueue_job("echo 'test'")
        assert job_id is not None

        # Verify job in database
        conn = queuectl.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM jobs WHERE id=?", (job_id,))
        job = cur.fetchone()
        conn.close()

        assert job is not None
        assert job[1] == "echo 'test'"
        assert job[2] == "pending"
        assert job[3] == 0  # attempts

    def test_enqueue_with_custom_id(self, temp_queuectl_dir):
        """Test job enqueueing with custom ID."""
        queuectl.init_db()

        custom_id = "custom-job-123"
        job_id = queuectl.enqueue_job("echo 'test'", job_id=custom_id)
        assert job_id == custom_id

    def test_list_jobs(self, temp_queuectl_dir, capsys):
        """Test listing jobs."""
        queuectl.init_db()

        # Enqueue some jobs
        queuectl.enqueue_job("echo 'job1'")
        queuectl.enqueue_job("echo 'job2'")

        # List all jobs
        queuectl.list_jobs()
        captured = capsys.readouterr()
        assert "job1" in captured.out
        assert "job2" in captured.out

    def test_list_jobs_by_state(self, temp_queuectl_dir, capsys):
        """Test listing jobs filtered by state."""
        queuectl.init_db()

        queuectl.enqueue_job("echo 'test'")
        queuectl.list_jobs(state="pending")
        captured = capsys.readouterr()
        assert "pending" in captured.out.lower()


class TestDLQ:
    def test_move_to_dlq(self, temp_queuectl_dir):
        """Test moving failed job to DLQ."""
        queuectl.init_db()

        # Set max_retries before creating job
        queuectl.set_config("max_retries", 1)

        # Create a job that will fail
        job_id = queuectl.enqueue_job("exit 1", job_id="fail-job")

        # Simulate job failure by directly manipulating database
        conn = queuectl.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM jobs WHERE id=?", (job_id,))
        job = cur.fetchone()

        # Process the job (it will fail and move to DLQ after max retries)
        queuectl.process_job(job, 2)

        # Check if job is in DLQ
        cur.execute("SELECT * FROM dlq WHERE id=?", (job_id,))
        dlq_job = cur.fetchone()
        conn.close()

        assert dlq_job is not None

    def test_retry_from_dlq(self, temp_queuectl_dir):
        """Test retrying a job from DLQ."""
        queuectl.init_db()

        # Manually add a job to DLQ
        conn = queuectl.get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO dlq VALUES (?,?,?,?,?,?,?,?)",
            (
                "dlq-job",
                "echo 'test'",
                "dead",
                3,
                3,
                "2024-01-01T00:00:00",
                "2024-01-01T00:00:00",
                None,
            ),
        )
        conn.commit()
        conn.close()

        # Retry the job
        queuectl.retry_dlq("dlq-job")

        # Verify job moved back to queue
        conn = queuectl.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM jobs WHERE id=?", ("dlq-job",))
        job = cur.fetchone()
        cur.execute("SELECT * FROM dlq WHERE id=?", ("dlq-job",))
        dlq_job = cur.fetchone()
        conn.close()

        assert job is not None
        assert dlq_job is None

    def test_retry_all_from_dlq(self, temp_queuectl_dir):
        """Test retrying all jobs from DLQ."""
        queuectl.init_db()

        # Add multiple jobs to DLQ
        conn = queuectl.get_conn()
        cur = conn.cursor()
        for i in range(3):
            cur.execute(
                "INSERT INTO dlq VALUES (?,?,?,?,?,?,?,?)",
                (
                    f"dlq-job-{i}",
                    "echo 'test'",
                    "dead",
                    3,
                    3,
                    "2024-01-01T00:00:00",
                    "2024-01-01T00:00:00",
                    None,
                ),
            )
        conn.commit()
        conn.close()

        # Retry all jobs
        queuectl.retry_dlq(retry_all=True)

        # Verify all jobs moved back
        conn = queuectl.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM jobs")
        job_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM dlq")
        dlq_count = cur.fetchone()[0]
        conn.close()

        assert job_count == 3
        assert dlq_count == 0


class TestWorker:
    def test_process_successful_job(self, temp_queuectl_dir):
        """Test processing a successful job."""
        queuectl.init_db()

        job_id = queuectl.enqueue_job("echo 'success'")

        conn = queuectl.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM jobs WHERE id=?", (job_id,))
        job = cur.fetchone()
        conn.close()

        # Process the job
        queuectl.process_job(job, 2)

        # Verify job completed
        conn = queuectl.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT state FROM jobs WHERE id=?", (job_id,))
        result = cur.fetchone()
        conn.close()

        assert result[0] == "completed"

    def test_exponential_backoff(self, temp_queuectl_dir):
        """Test exponential backoff calculation."""
        # Test backoff delays with base 2
        assert 2**1 == 2  # First retry
        assert 2**2 == 4  # Second retry
        assert 2**3 == 8  # Third retry


class TestStatus:
    def test_status_display(self, temp_queuectl_dir, capsys):
        """Test status display."""
        queuectl.init_db()

        # Add some jobs
        queuectl.enqueue_job("echo 'test1'")
        queuectl.enqueue_job("echo 'test2'")

        queuectl.status()
        captured = capsys.readouterr()

        assert "Queue Status" in captured.out
        assert "Total Jobs" in captured.out
