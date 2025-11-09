import os
import sys
import sqlite3
import json
import argparse
import subprocess
import time
import signal
import uuid
import logging
import fcntl
from datetime import datetime
from pathlib import Path
from multiprocessing import Process, current_process

# Paths
BASE_DIR = Path.home() / ".queuectl"
DB_PATH = BASE_DIR / "jobs.db"
CONFIG_PATH = BASE_DIR / "config.json"
WORKERS_PATH = BASE_DIR / "workers.json"

DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_BASE = 2

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("queuectl")


# ========== UTILITIES ==========
def get_conn():
    """Get a connection to the SQLite database."""
    os.makedirs(BASE_DIR, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    return conn


def init_db():
    """Initialize the database and configuration."""
    try:
        os.makedirs(BASE_DIR, exist_ok=True)
        conn = get_conn()
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                command TEXT,
                state TEXT,
                attempts INTEGER,
                max_retries INTEGER,
                created_at TEXT,
                updated_at TEXT,
                error_message TEXT
            )
        """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS dlq (
                id TEXT PRIMARY KEY,
                command TEXT,
                state TEXT,
                attempts INTEGER,
                max_retries INTEGER,
                created_at TEXT,
                updated_at TEXT,
                error_message TEXT
            )
        """
        )

        conn.commit()
        conn.close()

        if not CONFIG_PATH.exists():
            with open(CONFIG_PATH, "w") as f:
                json.dump(
                    {
                        "max_retries": DEFAULT_MAX_RETRIES,
                        "backoff_base": DEFAULT_BACKOFF_BASE,
                    },
                    f,
                    indent=2,
                )

        print("QueueCTL initialized successfully.")
        logger.info("Database initialized at %s", DB_PATH)
    except Exception as e:
        logger.error("Failed to initialize database: %s", e)
        print(f"Error initializing QueueCTL: {e}")
        sys.exit(1)


def normalize_config_key(key):
    """Convert hyphenated keys to underscore format for internal storage."""
    return key.replace("-", "_")


def display_config_key(key):
    """Convert underscore keys to hyphenated format for display."""
    return key.replace("_", "-")


def get_config(key=None, default=None):
    """Get configuration value(s) from config file."""
    if not CONFIG_PATH.exists():
        return default
    with open(CONFIG_PATH, "r") as f:
        cfg = json.load(f)
    return cfg.get(key, default) if key else cfg


def set_config(key, value):
    """Set a configuration value in the config file."""
    cfg = get_config() or {}
    cfg[key] = value
    with open(CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)


# ========== WORKER REGISTRY ==========
def register_worker(pid, name, backoff_base):
    """Register a worker process in the worker registry."""
    try:
        os.makedirs(BASE_DIR, exist_ok=True)

        # Read existing registry with file locking
        if WORKERS_PATH.exists():
            with open(WORKERS_PATH, "r") as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                registry = json.load(f)
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        else:
            registry = {"workers": []}

        # Add new worker
        worker_entry = {
            "pid": pid,
            "name": name,
            "started_at": datetime.utcnow().isoformat(),
            "backoff_base": backoff_base,
        }
        registry["workers"].append(worker_entry)

        # Write registry with file locking
        with open(WORKERS_PATH, "w") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            json.dump(registry, f, indent=2)
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)

        logger.info("Registered worker %s (PID: %d)", name, pid)
    except Exception as e:
        logger.error("Failed to register worker %s: %s", name, e)


def unregister_worker(pid):
    """Remove a worker process from the worker registry."""
    try:
        if not WORKERS_PATH.exists():
            return

        # Read existing registry with file locking
        with open(WORKERS_PATH, "r") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_SH)
            registry = json.load(f)
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)

        # Remove worker with matching PID
        original_count = len(registry["workers"])
        registry["workers"] = [w for w in registry["workers"] if w["pid"] != pid]

        # Write updated registry with file locking
        with open(WORKERS_PATH, "w") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            json.dump(registry, f, indent=2)
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)

        if len(registry["workers"]) < original_count:
            logger.info("Unregistered worker with PID: %d", pid)
    except Exception as e:
        logger.error("Failed to unregister worker PID %d: %s", pid, e)


def cleanup_stale_workers():
    """Remove non-existent PIDs from the worker registry."""
    try:
        if not WORKERS_PATH.exists():
            return []

        # Read existing registry with file locking
        with open(WORKERS_PATH, "r") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_SH)
            registry = json.load(f)
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)

        # Check which workers are still alive
        active_workers = []
        stale_pids = []

        for worker in registry["workers"]:
            pid = worker["pid"]
            try:
                # Check if process exists (signal 0 doesn't kill, just checks)
                os.kill(pid, 0)
                active_workers.append(worker)
            except OSError:
                # Process doesn't exist
                stale_pids.append(pid)
                logger.warning("Removing stale worker PID %d from registry", pid)

        # Update registry if we found stale workers
        if stale_pids:
            registry["workers"] = active_workers
            with open(WORKERS_PATH, "w") as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                json.dump(registry, f, indent=2)
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)

        return stale_pids
    except Exception as e:
        logger.error("Failed to cleanup stale workers: %s", e)
        return []


def get_active_workers():
    """Read and validate the worker registry, returning list of active workers."""
    try:
        # Clean up stale workers first
        cleanup_stale_workers()

        if not WORKERS_PATH.exists():
            return []

        # Read registry with file locking
        with open(WORKERS_PATH, "r") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_SH)
            registry = json.load(f)
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)

        return registry.get("workers", [])
    except Exception as e:
        logger.error("Failed to get active workers: %s", e)
        return []


# ========== JOB MANAGEMENT ==========
def parse_job_spec(input_str):
    """
    Parse job specification from JSON or simple string format.

    Tries to parse input as JSON first. If successful, extracts 'id' and 'command' fields.
    If JSON parsing fails, treats the entire input as a simple command string.

    Args:
        input_str: Job specification as JSON string or simple command string

    Returns:
        tuple: (job_id, command) where job_id may be None if not specified

    Raises:
        ValueError: If JSON is valid but missing required 'command' field
    """
    try:
        # Try parsing as JSON
        spec = json.loads(input_str)

        # Validate that 'command' field exists
        if "command" not in spec:
            raise ValueError("JSON job specification must include 'command' field")

        job_id = spec.get("id", None)
        command = spec["command"]
        return (job_id, command)
    except json.JSONDecodeError:
        # Fall back to simple string format
        return (None, input_str)


def enqueue_job(command, job_id=None):
    """Enqueue a new job with the given command."""
    try:
        conn = get_conn()
        cur = conn.cursor()

        if job_id is None:
            job_id = str(uuid.uuid4())

        max_retries = int(get_config("max_retries", DEFAULT_MAX_RETRIES))
        now = datetime.utcnow().isoformat()

        cur.execute(
            "INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?)",
            (job_id, command, "pending", 0, max_retries, now, now, None),
        )
        conn.commit()
        conn.close()

        print(f"Job {job_id} enqueued.")
        logger.info("Job %s enqueued with command: %s", job_id, command)
        return job_id
    except sqlite3.IntegrityError:
        print(f"Job with ID {job_id} already exists.")
        logger.error("Job %s already exists", job_id)
    except Exception as e:
        print(f"Error enqueueing job: {e}")
        logger.error("Error enqueueing job: %s", e)


def list_jobs(state=None):
    """List all jobs or jobs with a specific state."""
    try:
        conn = get_conn()
        cur = conn.cursor()
        if state:
            cur.execute("SELECT * FROM jobs WHERE state=?", (state,))
        else:
            cur.execute("SELECT * FROM jobs")
        rows = cur.fetchall()
        conn.close()

        if not rows:
            print("No jobs found.")
            return

        # Format output nicely
        print("\nJobs:")
        for row in rows:
            print(f"  ID: {row[0]}")
            print(f"  Command: {row[1]}")
            print(f"  State: {row[2]}")
            print(f"  Attempts: {row[3]}/{row[4]}")
            print(f"  Created: {row[5]}")
            print(f"  Updated: {row[6]}")
            if row[7]:
                print(f"  Error: {row[7]}")
            print()
    except Exception as e:
        print(f"Error listing jobs: {e}")
        logger.error("Error listing jobs: %s", e)


def list_dlq():
    """List all jobs in the Dead Letter Queue."""
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM dlq")
        rows = cur.fetchall()
        conn.close()

        if not rows:
            print("No jobs in DLQ.")
            return

        print("\nDead Letter Queue:")
        for row in rows:
            print(f"  ID: {row[0]}")
            print(f"  Command: {row[1]}")
            print(f"  Attempts: {row[3]}/{row[4]}")
            print(f"  Created: {row[5]}")
            print(f"  Failed: {row[6]}")
            if row[7]:
                print(f"  Error: {row[7]}")
            print()
    except Exception as e:
        print(f"Error listing DLQ: {e}")
        logger.error("Error listing DLQ: %s", e)


def retry_dlq(job_id=None, retry_all=False):
    """Retry a specific job or all jobs from the DLQ."""
    try:
        conn = get_conn()
        cur = conn.cursor()

        if retry_all:
            cur.execute("SELECT * FROM dlq")
            rows = cur.fetchall()
            if not rows:
                print("No jobs in DLQ to retry.")
                return

            for row in rows:
                # Reset state and attempts
                cur.execute("DELETE FROM dlq WHERE id=?", (row[0],))
                cur.execute(
                    "INSERT OR REPLACE INTO jobs VALUES (?,?,?,?,?,?,?,?)",
                    (
                        row[0],
                        row[1],
                        "pending",
                        0,
                        row[4],
                        row[5],
                        datetime.utcnow().isoformat(),
                        None,
                    ),
                )
            conn.commit()
            print(f"{len(rows)} jobs moved back to queue.")
            logger.info("Retried %d jobs from DLQ", len(rows))
        else:
            if not job_id:
                print("Job ID required when not using --all flag.")
                return

            cur.execute("SELECT * FROM dlq WHERE id=?", (job_id,))
            row = cur.fetchone()
            if not row:
                print("Job not found in DLQ.")
                return

            cur.execute("DELETE FROM dlq WHERE id=?", (job_id,))
            cur.execute(
                "INSERT OR REPLACE INTO jobs VALUES (?,?,?,?,?,?,?,?)",
                (
                    row[0],
                    row[1],
                    "pending",
                    0,
                    row[4],
                    row[5],
                    datetime.utcnow().isoformat(),
                    None,
                ),
            )
            conn.commit()
            print(f"Job {job_id} moved back to queue.")
            logger.info("Retried job %s from DLQ", job_id)

        conn.close()
    except Exception as e:
        print(f"Error retrying DLQ job: {e}")
        logger.error("Error retrying DLQ job: %s", e)


# ========== WORKER ==========
def process_job(job, base):
    """Process a single job with retry logic and exponential backoff."""
    conn = get_conn()
    cur = conn.cursor()
    job_id = job[0]
    command = job[1]

    try:
        cur.execute(
            "UPDATE jobs SET state='processing', updated_at=? WHERE id=?",
            (datetime.utcnow().isoformat(), job_id),
        )
        conn.commit()
        logger.info("Processing job %s: %s", job_id, command)

        result = subprocess.run(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=300,
        )

        if result.returncode == 0:
            cur.execute(
                "UPDATE jobs SET state='completed', updated_at=?, error_message=? WHERE id=?",
                (datetime.utcnow().isoformat(), None, job_id),
            )
            conn.commit()
            print(f"Job {job_id} completed.")
            logger.info("Job %s completed successfully", job_id)
        else:
            error_msg = result.stderr.decode("utf-8")[
                :500
            ]  # Limit error message length
            raise Exception(
                f"Command failed with return code {result.returncode}: {error_msg}"
            )

    except subprocess.TimeoutExpired:
        error_msg = "Job timed out after 300 seconds"
        logger.error("Job %s timed out", job_id)
        handle_job_failure(cur, job, base, error_msg)
        conn.commit()
    except Exception as e:
        error_msg = str(e)[:500]
        logger.error("Job %s failed: %s", job_id, error_msg)
        handle_job_failure(cur, job, base, error_msg)
        conn.commit()
    finally:
        conn.close()


def handle_job_failure(cur, job, base, error_msg):
    """Handle job failure with retry logic or move to DLQ."""
    job_id = job[0]
    attempts = job[3] + 1

    if attempts >= job[4]:
        # Move to DLQ
        cur.execute("DELETE FROM jobs WHERE id=?", (job_id,))
        cur.execute(
            "INSERT OR REPLACE INTO dlq VALUES (?,?,?,?,?,?,?,?)",
            (
                job[0],
                job[1],
                "dead",
                attempts,
                job[4],
                job[5],
                datetime.utcnow().isoformat(),
                error_msg,
            ),
        )
        print(f"Job {job_id} moved to DLQ after {attempts} attempts.")
        logger.warning("Job %s moved to DLQ after %d attempts", job_id, attempts)
    else:
        # Retry with exponential backoff
        delay = base**attempts
        print(
            f"Job {job_id} failed (attempt {attempts}/{job[4]}). Retrying in {delay}s..."
        )
        logger.info("Job %s will retry in %d seconds", job_id, delay)
        time.sleep(delay)
        cur.execute(
            "UPDATE jobs SET state='pending', attempts=?, updated_at=?, error_message=? WHERE id=?",
            (attempts, datetime.utcnow().isoformat(), error_msg, job_id),
        )


def worker_loop(base):
    """Main worker loop that processes jobs from the queue."""
    worker_id = current_process().name
    logger.info("Worker %s started", worker_id)

    while True:
        try:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute(
                "SELECT * FROM jobs WHERE state='pending' ORDER BY created_at LIMIT 1"
            )
            job = cur.fetchone()
            conn.close()

            if job:
                process_job(job, base)
            else:
                time.sleep(2)
        except KeyboardInterrupt:
            logger.info("Worker %s received shutdown signal", worker_id)
            break
        except Exception as e:
            logger.error("Worker %s encountered error: %s", worker_id, e)
            time.sleep(5)  # Wait before retrying


def stop_workers():
    """Stop all running worker processes."""
    try:
        # Get active workers from registry
        workers = get_active_workers()

        if not workers:
            print("No active workers found.")
            logger.info("No active workers to stop")
            return

        print(f"Stopping {len(workers)} worker(s)...")
        logger.info("Stopping %d workers", len(workers))

        # Send SIGTERM to all workers
        for worker in workers:
            pid = worker["pid"]
            name = worker["name"]
            try:
                os.kill(pid, signal.SIGTERM)
                logger.info("Sent SIGTERM to worker %s (PID: %d)", name, pid)
            except OSError as e:
                logger.warning("Failed to send SIGTERM to PID %d: %s", pid, e)

        # Wait up to 10 seconds for graceful shutdown
        print("Waiting for graceful shutdown (up to 10 seconds)...")
        start_time = time.time()
        remaining_workers = workers.copy()

        while remaining_workers and (time.time() - start_time) < 10:
            still_alive = []
            for worker in remaining_workers:
                pid = worker["pid"]
                try:
                    # Check if process still exists
                    os.kill(pid, 0)
                    still_alive.append(worker)
                except OSError:
                    # Process has terminated
                    logger.info("Worker PID %d terminated gracefully", pid)

            remaining_workers = still_alive
            if remaining_workers:
                time.sleep(0.5)

        # Send SIGKILL to processes that didn't terminate
        if remaining_workers:
            print(
                f"WARNING: Force killing {len(remaining_workers)} worker(s) that didn't stop gracefully..."
            )
            for worker in remaining_workers:
                pid = worker["pid"]
                name = worker["name"]
                try:
                    os.kill(pid, signal.SIGKILL)
                    logger.warning("Sent SIGKILL to worker %s (PID: %d)", name, pid)
                except OSError as e:
                    logger.warning("Failed to send SIGKILL to PID %d: %s", pid, e)

        # Clean up worker registry
        if WORKERS_PATH.exists():
            with open(WORKERS_PATH, "w") as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                json.dump({"workers": []}, f, indent=2)
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            logger.info("Worker registry cleaned up")

        print("All workers stopped.")

    except Exception as e:
        print(f"Error stopping workers: {e}")
        logger.error("Error stopping workers: %s", e)


def start_workers(count, base):
    """Start multiple worker processes."""
    print(f"Starting {count} workers...")
    logger.info("Starting %d workers with backoff base %d", count, base)
    processes = []

    for i in range(count):
        p = Process(target=worker_loop, args=(base,), name=f"Worker-{i+1}")
        p.start()
        processes.append(p)

        # Register worker in registry
        register_worker(p.pid, p.name, base)

        logger.info("Started worker %s (PID: %d)", p.name, p.pid)

    def stop_all(signum, frame):
        print("\nStopping workers gracefully...")
        logger.info("Shutdown signal received, stopping workers")
        for p in processes:
            p.terminate()
            p.join(timeout=5)
            if p.is_alive():
                p.kill()
            # Unregister worker from registry
            unregister_worker(p.pid)
        logger.info("All workers stopped")
        sys.exit(0)

    signal.signal(signal.SIGINT, stop_all)
    signal.signal(signal.SIGTERM, stop_all)

    print(f"{count} workers running. Press Ctrl+C to stop.")
    for p in processes:
        p.join()


# ========== STATUS ==========
def status():
    """Display queue status and statistics."""
    try:
        conn = get_conn()
        cur = conn.cursor()

        # Job statistics
        cur.execute("SELECT state, COUNT(*) FROM jobs GROUP BY state")
        summary = cur.fetchall()

        # DLQ count
        cur.execute("SELECT COUNT(*) FROM dlq")
        dlq_count = cur.fetchone()[0]

        # Total jobs
        cur.execute("SELECT COUNT(*) FROM jobs")
        total_jobs = cur.fetchone()[0]

        conn.close()

        print("\nQueue Status:")
        print(f"  Total Jobs: {total_jobs}")
        if summary:
            for s, c in summary:
                print(f"  {s.capitalize()}: {c}")
        else:
            print("  No jobs in queue")
        print(f"  Dead Letter Queue: {dlq_count}")
        print()
    except Exception as e:
        print(f"Error getting status: {e}")
        logger.error("Error getting status: %s", e)


# ========== CLI ==========
def main():
    """Main CLI entry point for QueueCTL."""
    parser = argparse.ArgumentParser(
        prog="queuectl",
        description="QueueCTL - CLI Job Queue System with Workers and DLQ",
    )
    subparsers = parser.add_subparsers(dest="cmd", help="Available commands")

    # Init command
    subparsers.add_parser("init", help="Initialize QueueCTL system")

    # Enqueue command
    enq = subparsers.add_parser("enqueue", help="Enqueue a new job")
    enq.add_argument(
        "command",
        help=(
            "Command to execute (string or JSON format: "
            '\'{"command":"echo test"}\' or \'{"id":"job1","command":"sleep 2"}\')'
        ),
    )

    # Status command
    subparsers.add_parser("status", help="Show queue status and statistics")

    # List jobs command
    listj = subparsers.add_parser("list", help="List jobs")
    listj.add_argument(
        "--state",
        default=None,
        help="Filter by state (pending/processing/completed/failed)",
    )

    # Worker command group
    worker_parser = subparsers.add_parser("worker", help="Manage worker processes")
    worker_subparsers = worker_parser.add_subparsers(
        dest="worker_cmd", help="Worker commands"
    )

    # Worker start subcommand
    wstart = worker_subparsers.add_parser("start", help="Start worker processes")
    wstart.add_argument(
        "--count", type=int, default=1, help="Number of workers to start (default: 1)"
    )
    wstart.add_argument(
        "--base",
        type=int,
        help="Exponential backoff base for retries (default: from config)",
    )

    # Worker stop subcommand
    worker_subparsers.add_parser("stop", help="Stop all running worker processes")

    # DLQ command group
    dlq_parser = subparsers.add_parser("dlq", help="Manage Dead Letter Queue")
    dlq_subparsers = dlq_parser.add_subparsers(dest="dlq_cmd", help="DLQ commands")

    # DLQ list subcommand
    dlq_subparsers.add_parser("list", help="List all jobs in the Dead Letter Queue")

    # DLQ retry subcommand
    dlqr = dlq_subparsers.add_parser(
        "retry", help="Retry job(s) from the Dead Letter Queue"
    )
    dlqr.add_argument(
        "job_id", nargs="?", help="Job ID to retry (required unless --all is used)"
    )
    dlqr.add_argument(
        "--all", action="store_true", help="Retry all jobs in the Dead Letter Queue"
    )

    # Config command group
    config_parser = subparsers.add_parser(
        "config", help="Manage configuration settings"
    )
    config_subparsers = config_parser.add_subparsers(
        dest="config_cmd", help="Configuration commands"
    )

    # Config set subcommand
    cfg_set = config_subparsers.add_parser("set", help="Set a configuration value")
    cfg_set.add_argument("key", help="Configuration key (max-retries, backoff-base)")
    cfg_set.add_argument("value", help="Configuration value")

    # Config get subcommand
    cfg_get = config_subparsers.add_parser("get", help="Get configuration value(s)")
    cfg_get.add_argument(
        "key", nargs="?", help="Configuration key to retrieve (omit to show all)"
    )

    # Backward compatibility aliases with deprecation warnings
    # Legacy worker-start command
    wstart_legacy = subparsers.add_parser(
        "worker-start", help="[DEPRECATED] Use 'worker start' instead"
    )
    wstart_legacy.add_argument(
        "--count", type=int, default=1, help="Number of workers (default: 1)"
    )
    wstart_legacy.add_argument(
        "--base", type=int, help="Exponential backoff base (default: from config)"
    )

    # Legacy dlq-list command
    subparsers.add_parser("dlq-list", help="[DEPRECATED] Use 'dlq list' instead")

    # Legacy dlq-retry command
    dlqr_legacy = subparsers.add_parser(
        "dlq-retry", help="[DEPRECATED] Use 'dlq retry' instead"
    )
    dlqr_legacy.add_argument("job_id", nargs="?", help="Job ID to retry")
    dlqr_legacy.add_argument("--all", action="store_true", help="Retry all jobs in DLQ")

    # Legacy config-set command
    cfg_legacy = subparsers.add_parser(
        "config-set", help="[DEPRECATED] Use 'config set' instead"
    )
    cfg_legacy.add_argument("key", help="Configuration key (max_retries/backoff_base)")
    cfg_legacy.add_argument("value", help="Configuration value")

    # Legacy config-get command
    cfgg_legacy = subparsers.add_parser(
        "config-get", help="[DEPRECATED] Use 'config get' instead"
    )
    cfgg_legacy.add_argument(
        "key", nargs="?", help="Configuration key (omit to show all)"
    )

    args = parser.parse_args()

    # Handle commands
    if args.cmd == "init":
        init_db()
    elif args.cmd == "enqueue":
        try:
            # Parse job specification (JSON or simple string)
            job_id, command = parse_job_spec(args.command)
            enqueue_job(command, job_id)
        except ValueError as e:
            print(f"Error: {e}")
            print("\nExpected JSON format:")
            print('  {"command": "echo test"}')
            print('  {"id": "job1", "command": "sleep 2"}')
            print("\nOr simple command string:")
            print('  "echo test"')
            logger.error("Invalid job specification: %s", e)
            sys.exit(1)
    elif args.cmd == "status":
        status()
    elif args.cmd == "list":
        list_jobs(args.state)

    # Worker commands
    elif args.cmd == "worker":
        if args.worker_cmd == "start":
            base = (
                args.base
                if args.base
                else int(get_config("backoff_base", DEFAULT_BACKOFF_BASE))
            )
            start_workers(args.count, base)
        elif args.worker_cmd == "stop":
            stop_workers()
        else:
            worker_parser.print_help()

    # DLQ commands
    elif args.cmd == "dlq":
        if args.dlq_cmd == "list":
            list_dlq()
        elif args.dlq_cmd == "retry":
            retry_dlq(args.job_id, args.all)
        else:
            dlq_parser.print_help()

    # Config commands
    elif args.cmd == "config":
        if args.config_cmd == "set":
            # Normalize key from hyphenated to underscore format
            normalized_key = normalize_config_key(args.key)
            set_config(normalized_key, args.value)
            print(f"Config {args.key} updated to {args.value}.")
        elif args.config_cmd == "get":
            if args.key:
                # Normalize key for lookup
                normalized_key = normalize_config_key(args.key)
                value = get_config(normalized_key)
                # Display with hyphenated format
                display_key = display_config_key(normalized_key)
                print(f"{display_key}: {value}")
            else:
                config = get_config()
                # Display all keys in hyphenated format
                display_config = {display_config_key(k): v for k, v in config.items()}
                print(json.dumps(display_config, indent=2))
        else:
            config_parser.print_help()

    # Legacy commands with deprecation warnings
    elif args.cmd == "worker-start":
        print(
            "DEPRECATION WARNING: 'worker-start' is deprecated. Use 'queuectl worker start' instead."
        )
        logger.warning(
            "Legacy command 'worker-start' used. Recommend using 'worker start'"
        )
        base = (
            args.base
            if args.base
            else int(get_config("backoff_base", DEFAULT_BACKOFF_BASE))
        )
        start_workers(args.count, base)
    elif args.cmd == "dlq-list":
        print(
            "DEPRECATION WARNING: 'dlq-list' is deprecated. Use 'queuectl dlq list' instead."
        )
        logger.warning("Legacy command 'dlq-list' used. Recommend using 'dlq list'")
        list_dlq()
    elif args.cmd == "dlq-retry":
        print(
            "DEPRECATION WARNING: 'dlq-retry' is deprecated. Use 'queuectl dlq retry' instead."
        )
        logger.warning("Legacy command 'dlq-retry' used. Recommend using 'dlq retry'")
        retry_dlq(args.job_id, args.all)
    elif args.cmd == "config-set":
        print(
            "DEPRECATION WARNING: 'config-set' is deprecated. Use 'queuectl config set' instead."
        )
        logger.warning("Legacy command 'config-set' used. Recommend using 'config set'")
        # Normalize key from hyphenated to underscore format
        normalized_key = normalize_config_key(args.key)
        set_config(normalized_key, args.value)
        print(f"Config {args.key} updated to {args.value}.")
    elif args.cmd == "config-get":
        print(
            "DEPRECATION WARNING: 'config-get' is deprecated. Use 'queuectl config get' instead."
        )
        logger.warning("Legacy command 'config-get' used. Recommend using 'config get'")
        if args.key:
            # Normalize key for lookup
            normalized_key = normalize_config_key(args.key)
            value = get_config(normalized_key)
            # Display with hyphenated format
            display_key = display_config_key(normalized_key)
            print(f"{display_key}: {value}")
        else:
            config = get_config()
            # Display all keys in hyphenated format
            display_config = {display_config_key(k): v for k, v in config.items()}
            print(json.dumps(display_config, indent=2))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
