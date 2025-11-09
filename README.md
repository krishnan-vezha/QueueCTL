# QueueCTL

A CLI-based background job queue system with worker processes, exponential backoff retries, and Dead Letter Queue (DLQ) support.

## Features

- **Multi-worker job processing** - Run multiple worker processes concurrently
- **Automatic retries with exponential backoff** - Failed jobs are retried with increasing delays
- **Dead Letter Queue** - Permanently failed jobs are moved to DLQ for manual review
- **Job status tracking** - Monitor job states (pending, processing, completed, failed)
- **Configurable settings** - Customize retry attempts and backoff behavior
- **SQLite-backed storage** - Persistent job storage with ACID guarantees
- **Comprehensive logging** - Track all job operations and worker activities

## Architecture
┌─────────────┐
│    CLI      │
└──────┬──────┘
       │
       ▼
┌─────────────┐        ┌──────────────┐        ┌─────────────────┐
│  Job Queue  │  ───▶  │   Workers    │  ───▶  │ Worker Registry │
│  (SQLite)   │        │ (Processes)  │        │    (JSON)       │
└─────────────┘        └──────┬───────┘        └─────────────────┘
                              │
                              ▼
                      ┌─────────────┐
                      │  Execute    │
                      │  Commands   │
                      └──────┬──────┘
                             │
                     ┌───────▼───────┐
                     │   Success?    │
                     └────┬────┬─────┘
                          │    │
                       Yes│    │No
                          ▼    ▼
                  ┌──────────────┐
                  │    Retry     │
                  │ (Backoff)    │
                  └──────┬───────┘
                         │
                ┌────────▼────────┐
                │ Max retries?    │
                └───┬─────┬───────┘
                    │     │
                  No│     │Yes
                    ▼     ▼
          ┌──────────┐ ┌──────────┐ ┌─────┐
          │Completed │ │  Failed  │ │ DLQ │
          └──────────┘ └──────────┘ └─────┘

## Setup

### Requirements
- Python 3.7 or higher
- No external dependencies required for core functionality
- Development dependencies (optional): pytest, pytest-cov, black, flake8

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd queuectl
```

2. (Optional) Create and activate a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. (Optional) Install development dependencies:
```bash
pip install -r requirements.txt
```

4. Initialize the QueueCTL system:
```bash
python queuectl.py init
```

This will create the necessary directory structure at `~/.queuectl/` with:
- `jobs.db` - SQLite database for job storage
- `config.json` - Configuration file for retry and backoff settings
- `workers.json` - Worker process registry (created when workers start)

## Usage

### Worker Management

Start worker processes to process jobs from the queue:

```bash
# Start a single worker
python queuectl.py worker start

# Start multiple workers
python queuectl.py worker start --count 3

# Start workers with custom backoff base
python queuectl.py worker start --count 2 --base 3
```

Example output:
```
Starting 3 workers...
3 workers running. Press Ctrl+C to stop.
```

Stop all running workers gracefully:

```bash
python queuectl.py worker stop
```

Example output:
```
Stopping 3 worker(s)...
Waiting for graceful shutdown (up to 10 seconds)...
All workers stopped.
```

### Job Management

Enqueue jobs using simple command strings:

```bash
python queuectl.py enqueue "echo Hello World"
python queuectl.py enqueue "sleep 5"
```

Enqueue jobs using JSON format (with optional custom ID):

```bash
# Auto-generated ID
python queuectl.py enqueue '{"command":"echo test"}'

# Custom ID
python queuectl.py enqueue '{"id":"job1","command":"sleep 2"}'
```

Example output:
```
Job 550e8400-e29b-41d4-a716-446655440000 enqueued.
```

List all jobs:

```bash
python queuectl.py list
```

List jobs by state:

```bash
python queuectl.py list --state pending
python queuectl.py list --state completed
python queuectl.py list --state processing
```

Example output:
```
Jobs:
  ID: job1
  Command: echo test
  State: completed
  Attempts: 1/3
  Created: 2025-11-09T10:30:00.123456
  Updated: 2025-11-09T10:30:02.456789
```

Check queue status:

```bash
python queuectl.py status
```

Example output:
```
Queue Status:
  Total Jobs: 5
  Pending: 2
  Processing: 1
  Completed: 2
  Dead Letter Queue: 0
```

### Dead Letter Queue (DLQ)

List jobs in the DLQ:

```bash
python queuectl.py dlq list
```

Example output:
```
Dead Letter Queue:
  ID: failed-job-1
  Command: exit 1
  Attempts: 3/3
  Created: 2025-11-09T10:00:00.123456
  Failed: 2025-11-09T10:05:30.789012
  Error: Command failed with return code 1
```

Retry a specific job from the DLQ:

```bash
python queuectl.py dlq retry job1
```

Retry all jobs in the DLQ:

```bash
python queuectl.py dlq retry --all
```

Example output:
```
Job job1 moved back to queue.
```

### Configuration Management

Set configuration values:

```bash
python queuectl.py config set max-retries 5
python queuectl.py config set backoff-base 3
```

Example output:
```
Config max-retries updated to 5.
```

Get a specific configuration value:

```bash
python queuectl.py config get max-retries
```

Example output:
```
max-retries: 5
```

Get all configuration values:

```bash
python queuectl.py config get
```

Example output:
```json
{
  "max-retries": 3,
  "backoff-base": 2
}
```

### Command Structure

QueueCTL uses a nested subcommand structure for better organization:

- `queuectl worker start/stop` - Worker process management
- `queuectl dlq list/retry` - Dead Letter Queue operations
- `queuectl config set/get` - Configuration management

Legacy commands (deprecated but still supported):
- `queuectl worker-start` → Use `queuectl worker start`
- `queuectl dlq-list` → Use `queuectl dlq list`
- `queuectl dlq-retry` → Use `queuectl dlq retry`
- `queuectl config-set` → Use `queuectl config set`
- `queuectl config-get` → Use `queuectl config get`

## Design Decisions and Trade-offs

### Worker Registry (File-based)
**Decision:** Store worker PIDs in `~/.queuectl/workers.json`

**Rationale:**
- Simple, human-readable JSON format
- Consistent with existing config.json approach
- Easy to debug and inspect manually
- No additional dependencies required

**Trade-off:** File-based locking is not as robust as database transactions, but it's sufficient for typical usage patterns with a reasonable number of workers (< 100).

### JSON Job Specification with Fallback
**Decision:** Try parsing input as JSON first, fall back to simple string format

**Rationale:**
- Maintains backward compatibility with existing simple string format
- Allows users to specify custom job IDs when needed
- Intuitive for users - both formats "just work"
- Follows the principle of least surprise

**Trade-off:** Ambiguous input like `"echo '{\"test\":1}'"` might be confusing, but this is rare in practice. The parser prioritizes JSON format, so users can always escape properly.

### Configuration Key Normalization
**Decision:** Accept hyphenated keys in CLI (max-retries), store as underscores internally (max_retries)

**Rationale:**
- CLI convention typically uses hyphens (--max-retries)
- Python convention uses underscores (max_retries)
- Transparent bidirectional conversion improves UX
- Consistent with standard CLI tools

**Trade-off:** Requires a conversion layer, but the overhead is negligible and the improved user experience is worth it.

### Graceful Worker Shutdown
**Decision:** Use SIGTERM with 10-second timeout, then SIGKILL as fallback

**Rationale:**
- Allows currently processing jobs to complete
- Prevents data corruption or partial job execution
- Standard Unix process management pattern
- Configurable timeout balances responsiveness and safety

**Trade-off:** Shutdown may take up to 10 seconds, but this ensures data integrity and proper job state management.

### SQLite for Job Storage
**Decision:** Use SQLite database for persistent job storage

**Rationale:**
- ACID guarantees for data consistency
- No separate database server required
- Built into Python standard library
- Sufficient performance for typical workloads
- Easy backup and migration (single file)

**Trade-off:** Not suitable for extremely high-throughput scenarios (thousands of jobs/second), but perfect for typical background job processing use cases.

### Exponential Backoff Retry Strategy
**Decision:** Implement exponential backoff with configurable base

**Rationale:**
- Prevents overwhelming failing services with rapid retries
- Gives transient issues time to resolve
- Industry-standard approach for retry logic
- Configurable to match different use cases

**Trade-off:** Failed jobs take longer to retry, but this is intentional to avoid cascading failures.

## Assumptions

1. **Single Machine Deployment:** Workers are started and stopped on the same machine where the queue database resides.

2. **Worker Registry Integrity:** The `workers.json` file is managed exclusively by QueueCTL and not manually edited by users.

3. **Reasonable Worker Count:** Maximum of ~100 concurrent workers. The file-based registry is optimized for typical usage patterns.

4. **Single-line JSON:** JSON job specifications are provided as single-line strings in the CLI.

5. **Configuration Immutability:** Configuration changes (max-retries, backoff-base) do not affect already-running workers. Workers use the configuration values that were active when they started.

6. **Job Timeout:** Jobs that run longer than 300 seconds (5 minutes) are automatically terminated and retried.

7. **Filesystem Reliability:** The filesystem where `~/.queuectl/` resides is reliable and supports file locking (fcntl).

8. **Process Permissions:** The user running QueueCTL has permission to send signals (SIGTERM, SIGKILL) to worker processes.

## Testing

### Running the Test Suite

Run all tests with pytest:

```bash
pytest
```

Run tests with coverage report:

```bash
pytest --cov=queuectl --cov-report=html
```

Run specific test file:

```bash
pytest tests/test_queue.py
```

Run tests with verbose output:

```bash
pytest -v
```

### Manual Testing Scenarios

#### Scenario 1: Basic Job Processing
```bash
# Initialize and start workers
python queuectl.py init
python queuectl.py worker start --count 2

# In another terminal, enqueue jobs
python queuectl.py enqueue "echo Test 1"
python queuectl.py enqueue '{"id":"test2","command":"echo Test 2"}'
python queuectl.py status
python queuectl.py list

# Stop workers
python queuectl.py worker stop
```

Expected: Jobs are processed successfully, status shows completed jobs.

#### Scenario 2: Retry and DLQ
```bash
# Start workers
python queuectl.py worker start

# In another terminal, enqueue a failing job
python queuectl.py enqueue "exit 1"
python queuectl.py status

# Wait for retries (will take ~2s + 4s + 8s = 14s with default backoff)
# Check DLQ after max retries
python queuectl.py dlq list

# Retry from DLQ
python queuectl.py dlq retry --all
```

Expected: Job fails, retries with exponential backoff, moves to DLQ after max retries, can be retried from DLQ.

#### Scenario 3: Configuration Management
```bash
# View current config
python queuectl.py config get

# Update configuration
python queuectl.py config set max-retries 5
python queuectl.py config set backoff-base 3
python queuectl.py config get

# Verify new config is used
python queuectl.py enqueue "exit 1"
python queuectl.py worker start
# Job should retry 5 times with base-3 backoff
```

Expected: Configuration changes persist and are used for new jobs.

#### Scenario 4: Worker Management
```bash
# Start multiple workers
python queuectl.py worker start --count 3

# Verify workers are registered
cat ~/.queuectl/workers.json

# Stop workers gracefully
python queuectl.py worker stop

# Verify registry is cleaned up
cat ~/.queuectl/workers.json
```

Expected: Workers are registered on start, cleaned up on stop, registry file is properly maintained.

### Expected Test Output

When running the test suite, you should see output similar to:

```
============================= test session starts ==============================
platform linux -- Python 3.10.0, pytest-7.4.0, pluggy-1.0.0
rootdir: /path/to/queuectl
collected 15 items

tests/test_queue.py ...............                                      [100%]

============================== 15 passed in 2.34s ===============================
```

## Demo Video

A demonstration video showing QueueCTL in action will be available at:

[Demo Video Link - To Be Added]

### Recording the Demo

To record a demo video, showcase the following features:

1. **Initialization:** Run `queuectl init` and show the created directory structure
2. **Worker Management:** Start workers with `worker start --count 3`, show them running, then stop with `worker stop`
3. **Job Enqueueing:** Demonstrate both simple string and JSON formats
4. **Job Processing:** Show jobs being processed by workers in real-time
5. **Status and Listing:** Use `status` and `list` commands to show queue state
6. **Retry Logic:** Enqueue a failing job and show exponential backoff retries
7. **DLQ Operations:** Show jobs moving to DLQ and retry functionality
8. **Configuration:** Demonstrate `config set` and `config get` commands

Recommended tools: asciinema, OBS Studio, or any screen recording software that captures terminal output clearly.
