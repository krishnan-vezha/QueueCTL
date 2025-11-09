import pytest
import os
import sys
import tempfile
import shutil
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import queuectl


@pytest.fixture
def temp_queuectl_dir(monkeypatch):
    """Create a temporary directory for queuectl data."""
    temp_dir = tempfile.mkdtemp()
    temp_path = Path(temp_dir) / ".queuectl"

    # Monkey patch the BASE_DIR
    monkeypatch.setattr(queuectl, "BASE_DIR", temp_path)
    monkeypatch.setattr(queuectl, "DB_PATH", temp_path / "jobs.db")
    monkeypatch.setattr(queuectl, "CONFIG_PATH", temp_path / "config.json")
    monkeypatch.setattr(queuectl, "WORKERS_PATH", temp_path / "workers.json")

    yield temp_path

    # Cleanup
    shutil.rmtree(temp_dir, ignore_errors=True)
