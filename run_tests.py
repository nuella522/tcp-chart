"""Run the project's standalone integration tests."""

import subprocess
import sys


for test_file in (
    "test_accounts.py",
    "test_messaging.py",
    "test_framing.py",
    "test_websocket.py",
):
    print(f"\n== {test_file} ==", flush=True)
    subprocess.run([sys.executable, test_file], check=True)
