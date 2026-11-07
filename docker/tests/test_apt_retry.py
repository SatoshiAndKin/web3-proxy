"""Exercise failed APT processes without network access or real delays."""

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

HELPER = Path(__file__).resolve().parents[1] / "apt-retry"


class AptRetryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.calls = self.root / "calls.jsonl"
        self.delays = self.root / "delays"
        apt = self.root / "apt-get"
        apt.write_text("""#!/usr/bin/env python3
import json, os, pathlib, sys
root = pathlib.Path(os.environ["APT_TEST_ROOT"])
state = root / "attempt"
number = int(state.read_text()) + 1 if state.exists() else 1
state.write_text(str(number))
with (root / "calls.jsonl").open("a") as log:
    log.write(json.dumps({"pid": os.getpid(), "args": sys.argv[1:]}) + "\\n")
sys.exit(42 if number <= int(os.environ["APT_TEST_FAILURES"]) else 0)
""")
        apt.chmod(0o755)
        sleep = self.root / "sleep"
        sleep.write_text("""#!/usr/bin/env python3
import os, pathlib, sys
with (pathlib.Path(os.environ["APT_TEST_ROOT"]) / "delays").open("a") as log:
    log.write(sys.argv[1] + "\\n")
""")
        sleep.chmod(0o755)

    def run_helper(self, failures, *arguments):
        env = dict(
            os.environ,
            APT_TEST_ROOT=str(self.root),
            APT_TEST_FAILURES=str(failures),
            PATH=str(self.root) + os.pathsep + os.environ["PATH"],
        )
        result = subprocess.run(
            [str(HELPER), *arguments],
            env=env,
            capture_output=True,
            text=True,
            timeout=10,
        )
        calls = [json.loads(line) for line in self.calls.read_text().splitlines()]
        for call in calls:
            self.assertEqual(
                call["args"],
                [
                    "-o",
                    "Acquire::Retries=0",
                    "-o",
                    "APT::Update::Error-Mode=any",
                    *arguments,
                ],
            )
        self.assertEqual(len({call["pid"] for call in calls}), len(calls))
        delays = self.delays.read_text().splitlines() if self.delays.exists() else []
        return result, calls, delays

    def test_success_preserves_arguments_without_a_delay(self):
        result, calls, delays = self.run_helper(
            0, "install", "--no-install-recommends", "-y", "package-name"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(calls), 1)
        self.assertEqual(delays, [])

    def test_transient_failure_uses_fresh_processes_and_backoff(self):
        result, calls, delays = self.run_helper(2, "update")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(calls), 3)
        self.assertEqual(delays, ["15", "30"])

    def test_permanent_failure_stops_after_five_attempts(self):
        result, calls, delays = self.run_helper(99, "update")
        self.assertEqual(result.returncode, 42)
        self.assertEqual(len(calls), 5)
        self.assertEqual(delays, ["15", "30", "60", "120"])
        self.assertIn("failed after 5 attempts (exit 42)", result.stderr)


if __name__ == "__main__":
    unittest.main()
