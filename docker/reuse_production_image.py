"""Select an unchanged image from a completed production build of this commit."""

import json
import os
import re
import subprocess
import tempfile
from pathlib import Path

REPOSITORY = "SatoshiAndKin/web3-proxy"
WORKFLOW = ".github/workflows/production-image.yml"
IMAGE = "ghcr.io/satoshiandkin/web3-proxy"
SHA = re.compile(r"[0-9a-f]{40}")
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")


def output(*args: str) -> str:
    return subprocess.check_output(
        args, text=True, stderr=subprocess.DEVNULL, timeout=20
    ).strip()


def api(endpoint: str) -> dict:
    return json.loads(output("gh", "api", f"/repos/{REPOSITORY}/{endpoint}"))


def reuse_digest(revision: str) -> str | None:
    if not SHA.fullmatch(revision):
        raise ValueError("Require a full source commit")
    runs = api(
        "actions/workflows/production-image.yml/runs"
        f"?branch=main&event=push&head_sha={revision}&status=completed&per_page=5"
    )["workflow_runs"]
    for run in runs:
        if (
            run["path"] != WORKFLOW
            or run["head_branch"] != "main"
            or run["head_sha"] != revision
            or run["event"] != "push"
            or run["status"] != "completed"
            or run["head_repository"]["full_name"] != REPOSITORY
        ):
            continue
        run_id = run["id"]
        attempt = run["run_attempt"]
        jobs = api(f"actions/runs/{run_id}/attempts/{attempt}/jobs?per_page=100")[
            "jobs"
        ]
        if not any(
            job["name"] in {"image", "image / image", "publish / image"}
            and job["conclusion"] == "success"
            and job["head_sha"] == revision
            and job["run_attempt"] == attempt
            for job in jobs
        ):
            continue
        with tempfile.TemporaryDirectory() as directory:
            try:
                output(
                    "gh",
                    "run",
                    "download",
                    str(run_id),
                    "--repo",
                    REPOSITORY,
                    "--name",
                    f"production-image-{revision}-{attempt}",
                    "--dir",
                    directory,
                )
                digest = (Path(directory) / "production-image.txt").read_text().strip()
            except (FileNotFoundError, subprocess.CalledProcessError):
                continue
        if not DIGEST.fullmatch(digest):
            continue
        current = output(
            "docker",
            "buildx",
            "imagetools",
            "inspect",
            "--format",
            "{{.Manifest.Digest}}",
            f"{IMAGE}:sha-{revision}",
        )
        if current == digest:
            return digest
    return None


def main() -> None:
    try:
        digest = reuse_digest(os.environ["GITHUB_SHA"])
    except (OSError, KeyError, ValueError, subprocess.SubprocessError):
        digest = None
    if digest:
        with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output_file:
            output_file.write(f"digest={digest}\n")
        print(f"Reuse validated production image {IMAGE}@{digest}.")
    else:
        print("Build and validate a production image for this commit.")


if __name__ == "__main__":
    main()
