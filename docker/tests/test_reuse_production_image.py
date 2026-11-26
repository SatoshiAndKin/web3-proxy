import subprocess
import unittest
from pathlib import Path
from unittest.mock import patch

from docker import reuse_production_image as image


class ReuseProductionImage(unittest.TestCase):
    revision = "1" * 40
    digest = "sha256:" + "2" * 64

    def setUp(self):
        self.run = {
            "id": 123,
            "run_attempt": 2,
            "path": image.WORKFLOW,
            "head_branch": "main",
            "head_sha": self.revision,
            "head_repository": {"full_name": image.REPOSITORY},
            "event": "push",
            "status": "completed",
        }
        self.job = {
            "name": "image",
            "conclusion": "success",
            "head_sha": self.revision,
            "run_attempt": 2,
        }

    def select(self, registry=None, artifact=True):
        def command(*args):
            if args[:3] == ("gh", "run", "download"):
                self.assertEqual(args[3], "123")
                self.assertEqual(args[7], f"production-image-{self.revision}-2")
                if not artifact:
                    raise subprocess.CalledProcessError(1, args)
                (Path(args[-1]) / "production-image.txt").write_text(self.digest + "\n")
                return ""
            self.assertEqual(args[-1], f"{image.IMAGE}:sha-{self.revision}")
            return self.digest if registry is None else registry

        with (
            patch.object(
                image,
                "api",
                side_effect=[{"workflow_runs": [self.run]}, {"jobs": [self.job]}],
            ),
            patch.object(image, "output", side_effect=command) as lookup,
        ):
            digest = image.reuse_digest(self.revision)
        return digest, lookup.call_count

    def test_reuses_matching_digest(self):
        self.assertEqual(self.select(), (self.digest, 2))

    def test_accepts_the_current_production_job_name(self):
        self.job["name"] = "publish / image"
        self.assertEqual(self.select(), (self.digest, 2))

    def test_same_revision_with_a_different_manifest_requires_a_build(self):
        self.assertEqual(self.select(registry="sha256:" + "3" * 64), (None, 2))

    def test_failed_validation_cannot_supply_a_manifest(self):
        self.job["conclusion"] = "failure"
        self.assertEqual(self.select(), (None, 0))

    def test_unavailable_build_record_requires_a_build(self):
        self.assertEqual(self.select(artifact=False), (None, 1))

    def test_only_the_exact_production_push_can_supply_a_manifest(self):
        for field, value in (
            ("event", "workflow_dispatch"),
            ("head_sha", "3" * 40),
            ("head_branch", "topic"),
            ("head_repository", {"full_name": "fork/web3-proxy"}),
        ):
            with self.subTest(field=field), patch.dict(self.run, {field: value}):
                self.assertEqual(self.select(), (None, 0))
