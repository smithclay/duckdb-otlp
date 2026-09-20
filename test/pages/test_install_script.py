"""Tests for site/public/install.sh, the `curl ... | sh` installer.

The script is published verbatim as https://smithclay.github.io/duckdb-otlp/install.sh,
so a broken one-liner is a broken release page. These tests run the real script
against a fake release (a local HTTP server serving a tarball and a SHA256SUMS)
via the DUCKDB_OTLP_BASE_URL / DUCKDB_OTLP_API_URL overrides it reads.
"""

import functools
import hashlib
import io
import json
import os
import platform
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import subprocess
import tarfile
import tempfile
import threading
import unittest

SCRIPT = Path(__file__).resolve().parents[2] / "site" / "public" / "install.sh"
TAG = "v9.9.9"
PAYLOAD = "#!/bin/sh\necho duckdb-otlp 9.9.9-fixture\n"


def host_platform():
    """The platform the script's own `uname` detection will pick on this machine.

    The fixture release is built for it so the tests exercise detection rather
    than always overriding it with --platform.
    """
    system = {"Linux": "linux", "Darwin": "darwin"}.get(platform.system())
    machine = {"x86_64": "amd64", "amd64": "amd64", "arm64": "arm64", "aarch64": "arm64"}.get(platform.machine())
    if not system or not machine:
        raise unittest.SkipTest(f"install.sh ships no binary for {platform.system()}/{platform.machine()}")
    return f"{system}-{machine}"


PLATFORM = host_platform()


class QuietHandler(SimpleHTTPRequestHandler):
    def log_message(self, *_args):
        pass


class InstallScriptTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.bin_dir = self.root / "bin"
        self.published = self.root / "published"
        (self.published / TAG).mkdir(parents=True)

        self.archive = f"duckdb-otlp-{TAG}-{PLATFORM}.tar.gz"
        archive_path = self.published / TAG / self.archive
        with tarfile.open(archive_path, "w:gz") as tar:
            member = tarfile.TarInfo("duckdb-otlp")
            member.size = len(PAYLOAD)
            member.mode = 0o644  # The installer, not the tarball, decides the mode.
            tar.addfile(member, io.BytesIO(PAYLOAD.encode()))
        self.digest = hashlib.sha256(archive_path.read_bytes()).hexdigest()
        self.write_sums(self.digest)

        # Only the tag_name matters to the installer; the rest of the release
        # payload is what GitHub actually returns around it.
        (self.published / "latest.json").write_text(json.dumps({"tag_name": TAG, "name": TAG, "draft": False}))

        handler = functools.partial(QuietHandler, directory=str(self.published))
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()
        self.addCleanup(self.stop_server)
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"

    def stop_server(self):
        self.server.shutdown()
        self.thread.join()
        self.server.server_close()

    def write_sums(self, digest):
        (self.published / TAG / "SHA256SUMS").write_text(f"{digest}  {self.archive}\n")

    def install(self, *args, env=None):
        environment = dict(os.environ)
        environment.update(
            {
                "DUCKDB_OTLP_BASE_URL": self.base_url,
                "DUCKDB_OTLP_API_URL": f"{self.base_url}/latest.json",
                "HOME": str(self.root / "home"),
            }
        )
        environment.update(env or {})
        return subprocess.run(
            ["sh", str(SCRIPT), *args],
            capture_output=True,
            text=True,
            env=environment,
        )

    def test_installs_the_requested_release_as_an_executable(self):
        result = self.install("--version", TAG, "--bin-dir", str(self.bin_dir))
        self.assertEqual(result.returncode, 0, result.stderr)
        installed = self.bin_dir / "duckdb-otlp"
        self.assertEqual(installed.read_text(), PAYLOAD)
        self.assertTrue(os.access(installed, os.X_OK), "the installed binary is not executable")
        self.assertIn(str(installed), result.stderr)
        self.assertEqual(result.stdout, "", "the installer writes nothing to stdout")
        # The installer runs what it installed, so a binary that cannot start here is reported now.
        self.assertIn("duckdb-otlp 9.9.9-fixture", result.stderr)

    def test_resolves_the_latest_release_when_no_version_is_given(self):
        result = self.install("--bin-dir", str(self.bin_dir))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(TAG, result.stderr)
        self.assertEqual((self.bin_dir / "duckdb-otlp").read_text(), PAYLOAD)

    def test_replaces_an_existing_binary_without_leaving_staging_files_behind(self):
        self.bin_dir.mkdir()
        (self.bin_dir / "duckdb-otlp").write_text("#!/bin/sh\necho stale\n")
        result = self.install("--version", TAG, "--bin-dir", str(self.bin_dir))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.bin_dir / "duckdb-otlp").read_text(), PAYLOAD)
        self.assertEqual([entry.name for entry in self.bin_dir.iterdir()], ["duckdb-otlp"])

    def test_a_tampered_download_installs_nothing(self):
        self.write_sums("0" * 64)
        result = self.install("--version", TAG, "--bin-dir", str(self.bin_dir))
        self.assertEqual(result.returncode, 1)
        self.assertIn("checksum mismatch", result.stderr)
        self.assertFalse(self.bin_dir.exists(), "a failed verification must not install anything")

    def test_a_release_without_a_checksum_entry_installs_nothing(self):
        (self.published / TAG / "SHA256SUMS").write_text(f"{self.digest}  something-else.tar.gz\n")
        result = self.install("--version", TAG, "--bin-dir", str(self.bin_dir))
        self.assertEqual(result.returncode, 1)
        self.assertIn("SHA256SUMS has no entry", result.stderr)
        self.assertFalse(self.bin_dir.exists())

    def test_an_unknown_tag_fails_without_installing(self):
        result = self.install("--version", "v0.0.0-nope", "--bin-dir", str(self.bin_dir))
        self.assertEqual(result.returncode, 1)
        self.assertIn("cannot download", result.stderr)
        self.assertFalse(self.bin_dir.exists())

    def test_defaults_to_local_bin_under_home(self):
        result = self.install("--version", TAG)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.root / "home" / ".local" / "bin" / "duckdb-otlp").read_text(), PAYLOAD)
        self.assertIn("is not on your PATH", result.stderr)

    def test_environment_defaults_are_overridden_by_flags(self):
        flag_dir = self.root / "from-flag"
        result = self.install(
            "--bin-dir",
            str(flag_dir),
            env={"DUCKDB_OTLP_BIN_DIR": str(self.root / "from-env"), "DUCKDB_OTLP_VERSION": TAG},
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue((flag_dir / "duckdb-otlp").exists())
        self.assertFalse((self.root / "from-env").exists())

    def test_an_unrunnable_binary_is_reported_without_failing_the_install(self):
        # A release binary built for another platform, or against a newer glibc.
        archive_path = self.published / TAG / self.archive
        with tarfile.open(archive_path, "w:gz") as tar:
            member = tarfile.TarInfo("duckdb-otlp")
            member.size = 4
            tar.addfile(member, io.BytesIO(b"\x7fELF"))
        self.write_sums(hashlib.sha256(archive_path.read_bytes()).hexdigest())
        result = self.install("--version", TAG, "--bin-dir", str(self.bin_dir))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("did not run here", result.stderr)
        self.assertTrue((self.bin_dir / "duckdb-otlp").exists())

    def test_a_bad_command_line_is_exit_2(self):
        for args in (["--platform", "windows-amd64"], ["--frobnicate"], ["--version"]):
            with self.subTest(args=args):
                result = subprocess.run(
                    ["sh", str(SCRIPT), *args],
                    capture_output=True,
                    text=True,
                    env={**os.environ, "DUCKDB_OTLP_BASE_URL": self.base_url},
                )
                self.assertEqual(result.returncode, 2, result.stderr)

    def test_help_exits_zero(self):
        result = subprocess.run(["sh", str(SCRIPT), "--help"], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0)
        self.assertIn("--bin-dir", result.stderr)


if __name__ == "__main__":
    unittest.main()
