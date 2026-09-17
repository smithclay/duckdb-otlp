import functools
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import io
from pathlib import Path
import sys
import tarfile
import tempfile
import threading
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from pages_extension_repository import ARCHIVE, assemble


class QuietHandler(SimpleHTTPRequestHandler):
    def log_message(self, *_args):
        pass


class ExtensionRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.published = self.root / "published"
        self.published.mkdir()
        handler = functools.partial(QuietHandler, directory=str(self.published))
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()
        self.addCleanup(self.stop_server)
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"
        self.paths = [
            "v1.5.4/linux_amd64/otlp.duckdb_extension.gz",
            "v1.5.4/wasm_eh/otlp.duckdb_extension.wasm",
        ]
        (self.published / "extensions").mkdir()
        (self.published / "extensions/index.html").write_text(
            "".join(f'<a href="../{path}">binary</a>' for path in self.paths)
        )
        for path in self.paths:
            binary = self.published / path
            binary.parent.mkdir(parents=True)
            binary.write_bytes(b"extension fixture")

    def stop_server(self):
        self.server.shutdown()
        self.thread.join()
        self.server.server_close()

    def test_recovers_expired_artifact_from_live_index_and_carries_archive_forward(
        self,
    ):
        first = self.root / "first"
        first.mkdir()
        (first / "index.html").write_text("new Astro site")
        assemble(first, base_url=self.base_url)
        for path in self.paths:
            self.assertEqual((first / path).read_bytes(), b"extension fixture")
        self.assertEqual((first / "index.html").read_text(), "new Astro site")
        # Future docs deploys need only the durable archive, not Actions history.
        (self.published / ARCHIVE).write_bytes((first / ARCHIVE).read_bytes())
        for path in self.paths:
            (self.published / path).unlink()
        (self.published / "extensions/index.html").unlink()
        second = self.root / "second"
        assemble(second, base_url=self.base_url)
        with tarfile.open(second / ARCHIVE) as archive:
            self.assertEqual(
                set(archive.getnames()),
                {".nojekyll", "extensions/index.html", *self.paths},
            )
        third = self.root / "third"
        assemble(third, archive=second / ARCHIVE)
        self.assertEqual((third / self.paths[1]).read_bytes(), b"extension fixture")

    def test_missing_binary_does_not_modify_site(self):
        (self.published / self.paths[1]).unlink()
        destination = self.root / "site"
        destination.mkdir()
        (destination / "index.html").write_text("site")
        with self.assertRaises(OSError):
            assemble(destination, base_url=self.base_url)
        self.assertEqual([p.name for p in destination.iterdir()], ["index.html"])

    def test_empty_index_is_not_a_repository(self):
        (self.published / "extensions/index.html").write_text("404 page")
        with self.assertRaisesRegex(ValueError, "no binaries"):
            assemble(self.root / "site", base_url=self.base_url)

    def test_invalid_archive_does_not_fall_back_to_older_index(self):
        (self.published / ARCHIVE).write_bytes(b"not a tarball")
        with self.assertRaises(tarfile.TarError):
            assemble(self.root / "site", base_url=self.base_url)

    def test_archive_cannot_overwrite_site_or_escape_destination(self):
        for path in ["index.html", "../escaped", "/absolute", "extensions/../../x"]:
            with self.subTest(path=path):
                archive = self.root / "bad.tar.gz"
                with tarfile.open(archive, "w:gz") as output:
                    info = tarfile.TarInfo(path)
                    info.size = 1
                    output.addfile(info, io.BytesIO(b"x"))
                with self.assertRaisesRegex(ValueError, "Unexpected"):
                    assemble(self.root / "site", archive=archive)


if __name__ == "__main__":
    unittest.main()
