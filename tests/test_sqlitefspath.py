import errno
import os
import shutil
from pathlib import Path, PurePosixPath
from tempfile import mkdtemp
from typing import Callable, Sequence, Tuple, Union
from unittest import TestCase

from sqlitefspath import sqlitefspath
from sqlitefspath.sqlitefspath import SqliteConnect, SqliteFsPurePath

USE_PATHLIB = bool(os.getenv("SQLITEFSPATHTEST_USE_PATHLIB", ""))
sqlitefspath.DISABLE_CACHE = bool(os.getenv("SQLITEFSPATHTEST_DISABLE_CACHE", ""))


def skip_not_implemented(func: Callable) -> Callable:
    def inner(self, *args, **kw):
        try:
            return func(self, *args, **kw)
        except NotImplementedError:
            self.skipTest("method not implemented")

    return inner


class PathlibPath(Path):
    def __repr__(self):
        return f"PathlibPath('{self}')"


class PathlibBase:
    def __init__(self, basepath: str):
        self.base = Path(basepath)

    def Path(self, *segments: str):
        return PathlibPath(*segments)

    def __len__(self):
        total = 0
        for _root, dirs, files in Path().walk():
            total += len(dirs)
            total += len(files)
        return total

    def __enter__(self):
        assert not self.base.exists()
        self.base.mkdir()
        self.cwd = os.getcwd()
        os.chdir(self.base)
        return self

    def __exit__(self, *args):
        os.chdir(self.cwd)
        shutil.rmtree(self.base)


def PurePath(*args, **kwargs):
    if USE_PATHLIB:
        return PurePosixPath(*args)
    else:
        return SqliteFsPurePath(*args)


def PathFactory():
    if USE_PATHLIB:
        return PathlibBase("tmp")
    else:
        return SqliteConnect(":memory:")


class AssertRaisesOSError:
    def __init__(self, instance, errno: Union[int, Sequence[int]]) -> None:
        self.instance = instance
        if isinstance(errno, int):
            self.errno: Tuple[int, ...] = (errno,)
        else:
            self.errno = tuple(errno)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if isinstance(exc_value, OSError):
            if exc_value.errno in self.errno:
                return True

            self.instance.fail(f"OSError was raised with errno={exc_value.errno}")
        self.instance.fail("OSError was not raised")


class SqliteFsPurePathTest(TestCase):
    def test_eq(self):
        p1 = PurePath("")
        p2 = PurePath("")
        self.assertEqual(p1, p2)

        p1 = PurePath("asd/qwe")
        p2 = PurePath("asd/qwe")
        self.assertEqual(p1, p2)

        p1 = PurePath("asd")
        p2 = PurePath("asd/")
        self.assertEqual(p1, p2)

        p1 = PurePath("asd/qwe")
        p2 = PurePath("asd//qwe")
        self.assertEqual(p1, p2)

        p1 = PurePath("asd/qwe/zxc")
        p2 = PurePath("asd", "qwe//", "zxc/")
        self.assertEqual(p1, p2)

        p1 = PurePath("asd")
        p2 = PurePath("qwe")
        self.assertNotEqual(p1, p2)

    def test_parent(self):
        p = PurePath("").parent
        truth = PurePath("")
        self.assertEqual(p, truth)

        p = PurePath("asd").parent
        truth = PurePath("")
        self.assertEqual(p, truth)

        p = PurePath("asd", "qwe").parent
        truth = PurePath("asd")
        self.assertEqual(p, truth)

    def test_name(self):
        p = PurePath().name
        truth = ""
        self.assertEqual(p, truth)

        p = PurePath("").name
        truth = ""
        self.assertEqual(p, truth)

        p = PurePath("asd").name
        truth = "asd"
        self.assertEqual(p, truth)

        p = (PurePath() / "asd").name
        truth = "asd"
        self.assertEqual(p, truth)

        p = PurePath("asd", "qwe").name
        truth = "qwe"
        self.assertEqual(p, truth)

        p = (PurePath("asd") / "qwe").name
        truth = "qwe"
        self.assertEqual(p, truth)

    def test_suffix(self):
        p = PurePath().suffix
        truth = ""
        self.assertEqual(p, truth)

        p = PurePath("").suffix
        truth = ""
        self.assertEqual(p, truth)

        p = PurePath("asd.").suffix
        truth = ""
        self.assertEqual(p, truth)

        p = PurePath("asd").suffix
        truth = ""
        self.assertEqual(p, truth)

        p = PurePath(".asd").suffix
        truth = ""
        self.assertEqual(p, truth)

        p = PurePath("asd.qwe").suffix
        truth = ".qwe"
        self.assertEqual(p, truth)

        p = PurePath(".asd.qwe").suffix
        truth = ".qwe"
        self.assertEqual(p, truth)

        p = PurePath("asd.qwe.zxc").suffix
        truth = ".zxc"
        self.assertEqual(p, truth)

        p = PurePath("123/").suffix
        truth = ""
        self.assertEqual(p, truth)

        p = PurePath("123/asd.").suffix
        truth = ""
        self.assertEqual(p, truth)

        p = PurePath("123/asd").suffix
        truth = ""
        self.assertEqual(p, truth)

        p = PurePath("123/.asd").suffix
        truth = ""
        self.assertEqual(p, truth)

        p = PurePath("123/asd.qwe").suffix
        truth = ".qwe"
        self.assertEqual(p, truth)

        p = PurePath("123/.asd.qwe").suffix
        truth = ".qwe"
        self.assertEqual(p, truth)

        p = PurePath("123/asd.qwe.zxc").suffix
        truth = ".zxc"
        self.assertEqual(p, truth)

    def test_stem(self):
        p = PurePath().stem
        truth = ""
        self.assertEqual(p, truth)

        p = PurePath("").stem
        truth = ""
        self.assertEqual(p, truth)

        p = PurePath("asd.").stem
        truth = "asd."
        self.assertEqual(p, truth)

        p = PurePath("asd").stem
        truth = "asd"
        self.assertEqual(p, truth)

        p = PurePath(".asd").stem
        truth = ".asd"
        self.assertEqual(p, truth)

        p = PurePath("asd.qwe").stem
        truth = "asd"
        self.assertEqual(p, truth)

        p = PurePath(".asd.qwe").stem
        truth = ".asd"
        self.assertEqual(p, truth)

        p = PurePath("asd.qwe.zxc").stem
        truth = "asd.qwe"
        self.assertEqual(p, truth)

        p = PurePath("123/").stem
        truth = "123"
        self.assertEqual(p, truth)

        p = PurePath("123/asd.").stem
        truth = "asd."
        self.assertEqual(p, truth)

        p = PurePath("123/asd").stem
        truth = "asd"
        self.assertEqual(p, truth)

        p = PurePath("123/.asd").stem
        truth = ".asd"
        self.assertEqual(p, truth)

        p = PurePath("123/asd.qwe").stem
        truth = "asd"
        self.assertEqual(p, truth)

        p = PurePath("123/.asd.qwe").stem
        truth = ".asd"
        self.assertEqual(p, truth)

        p = PurePath("123/asd.qwe.zxc").stem
        truth = "asd.qwe"
        self.assertEqual(p, truth)

    def test_suffixes(self):
        p = PurePath().suffixes
        truth = []
        self.assertEqual(p, truth)

        p = PurePath("").suffixes
        truth = []
        self.assertEqual(p, truth)

        p = PurePath("asd.").suffixes
        truth = []
        self.assertEqual(p, truth)

        p = PurePath("asd").suffixes
        truth = []
        self.assertEqual(p, truth)

        p = PurePath(".asd").suffixes
        truth = []
        self.assertEqual(p, truth)

        p = PurePath("asd.qwe").suffixes
        truth = [".qwe"]
        self.assertEqual(p, truth)

        p = PurePath(".asd.qwe").suffixes
        truth = [".qwe"]
        self.assertEqual(p, truth)

        p = PurePath("asd.qwe.zxc").suffixes
        truth = [".qwe", ".zxc"]
        self.assertEqual(p, truth)

        p = PurePath("asd.qwe.zxc.").suffixes
        truth = []
        self.assertEqual(p, truth)

        p = PurePath("123/").suffixes
        truth = []
        self.assertEqual(p, truth)

        p = PurePath("123/asd.").suffixes
        truth = []
        self.assertEqual(p, truth)

        p = PurePath("123/asd").suffixes
        truth = []
        self.assertEqual(p, truth)

        p = PurePath("123/.asd").suffixes
        truth = []
        self.assertEqual(p, truth)

        p = PurePath("123/asd.qwe").suffixes
        truth = [".qwe"]
        self.assertEqual(p, truth)

        p = PurePath("123/.asd.qwe").suffixes
        truth = [".qwe"]
        self.assertEqual(p, truth)

        p = PurePath("123/asd.qwe.zxc").suffixes
        truth = [".qwe", ".zxc"]
        self.assertEqual(p, truth)

    def test_with_name(self):
        with self.assertRaises(ValueError):
            p = PurePath().with_name("asd")

        with self.assertRaises(ValueError):
            p = PurePath("").with_name("asd")

        p = PurePath("asd").with_name("qwe")
        truth = PurePath("qwe")
        self.assertEqual(p, truth)

        p = PurePath("asd/").with_name("qwe")
        truth = PurePath("qwe")
        self.assertEqual(p, truth)

        p = PurePath("asd", "qwe").with_name("zxc")
        truth = PurePath("asd", "zxc")
        self.assertEqual(p, truth)


class SqliteFsPathTest(TestCase):
    def test_repr(self):
        with PathFactory() as sqlite:
            p = sqlite.Path("s1-1")
            self.assertEqual(f"{p.__class__.__name__}('s1-1')", repr(p))

    def test_mkdir_is_file_dir(self):
        with PathFactory() as sqlite:
            p = sqlite.Path("s1-1")

            self.assertFalse(p.is_dir())
            self.assertFalse(p.is_file())
            p.mkdir()
            with AssertRaisesOSError(self, errno.EEXIST):
                p.mkdir(exist_ok=False)
            self.assertTrue(p.is_dir())
            self.assertFalse(p.is_file())

            p = sqlite.Path("s1-1/s2-1")
            self.assertFalse(p.is_dir())
            self.assertFalse(p.is_file())
            p.mkdir()
            with AssertRaisesOSError(self, errno.EEXIST):
                p.mkdir(exist_ok=False)
            p.mkdir(exist_ok=True)
            self.assertTrue(p.is_dir())
            self.assertFalse(p.is_file())

            p = sqlite.Path("s1-2/s2-2")
            self.assertFalse(p.is_dir())

            with AssertRaisesOSError(self, errno.ENOENT):
                p.mkdir(parents=False)
            p.mkdir(parents=True, exist_ok=False)
            with AssertRaisesOSError(self, errno.EEXIST):
                p.mkdir(parents=True, exist_ok=False)

    def test_read_write_bytes(self):
        with PathFactory() as sqlite:
            p = sqlite.Path("s1-1")

            with AssertRaisesOSError(self, errno.ENOENT):
                p.read_bytes()

            p.write_bytes(b"")
            self.assertEqual(len(sqlite), 1)
            self.assertEqual(p.read_bytes(), b"")
            p.write_bytes(b"asd")
            self.assertEqual(len(sqlite), 1)
            self.assertEqual(p.read_bytes(), b"asd")
            p.write_bytes(b"qwe")
            self.assertEqual(len(sqlite), 1)
            self.assertEqual(p.read_bytes(), b"qwe")
            p.write_bytes(b"")
            self.assertEqual(len(sqlite), 1)
            self.assertEqual(p.read_bytes(), b"")

            p = sqlite.Path("s1-1/s2-1")
            with AssertRaisesOSError(self, (errno.ENOENT, errno.ENOTDIR)):  # windows, linux
                p.read_bytes()
            with AssertRaisesOSError(self, (errno.ENOENT, errno.ENOTDIR)):  # windows, linux
                p.write_bytes(b"asd")
            self.assertEqual(len(sqlite), 1)

    def test_read_write_bytes_mkdir(self):
        with PathFactory() as sqlite:
            p = sqlite.Path("s1-2")
            p.mkdir()
            self.assertEqual(len(sqlite), 1)
            with AssertRaisesOSError(self, (errno.EACCES, errno.EISDIR)):  # windows, linux
                p.read_bytes()
            with AssertRaisesOSError(self, (errno.EACCES, errno.EISDIR)):  # windows, linux
                p.write_bytes(b"asd")
            self.assertEqual(len(sqlite), 1)
            self.assertFalse(p.is_file())
            self.assertTrue(p.is_dir())

            p = sqlite.Path("s1-2/s2-1")
            with AssertRaisesOSError(self, errno.ENOENT):
                p.read_bytes()
            p.write_bytes(b"asd")
            self.assertEqual(len(sqlite), 2)
            self.assertEqual(p.read_bytes(), b"asd")
            self.assertTrue(p.is_file())
            self.assertFalse(p.is_dir())

    def test_stat(self):
        with PathFactory() as sqlite:
            p = sqlite.Path("s1-1")
            p.write_bytes(b"")
            self.assertEqual(p.stat().st_size, 0)
            p.write_bytes(b"asd")
            self.assertEqual(p.stat().st_size, 3)
            p.write_bytes(b"\x00\x01\x02")
            self.assertEqual(p.stat().st_size, 3)
            p.write_bytes(b"\xe4\xb8\xad\xe6\x96\x87")
            self.assertEqual(p.stat().st_size, 6)

            p = sqlite.Path("s1-2")
            p.mkdir()
            p = sqlite.Path("s1-2/s2-1")
            p.write_bytes(b"")
            self.assertEqual(p.stat().st_size, 0)
            p.write_bytes(b"asd")
            self.assertEqual(p.stat().st_size, 3)
            p.write_bytes(b"\x00\x01\x02")
            self.assertEqual(p.stat().st_size, 3)
            p.write_bytes(b"\xe4\xb8\xad\xe6\x96\x87")
            self.assertEqual(p.stat().st_size, 6)

    def test_iterdir(self):
        with PathFactory() as sqlite:
            truth = []

            basepath = sqlite.Path()
            self.assertEqual(list(basepath.iterdir()), truth)

            subpath = sqlite.Path("s1-1")
            subpath.mkdir()
            truth.append(subpath)
            self.assertEqual(list(basepath.iterdir()), truth)

            subpath = sqlite.Path("s1-2")
            subpath.write_bytes(b"asd")
            truth.append(subpath)
            self.assertEqual(list(basepath.iterdir()), truth)

    @skip_not_implemented
    def test_open(self):
        with PathFactory() as sqlite:
            data = b"asd"

            p = sqlite.Path("s1-2")
            p.write_bytes(data)
            with p.open("rb") as fr:
                self.assertEqual(fr.read(), data)

    def test_joinpath(self):
        with PathFactory() as sqlite:
            p1 = sqlite.Path("a/b")
            p2 = sqlite.Path("a") / "b"
            self.assertEqual(p1, p2)

            p1 = sqlite.Path("a/b")
            p2 = "a" / sqlite.Path("b")
            self.assertEqual(p1, p2)

            p1 = sqlite.Path("a/b/c")
            p2 = sqlite.Path("a").joinpath("b", "c")
            self.assertEqual(p1, p2)

    def test_hardlink_to(self):
        with PathFactory() as sqlite:
            p1 = sqlite.Path("f1")
            p1.write_bytes(b"")
            self.assertEqual(p1.stat().st_nlink, 1)

            p_dir = sqlite.Path("d1")
            p_dir.mkdir()

            p2 = sqlite.Path("f2")

            with AssertRaisesOSError(self, (errno.EACCES, errno.EPERM)):  # windows, linux
                p2.hardlink_to("d1")
            with AssertRaisesOSError(self, errno.ENOENT):
                p2.hardlink_to("f3")

            p2.hardlink_to("f1")
            self.assertEqual(p1.stat().st_nlink, 2)
            self.assertEqual(p2.stat().st_nlink, 2)

            with AssertRaisesOSError(self, errno.EEXIST):
                p2.hardlink_to("f1")

            with AssertRaisesOSError(self, errno.ENOENT):
                p2.hardlink_to("f3")

            p3 = sqlite.Path("d2/f1")
            with AssertRaisesOSError(self, errno.ENOENT):
                p3.hardlink_to("f1")

            with AssertRaisesOSError(self, errno.EEXIST):
                p_dir.hardlink_to("f1")

    def test_unlink(self):
        with PathFactory() as sqlite:
            p = sqlite.Path("f1")
            with AssertRaisesOSError(self, errno.ENOENT):
                p.unlink()

            p.write_bytes(b"")
            p.unlink()
            self.assertFalse(p.exists())

            p1 = sqlite.Path("f1")
            p1.write_bytes(b"")
            p2 = sqlite.Path("f2")
            p2.hardlink_to("f1")
            self.assertEqual(p1.stat().st_nlink, 2)
            self.assertEqual(p2.stat().st_nlink, 2)

            p2.unlink()
            self.assertEqual(p1.stat().st_nlink, 1)

            p1.unlink()
            with AssertRaisesOSError(self, errno.ENOENT):
                p1.unlink()
            p1.unlink(missing_ok=True)

            p = sqlite.Path("f1")
            p.mkdir()
            with AssertRaisesOSError(self, (errno.EACCES, errno.EISDIR, errno.EPERM)):  # windows, linux, macos
                p.unlink()


if not USE_PATHLIB:

    class SqliteFsPathFileTest(TestCase):
        def test_repeat_open(self):
            path = Path(mkdtemp(), "tmp.sqlite")

            try:
                with SqliteConnect(path) as sqlite:
                    p = sqlite.Path("f1")
                    p.write_bytes(b"")

                with SqliteConnect(path) as sqlite:
                    p = sqlite.Path("f1")
                    self.assertTrue(p.exists())
            finally:
                path.unlink()
                path.parent.rmdir()


if __name__ == "__main__":
    import unittest

    unittest.main()
