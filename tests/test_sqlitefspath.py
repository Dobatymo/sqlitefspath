from typing import Callable
from unittest import TestCase

from sqlitefspath.sqlitefspath import SqliteConnect, SqliteFsPurePath


def skip_not_implemented(func: Callable) -> Callable:
    def inner(self, *args, **kw):
        try:
            return func(self, *args, **kw)
        except NotImplementedError:
            self.skipTest("method not implemented")

    return inner


class SqliteFsPurePathTest(TestCase):
    def test_parent(self):
        p = SqliteFsPurePath("").parent
        truth = SqliteFsPurePath("")
        self.assertEqual(p, truth)

        p = SqliteFsPurePath("asd").parent
        truth = SqliteFsPurePath("")
        self.assertEqual(p, truth)

        p = SqliteFsPurePath("asd", "qwe").parent
        truth = SqliteFsPurePath("asd")
        self.assertEqual(p, truth)

    def test_name(self):
        p = SqliteFsPurePath().name
        truth = ""
        self.assertEqual(p, truth)

        p = SqliteFsPurePath("").name
        truth = ""
        self.assertEqual(p, truth)

        p = SqliteFsPurePath("asd").name
        truth = "asd"
        self.assertEqual(p, truth)

        p = SqliteFsPurePath("asd", "qwe").name
        truth = "qwe"
        self.assertEqual(p, truth)

    def test_with_name(self):
        p = SqliteFsPurePath().with_name("asd")
        truth = SqliteFsPurePath("asd")
        self.assertEqual(p, truth)

        p = SqliteFsPurePath("").with_name("asd")
        truth = SqliteFsPurePath("asd")
        self.assertEqual(p, truth)

        p = SqliteFsPurePath("asd").with_name("asd")
        truth = SqliteFsPurePath("asd")
        self.assertEqual(p, truth)

        p = SqliteFsPurePath("asd", "qwe").with_name("zxc")
        truth = SqliteFsPurePath("asd", "zxc")
        self.assertEqual(p, truth)


class SqliteFsPathTest(TestCase):
    def test_repr(self):
        with SqliteConnect(":memory:") as sqlite:
            p = sqlite.Path("s1-1")
            self.assertEqual("SqliteFsPath('s1-1')", repr(p))

    def test_mkdir_is_file_dir(self):
        with SqliteConnect(":memory:") as sqlite:
            p = sqlite.Path("s1-1")

            self.assertFalse(p.is_dir())
            self.assertFalse(p.is_file())
            p.mkdir()
            with self.assertRaises(FileExistsError):
                p.mkdir(exist_ok=False)
            self.assertTrue(p.is_dir())
            self.assertFalse(p.is_file())

            p = sqlite.Path("s1-1/s2-1")
            self.assertFalse(p.is_dir())
            self.assertFalse(p.is_file())
            p.mkdir()
            with self.assertRaises(FileExistsError):
                p.mkdir(exist_ok=False)
            p.mkdir(exist_ok=True)
            self.assertTrue(p.is_dir())
            self.assertFalse(p.is_file())

            p = sqlite.Path("s1-2/s2-2")
            self.assertFalse(p.is_dir())

            with self.assertRaises(FileNotFoundError):
                p.mkdir(parents=False)
            p.mkdir(parents=True, exist_ok=False)
            with self.assertRaises(FileExistsError):
                p.mkdir(parents=True, exist_ok=False)

    def test_read_write_bytes(self):
        with SqliteConnect(":memory:") as sqlite:
            p = sqlite.Path("s1-1")

            with self.assertRaises(FileNotFoundError):
                p.read_bytes()

            p.write_bytes(b"asd")
            self.assertEqual(len(sqlite), 1)
            self.assertEqual(p.read_bytes(), b"asd")
            p.write_bytes(b"qwe")
            self.assertEqual(len(sqlite), 1)
            self.assertEqual(p.read_bytes(), b"qwe")

            p = sqlite.Path("s1-1/s2-1")
            with self.assertRaises(FileNotFoundError):
                p.read_bytes()
            with self.assertRaises(FileNotFoundError):
                p.write_bytes(b"asd")
            self.assertEqual(len(sqlite), 1)

    def test_read_write_bytes_mkdir(self):
        with SqliteConnect(":memory:") as sqlite:
            p = sqlite.Path("s1-2")
            p.mkdir()
            self.assertEqual(len(sqlite), 1)
            with self.assertRaises(PermissionError):
                p.read_bytes()
            with self.assertRaises(PermissionError):
                p.write_bytes(b"asd")
            self.assertEqual(len(sqlite), 1)
            self.assertFalse(p.is_file())
            self.assertTrue(p.is_dir())

            p = sqlite.Path("s1-2/s2-1")
            with self.assertRaises(FileNotFoundError):
                p.read_bytes()
            p.write_bytes(b"asd")
            self.assertEqual(len(sqlite), 2)
            self.assertEqual(p.read_bytes(), b"asd")
            self.assertTrue(p.is_file())
            self.assertFalse(p.is_dir())

    def test_iterdir(self):
        with SqliteConnect(":memory:") as sqlite:
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
        with SqliteConnect(":memory:") as sqlite:
            data = b"asd"

            p = sqlite.Path("s1-2")
            p.write_bytes(data)
            with p.open("rb") as fr:
                self.assertEqual(fr.read(), data)

    def test_joinpath(self):
        with SqliteConnect(":memory:") as sqlite:
            p1 = sqlite.Path("a/b")
            p2 = sqlite.Path("a") / "b"
            self.assertEqual(p1, p2)

            p1 = sqlite.Path("a/b")
            p2 = "a" / sqlite.Path("b")
            self.assertEqual(p1, p2)

            p1 = sqlite.Path("a/b/c")
            p2 = sqlite.Path("a").joinpath("b", "c")
            self.assertEqual(p1, p2)


if __name__ == "__main__":
    import unittest

    unittest.main()
