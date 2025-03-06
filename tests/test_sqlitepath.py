from unittest import TestCase

from sqlitepath.sqlitepath import SqliteConnect


class SqlitePathTest(TestCase):
    def test_repr(self):
        with SqliteConnect(":memory:") as sqlite:
            p = sqlite.Path("s1-1")
            self.assertEqual("SqlitePath('s1-1')", repr(p))

    def test_mkdir(self):
        with SqliteConnect(":memory:") as sqlite:
            p = sqlite.Path("s1-1")

            self.assertFalse(p.is_dir())
            p.mkdir()
            with self.assertRaises(FileExistsError):
                p.mkdir(exist_ok=False)
            self.assertTrue(p.is_dir())

            p = sqlite.Path("s1-1/s2-1")
            self.assertFalse(p.is_dir())
            p.mkdir()
            with self.assertRaises(FileExistsError):
                p.mkdir(exist_ok=False)
            p.mkdir(exist_ok=True)
            self.assertTrue(p.is_dir())

            p = sqlite.Path("s1-2/s2-2")
            self.assertFalse(p.is_dir())

            with self.assertRaises(FileNotFoundError):
                p.mkdir(parents=False)
            p.mkdir(parents=True, exist_ok=False)
            with self.assertRaises(FileExistsError):
                p.mkdir(parents=True, exist_ok=False)

    def test_write_bytes(self):
        with SqliteConnect(":memory:") as sqlite:
            p = sqlite.Path("s1-1")
            p.write_bytes(b"asd")
            self.assertEqual(len(sqlite), 1)
            p.write_bytes(b"qwe")
            self.assertEqual(len(sqlite), 1)

            p = sqlite.Path("s1-2")
            p.mkdir()
            self.assertEqual(len(sqlite), 2)
            with self.assertRaises(PermissionError):
                p.write_bytes(b"asd")
            self.assertEqual(len(sqlite), 2)

            p = sqlite.Path("s1-1/s2-1")
            with self.assertRaises(FileNotFoundError):
                p.write_bytes(b"asd")
            self.assertEqual(len(sqlite), 2)


if __name__ == "__main__":
    import unittest

    unittest.main()
