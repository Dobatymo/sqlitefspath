import os
import sqlite3
import sys
from contextlib import contextmanager
from typing import Iterator, List, Optional, Tuple, Union

ROOT_ID = 1


class SqlitePath:
    def _find_node(self, parent_id: Optional[int], segment: str) -> Tuple[int, int]:
        if parent_id is None:
            cur = self.conn.execute(
                "SELECT id, file_id FROM fs WHERE parent_id IS NULL and name = ?",
                (segment,),
            )
        else:
            cur = self.conn.execute(
                "SELECT id, file_id FROM fs WHERE parent_id = ? and name = ?",
                (parent_id, segment),
            )
        res = cur.fetchone()
        if res is None:
            raise FileNotFoundError(segment)
        node_id, file_id = res
        return node_id, file_id

    def _insert_directory(self, segment: str, parent_id: int) -> int:
        sql_insert = "INSERT INTO fs (name, parent_id, file_id) VALUES (?, ?, ?) RETURNING rowid"
        try:
            cur = self.conn.execute(sql_insert, (segment, parent_id, None))
            (rowid,) = cur.fetchone()
            return rowid
        except sqlite3.IntegrityError as e:
            raise FileExistsError(segment) from e

    def _insert_ignore_directory(self, segment: str, parent_id: int) -> int:
        sql_insert_update = "INSERT INTO fs (name, parent_id, file_id) VALUES (?, ?, ?) ON CONFLICT DO UPDATE SET file_id = file_id RETURNING rowid, file_id"
        cur = self.conn.execute(sql_insert_update, (segment, parent_id, None))
        rowid, file_id = cur.fetchone()
        if file_id is not None:
            raise FileExistsError(f"{segment} is a file")
        return rowid

    def _select_directory(self, segment: str, parent_id: int) -> int:
        sql_select = "SELECT id FROM fs WHERE name = ? AND parent_id = ? AND file_id IS NULL"
        cur = self.conn.execute(sql_select, (segment, parent_id))
        result = cur.fetchone()
        if result is None:
            raise FileNotFoundError(segment)
        (rowid,) = result
        return rowid

    def _insert_file_overwrite(self, segment: str, parent_id: int, data: bytes) -> None:
        sql = "SELECT file_id FROM fs WHERE name = ? and parent_id = ?"
        cur = self.conn.execute(sql, (segment, parent_id))
        result = cur.fetchone()
        if result is None:
            sql = "INSERT INTO data (link_count, data) VALUES (1, ?) RETURNING file_id"
            cur = self.conn.execute(sql, (data,))
            result = cur.fetchone()
            assert result is not None
            (file_id,) = result
            sql = "INSERT INTO fs (name, parent_id, file_id) VALUES (?, ?, ?) RETURNING rowid"
            cur = self.conn.execute(sql, (segment, parent_id, file_id))
            result = cur.fetchone()
            assert result is not None
            (rowid,) = result
        else:
            (file_id,) = result
            if file_id is None:
                raise PermissionError(segment)
            sql = "UPDATE data SET data = ? WHERE file_id = ?"
            self.conn.execute(sql, (data, file_id))

    def __init__(self, conn: sqlite3.Connection, *pathsegments: str) -> None:
        assert len(pathsegments) == 1
        self.conn = conn
        self.segments = pathsegments[0].split("/")
        self.parent_id = ROOT_ID
        self.debug = False

    def __str__(self) -> str:
        return "/".join(self.segments)

    def __repr__(self) -> str:
        return f"SqlitePath({repr(str(self))})"

    # Parsing and generating URIs

    @classmethod
    def from_uri(cls, uri: str) -> "SqlitePath":
        raise NotImplementedError

    def as_uri(self) -> str:
        raise NotImplementedError

    # Expanding and resolving paths

    @classmethod
    def home(cls) -> "SqlitePath":
        raise NotImplementedError

    def expanduser(self) -> "SqlitePath":
        raise NotImplementedError

    @classmethod
    def cwd(cls) -> "SqlitePath":
        raise NotImplementedError

    def absolute(self) -> "SqlitePath":
        raise NotImplementedError

    def resolve(self, strict: bool = False) -> "SqlitePath":
        raise NotImplementedError

    def readlink(self) -> "SqlitePath":
        raise NotImplementedError

    # Querying file type and status

    def stat(self, *, follow_symlinks: bool = True) -> os.stat_result:
        raise NotImplementedError

    def lstat(self) -> os.stat_result:
        raise NotImplementedError

    def exists(self, *, follow_symlinks: bool = True) -> bool:
        parent_id = self.parent_id
        try:
            for segment in self.segments:
                node_id, file_id = self._find_node(parent_id, segment)
                parent_id = node_id
            return True
        except FileNotFoundError:
            return False

    def is_file(self, *, follow_symlinks: bool = True) -> bool:
        parent_id = self.parent_id
        try:
            for segment in self.segments:
                node_id, file_id = self._find_node(parent_id, segment)
                parent_id = node_id
            return file_id is not None
        except FileNotFoundError:
            return False

    def is_dir(self, *, follow_symlinks: bool = True) -> bool:
        parent_id = self.parent_id
        try:
            for segment in self.segments:
                node_id, file_id = self._find_node(parent_id, segment)
                parent_id = node_id
            return file_id is None
        except FileNotFoundError:
            return False

    def is_symlink(self) -> bool:
        return False

    def is_junction(self) -> bool:
        return False

    def is_mount(self) -> bool:
        return False

    def is_socket(self) -> bool:
        return False

    def is_fifo(self) -> bool:
        return False

    def is_block_device(self) -> bool:
        return False

    def is_char_device(self) -> bool:
        return False

    def samefile(self, other_path: "Union[str, SqlitePath]") -> bool:
        raise NotImplementedError

    # Reading and writing files

    @contextmanager
    def open(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
    ) -> "sqlite3.Blob":
        if sys.version < (3, 11):
            raise NotImplementedError

        parent_id = self.parent_id
        if mode == "rb":
            for segment in self.segments:
                node_id, file_id = self._find_node(parent_id, segment)
                parent_id = node_id

            if file_id is None:
                raise PermissionError(str(self))

            with self.conn.blobopen("data", "data", file_id, readonly=True) as blob:
                yield blob
        else:
            raise NotImplementedError

    def read_text(
        self, encoding: Optional[str] = None, errors: Optional[str] = None, newline: Optional[str] = None
    ) -> str:
        raise NotImplementedError

    def read_bytes(self) -> bytes:
        raise NotImplementedError

    def write_text(
        self, data: str, encoding: Optional[str] = None, errors: Optional[str] = None, newline: Optional[str] = None
    ) -> None:
        raise NotImplementedError

    def write_bytes(self, data: bytes) -> None:
        parent_id = self.parent_id

        for segment in self.segments[:-1]:
            parent_id = self._select_directory(segment, parent_id)

        segment = self.segments[-1]
        self._insert_file_overwrite(segment, parent_id, data)
        self.conn.commit()

    # Reading directories

    def iterdir(self) -> "Iterator[SqlitePath]":
        raise NotImplementedError

    def glob(self, pattern, *, case_sensitive: bool = None, recurse_symlinks: bool = False) -> "Iterator[SqlitePath]":
        raise NotImplementedError

    def rglob(self, pattern, *, case_sensitive: bool = None, recurse_symlinks: bool = False) -> "Iterator[SqlitePath]":
        raise NotImplementedError

    def walk(
        self, top_down=True, on_error=None, follow_symlinks: bool = False
    ) -> "Iterator[Tuple[SqlitePath, List[str], List[str]]]":
        raise NotImplementedError

    # Creating files and directories

    def touch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
        raise NotImplementedError

    def mkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None:
        parent_id = self.parent_id

        if parents:
            for segment in self.segments[:-1]:
                parent_id = self._insert_ignore_directory(segment, parent_id)
        else:
            for segment in self.segments[:-1]:
                parent_id = self._select_directory(segment, parent_id)

        segment = self.segments[-1]

        if exist_ok:
            parent_id = self._insert_ignore_directory(segment, parent_id)
        else:
            parent_id = self._insert_directory(segment, parent_id)

        self.conn.commit()

    def symlink_to(self, target: str, target_is_directory: bool = False) -> None:
        raise NotImplementedError

    def hardlink_to(self, target: str) -> None:
        raise NotImplementedError

    # Renaming and deleting

    def rename(self, target: "Union[str, SqlitePath]") -> "SqlitePath":
        raise NotImplementedError

    def replace(self, target: "Union[str, SqlitePath]") -> "SqlitePath":
        raise NotImplementedError

    def unlink(self, missing_ok: bool = False) -> None:
        raise NotImplementedError

    def rmdir(self) -> None:
        raise NotImplementedError

    # Permissions and ownership

    def owner(self, *, follow_symlinks: bool = True) -> str:
        raise NotImplementedError

    def group(self, *, follow_symlinks: bool = True) -> str:
        raise NotImplementedError

    def chmod(self, mode: int, *, follow_symlinks: bool = True) -> None:
        raise NotImplementedError

    def lchmod(self, mode: int) -> None:
        raise NotImplementedError


class SqliteConnect:
    def __init__(self, database: str, **kwargs) -> None:
        self.conn = sqlite3.connect(database, **kwargs)
        self.clear()
        sql = """CREATE TABLE IF NOT EXISTS fs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    parent_id INTEGER,  -- NULL for root entry
    file_id INTEGER,  -- NULL for directories, maps to file data
    FOREIGN KEY (parent_id) REFERENCES files(id)
    UNIQUE(name, parent_id)
)"""
        self.conn.execute(sql)
        cur = self.conn.execute(
            "INSERT INTO fs (id, name, parent_id) VALUES (?, ?, ?) RETURNING rowid",
            (ROOT_ID, "root", None),
        )
        (rowid,) = cur.fetchone()
        assert rowid == ROOT_ID, rowid

        sql = """CREATE TABLE IF NOT EXISTS data (
    file_id INTEGER PRIMARY KEY,
    link_count INTEGER,
    data BLOB,
    FOREIGN KEY (file_id) REFERENCES files(file_id)
)"""
        self.conn.execute(sql)
        self.conn.commit()

    def __enter__(self) -> "SqliteConnect":
        return self

    def __exit__(self, *args):
        self.conn.close()

    def __len__(self) -> int:
        sql = "SELECT count(*) FROM fs"
        cur = self.conn.execute(sql)
        (result,) = cur.fetchone()
        return result - 1  # ignore root

    def __bool__(self) -> bool:
        sql = "SELECT EXISTS (SELECT 1 FROM fs)"
        cur = self.conn.execute(sql)
        (result,) = cur.fetchone()
        return result == 1

    def clear(self) -> None:
        try:
            sql = "DELETE FROM data"
            self.conn.execute(sql)
            sql = "DELETE FROM fs"
            self.conn.execute(sql)
        except sqlite3.OperationalError as e:
            if e.args[0] not in ("no such table: data", "no such table: fs"):
                raise

    def Path(self, *pathsegments: str) -> SqlitePath:
        return SqlitePath(self.conn, *pathsegments)
