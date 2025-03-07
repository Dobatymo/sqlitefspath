import os
import sqlite3
import sys
from contextlib import contextmanager
from itertools import chain
from typing import Iterator, List, Optional, Tuple, Union

from typing_extensions import Self

ROOT_ID = 1


class SqliteFsPurePath:
    __slots__ = ("segments",)

    def __init__(self, *pathsegments: str) -> None:
        if pathsegments == ("",):
            self.segments = []
        else:
            self.segments = list(chain.from_iterable(segment.split("/") for segment in pathsegments))

    def __hash__(self) -> int:
        return hash(self.segments)

    def __eq__(self, other) -> bool:
        return self.segments == other.segments

    def __lt__(self, other) -> bool:
        return self.segments < other.segments

    # Operators

    def __truediv__(self, other) -> Self:
        return self.joinpath(other)

    def __rtruediv__(self, other) -> Self:
        return self.with_segments(other, *self.segments)

    # Accessing individual parts

    @property
    def parts(self) -> Tuple[str, ...]:
        raise NotImplementedError

    # Methods and properties

    @property
    def parser(self):
        raise NotImplementedError

    @property
    def drive(self) -> str:
        return ""

    @property
    def root(self) -> str:
        raise NotImplementedError

    @property
    def anchor(self) -> str:
        return self.drive + self.root

    @property
    def parents(self) -> Tuple[str, ...]:
        raise NotImplementedError

    @property
    def parent(self) -> Self:
        return self.with_segments(*self.segments[:-1])

    @property
    def name(self) -> str:
        if self.segments:
            return self.segments[-1]
        else:
            return ""

    @property
    def suffix(self) -> str:
        raise NotImplementedError

    @property
    def suffixes(self) -> List[str]:
        raise NotImplementedError

    @property
    def stem(self) -> str:
        raise NotImplementedError

    def as_posix(self) -> str:
        raise NotImplementedError

    def is_absolute(self) -> bool:
        raise NotImplementedError

    def is_relative_to(self, other) -> bool:
        raise NotImplementedError

    def is_reserved(self) -> bool:
        raise NotImplementedError

    def joinpath(self, *pathsegments: str) -> Self:
        return self.with_segments(*self.segments, *pathsegments)

    def full_match(self, pattern: str, *, case_sensitive: Optional[bool] = None) -> bool:
        raise NotImplementedError

    def match(self, pattern: str, *, case_sensitive: Optional[bool] = None) -> bool:
        raise NotImplementedError

    def relative_to(self, other, walk_up: bool = False) -> Self:
        raise NotImplementedError

    def with_name(self, name: str) -> Self:
        return self.with_segments(*self.segments[:-1], name)

    def with_stem(self, stem: str) -> Self:
        raise NotImplementedError

    def with_suffix(self, suffix: str) -> Self:
        raise NotImplementedError

    def with_segments(self, *pathsegments: str) -> Self:
        return type(self)(*pathsegments)


class SqliteFsPath(SqliteFsPurePath):
    __slots__ = ("conn", "parent_id", "_node_id", "_file_id")

    def with_segments(self, *pathsegments: str) -> Self:
        return type(self)(self.conn, *pathsegments)

    # internal helpers

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

    def _get_file_id(self) -> Optional[int]:
        parent_id = self.parent_id
        for segment in self.segments:
            node_id, file_id = self._find_node(parent_id, segment)
            parent_id = node_id
        return file_id

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

    def _read_file_id(self, file_id: int) -> bytes:
        sql = "SELECT data FROM data WHERE file_id = ?"
        cur = self.conn.execute(sql, (file_id,))
        result = cur.fetchone()
        assert result is not None
        (data,) = result
        return data

    def _read_file_existing(self, segment: str, parent_id: int) -> Tuple[bytes, int]:
        sql = "SELECT file_id FROM fs WHERE name = ? and parent_id = ?"
        cur = self.conn.execute(sql, (segment, parent_id))
        result = cur.fetchone()

        if result is None:
            raise FileNotFoundError(segment)

        (file_id,) = result

        if file_id is None:
            raise PermissionError(f"{segment} is a directory")

        data = self._read_file_id(file_id)
        return data, file_id

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

    # methods

    def __init__(self, conn: sqlite3.Connection, *pathsegments: str) -> None:
        super().__init__(*pathsegments)
        self.conn = conn

        self.parent_id = ROOT_ID
        self._node_id: Optional[int] = None
        self._file_id: Optional[int] = None

    def __str__(self) -> str:
        return "/".join(self.segments)

    def __repr__(self) -> str:
        return f"SqliteFsPath({repr(str(self))})"

    def __eq__(self, other) -> bool:
        return self.segments == other.segments and self.parent_id == other.parent_id

    @classmethod
    def with_meta(
        self, conn: sqlite3.Connection, *pathsegments: str, node_id: Optional[int] = None, file_id: Optional[int] = None
    ) -> "SqliteFsPath":
        path = SqliteFsPath(conn, *pathsegments)
        path._node_id = node_id
        path._file_id = file_id
        return path

    # Parsing and generating URIs

    @classmethod
    def from_uri(cls, uri: str) -> "SqliteFsPath":
        raise NotImplementedError

    def as_uri(self) -> str:
        raise NotImplementedError

    # Expanding and resolving paths

    @classmethod
    def home(cls) -> "SqliteFsPath":
        raise NotImplementedError

    def expanduser(self) -> "SqliteFsPath":
        raise NotImplementedError

    @classmethod
    def cwd(cls) -> "SqliteFsPath":
        raise NotImplementedError

    def absolute(self) -> "SqliteFsPath":
        raise NotImplementedError

    def resolve(self, strict: bool = False) -> "SqliteFsPath":
        raise NotImplementedError

    def readlink(self) -> "SqliteFsPath":
        raise NotImplementedError

    # Querying file type and status

    def stat(self, *, follow_symlinks: bool = True) -> os.stat_result:
        raise NotImplementedError

    def lstat(self) -> os.stat_result:
        raise NotImplementedError

    def exists(self, *, follow_symlinks: bool = True) -> bool:
        try:
            self._get_file_id()
        except FileNotFoundError:
            return False

        return True

    def is_file(self, *, follow_symlinks: bool = True) -> bool:
        if self._file_id is None:
            try:
                file_id = self._get_file_id()
            except FileNotFoundError:
                return False
            self._file_id = file_id
        else:
            file_id = self._file_id

        return file_id is not None

    def is_dir(self, *, follow_symlinks: bool = True) -> bool:
        if self._file_id is None:
            try:
                file_id = self._get_file_id()
            except FileNotFoundError:
                return False
            self._file_id = file_id
        else:
            file_id = self._file_id

        return file_id is None

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

    def samefile(self, other_path: "Union[str, SqliteFsPath]") -> bool:
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
        if sys.version_info < (3, 11):
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
        if self._file_id is None:
            parent_id = self.parent_id

            for segment in self.segments[:-1]:
                parent_id = self._select_directory(segment, parent_id)

            segment = self.segments[-1]
            data, file_id = self._read_file_existing(segment, parent_id)
            self._file_id = file_id
        else:
            data = self._read_file_id(self._file_id)

        return data

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

    def iterdir(self) -> "Iterator[SqliteFsPath]":
        parent_id = self.parent_id

        for segment in self.segments:
            parent_id = self._select_directory(segment, parent_id)

        sql = "SELECT id, name, file_id FROM fs WHERE parent_id = ?"
        cur = self.conn.execute(sql, (parent_id,))
        for node_id, name, file_id in cur:
            yield SqliteFsPath.with_meta(self.conn, *self.segments, name, node_id=node_id, file_id=file_id)

    def glob(self, pattern, *, case_sensitive: bool = None, recurse_symlinks: bool = False) -> "Iterator[SqliteFsPath]":
        raise NotImplementedError

    def rglob(
        self, pattern, *, case_sensitive: bool = None, recurse_symlinks: bool = False
    ) -> "Iterator[SqliteFsPath]":
        raise NotImplementedError

    def walk(
        self, top_down=True, on_error=None, follow_symlinks: bool = False
    ) -> "Iterator[Tuple[SqliteFsPath, List[str], List[str]]]":
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

    def rename(self, target: "Union[str, SqliteFsPath]") -> "SqliteFsPath":
        raise NotImplementedError

    def replace(self, target: "Union[str, SqliteFsPath]") -> "SqliteFsPath":
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
        sqlite_version = tuple(map(int, sqlite3.sqlite_version.split(".")))

        if sqlite_version < (3, 35, 0):
            raise RuntimeError("SQLite version 3.35.0 or higher is required")

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
        if sqlite_version >= (3, 37, 0):
            sql += " STRICT"

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
        if sqlite_version >= (3, 37, 0):
            sql += " STRICT"

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

    def Path(self, *pathsegments: str) -> SqliteFsPath:
        return SqliteFsPath(self.conn, *pathsegments)
