import errno
import os
import sqlite3
import sys
from contextlib import contextmanager
from itertools import chain
from os import strerror
from typing import Callable, Final, Iterator, List, NamedTuple, Optional, Tuple, Union

from typing_extensions import Self

ROOT_ID = 1
DISABLE_CACHE = False


class UnsetType:
    pass


UNSET: Final = UnsetType()


class StatResult(NamedTuple):
    st_mode: int
    st_ino: int
    st_dev: int = 0
    st_nlink: Optional[int] = None
    st_uid: Optional[int] = None
    st_gid: Optional[int] = None
    st_size: Optional[int] = None
    st_atime: Optional[int] = None
    st_mtime: Optional[int] = None
    st_ctime: Optional[int] = None
    st_atime_ns: Optional[int] = None
    st_mtime_ns: Optional[int] = None
    st_ctime_ns: Optional[int] = None


class SqliteFsPurePath:
    __slots__ = ("segments",)

    def __init__(self, *pathsegments: str) -> None:
        for segment in pathsegments:
            if segment.startswith("/"):
                raise ValueError("paths starting with / are currently not supported")

        if pathsegments == ("",):
            self.segments = []
        else:
            self.segments = list(s for s in chain.from_iterable(segment.split("/") for segment in pathsegments) if s)

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
        name = self.name
        if name.endswith("."):
            return ""
        pos = name.rfind(".")
        if pos == -1:
            return ""
        elif pos == 0:
            return ""
        else:
            return name[pos:]

    @property
    def suffixes(self) -> List[str]:
        parts = self.name.split(".")
        if len(parts) == 1 or not parts[-1]:
            return []
        elif len(parts) > 1:
            if parts[0]:
                return ["." + part for part in parts[1:]]
            else:
                return ["." + part for part in parts[2:]]
        else:
            assert False

    @property
    def stem(self) -> str:
        name = self.name
        pos = name.rfind(".")
        if name.endswith("."):
            return name
        if pos == -1:
            return name
        elif pos == 0:
            return name
        else:
            return name[:pos]

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
        if self.segments:
            return self.with_segments(*self.segments[:-1], name)

        raise ValueError("path has an empty name")

    def with_stem(self, stem: str) -> Self:
        raise NotImplementedError

    def with_suffix(self, suffix: str) -> Self:
        return self.with_name(self.stem + suffix)

    def with_segments(self, *pathsegments: str) -> Self:
        return type(self)(*pathsegments)


class SqliteFsPath(SqliteFsPurePath):
    __slots__ = ("conn", "parent_id", "_node_id", "_file_id")

    _node_id: Optional[int]
    _file_id: Union[UnsetType, None, int]

    def __init__(self, conn: sqlite3.Connection, *pathsegments: str) -> None:
        super().__init__(*pathsegments)
        self.conn = conn

        self.parent_id = ROOT_ID

        if DISABLE_CACHE:

            def getter(value) -> Callable:
                return lambda self: value

            def setter(self, value) -> None:
                pass

            SqliteFsPath._node_id = property(getter(None), setter)  # type: ignore[assignment,misc]
            SqliteFsPath._file_id = property(getter(UNSET), setter)  # type: ignore[assignment,misc]
        else:
            self._node_id = None
            self._file_id = UNSET

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
            raise OSError(errno.ENOENT, strerror(errno.ENOENT), segment)
        node_id, file_id = res
        return node_id, file_id

    def _get_file_id_by_node_id(self, node_id: int) -> Optional[int]:
        sql = "SELECT file_id FROM fs WHERE id = ?"
        cur = self.conn.execute(sql, (node_id,))
        res = cur.fetchone()
        if res is None:
            raise OSError(errno.ENOENT, strerror(errno.ENOENT), str(self))
        (file_id,) = res
        return file_id

    def _get_ids(self) -> Tuple[int, Optional[int]]:
        node_id = self._node_id
        file_id = self._file_id

        if isinstance(file_id, UnsetType):
            if node_id is None:
                parent_id = self.parent_id
                for segment in self.segments:
                    node_id, file_id = self._find_node(parent_id, segment)
                    parent_id = node_id
            else:
                file_id = self._get_file_id_by_node_id(node_id)
        assert node_id is not None
        assert not isinstance(file_id, UnsetType)

        self._node_id = node_id
        self._file_id = file_id
        return node_id, file_id

    def _insert_directory(self, segment: str, parent_id: int) -> Tuple[int, None]:
        sql_insert = "INSERT INTO fs (name, parent_id, file_id) VALUES (?, ?, ?) RETURNING rowid"
        file_id = None
        try:
            cur = self.conn.execute(sql_insert, (segment, parent_id, file_id))
            (node_id,) = cur.fetchone()
            return node_id, file_id
        except sqlite3.IntegrityError as e:
            raise OSError(errno.EEXIST, strerror(errno.EEXIST), segment) from e

    def _insert_ignore_directory(self, segment: str, parent_id: int) -> Tuple[int, None]:
        """Returns a node_id, file_id tuple"""

        sql_insert_update = "INSERT INTO fs (name, parent_id, file_id) VALUES (?, ?, ?) ON CONFLICT DO UPDATE SET file_id = file_id RETURNING rowid, file_id"
        cur = self.conn.execute(sql_insert_update, (segment, parent_id, None))
        node_id, file_id = cur.fetchone()
        if file_id is not None:
            raise OSError(errno.EEXIST, strerror(errno.EEXIST), segment)
        return node_id, file_id

    def _select_directory(self, segment: str, parent_id: int) -> int:
        sql = "SELECT id FROM fs WHERE name = ? AND parent_id = ? AND file_id IS NULL"
        cur = self.conn.execute(sql, (segment, parent_id))
        result = cur.fetchone()
        if result is None:
            raise OSError(errno.ENOENT, strerror(errno.ENOENT), segment)
        (node_id,) = result
        return node_id

    def _remove_directory_by_node_id(self, node_id: int) -> None:
        sql = "DELETE FROM fs WHERE id = ? and file_id IS NULL RETURNING 1"
        cur = self.conn.execute(sql, (node_id,))
        result = cur.fetchone()
        if result is None:
            raise OSError(errno.ENOENT, strerror(errno.ENOENT), str(self))

    def _remove_directory(self, segment: str, parent_id: int) -> int:
        sql = "DELETE FROM fs WHERE name = ? AND parent_id = ? AND file_id IS NULL RETURNING rowid"
        cur = self.conn.execute(sql, (segment, parent_id))
        result = cur.fetchone()
        if result is None:
            raise OSError(errno.ENOENT, strerror(errno.ENOENT), segment)
        (node_id,) = result
        return node_id

    def _read_file_id(self, file_id: int) -> Tuple[int, bytes]:
        assert isinstance(file_id, int)
        sql = "SELECT length(data), data FROM data WHERE file_id = ?"
        cur = self.conn.execute(sql, (file_id,))
        result = cur.fetchone()
        assert result is not None
        filesize, data = result
        return filesize, data

    def _read_file_meta_id(self, file_id: int) -> Tuple[int, int]:
        sql = "SELECT length(data), link_count FROM data WHERE file_id = ?"
        cur = self.conn.execute(sql, (file_id,))
        result = cur.fetchone()
        assert result is not None
        filesize, link_count = result
        return filesize, link_count

    def _read_file_by_node_id(self, node_id: int) -> Tuple[int, bytes, int]:
        sql = "SELECT file_id FROM fs WHERE id = ?"
        cur = self.conn.execute(sql, (node_id,))
        result = cur.fetchone()

        if result is None:
            raise OSError(errno.ENOENT, strerror(errno.ENOENT), str(self))

        (file_id,) = result

        if file_id is None:
            raise OSError(errno.EACCES, strerror(errno.EACCES), str(self))

        filesize, data = self._read_file_id(file_id)
        return filesize, data, file_id

    def _read_file_existing(self, segment: str, parent_id: int) -> Tuple[int, bytes, int]:
        sql = "SELECT file_id FROM fs WHERE name = ? and parent_id = ?"
        cur = self.conn.execute(sql, (segment, parent_id))
        result = cur.fetchone()

        if result is None:
            raise OSError(errno.ENOENT, strerror(errno.ENOENT), segment)

        (file_id,) = result

        if file_id is None:
            raise OSError(errno.EACCES, strerror(errno.EACCES), str(self))

        filesize, data = self._read_file_id(file_id)
        return filesize, data, file_id

    def _insert_file_overwrite_id(self, data: bytes, file_id: int) -> None:
        sql = "UPDATE data SET data = ? WHERE file_id = ?"
        self.conn.execute(sql, (data, file_id))

    def _insert_file_by_node_id(self, node_id: int, data: bytes) -> Optional[int]:
        sql = "SELECT file_id FROM fs WHERE id = ?"
        cur = self.conn.execute(sql, (node_id,))
        result = cur.fetchone()
        assert result is not None
        (file_id,) = result
        if file_id is None:
            return file_id
        self._insert_file_overwrite_id(data, file_id)
        return file_id

    def _insert_file_overwrite(self, segment: str, parent_id: int, data: bytes) -> Tuple[int, Optional[int]]:
        """Returns node_id, file_id tuple.
        If file_id is None, the path points to a directory and a PermissionError should be raised.
        """

        sql = "SELECT id, file_id FROM fs WHERE name = ? and parent_id = ?"
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
            (node_id,) = result
        else:
            node_id, file_id = result
            if file_id is None:
                return node_id, file_id
            self._insert_file_overwrite_id(data, file_id)
        return node_id, file_id

    def _insert_hardlink_new(self, segment: str, parent_id: int, file_id: int) -> Tuple[int, int]:
        try:
            sql = "INSERT INTO fs (name, parent_id, file_id) VALUES (?, ?, ?) RETURNING rowid"
            cur = self.conn.execute(sql, (segment, parent_id, file_id))
            result = cur.fetchone()
            assert result is not None
            (node_id,) = result
        except sqlite3.IntegrityError as e:
            raise OSError(errno.EEXIST, strerror(errno.EEXIST), segment) from e

        sql = "UPDATE data SET link_count = link_count + 1 WHERE file_id = ? RETURNING link_count"
        cur = self.conn.execute(sql, (file_id,))
        result = cur.fetchone()
        assert result is not None
        (link_count,) = result
        return node_id, link_count

    # methods

    def __str__(self) -> str:
        return "/".join(self.segments)

    def __repr__(self) -> str:
        return f"SqliteFsPath({repr(str(self))})"

    def __eq__(self, other) -> bool:
        return self.segments == other.segments and self.parent_id == other.parent_id

    @classmethod
    def with_meta(
        cls,
        conn: sqlite3.Connection,
        *pathsegments: str,
        node_id: Optional[int] = None,
        file_id: Union[UnsetType, None, int] = UNSET,
    ) -> Self:
        path = cls(conn, *pathsegments)
        path._node_id = node_id
        path._file_id = file_id
        return path

    # Parsing and generating URIs

    @classmethod
    def from_uri(cls, uri: str) -> Self:
        raise NotImplementedError

    def as_uri(self) -> str:
        raise NotImplementedError

    # Expanding and resolving paths

    @classmethod
    def home(cls) -> Self:
        raise NotImplementedError

    def expanduser(self) -> Self:
        raise NotImplementedError

    @classmethod
    def cwd(cls) -> Self:
        raise NotImplementedError

    def absolute(self) -> Self:
        raise NotImplementedError

    def resolve(self, strict: bool = False) -> Self:
        raise NotImplementedError

    def readlink(self) -> Self:
        raise NotImplementedError

    # Querying file type and status

    def stat(self, *, follow_symlinks: bool = True) -> StatResult:
        node_id, file_id = self._get_ids()
        if file_id is None:
            st_mode = 0o040000
            st_ino = node_id
            st_size = None
            st_nlink = 1
        else:
            st_mode = 0o100000
            st_ino = node_id
            st_size, st_nlink = self._read_file_meta_id(file_id)
        return StatResult(st_mode=st_mode, st_ino=st_ino, st_nlink=st_nlink, st_size=st_size)

    def lstat(self) -> os.stat_result:
        raise NotImplementedError

    def exists(self, *, follow_symlinks: bool = True) -> bool:
        try:
            self._get_ids()
        except FileNotFoundError:
            return False

        return True

    def is_file(self, *, follow_symlinks: bool = True) -> bool:
        try:
            node_id, file_id = self._get_ids()
        except FileNotFoundError:
            return False

        return file_id is not None

    def is_dir(self, *, follow_symlinks: bool = True) -> bool:
        try:
            node_id, file_id = self._get_ids()
        except FileNotFoundError:
            return False

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
    ) -> Iterator["sqlite3.Blob"]:  # type: ignore[name-defined,unused-ignore]
        if sys.version_info < (3, 11):
            raise NotImplementedError

        if mode == "rb":  # type: ignore[unreachable,unused-ignore]
            node_id, file_id = self._get_ids()

            if file_id is None:
                raise OSError(errno.EACCES, strerror(errno.EACCES), str(self))

            with self.conn.blobopen("data", "data", file_id, readonly=True) as blob:
                yield blob
        else:
            raise NotImplementedError

    def read_text(
        self, encoding: Optional[str] = None, errors: Optional[str] = None, newline: Optional[str] = None
    ) -> str:
        raise NotImplementedError

    def read_bytes(self) -> bytes:
        node_id = self._node_id
        file_id = self._file_id

        if isinstance(file_id, UnsetType):
            if node_id is None:
                parent_id = self.parent_id

                for segment in self.segments[:-1]:
                    parent_id = self._select_directory(segment, parent_id)

                segment = self.segments[-1]
                filesize, data, file_id = self._read_file_existing(segment, parent_id)
            else:
                filesize, data, file_id = self._read_file_by_node_id(node_id)
            self._file_id = file_id

        if file_id is None:  # is a directory
            raise OSError(errno.EACCES, strerror(errno.EACCES), str(self))

        filesize, data = self._read_file_id(file_id)

        return data

    def write_text(
        self, data: str, encoding: Optional[str] = None, errors: Optional[str] = None, newline: Optional[str] = None
    ) -> None:
        raise NotImplementedError

    def write_bytes(self, data: bytes) -> None:
        node_id = self._node_id
        file_id = self._file_id

        if file_id is None:
            raise OSError(errno.EACCES, strerror(errno.EACCES), str(self))

        elif isinstance(file_id, int):
            with self.conn:
                self._insert_file_overwrite_id(data, file_id)

        else:  # file_id is UNSET
            with self.conn:
                if node_id is None:
                    parent_id = self.parent_id

                    for segment in self.segments[:-1]:
                        parent_id = self._select_directory(segment, parent_id)

                    segment = self.segments[-1]
                    node_id, file_id = self._insert_file_overwrite(segment, parent_id, data)
                    self._node_id = node_id
                else:
                    file_id = self._insert_file_by_node_id(node_id, data)

            self._file_id = file_id

            if file_id is None:
                raise OSError(errno.EACCES, strerror(errno.EACCES), str(self))

    # Reading directories

    def iterdir(self) -> "Iterator[SqliteFsPath]":
        node_id = self._node_id

        if node_id is None:
            parent_id = self.parent_id
            for segment in self.segments:
                parent_id = self._select_directory(segment, parent_id)
            node_id = parent_id
            self._node_id = node_id

        sql = "SELECT id, name, file_id FROM fs WHERE parent_id = ?"
        cur = self.conn.execute(sql, (node_id,))
        for child_node_id, child_name, child_file_id in cur:
            yield SqliteFsPath.with_meta(
                self.conn, *self.segments, child_name, node_id=child_node_id, file_id=child_file_id
            )

    def glob(
        self, pattern, *, case_sensitive: Optional[bool] = None, recurse_symlinks: bool = False
    ) -> "Iterator[SqliteFsPath]":
        raise NotImplementedError

    def rglob(
        self, pattern, *, case_sensitive: Optional[bool] = None, recurse_symlinks: bool = False
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
        node_id = self._node_id

        if node_id is not None and not exist_ok:
            raise OSError(errno.EEXIST, strerror(errno.EEXIST), str(self))

        parent_id = self.parent_id

        with self.conn:
            if parents:
                for segment in self.segments[:-1]:
                    parent_id, _file_id = self._insert_ignore_directory(segment, parent_id)
            else:
                for segment in self.segments[:-1]:
                    parent_id = self._select_directory(segment, parent_id)

            segment = self.segments[-1]

            if exist_ok:
                node_id, file_id = self._insert_ignore_directory(segment, parent_id)
            else:
                node_id, file_id = self._insert_directory(segment, parent_id)

        self._node_id = node_id
        self._file_id = file_id

    def symlink_to(self, target: str, target_is_directory: bool = False) -> None:
        raise NotImplementedError

    def hardlink_to(self, target: str) -> None:
        """Possible exceptions:
        - if parent directories of self don't exist: FileNotFoundError
        - if target does not exists: FileNotFoundError
        - if target is a directory: PermissionError
        - if self already exists (file / directory): FileExistsError
        - if self already exists, but target does not: FileNotFoundError
        """

        file_id = self._file_id

        parent_id = self.parent_id
        for segment in self.segments[:-1]:
            parent_id = self._select_directory(segment, parent_id)

        target_node_id, target_file_id = SqliteFsPath(self.conn, target)._get_ids()
        if target_file_id is None:
            raise OSError(errno.EACCES, strerror(errno.EACCES), target)

        if isinstance(file_id, int) or file_id is None:
            raise OSError(errno.EEXIST, strerror(errno.EEXIST), str(self))

        segment = self.segments[-1]
        node_id, link_count = self._insert_hardlink_new(segment, parent_id, target_file_id)

        self._node_id = node_id
        self._file_id = target_file_id

    # Renaming and deleting

    def rename(self, target: "Union[str, SqliteFsPath]") -> Self:
        raise NotImplementedError

    def replace(self, target: "Union[str, SqliteFsPath]") -> Self:
        raise NotImplementedError

    def unlink(self, missing_ok: bool = False) -> None:
        try:
            node_id, file_id = self._get_ids()
        except FileNotFoundError:
            if missing_ok:
                return
            raise

        if file_id is None:
            raise OSError(errno.EACCES, strerror(errno.EACCES), str(self))

        with self.conn:
            # todo: use executescript
            sql = "DELETE FROM fs WHERE id = ?"
            self.conn.execute(sql, (node_id,))

            sql = "UPDATE data SET link_count = link_count - 1 WHERE file_id = ?"
            self.conn.execute(sql, (file_id,))

        self._node_id = None
        self._file_id = UNSET

    def rmdir(self) -> None:
        node_id = self._node_id
        file_id = self._file_id

        if node_id is not None:
            if file_id is None:
                raise OSError(errno.EACCES, strerror(errno.EACCES), str(self))
            self._remove_directory_by_node_id(node_id)
        else:
            parent_id = self.parent_id
            for segment in self.segments[:-1]:
                parent_id = self._select_directory(segment, parent_id)

            segment = self.segments[-1]
            self._remove_directory(segment, parent_id)

        self._node_id = None
        self._file_id = UNSET

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

        with self.conn:
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
            try:
                cur = self.conn.execute(
                    "INSERT INTO fs (id, name, parent_id) VALUES (?, ?, ?) RETURNING rowid",
                    (ROOT_ID, "root", None),
                )
                (node_id,) = cur.fetchone()
                assert node_id == ROOT_ID, node_id
            except sqlite3.IntegrityError:
                pass  # root already exists

            sql = """CREATE TABLE IF NOT EXISTS data (
    file_id INTEGER PRIMARY KEY,
    link_count INTEGER,
    data BLOB,
    FOREIGN KEY (file_id) REFERENCES files(file_id)
)"""
            if sqlite_version >= (3, 37, 0):
                sql += " STRICT"

            self.conn.execute(sql)

            sql = """CREATE TRIGGER IF NOT EXISTS unlink_unreferenced
AFTER UPDATE OF link_count ON data
FOR EACH ROW
WHEN NEW.link_count = 0
BEGIN
    DELETE FROM data WHERE file_id = NEW.file_id;
END"""
            self.conn.execute(sql)

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
