def pytest_sessionstart(session):
    from sqlite3 import sqlite_version

    print("sqlite3.sqlite_version", sqlite_version)
