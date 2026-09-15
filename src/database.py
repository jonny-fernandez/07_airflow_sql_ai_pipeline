import sqlite3

from src.config import DB_PATH, SCHEMA_PATH


def get_connection():
    """
    Return a SQLite connection configured for this project.

    Foreign-key enforcement is enabled on every connection so that
    relational-integrity rules are not dependent on individual callers.
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row

    connection.execute("PRAGMA foreign_keys = ON;")

    return connection


def initialize_database():
    """
    Create all database tables and indexes defined in create_tables.sql.

    CREATE TABLE IF NOT EXISTS makes this operation safe to rerun.
    """
    if not SCHEMA_PATH.exists():
        raise FileNotFoundError(
            f"Database schema file not found: {SCHEMA_PATH}"
        )

    # utf-8-sig safely handles the BOM that PowerShell may write.
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8-sig")

    connection = get_connection()

    try:
        connection.executescript(schema_sql)
        connection.commit()
    finally:
        connection.close()

    return DB_PATH


def get_table_names():
    """
    Return user-created SQLite tables for validation/observability.
    """
    connection = get_connection()

    try:
        rows = connection.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
              AND name NOT LIKE 'sqlite_%'
            ORDER BY name;
            """
        ).fetchall()

        return [row["name"] for row in rows]

    finally:
        connection.close()


if __name__ == "__main__":
    database_path = initialize_database()

    print(f"Database initialized: {database_path}")
    print()
    print("Tables:")

    for table_name in get_table_names():
        print(f"  - {table_name}")
