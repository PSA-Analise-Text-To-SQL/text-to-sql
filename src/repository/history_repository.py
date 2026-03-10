import mysql.connector
from src.models.history_entry import HistoryEntry
from datetime import datetime


class HistoryRepository:
    def __init__(self, host: str, user: str, password: str, database: str, port: int = 3306):
        self._config = {
            "host": host,
            "user": user,
            "password": password,
            "database": database,
            "port": port
        }
        self._ensure_table()

    def _get_connection(self):
        return mysql.connector.connect(**self._config)

    def _ensure_table(self):
        """Cria a tabela se ela não existir."""
        sql = """
            CREATE TABLE IF NOT EXISTS query_history (
                id            INT AUTO_INCREMENT PRIMARY KEY,
                database_name VARCHAR(255)  NOT NULL,
                question      TEXT          NOT NULL,
                generated_query TEXT        NOT NULL,
                result_preview  TEXT        NOT NULL,
                created_at    DATETIME      NOT NULL
            )
        """
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
            conn.commit()

    def save(self, entry: HistoryEntry) -> HistoryEntry:
        sql = """
            INSERT INTO query_history
                (database_name, question, generated_query, result_preview, created_at)
            VALUES (%s, %s, %s, %s, %s)
        """
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (
                    entry.database_name,
                    entry.question,
                    entry.generated_query,
                    entry.result_preview,
                    entry.created_at
                ))
            conn.commit()
            entry.id = cursor.lastrowid
        return entry

    def find_all(self) -> list[HistoryEntry]:
        sql = "SELECT id, database_name, question, generated_query, result_preview, created_at FROM query_history ORDER BY created_at DESC"
        with self._get_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
        return [HistoryEntry(**row) for row in rows]

    def clear(self):
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM query_history")
            conn.commit()
