import sqlite3
from typing import Dict, Any, Optional, List
import json
from pathlib import Path


class SQLiteMetastore:
    def __init__(self, db_path: str = "metastore.db"):
        self.db_path = Path(db_path)
        self.conn = sqlite3.connect(self.db_path)
        self._initialize_db()

    def _initialize_db(self):
        """Create tables if they don't exist"""
        cursor = self.conn.cursor()

        # Main metadata table
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        )

        # Version history table (optional)
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS metadata_versions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            key TEXT,
            value TEXT,
            created_at TIMESTAMP,
            FOREIGN KEY(key) REFERENCES metadata(key)
        )
        """
        )

        self.conn.commit()

    def set_metadata(self, key: str, value: Any, versioned: bool = False):
        """Store metadata with the given key"""
        value_str = json.dumps(value) if not isinstance(value, str) else value

        cursor = self.conn.cursor()

        # Check if key exists
        cursor.execute("SELECT 1 FROM metadata WHERE key = ?", (key,))
        exists = cursor.fetchone()

        if exists:
            if versioned:
                # Save current version to history before updating
                cursor.execute(
                    "INSERT INTO metadata_versions (key, value, created_at) "
                    "SELECT key, value, updated_at FROM metadata WHERE key = ?",
                    (key,),
                )
            # Update existing record
            cursor.execute(
                "UPDATE metadata SET value = ?, updated_at = CURRENT_TIMESTAMP WHERE key = ?",
                (value_str, key),
            )
        else:
            # Insert new record
            cursor.execute(
                "INSERT INTO metadata (key, value) VALUES (?, ?)", (key, value_str)
            )

        self.conn.commit()

    def get_metadata(self, key: str, default: Optional[Any] = None) -> Any:
        """Retrieve metadata for the given key"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT value FROM metadata WHERE key = ?", (key,))
        result = cursor.fetchone()

        if result:
            try:
                return json.loads(result[0])
            except json.JSONDecodeError:
                return result[0]
        return default

    def get_metadata_with_timestamps(self, key: str) -> Optional[Dict[str, Any]]:
        """Retrieve metadata along with timestamps"""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT key, value, created_at, updated_at FROM metadata WHERE key = ?",
            (key,),
        )
        result = cursor.fetchone()

        if result:
            try:
                value = json.loads(result[1])
            except json.JSONDecodeError:
                value = result[1]

            return {
                "key": result[0],
                "value": value,
                "created_at": result[2],
                "updated_at": result[3],
            }
        return None

    def get_metadata_versions(self, key: str) -> List[Dict[str, Any]]:
        """Get version history for a key"""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT value, created_at FROM metadata_versions WHERE key = ? ORDER BY created_at DESC",
            (key,),
        )

        versions = []
        for row in cursor.fetchall():
            try:
                value = json.loads(row[0])
            except json.JSONDecodeError:
                value = row[0]

            versions.append({"value": value, "created_at": row[1]})

        return versions

    def search_metadata(self, search_term: str = None) -> Dict[str, Any]:
        """Search metadata using SQL LIKE operator"""
        cursor = self.conn.cursor()

        if search_term:
            cursor.execute(
                "SELECT key, value FROM metadata WHERE value LIKE ?",
                (f"%{search_term}%",),
            )
        else:
            cursor.execute("SELECT key, value FROM metadata")

        results = {}
        for row in cursor.fetchall():
            try:
                value = json.loads(row[1])
            except json.JSONDecodeError:
                value = row[1]
            results[row[0]] = value

        return results

    def delete_metadata(self, key: str):
        """Remove metadata for the given key"""
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM metadata WHERE key = ?", (key,))
        cursor.execute("DELETE FROM metadata_versions WHERE key = ?", (key,))
        self.conn.commit()

    def list_keys(self) -> List[str]:
        """List all keys in the metastore"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT key FROM metadata")
        return [row[0] for row in cursor.fetchall()]

    def clear(self):
        """Clear all metadata"""
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM metadata")
        cursor.execute("DELETE FROM metadata_versions")
        self.conn.commit()

    def __del__(self):
        """Close the database connection when the object is destroyed"""
        self.conn.close()
