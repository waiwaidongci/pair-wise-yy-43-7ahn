from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from .audit import make_entry, utc_now
from .domain import ConflictError, NotFoundError
from .rules import ID_PREFIX, STATES, sync_decision


class Repository:
    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self._create_schema()

    def _create_schema(self) -> None:
        statuses = ",".join("'" + s.replace("'", "''") + "'" for s in STATES)
        with self.conn:
            self.conn.executescript(f"""
                CREATE TABLE IF NOT EXISTS items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    quantity REAL NOT NULL DEFAULT 0,
                    threshold REAL NOT NULL DEFAULT 1,
                    status TEXT NOT NULL CHECK(status IN ({statuses})),
                    version INTEGER NOT NULL DEFAULT 1,
                    external_ref TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS ux_items_external_ref
                    ON items(external_ref) WHERE external_ref IS NOT NULL;
                CREATE TABLE IF NOT EXISTS records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                    kind TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open'
                        CHECK(status IN ('open','closed')),
                    external_ref TEXT,
                    source TEXT NOT NULL DEFAULT 'center',
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(item_id, external_ref)
                );
                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    action TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    entity_id INTEGER NOT NULL,
                    actor TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    previous_hash TEXT NOT NULL,
                    entry_hash TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sync_ops (
                    op_id TEXT PRIMARY KEY,
                    batch_id TEXT NOT NULL,
                    item_id INTEGER NOT NULL,
                    decision TEXT NOT NULL,
                    record_id INTEGER,
                    record_created INTEGER NOT NULL DEFAULT 0,
                    pending_id INTEGER,
                    message TEXT,
                    actor TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS pending_transitions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                    target TEXT NOT NULL,
                    op_id TEXT NOT NULL,
                    batch_id TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending'
                        CHECK(status IN ('pending','confirmed','rejected')),
                    reason TEXT,
                    created_at TEXT NOT NULL,
                    resolved_by TEXT,
                    resolved_at TEXT
                );
            """)
            record_columns = {row[1] for row in self.conn.execute("PRAGMA table_info(records)")}
            if "source" not in record_columns:
                self.conn.execute(
                    "ALTER TABLE records ADD COLUMN source TEXT NOT NULL DEFAULT 'center'")

    @staticmethod
    def _item(row: sqlite3.Row) -> Dict[str, Any]:
        return dict(row)

    def create_item(self, title: str, description: str, severity: str,
                    quantity: float, threshold: float, external_ref: Optional[str],
                    actor: str) -> Dict[str, Any]:
        now = utc_now()
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO items(title, description, severity, quantity, threshold,
                       status, version, external_ref, created_by, created_at, updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                    (title, description, severity, quantity, threshold, STATES[0], 1,
                     external_ref, actor, now, now),
                )
                item_id = int(cur.lastrowid)
        except sqlite3.IntegrityError as exc:
            raise ConflictError("external_ref已存在") from exc
        return self.get_item(item_id)

    def get_item(self, item_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
        if row is None:
            raise NotFoundError("项目不存在")
        return self._item(row)

    def list_items(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM items"
        params: tuple = ()
        if status:
            sql += " WHERE status=?"
            params = (status,)
        sql += " ORDER BY id DESC"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        return [self._item(row) for row in rows]

    def transition_item(self, item_id: int, target: str, expected_version: int,
                        actor: str) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                """UPDATE items SET status=?, version=version+1, updated_at=?
                   WHERE id=? AND version=?""",
                (target, now, item_id, expected_version),
            )
            if cur.rowcount == 0:
                exists = self.conn.execute("SELECT 1 FROM items WHERE id=?", (item_id,)).fetchone()
                if exists is None:
                    raise NotFoundError("项目不存在")
                raise ConflictError("版本冲突，请刷新后重试")
        return self.get_item(item_id)

    def add_record(self, item_id: int, kind: str, detail: str, status: str,
                   external_ref: Optional[str], actor: str,
                   source: str = "center") -> Dict[str, Any]:
        now = utc_now()
        self.get_item(item_id)
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO records(item_id, kind, detail, status, external_ref,
                       source, created_by, created_at) VALUES(?,?,?,?,?,?,?,?)""",
                    (item_id, kind, detail, status, external_ref, source, actor, now),
                )
                record_id = int(cur.lastrowid)
        except sqlite3.IntegrityError as exc:
            raise ConflictError("记录唯一标识已存在") from exc
        with self._lock:
            row = self.conn.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
        return dict(row)

    def list_records(self, item_id: int) -> List[Dict[str, Any]]:
        self.get_item(item_id)
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM records WHERE item_id=? ORDER BY id", (item_id,)
            ).fetchall()
        return [dict(row) for row in rows]

    def open_record_count(self, item_id: int) -> int:
        with self._lock:
            row = self.conn.execute(
                "SELECT COUNT(*) AS n FROM records WHERE item_id=? AND status='open'",
                (item_id,),
            ).fetchone()
        return int(row["n"])

    def get_item_by_ref(self, external_ref: str) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM items WHERE external_ref=?", (external_ref,)).fetchone()
        if row is None:
            raise NotFoundError("项目不存在")
        return self._item(row)

    def get_sync_op(self, op_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM sync_ops WHERE op_id=?", (op_id,)).fetchone()
        return dict(row) if row is not None else None

    def apply_sync_op(self, item_id: int, record_fields: Optional[Dict[str, Any]],
                      target: Optional[str], op_id: str, batch_id: str,
                      actor: str) -> Dict[str, Any]:
        """单条现场操作原子入库：记录补充、阶段裁决和同步台账同事务写入。"""
        now = utc_now()
        try:
            with self._lock, self.conn:
                row = self.conn.execute(
                    "SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
                if row is None:
                    raise NotFoundError("项目不存在")
                current = row["status"]
                decision = "applied" if target is None else sync_decision(current, target)
                record_id: Optional[int] = None
                record_created = False
                if record_fields is not None:
                    try:
                        cur = self.conn.execute(
                            """INSERT INTO records(item_id, kind, detail, status, external_ref,
                               source, created_by, created_at) VALUES(?,?,?,?,?,'field',?,?)""",
                            (item_id, record_fields["kind"], record_fields["detail"],
                             record_fields["status"], record_fields["external_ref"],
                             actor, now),
                        )
                        record_id = int(cur.lastrowid)
                        record_created = True
                    except sqlite3.IntegrityError:
                        dup = self.conn.execute(
                            """SELECT id FROM records WHERE item_id=? AND external_ref=?""",
                            (item_id, record_fields["external_ref"])).fetchone()
                        if dup is None:
                            raise ConflictError("记录唯一标识冲突")
                        record_id = int(dup["id"])
                pending_id: Optional[int] = None
                message: Optional[str] = None
                if target is not None:
                    if decision == "applied":
                        cur = self.conn.execute(
                            """UPDATE items SET status=?, version=version+1, updated_at=?
                               WHERE id=? AND version=?""",
                            (target, now, item_id, row["version"]),
                        )
                        if cur.rowcount == 0:
                            raise ConflictError("版本冲突，请刷新后重试")
                    elif decision == "pending":
                        cur = self.conn.execute(
                            """INSERT INTO pending_transitions(item_id, target, op_id, batch_id,
                               actor, status, created_at) VALUES(?,?,?,?,?,'pending',?)""",
                            (item_id, target, op_id, batch_id, actor, now),
                        )
                        pending_id = int(cur.lastrowid)
                        message = "目标阶段需人工确认，已挂起"
                    else:
                        if STATES.index(target) <= STATES.index(current):
                            message = "现场阶段早于中心，仅补充证据"
                        else:
                            message = "阶段需中心按流程推进，仅补充证据"
                if record_fields is not None and not record_created:
                    note = "记录已存在，未重复入库"
                    message = f"{message}；{note}" if message else note
                self.conn.execute(
                    """INSERT INTO sync_ops(op_id, batch_id, item_id, decision, record_id,
                       record_created, pending_id, message, actor, created_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?)""",
                    (op_id, batch_id, item_id, decision, record_id, int(record_created),
                     pending_id, message, actor, now),
                )
                return {"item_id": item_id, "decision": decision, "record_id": record_id,
                        "record_created": record_created, "pending_id": pending_id,
                        "from_status": current, "message": message, "replayed": False}
        except sqlite3.IntegrityError:
            # 并发重复同步：台账已存在则按原结果返回
            stored = self.get_sync_op(op_id)
            if stored is None:
                raise ConflictError("同步冲突，请重试")
            stored["replayed"] = True
            return stored

    def add_pending(self, item_id: int, target: str, op_id: str, batch_id: str,
                    actor: str) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                """INSERT INTO pending_transitions(item_id, target, op_id, batch_id, actor,
                   status, created_at) VALUES(?,?,?,?,?,'pending',?)""",
                (item_id, target, op_id, batch_id, actor, now),
            )
            pending_id = int(cur.lastrowid)
        return self.get_pending(pending_id)

    def get_pending(self, pending_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM pending_transitions WHERE id=?", (pending_id,)).fetchone()
        if row is None:
            raise NotFoundError("挂起记录不存在")
        return dict(row)

    def list_pending(self, item_id: Optional[int] = None,
                     status: Optional[str] = "pending") -> List[Dict[str, Any]]:
        sql = "SELECT * FROM pending_transitions"
        clauses: List[str] = []
        params: List[Any] = []
        if item_id is not None:
            clauses.append("item_id=?")
            params.append(item_id)
        if status is not None:
            clauses.append("status=?")
            params.append(status)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY id"
        with self._lock:
            rows = self.conn.execute(sql, tuple(params)).fetchall()
        return [dict(row) for row in rows]

    def resolve_pending(self, pending_id: int, status: str, actor: str,
                        reason: Optional[str] = None) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                """UPDATE pending_transitions SET status=?, resolved_by=?, resolved_at=?,
                   reason=? WHERE id=? AND status='pending'""",
                (status, actor, now, reason, pending_id),
            )
            if cur.rowcount == 0:
                raise ConflictError("该挂起已处理")
        return self.get_pending(pending_id)

    def append_audit(self, action: str, entity_type: str, entity_id: int,
                     actor: str, detail: dict) -> Dict[str, Any]:
        with self._lock, self.conn:
            row = self.conn.execute(
                "SELECT entry_hash FROM audit_events ORDER BY id DESC LIMIT 1"
            ).fetchone()
            previous = row["entry_hash"] if row else "GENESIS"
            event = make_entry(action, entity_type, entity_id, actor, detail, previous)
            cur = self.conn.execute(
                """INSERT INTO audit_events(action, entity_type, entity_id, actor, detail,
                   previous_hash, entry_hash, created_at) VALUES(?,?,?,?,?,?,?,?)""",
                (event["action"], event["entity_type"], event["entity_id"], event["actor"],
                 json.dumps(event["detail"], ensure_ascii=False, sort_keys=True),
                 event["previous_hash"], event["entry_hash"], event["created_at"]),
            )
            event_id = int(cur.lastrowid)
        event["id"] = event_id
        return event

    def list_audit(self, entity_id: Optional[int] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM audit_events"
        params: tuple = ()
        if entity_id is not None:
            sql += " WHERE entity_id=?"
            params = (entity_id,)
        sql += " ORDER BY id"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["detail"] = json.loads(item["detail"])
            result.append(item)
        return result

    def verify_audit_chain(self) -> bool:
        from .audit import calculate_hash
        with self._lock:
            rows = self.conn.execute("SELECT * FROM audit_events ORDER BY id").fetchall()
        previous = "GENESIS"
        for row in rows:
            if row["previous_hash"] != previous:
                return False
            payload = {
                "action": row["action"], "entity_type": row["entity_type"],
                "entity_id": row["entity_id"], "actor": row["actor"],
                "detail": json.loads(row["detail"]), "created_at": row["created_at"],
            }
            if calculate_hash(previous, payload) != row["entry_hash"]:
                return False
            previous = row["entry_hash"]
        return True

    def close(self) -> None:
        with self._lock:
            self.conn.close()
