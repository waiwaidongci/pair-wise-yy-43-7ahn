from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from .audit import make_entry, utc_now
from .domain import ConflictError, NotFoundError, ValidationError
from .rules import (ENTITY, ID_PREFIX, STAGE_ORDER, STATES, can_transition,
                    completion_blockers, role_for_transition)


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
            """)
        self._create_sync_schema()

    def _create_sync_schema(self) -> None:
        with self.conn:
            self.conn.executescript("""
                CREATE TABLE IF NOT EXISTS sync_batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_key TEXT NOT NULL UNIQUE,
                    actor TEXT NOT NULL,
                    role TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open',
                    summary TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sync_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_key TEXT NOT NULL,
                    client_ref TEXT NOT NULL UNIQUE,
                    item_id INTEGER,
                    kind TEXT,
                    detail TEXT,
                    status TEXT NOT NULL DEFAULT 'open',
                    evidence TEXT,
                    target_stage TEXT,
                    outcome TEXT NOT NULL,
                    reason TEXT,
                    record_id INTEGER,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_sync_records_batch
                    ON sync_records(batch_key);
                CREATE INDEX IF NOT EXISTS idx_sync_records_outcome
                    ON sync_records(outcome);
            """)

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
                   external_ref: Optional[str], actor: str) -> Dict[str, Any]:
        now = utc_now()
        self.get_item(item_id)
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO records(item_id, kind, detail, status, external_ref,
                       created_by, created_at) VALUES(?,?,?,?,?,?,?)""",
                    (item_id, kind, detail, status, external_ref, actor, now),
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

    # ------------------------------------------------------------------
    # 离线同步（断网批量回传、幂等去重、阶段冲突挂起、失败续传）
    # ------------------------------------------------------------------
    def apply_sync(self, batch_key: str, actor: str, role: str,
                   records: List[Dict[str, Any]], allowed_roles: set,
                   evidence_required: bool, suspend_stages: set) -> Dict[str, Any]:
        now = utc_now()
        summary = {"accepted": 0, "rejected": 0, "pending": 0, "duplicate": 0}
        outcomes: List[Dict[str, Any]] = []
        with self._lock, self.conn:
            self.conn.execute(
                """INSERT INTO sync_batches(batch_key, actor, role, status, summary,
                   created_at, updated_at) VALUES(?,?,?,?,?,?,?)
                   ON CONFLICT(batch_key) DO UPDATE SET updated_at=excluded.updated_at""",
                (batch_key, actor, role, "open", "{}", now, now),
            )
            for rec in records:
                outcome = self._process_sync_record(
                    rec, batch_key, actor, role, allowed_roles,
                    evidence_required, suspend_stages, now)
                outcomes.append(outcome)
                name = outcome["outcome"]
                summary[name] = summary.get(name, 0) + 1
            self.conn.execute(
                "UPDATE sync_batches SET summary=?, updated_at=? WHERE batch_key=?",
                (json.dumps(summary, ensure_ascii=False, sort_keys=True), now, batch_key),
            )
        return {"batch_key": batch_key, "outcomes": outcomes, "summary": summary}

    def _process_sync_record(self, rec: Dict[str, Any], batch_key: str, actor: str,
                              role: str, allowed_roles: set, evidence_required: bool,
                              suspend_stages: set, now: str) -> Dict[str, Any]:
        client_ref = rec.get("client_ref")
        if not isinstance(client_ref, str) or not client_ref.strip():
            return {"client_ref": None, "outcome": "rejected", "reason": "缺少client_ref，无法去重"}
        client_ref = client_ref.strip()

        existing = self.conn.execute(
            "SELECT * FROM sync_records WHERE client_ref=?", (client_ref,)
        ).fetchone()
        if existing is not None:
            if existing["outcome"] == "accepted":
                return {"client_ref": client_ref, "outcome": "duplicate",
                        "record_id": existing["record_id"], "reason": "已同步，忽略重复上报"}
            if existing["outcome"] == "pending":
                return {"client_ref": client_ref, "outcome": "pending",
                        "reason": existing["reason"] or "待人工确认"}
            # rejected → 允许按新内容重试续传

        if role not in allowed_roles:
            return self._reject_sync(client_ref, batch_key, rec, "越权：当前角色无权登记处置记录", now)

        kind = rec.get("kind")
        detail = rec.get("detail")
        if not isinstance(kind, str) or not kind.strip():
            return self._reject_sync(client_ref, batch_key, rec, "kind缺失", now)
        if not isinstance(detail, str) or not detail.strip():
            return self._reject_sync(client_ref, batch_key, rec, "detail缺失", now)
        kind = kind.strip()
        detail = detail.strip()

        evidence = rec.get("evidence")
        if evidence_required and (not isinstance(evidence, str) or not evidence.strip()):
            return self._reject_sync(client_ref, batch_key, rec, "证据缺失", now)
        evidence = evidence.strip() if isinstance(evidence, str) else None

        status = rec.get("status", "open")
        if status not in ("open", "closed"):
            status = "open"

        item_id = rec.get("item_id")
        if not isinstance(item_id, int) or isinstance(item_id, bool):
            return self._reject_sync(client_ref, batch_key, rec, "item_id缺失", now)
        item = self.conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
        if item is None:
            return self._reject_sync(client_ref, batch_key, rec, "项目不存在", now)

        target_stage = rec.get("target_stage")
        if target_stage is not None and target_stage not in STAGE_ORDER:
            return self._reject_sync(client_ref, batch_key, rec, f"未知阶段{target_stage}", now)

        if (target_stage is not None
                and STAGE_ORDER[target_stage] > STAGE_ORDER[item["status"]]
                and target_stage in suspend_stages):
            self._upsert_sync(batch_key, client_ref, item_id, kind, detail, status,
                              evidence, target_stage, "pending",
                              "现场阶段超前（监控/关闭），等待人工确认", None, now)
            return {"client_ref": client_ref, "outcome": "pending",
                    "reason": "现场阶段超前（监控/关闭），等待人工确认",
                    "target_stage": target_stage}

        record_id, duplicate = self._insert_accepted(
            item_id, kind, detail, status, client_ref, actor, role,
            target_stage, batch_key, "sync", now)
        if duplicate:
            self._upsert_sync(batch_key, client_ref, item_id, kind, detail, status,
                              evidence, target_stage, "duplicate",
                              "历史已存在相同外部标识，按重复处理", record_id, now)
            return {"client_ref": client_ref, "outcome": "duplicate",
                    "record_id": record_id, "reason": "历史已存在相同外部标识，按重复处理"}
        self._upsert_sync(batch_key, client_ref, item_id, kind, detail, status,
                          evidence, target_stage, "accepted", None, record_id, now)
        return {"client_ref": client_ref, "outcome": "accepted",
                "record_id": record_id, "target_stage": target_stage}

    def _insert_accepted(self, item_id: int, kind: str, detail: str, status: str,
                         client_ref: str, actor: str, role: str,
                         target_stage: Optional[str], batch_key: str,
                         via: str, now: str) -> tuple:
        try:
            cur = self.conn.execute(
                """INSERT INTO records(item_id, kind, detail, status, external_ref,
                   created_by, created_at) VALUES(?,?,?,?,?,?,?)""",
                (item_id, kind, detail, status, client_ref, actor, now),
            )
        except sqlite3.IntegrityError:
            existing = self.conn.execute(
                "SELECT id FROM records WHERE external_ref=? ORDER BY id LIMIT 1",
                (client_ref,)).fetchone()
            record_id = int(existing["id"]) if existing else None
            return record_id, True
        record_id = int(cur.lastrowid)
        self._append_audit_tx("record", ENTITY, item_id, actor, {
            "record_id": record_id, "kind": kind, "status": status,
            "via": via, "batch_key": batch_key, "client_ref": client_ref,
        })
        if target_stage is not None:
            item = self.conn.execute(
                "SELECT status FROM items WHERE id=?", (item_id,)).fetchone()
            if STAGE_ORDER[target_stage] > STAGE_ORDER[item["status"]]:
                self._advance_toward(item_id, item["status"], target_stage, actor, role, now)
        return record_id, False

    def _advance_toward(self, item_id: int, current: str, target: str,
                        actor: str, role: str, now: str) -> None:
        status = current
        while STAGE_ORDER[status] < STAGE_ORDER[target]:
            nxt = next((s for s in STATES
                         if can_transition(status, s) and STAGE_ORDER[s] == STAGE_ORDER[status] + 1),
                       None)
            if nxt is None or role not in role_for_transition(nxt):
                break
            open_count = self.conn.execute(
                "SELECT COUNT(*) AS n FROM records WHERE item_id=? AND status='open'",
                (item_id,)).fetchone()["n"]
            if completion_blockers(nxt, open_count):
                break
            row = self.conn.execute(
                "SELECT version FROM items WHERE id=?", (item_id,)).fetchone()
            res = self.conn.execute(
                """UPDATE items SET status=?, version=version+1, updated_at=?
                   WHERE id=? AND version=?""",
                (nxt, now, item_id, row["version"]),
            )
            if res.rowcount == 0:
                break
            self._append_audit_tx("transition", ENTITY, item_id, actor, {
                "from": status, "to": nxt, "via": "sync", "batch_key": None,
            })
            status = nxt

    def _append_audit_tx(self, action: str, entity_type: str, entity_id: int,
                         actor: str, detail: dict) -> None:
        row = self.conn.execute(
            "SELECT entry_hash FROM audit_events ORDER BY id DESC LIMIT 1"
        ).fetchone()
        previous = row["entry_hash"] if row else "GENESIS"
        event = make_entry(action, entity_type, entity_id, actor, detail, previous)
        self.conn.execute(
            """INSERT INTO audit_events(action, entity_type, entity_id, actor, detail,
               previous_hash, entry_hash, created_at) VALUES(?,?,?,?,?,?,?,?)""",
            (event["action"], event["entity_type"], event["entity_id"], event["actor"],
             json.dumps(event["detail"], ensure_ascii=False, sort_keys=True),
             event["previous_hash"], event["entry_hash"], event["created_at"]),
        )

    def _reject_sync(self, client_ref: str, batch_key: str, rec: Dict[str, Any],
                     reason: str, now: str) -> Dict[str, Any]:
        self._upsert_sync(batch_key, client_ref, rec.get("item_id"), rec.get("kind"),
                          rec.get("detail"), rec.get("status", "open"),
                          rec.get("evidence"), rec.get("target_stage"),
                          "rejected", reason, None, now)
        return {"client_ref": client_ref, "outcome": "rejected", "reason": reason}

    def _upsert_sync(self, batch_key: str, client_ref: str, item_id, kind, detail,
                     status, evidence, target_stage, outcome, reason,
                     record_id, now: str) -> None:
        existing = self.conn.execute(
            "SELECT id FROM sync_records WHERE client_ref=?", (client_ref,)
        ).fetchone()
        if existing is not None:
            self.conn.execute(
                """UPDATE sync_records SET batch_key=?, item_id=?, kind=?, detail=?,
                   status=?, evidence=?, target_stage=?, outcome=?, reason=?,
                   record_id=?, updated_at=? WHERE client_ref=?""",
                (batch_key, item_id, kind, detail, status, evidence, target_stage,
                 outcome, reason, record_id, now, client_ref),
            )
        else:
            self.conn.execute(
                """INSERT INTO sync_records(batch_key, client_ref, item_id, kind, detail,
                   status, evidence, target_stage, outcome, reason, record_id,
                   created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (batch_key, client_ref, item_id, kind, detail, status, evidence,
                 target_stage, outcome, reason, record_id, now, now),
            )

    def decide_sync_record(self, client_ref: str, decision: str, actor: str,
                           role: str, reason: Optional[str] = None) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            row = self.conn.execute(
                "SELECT * FROM sync_records WHERE client_ref=?", (client_ref,)
            ).fetchone()
            if row is None:
                raise NotFoundError("同步记录不存在")
            if row["outcome"] == "accepted":
                return {"client_ref": client_ref, "outcome": "duplicate",
                        "record_id": row["record_id"], "reason": "已确认入账，忽略重复裁决"}
            if row["outcome"] not in ("pending", "rejected"):
                raise ConflictError("仅待确认或已退回的记录可以人工裁决")
            if decision == "reject":
                self.conn.execute(
                    "UPDATE sync_records SET outcome='rejected', reason=?, updated_at=? WHERE client_ref=?",
                    (reason or "人工退回", now, client_ref),
                )
                return {"client_ref": client_ref, "outcome": "rejected",
                        "reason": reason or "人工退回"}
            if decision != "confirm":
                raise ValidationError("decision必须是confirm或reject")
            record_id, duplicate = self._insert_accepted(
                row["item_id"], row["kind"], row["detail"], row["status"],
                client_ref, actor, role, row["target_stage"],
                row["batch_key"], "sync_confirm", now)
            if duplicate:
                self.conn.execute(
                    """UPDATE sync_records SET outcome='duplicate', reason=?, record_id=?,
                       updated_at=? WHERE client_ref=?""",
                    ("历史已存在相同外部标识，按重复处理", record_id, now, client_ref),
                )
                return {"client_ref": client_ref, "outcome": "duplicate",
                        "record_id": record_id}
            self.conn.execute(
                """UPDATE sync_records SET outcome='accepted', reason=NULL, record_id=?,
                   updated_at=? WHERE client_ref=?""",
                (record_id, now, client_ref),
            )
            return {"client_ref": client_ref, "outcome": "accepted",
                    "record_id": record_id}

    def get_sync_batch(self, batch_key: str) -> Dict[str, Any]:
        with self._lock:
            batch = self.conn.execute(
                "SELECT * FROM sync_batches WHERE batch_key=?", (batch_key,)
            ).fetchone()
            if batch is None:
                raise NotFoundError("同步批次不存在")
            rows = self.conn.execute(
                "SELECT * FROM sync_records WHERE batch_key=? ORDER BY id",
                (batch_key,),
            ).fetchall()
        result = dict(batch)
        result["summary"] = json.loads(result["summary"])
        result["records"] = [dict(r) for r in rows]
        return result

    def list_pending_sync_records(self) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                """SELECT * FROM sync_records WHERE outcome='pending'
                   ORDER BY id"""
            ).fetchall()
        return [dict(r) for r in rows]

    def list_pending_for_batch(self, batch_key: str) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                """SELECT * FROM sync_records WHERE batch_key=? AND outcome='pending'
                   ORDER BY id""", (batch_key,)
            ).fetchall()
        return [dict(r) for r in rows]

    def close(self) -> None:
        with self._lock:
            self.conn.close()
