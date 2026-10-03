from __future__ import annotations

from typing import Any, Dict, Optional

from .domain import (ValidationError, ensure_role, normalize_severity,
                     require_number, require_text)
from .repository import Repository
from .rules import (AUDIT_ROLES, CREATE_ROLES, EVIDENCE_REQUIRED, ENTITY,
                    RECORD_ROLES, STAGE_ORDER, SUSPEND_STAGES, TITLE,
                    VIEW_ROLES, completion_blockers, escalation_required,
                    priority_score, response_deadline_hours, role_for_transition,
                    validate_transition)


class Service:
    def __init__(self, repository: Repository):
        self.repository = repository

    def _view(self, role: str) -> None:
        ensure_role(role, VIEW_ROLES)

    def create_item(self, payload: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, CREATE_ROLES)
        actor = require_text(actor, "actor", 100)
        title = require_text(payload.get("title"), "title", 200)
        description = require_text(payload.get("description"), "description")
        severity = normalize_severity(payload.get("severity"))
        quantity = require_number(payload.get("quantity", 0), "quantity")
        threshold = require_number(payload.get("threshold", 1), "threshold", 0.000001)
        external_ref = payload.get("external_ref")
        if external_ref is not None:
            external_ref = require_text(external_ref, "external_ref", 100)
        item = self.repository.create_item(title, description, severity, quantity,
                                           threshold, external_ref, actor)
        self.repository.append_audit("create", ENTITY, item["id"], actor, {
            "title": title, "severity": severity, "quantity": quantity,
            "priority": priority_score(severity, quantity, threshold),
        })
        return self.enrich(item)

    def add_record(self, item_id: int, payload: Dict[str, Any], actor: str,
                   role: str) -> Dict[str, Any]:
        ensure_role(role, RECORD_ROLES)
        actor = require_text(actor, "actor", 100)
        kind = require_text(payload.get("kind"), "kind", 100)
        detail = require_text(payload.get("detail"), "detail")
        status = payload.get("status", "open")
        if status not in ("open", "closed"):
            raise ValueError("status必须是open或closed")
        external_ref = payload.get("external_ref")
        if external_ref is not None:
            external_ref = require_text(external_ref, "external_ref", 100)
        record = self.repository.add_record(item_id, kind, detail, status,
                                            external_ref, actor)
        self.repository.append_audit("record", ENTITY, item_id, actor, {
            "record_id": record["id"], "kind": kind, "status": status,
        })
        return record

    def transition(self, item_id: int, target: str, expected_version: int,
                   actor: str, role: str) -> Dict[str, Any]:
        actor = require_text(actor, "actor", 100)
        item = self.repository.get_item(item_id)
        validate_transition(item["status"], target)
        ensure_role(role, role_for_transition(target))
        if not isinstance(expected_version, int) or expected_version < 1:
            raise ValueError("expected_version必须是正整数")
        blockers = completion_blockers(target, self.repository.open_record_count(item_id))
        if blockers:
            from .domain import ConflictError
            raise ConflictError("；".join(blockers))
        updated = self.repository.transition_item(item_id, target, expected_version, actor)
        self.repository.append_audit("transition", ENTITY, item_id, actor, {
            "from": item["status"], "to": target,
            "escalation_required": escalation_required(
                item["severity"], item["quantity"], item["threshold"]),
        })
        return self.enrich(updated)

    def get_item(self, item_id: int, role: str) -> Dict[str, Any]:
        self._view(role)
        return self.enrich(self.repository.get_item(item_id))

    def list_items(self, role: str, status: Optional[str] = None) -> list:
        self._view(role)
        return [self.enrich(item) for item in self.repository.list_items(status)]

    def list_records(self, item_id: int, role: str) -> list:
        self._view(role)
        return self.repository.list_records(item_id)

    def audit(self, role: str, item_id: Optional[int] = None) -> list:
        ensure_role(role, AUDIT_ROLES)
        return self.repository.list_audit(item_id)

    # ------------------------------------------------------------------
    # 离线同步
    # ------------------------------------------------------------------
    def sync_batch(self, payload: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
        actor = require_text(actor, "actor", 100)
        batch_key = require_text(payload.get("batch_key"), "batch_key", 100)
        records = payload.get("records")
        if not isinstance(records, list) or not records:
            raise ValidationError("records必须是非空数组")
        return self.repository.apply_sync(
            batch_key, actor, role, records,
            allowed_roles=RECORD_ROLES,
            evidence_required=EVIDENCE_REQUIRED,
            suspend_stages=SUSPEND_STAGES,
        )

    def get_sync_batch(self, batch_key: str, role: str) -> Dict[str, Any]:
        self._view(role)
        return self.repository.get_sync_batch(batch_key)

    def list_pending_sync(self, role: str) -> list:
        ensure_role(role, set(["response_commander", "operations"]))
        return self.repository.list_pending_sync_records()

    def decide_sync(self, batch_key: str, client_ref: Optional[str], decision: str,
                    actor: str, role: str, reason: Optional[str] = None) -> Dict[str, Any]:
        ensure_role(role, set(["response_commander"]))
        actor = require_text(actor, "actor", 100)
        if decision not in ("confirm", "reject"):
            raise ValidationError("decision必须是confirm或reject")
        if client_ref is not None:
            require_text(client_ref, "client_ref", 100)
            return self.repository.decide_sync_record(client_ref, decision, actor, role, reason)
        results = []
        for row in self.repository.list_pending_for_batch(batch_key):
            results.append(self.repository.decide_sync_record(
                row["client_ref"], decision, actor, role, reason))
        return {"batch_key": batch_key, "decision": decision, "results": results}

    @staticmethod
    def enrich(item: Dict[str, Any]) -> Dict[str, Any]:
        result = dict(item)
        result["priority"] = priority_score(
            item["severity"], item["quantity"], item["threshold"])
        result["deadline_hours"] = response_deadline_hours(
            item["severity"], item["quantity"], item["threshold"])
        result["escalation_required"] = escalation_required(
            item["severity"], item["quantity"], item["threshold"])
        return result
