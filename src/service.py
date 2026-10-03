from __future__ import annotations

from typing import Any, Dict, Optional

from .domain import (ConflictError, DomainError, PermissionDenied, ValidationError,
                     ensure_role, normalize_severity, require_number, require_text)
from .repository import Repository
from .rules import (AUDIT_ROLES, CREATE_ROLES, ENTITY, RECORD_ROLES, STATES,
                    SYNC_BATCH_LIMIT, SYNC_ROLES, TITLE, VIEW_ROLES,
                    completion_blockers, escalation_required, priority_score,
                    response_deadline_hours, role_for_transition, validate_transition)


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
            raise ConflictError("；".join(blockers))
        updated = self.repository.transition_item(item_id, target, expected_version, actor)
        self.repository.append_audit("transition", ENTITY, item_id, actor, {
            "from": item["status"], "to": target,
            "escalation_required": escalation_required(
                item["severity"], item["quantity"], item["threshold"]),
        })
        return self.enrich(updated)

    def sync_batch(self, payload: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
        """现场离线台账批量同步：逐条独立处理，失败的可重传续传。"""
        ensure_role(role, SYNC_ROLES)
        actor = require_text(actor, "actor", 100)
        batch_id = require_text(payload.get("batch_id"), "batch_id", 100)
        operations = payload.get("operations")
        if not isinstance(operations, list) or not operations:
            raise ValidationError("operations不能为空")
        if len(operations) > SYNC_BATCH_LIMIT:
            raise ValidationError(f"operations不能超过{SYNC_BATCH_LIMIT}条")
        results = []
        for op in operations:
            if not isinstance(op, dict):
                results.append({"op_id": None, "status": "rejected", "reason": "操作格式错误"})
                continue
            results.append(self._process_sync_op(op, batch_id, actor))
        summary: Dict[str, int] = {}
        for result in results:
            summary[result["status"]] = summary.get(result["status"], 0) + 1
        return {"batch_id": batch_id, "results": results, "summary": summary}

    def _process_sync_op(self, op: Dict[str, Any], batch_id: str,
                         sync_actor: str) -> Dict[str, Any]:
        raw_op_id = op.get("op_id")
        try:
            op_id = require_text(raw_op_id, "op_id", 100)
        except DomainError as exc:
            return {"op_id": raw_op_id, "status": "rejected", "reason": exc.message}
        existing = self.repository.get_sync_op(op_id)
        if existing is not None:
            return self._sync_result(op_id, existing, replayed=True)
        try:
            return self._apply_sync_op(op, batch_id, op_id, sync_actor)
        except DomainError as exc:
            return {"op_id": op_id, "status": "rejected", "reason": exc.message}

    def _apply_sync_op(self, op: Dict[str, Any], batch_id: str, op_id: str,
                       sync_actor: str) -> Dict[str, Any]:
        op_actor = require_text(op.get("actor"), "actor", 100)
        op_role = op.get("role", "")
        item_id = op.get("item_id")
        item_ref = op.get("item_external_ref")
        if item_id is not None:
            if isinstance(item_id, bool) or not isinstance(item_id, int):
                raise ValidationError("item_id必须是整数")
            item = self.repository.get_item(item_id)
        elif isinstance(item_ref, str) and item_ref.strip():
            item = self.repository.get_item_by_ref(item_ref.strip())
        else:
            raise ValidationError("缺少item_id或item_external_ref")
        record_fields = None
        record = op.get("record")
        if record is not None:
            if not isinstance(record, dict):
                raise ValidationError("record必须是对象")
            kind = require_text(record.get("kind"), "kind", 100)
            detail = record.get("detail")
            external_ref = record.get("external_ref")
            if (not isinstance(detail, str) or not detail.strip()
                    or not isinstance(external_ref, str) or not external_ref.strip()):
                raise ValidationError("证据缺失：detail和external_ref不能为空")
            detail = require_text(detail, "detail")
            external_ref = require_text(external_ref, "external_ref", 100)
            status = record.get("status", "open")
            if status not in ("open", "closed"):
                raise ValidationError("status必须是open或closed")
            if op_role not in RECORD_ROLES:
                raise PermissionDenied("越权：角色无权登记处置记录")
            record_fields = {"kind": kind, "detail": detail, "status": status,
                             "external_ref": external_ref}
        target = op.get("target_status")
        if target is not None:
            target = require_text(target, "target_status", 50)
            if target not in STATES:
                raise ValidationError("未知状态")
            if (STATES.index(target) > STATES.index(item["status"])
                    and op_role not in role_for_transition(target)):
                raise PermissionDenied("越权：角色无权推进到该阶段")
        field_status = op.get("field_status")
        if field_status is not None and field_status not in STATES:
            raise ValidationError("未知状态")
        if record_fields is None and target is None:
            raise ValidationError("操作内容为空")
        outcome = self.repository.apply_sync_op(item["id"], record_fields, target,
                                                op_id, batch_id, op_actor)
        if outcome.get("replayed"):
            return self._sync_result(op_id, outcome)
        audit_base = {"op_id": op_id, "batch_id": batch_id, "synced_by": sync_actor,
                      "field_status": field_status}
        if outcome["record_created"]:
            self.repository.append_audit("record", ENTITY, item["id"], op_actor, dict(
                audit_base, record_id=outcome["record_id"], kind=record_fields["kind"],
                status=record_fields["status"], source="field"))
        if target is not None and outcome["decision"] == "applied":
            self.repository.append_audit("transition", ENTITY, item["id"], op_actor, dict(
                audit_base, **{"from": outcome["from_status"], "to": target,
                               "source": "field"}))
        elif target is not None and outcome["decision"] == "pending":
            self.repository.append_audit("pending", ENTITY, item["id"], op_actor, dict(
                audit_base, target=target, pending_id=outcome["pending_id"]))
        return self._sync_result(op_id, outcome)

    @staticmethod
    def _sync_result(op_id: str, outcome: Dict[str, Any],
                     replayed: bool = False) -> Dict[str, Any]:
        return {"op_id": op_id, "item_id": outcome["item_id"],
                "status": outcome["decision"], "record_id": outcome["record_id"],
                "record_created": bool(outcome["record_created"]),
                "pending_id": outcome["pending_id"], "message": outcome["message"],
                "replayed": replayed or bool(outcome.get("replayed"))}

    def list_pending(self, role: str, item_id: Optional[int] = None) -> list:
        ensure_role(role, VIEW_ROLES)
        return self.repository.list_pending(item_id)

    def confirm_pending(self, pending_id: int, actor: str, role: str) -> Dict[str, Any]:
        actor = require_text(actor, "actor", 100)
        pend = self.repository.get_pending(pending_id)
        if pend["status"] != "pending":
            raise ConflictError("该挂起已处理")
        item = self.repository.get_item(pend["item_id"])
        target = pend["target"]
        ensure_role(role, role_for_transition(target))
        if STATES.index(item["status"]) >= STATES.index(target):
            self.repository.resolve_pending(pending_id, "confirmed", actor, "中心已处于更晚阶段")
            self.repository.append_audit("pending_confirm", ENTITY, item["id"], actor, {
                "pending_id": pending_id, "target": target, "note": "中心已处于更晚阶段"})
        else:
            validate_transition(item["status"], target)
            blockers = completion_blockers(target, self.repository.open_record_count(item["id"]))
            if blockers:
                raise ConflictError("；".join(blockers))
            self.repository.transition_item(item["id"], target, item["version"], actor)
            self.repository.resolve_pending(pending_id, "confirmed", actor, None)
            self.repository.append_audit("transition", ENTITY, item["id"], actor, {
                "from": item["status"], "to": target, "pending_id": pending_id,
                "source": "field_confirm"})
        return {"pending": self.repository.get_pending(pending_id),
                "item": self.enrich(self.repository.get_item(item["id"]))}

    def reject_pending(self, pending_id: int, actor: str, role: str,
                       reason: Optional[str] = None) -> Dict[str, Any]:
        actor = require_text(actor, "actor", 100)
        ensure_role(role, RECORD_ROLES)
        pend = self.repository.get_pending(pending_id)
        if pend["status"] != "pending":
            raise ConflictError("该挂起已处理")
        if reason is not None:
            reason = require_text(reason, "reason", 500)
        self.repository.resolve_pending(pending_id, "rejected", actor, reason)
        self.repository.append_audit("pending_reject", ENTITY, pend["item_id"], actor, {
            "pending_id": pending_id, "target": pend["target"], "reason": reason})
        return {"pending": self.repository.get_pending(pending_id)}

    def get_item(self, item_id: int, role: str) -> Dict[str, Any]:
        self._view(role)
        result = self.enrich(self.repository.get_item(item_id))
        result["pending_transitions"] = self.repository.list_pending(item_id)
        return result

    def list_items(self, role: str, status: Optional[str] = None) -> list:
        self._view(role)
        return [self.enrich(item) for item in self.repository.list_items(status)]

    def list_records(self, item_id: int, role: str) -> list:
        self._view(role)
        return self.repository.list_records(item_id)

    def audit(self, role: str, item_id: Optional[int] = None) -> list:
        ensure_role(role, AUDIT_ROLES)
        return self.repository.list_audit(item_id)

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
