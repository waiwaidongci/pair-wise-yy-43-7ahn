import tempfile
import unittest
from pathlib import Path

from src.domain import ConflictError, PermissionDenied
from src.repository import Repository
from src.service import Service


def record_op(op_id, item_id, ref, role="operations", detail="现场处置记录",
              actor="field-a", **extra):
    op = {"op_id": op_id, "item_id": item_id, "actor": actor, "role": role,
          "record": {"kind": "recovery", "detail": detail, "status": "closed",
                     "external_ref": ref}}
    op.update(extra)
    return op


class SyncTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Repository(str(Path(self.tmp.name) / "test.db"))
        self.service = Service(self.repo)
        self.item = self.service.create_item(
            {"title": "offline item", "description": "offline sync scenarios",
             "severity": "major", "quantity": 12, "threshold": 6,
             "external_ref": "SYNC-ITEM-1"}, "creator", "observer")
        self.item_id = self.item["id"]

    def tearDown(self):
        self.repo.close()
        self.tmp.cleanup()

    def sync(self, batch_id, operations, role="operations"):
        return self.service.sync_batch(
            {"batch_id": batch_id, "operations": operations}, "officer", role)

    def test_batch_sync_and_duplicate_replay(self):
        ops = [
            record_op("OP-1", self.item_id, "LED-1"),
            record_op("OP-2", self.item_id, "LED-2", role="response_commander",
                      target_status="assessing", field_status="reported"),
        ]
        first = self.sync("B-1", ops)
        self.assertEqual([r["status"] for r in first["results"]], ["applied", "applied"])
        item = self.service.get_item(self.item_id, "viewer")
        self.assertEqual(item["status"], "assessing")
        self.assertEqual(len(self.service.list_records(self.item_id, "viewer")), 2)
        audit_count = len(self.service.audit("viewer", self.item_id))
        # 断网导致同一台账批次重传：结果回放，不多记录、不多审计
        second = self.sync("B-1", ops)
        self.assertEqual([r["status"] for r in second["results"]], ["applied", "applied"])
        self.assertTrue(all(r["replayed"] for r in second["results"]))
        self.assertEqual(len(self.service.list_records(self.item_id, "viewer")), 2)
        self.assertEqual(len(self.service.audit("viewer", self.item_id)), audit_count)
        replayed = self.service.get_item(self.item_id, "viewer")
        self.assertEqual(replayed["version"], item["version"])
        self.assertTrue(self.repo.verify_audit_chain())

    def test_stale_stage_only_supplements_evidence(self):
        current = self.service.get_item(self.item_id, "viewer")
        for target in ("assessing", "containing"):
            current = self.service.transition(
                self.item_id, target, current["version"], "center", "response_commander")
        ops = [record_op("OP-S1", self.item_id, "LED-S1",
                         target_status="assessing", field_status="reported")]
        result = self.sync("B-S", ops)
        self.assertEqual(result["results"][0]["status"], "evidence_only")
        item = self.service.get_item(self.item_id, "viewer")
        self.assertEqual(item["status"], "containing")
        self.assertEqual(len(self.service.list_records(self.item_id, "viewer")), 1)
        # 跨阶段推进同样只补证据，中心保持流程权威
        fresh = self.service.create_item(
            {"title": "gap item", "description": "stage gap scenario",
             "severity": "minor", "quantity": 1, "threshold": 5,
             "external_ref": "SYNC-ITEM-2"}, "creator", "observer")
        skipped = self.sync("B-S2", [{"op_id": "OP-S2", "item_id": fresh["id"],
                                      "actor": "field-a", "role": "response_commander",
                                      "target_status": "containing"}])
        self.assertEqual(skipped["results"][0]["status"], "evidence_only")
        self.assertEqual(self.service.get_item(fresh["id"], "viewer")["status"],
                         "reported")

    def test_monitoring_and_closed_held_for_confirmation(self):
        ops = [
            {"op_id": "OP-C1", "item_id": self.item_id, "actor": "f", "role": "response_commander", "target_status": "assessing"},
            {"op_id": "OP-C2", "item_id": self.item_id, "actor": "f", "role": "response_commander", "target_status": "containing"},
            {"op_id": "OP-C3", "item_id": self.item_id, "actor": "f", "role": "operations", "target_status": "recovering"},
        ]
        result = self.sync("B-C", ops)
        self.assertEqual([r["status"] for r in result["results"]],
                         ["applied", "applied", "applied"])
        # 现场记到监控而中心更早：挂起等确认
        held = self.sync("B-C2", [{"op_id": "OP-C4", "item_id": self.item_id, "actor": "f",
                                   "role": "operations", "target_status": "monitoring",
                                   "field_status": "recovering"}])
        self.assertEqual(held["results"][0]["status"], "pending")
        pending_id = held["results"][0]["pending_id"]
        item = self.service.get_item(self.item_id, "viewer")
        self.assertEqual(item["status"], "recovering")
        self.assertEqual(len(item["pending_transitions"]), 1)
        with self.assertRaises(PermissionDenied):
            self.service.confirm_pending(pending_id, "x", "viewer")
        confirmed = self.service.confirm_pending(pending_id, "ops-lead", "operations")
        self.assertEqual(confirmed["item"]["status"], "monitoring")
        with self.assertRaises(ConflictError):
            self.service.confirm_pending(pending_id, "ops-lead", "operations")
        # 现场记到关闭而中心更早：挂起后可否决
        held_closed = self.sync("B-C3", [{"op_id": "OP-C5", "item_id": self.item_id,
                                          "actor": "f", "role": "response_commander",
                                          "target_status": "closed"}])
        self.assertEqual(held_closed["results"][0]["status"], "pending")
        closed_id = held_closed["results"][0]["pending_id"]
        rejected = self.service.reject_pending(
            closed_id, "commander", "response_commander", "证据不足，退回现场")
        self.assertEqual(rejected["pending"]["status"], "rejected")
        self.assertEqual(self.service.get_item(self.item_id, "viewer")["status"],
                         "monitoring")
        # 仍有未关闭事项时，关闭确认被拦截
        held_again = self.sync("B-C4", [{"op_id": "OP-C6", "item_id": self.item_id,
                                         "actor": "f", "role": "response_commander",
                                         "target_status": "closed"}])
        blocked_id = held_again["results"][0]["pending_id"]
        self.service.add_record(self.item_id, {"kind": "cleanup", "detail": "遗留事项",
                                               "status": "open", "external_ref": "LED-OPEN"},
                                "recorder", "operations")
        with self.assertRaises(ConflictError):
            self.service.confirm_pending(blocked_id, "commander", "response_commander")
        self.assertTrue(self.repo.verify_audit_chain())

    def test_rejected_ops_returned_and_retry_resumes(self):
        ops = [
            {"op_id": "OP-R1", "item_id": self.item_id, "actor": "f", "role": "operations",
             "record": {"kind": "recovery", "detail": "有记录但缺证据号", "status": "closed"}},
            record_op("OP-R2", self.item_id, "LED-R2", role="viewer"),
            record_op("OP-R3", self.item_id, "LED-R3"),
        ]
        result = self.sync("B-R", ops)
        self.assertEqual([r["status"] for r in result["results"]],
                         ["rejected", "rejected", "applied"])
        self.assertIn("证据缺失", result["results"][0]["reason"])
        self.assertIn("越权", result["results"][1]["reason"])
        self.assertEqual(len(self.service.list_records(self.item_id, "viewer")), 1)
        # 补全证据后整批重发：失败条目重新处理，已入库条目去重
        retry = [
            record_op("OP-R1", self.item_id, "LED-R1"),
            record_op("OP-R2", self.item_id, "LED-R2", role="viewer"),
            record_op("OP-R3", self.item_id, "LED-R3"),
        ]
        resumed = self.sync("B-R", retry)
        self.assertEqual([r["status"] for r in resumed["results"]],
                         ["applied", "rejected", "applied"])
        self.assertFalse(resumed["results"][0]["replayed"])
        self.assertTrue(resumed["results"][2]["replayed"])
        self.assertEqual(len(self.service.list_records(self.item_id, "viewer")), 2)

    def test_historical_data_treated_as_unsynced(self):
        # 值班员回港补录的历史记录（旧接口，无同步来源）
        self.service.add_record(self.item_id, {"kind": "recovery", "detail": "值班员补录",
                                               "status": "closed", "external_ref": "LED-H1"},
                                "clerk", "operations")
        audit_count = len(self.service.audit("viewer", self.item_id))
        # 现场台账随后同步同一证据号：只收一次
        result = self.sync("B-H", [record_op("OP-H1", self.item_id, "LED-H1")])
        self.assertEqual(result["results"][0]["status"], "applied")
        self.assertFalse(result["results"][0]["record_created"])
        self.assertEqual(len(self.service.list_records(self.item_id, "viewer")), 1)
        self.assertEqual(len(self.service.audit("viewer", self.item_id)), audit_count)
        stored = self.service.list_records(self.item_id, "viewer")[0]
        self.assertEqual(stored["source"], "center")

    def test_unknown_item_and_sync_permission(self):
        missing = self.sync("B-X", [record_op("OP-X1", 9999, "LED-X1")])
        self.assertEqual(missing["results"][0]["status"], "rejected")
        self.assertIn("项目不存在", missing["results"][0]["reason"])
        with self.assertRaises(PermissionDenied):
            self.sync("B-X2", [record_op("OP-X2", self.item_id, "LED-X2")], role="viewer")
        # 允许按台账编号定位事件
        by_ref = self.sync("B-X3", [record_op("OP-X3", None, "LED-X3",
                                              item_external_ref="SYNC-ITEM-1")])
        self.assertEqual(by_ref["results"][0]["status"], "applied")
        self.assertEqual(by_ref["results"][0]["item_id"], self.item_id)


if __name__ == "__main__":
    unittest.main()
