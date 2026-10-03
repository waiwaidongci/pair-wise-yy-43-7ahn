import json
import tempfile
import unittest
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from pathlib import Path

from src.http_api import make_handler
from src.repository import Repository
from src.service import Service


class SyncTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Repository(str(Path(self.tmp.name) / "test.db"))
        self.service = Service(self.repo)
        self.item = self.service.create_item(
            {"title": "sync item", "description": "offline batch", "severity": "major",
             "quantity": 5, "threshold": 10, "external_ref": "SYNC-1"},
            "creator", "observer")

    def tearDown(self):
        self.repo.close()
        self.tmp.cleanup()

    def _sync(self, batch_key, records, actor="duty", role="response_commander"):
        return self.service.sync_batch(
            {"batch_key": batch_key, "records": records}, actor, role)

    def test_offline_batch_sync_creates_records_and_audit(self):
        res = self._sync("b1", [
            {"client_ref": "c1", "item_id": self.item["id"], "kind": "containment",
             "detail": "boom deployed", "evidence": "photo-1", "target_stage": "containing"},
            {"client_ref": "c2", "item_id": self.item["id"], "kind": "recovery",
             "detail": "skimmer running", "evidence": "photo-2"},
        ])
        self.assertEqual(res["summary"]["accepted"], 2)
        records = self.service.list_records(self.item["id"], "viewer")
        self.assertEqual(len(records), 2)
        # 审计链完整，且同步产生了 record 与 transition 事件
        self.assertTrue(self.repo.verify_audit_chain())
        actions = [e["action"] for e in self.service.audit("viewer", self.item["id"])]
        self.assertIn("record", actions)
        self.assertIn("transition", actions)

    def test_duplicate_sync_only_accepted_once(self):
        records = [{"client_ref": "d1", "item_id": self.item["id"], "kind": "evidence",
                    "detail": "first copy", "evidence": "p1"}]
        self._sync("b-dup", records)
        before_records = len(self.service.list_records(self.item["id"], "viewer"))
        before_audit = len(self.service.audit("viewer", self.item["id"]))
        # 断网重发同一批次
        res = self._sync("b-dup", records)
        self.assertEqual(res["summary"]["duplicate"], 1)
        self.assertEqual(len(self.service.list_records(self.item["id"], "viewer")), before_records)
        self.assertEqual(len(self.service.audit("viewer", self.item["id"])), before_audit)
        self.assertTrue(self.repo.verify_audit_chain())

    def test_late_evidence_when_field_behind_only_supplements(self):
        # 中心已到 recovering，现场还在 containing
        cur = self.item
        for target, role in [("assessing", "response_commander"),
                             ("containing", "response_commander"),
                             ("recovering", "operations")]:
            cur = self.service.transition(cur["id"], target, cur["version"], "rev", role)
        self.assertEqual(cur["status"], "recovering")
        res = self._sync("b-late", [
            {"client_ref": "late1", "item_id": self.item["id"], "kind": "evidence",
             "detail": "late containing log", "evidence": "p", "target_stage": "containing"},
        ], role="operations")
        self.assertEqual(res["outcomes"][0]["outcome"], "accepted")
        # 状态不得回退
        self.assertEqual(self.service.get_item(self.item["id"], "viewer")["status"], "recovering")
        self.assertEqual(len(self.service.list_records(self.item["id"], "viewer")), 1)

    def test_field_ahead_monitoring_or_closed_is_suspended(self):
        res = self._sync("b-ahead", [
            {"client_ref": "a1", "item_id": self.item["id"], "kind": "final",
             "detail": "all done", "evidence": "p", "target_stage": "closed"},
        ])
        self.assertEqual(res["outcomes"][0]["outcome"], "pending")
        # 挂起期间不进入正式台账
        self.assertEqual(len(self.service.list_records(self.item["id"], "viewer")), 0)
        pending = self.service.list_pending_sync("response_commander")
        self.assertEqual([p["client_ref"] for p in pending], ["a1"])
        # 人工退回
        dec = self.service.decide_sync("b-ahead", "a1", "reject", "boss",
                                       "response_commander", "证据不足")
        self.assertEqual(dec["outcome"], "rejected")
        self.assertEqual(len(self.service.list_records(self.item["id"], "viewer")), 0)

    def test_confirm_pending_adds_record_and_keeps_chain(self):
        self._sync("b-ok", [
            {"client_ref": "m1", "item_id": self.item["id"], "kind": "shoreline",
             "detail": "shore checked", "evidence": "p", "target_stage": "monitoring"},
        ])
        dec = self.service.decide_sync("b-ok", "m1", "confirm", "boss",
                                       "response_commander")
        self.assertEqual(dec["outcome"], "accepted")
        self.assertEqual(len(self.service.list_records(self.item["id"], "viewer")), 1)
        # 重复确认不重复入账
        again = self.service.decide_sync("b-ok", "m1", "confirm", "boss",
                                         "response_commander")
        self.assertEqual(again["outcome"], "duplicate")
        self.assertEqual(len(self.service.list_records(self.item["id"], "viewer")), 1)
        self.assertTrue(self.repo.verify_audit_chain())

    def test_missing_evidence_record_rejected_individually(self):
        res = self._sync("b-mix", [
            {"client_ref": "g1", "item_id": self.item["id"], "kind": "evidence",
             "detail": "ok", "evidence": "p"},
            {"client_ref": "b1r", "item_id": self.item["id"], "kind": "evidence",
             "detail": "no evidence"},
        ])
        outcomes = {o["client_ref"]: o["outcome"] for o in res["outcomes"]}
        self.assertEqual(outcomes["g1"], "accepted")
        self.assertEqual(outcomes["b1r"], "rejected")
        self.assertEqual(len(self.service.list_records(self.item["id"], "viewer")), 1)

    def test_rejected_record_can_retry_with_evidence(self):
        self._sync("b-retry", [
            {"client_ref": "fix1", "item_id": self.item["id"], "kind": "evidence",
             "detail": "photo", "evidence": ""},
        ])
        self.assertEqual(len(self.service.list_records(self.item["id"], "viewer")), 0)
        # 补证据后用同一 client_ref 重传续传
        res = self._sync("b-retry", [
            {"client_ref": "fix1", "item_id": self.item["id"], "kind": "evidence",
             "detail": "photo", "evidence": "photo-99"},
        ])
        self.assertEqual(res["outcomes"][0]["outcome"], "accepted")
        self.assertEqual(len(self.service.list_records(self.item["id"], "viewer")), 1)
        # 再次重传只算重复
        res2 = self._sync("b-retry", [
            {"client_ref": "fix1", "item_id": self.item["id"], "kind": "evidence",
             "detail": "photo", "evidence": "photo-99"},
        ])
        self.assertEqual(res2["outcomes"][0]["outcome"], "duplicate")
        self.assertEqual(len(self.service.list_records(self.item["id"], "viewer")), 1)

    def test_unauthorized_record_rejected_individually(self):
        res = self._sync("b-role", [
            {"client_ref": "u1", "item_id": self.item["id"], "kind": "evidence",
             "detail": "x", "evidence": "p"},
        ], actor="viewer", role="viewer")
        self.assertEqual(res["outcomes"][0]["outcome"], "rejected")
        self.assertIn("越权", res["outcomes"][0]["reason"])
        self.assertEqual(len(self.service.list_records(self.item["id"], "viewer")), 0)

    def test_historical_records_without_client_ref_still_work(self):
        # 旧接口补录的历史记录没有 client_ref，按未同步兼容处理
        self.service.add_record(self.item["id"], {
            "kind": "action", "detail": "legacy entry", "status": "open",
            "external_ref": "LEGACY-1"}, "clerk", "response_commander")
        records = self.service.list_records(self.item["id"], "viewer")
        self.assertEqual(len(records), 1)
        # 同步功能不受历史数据影响
        res = self._sync("b-new", [
            {"client_ref": "n1", "item_id": self.item["id"], "kind": "evidence",
             "detail": "new", "evidence": "p"},
        ])
        self.assertEqual(res["summary"]["accepted"], 1)
        self.assertEqual(len(self.service.list_records(self.item["id"], "viewer")), 2)
        self.assertTrue(self.repo.verify_audit_chain())


class SyncHttpTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Repository(str(Path(self.tmp.name) / "test.db"))
        self.service = Service(self.repo)
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0),
                                          make_handler(self.service, str(Path(__file__).parent.parent / "static")))
        self.port = self.httpd.server_address[1]
        import threading
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        self.item = self.service.create_item(
            {"title": "http item", "description": "d", "severity": "minor",
             "quantity": 1, "threshold": 10}, "creator", "observer")

    def tearDown(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.repo.close()
        self.tmp.cleanup()

    def _post(self, path, body, actor="duty", role="response_commander"):
        conn = HTTPConnection("127.0.0.1", self.port)
        conn.request("POST", path, json.dumps(body).encode("utf-8"),
                     {"Content-Type": "application/json", "X-Actor": actor, "X-Role": role})
        resp = conn.getresponse()
        data = json.loads(resp.read().decode("utf-8"))
        conn.close()
        return resp.status, data

    def _get(self, path, actor="duty", role="response_commander"):
        conn = HTTPConnection("127.0.0.1", self.port)
        conn.request("GET", path, headers={"X-Actor": actor, "X-Role": role})
        resp = conn.getresponse()
        data = json.loads(resp.read().decode("utf-8"))
        conn.close()
        return resp.status, data

    def test_sync_endpoint_and_pending_flow(self):
        status, data = self._post("/api/sync", {
            "batch_key": "http-b1",
            "records": [
                {"client_ref": "h1", "item_id": self.item["id"], "kind": "evidence",
                 "detail": "ok", "evidence": "p"},
                {"client_ref": "h2", "item_id": self.item["id"], "kind": "final",
                 "detail": "done", "evidence": "p", "target_stage": "closed"},
            ],
        })
        self.assertEqual(status, 200)
        self.assertEqual(data["summary"]["accepted"], 1)
        self.assertEqual(data["summary"]["pending"], 1)
        # 挂起列表
        status, data = self._get("/api/sync/pending")
        self.assertEqual(status, 200)
        self.assertEqual([r["client_ref"] for r in data["records"]], ["h2"])
        # 批次查询
        status, data = self._get("/api/sync/batches/http-b1")
        self.assertEqual(status, 200)
        self.assertEqual(len(data["records"]), 2)
        # 人工确认挂起记录
        status, data = self._post("/api/sync/batches/http-b1/confirm",
                                   {"client_ref": "h2", "decision": "confirm"},
                                   actor="boss", role="response_commander")
        self.assertEqual(status, 200)
        self.assertEqual(data["outcome"], "accepted")


if __name__ == "__main__":
    unittest.main()
