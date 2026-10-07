# -*- coding: utf-8 -*-
"""端到端验证故障撤销（误报）：SQLite + FastAPI TestClient。

覆盖：撤销表单/服务、幂等与冲突、明确拒绝且不留部分变更、
撤销不解除隔离/不恢复交接、证据报告与 CSV 的撤销列示与判定规则、
原故障登记幂等性不受影响。
"""
import os
os.environ["DATABASE_URL"] = "sqlite:///./test_revocation.db"
os.environ["SEED_DEMO"] = "false"

from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient
from sqlalchemy import func, select

from app.main import app
from app.database import Base, engine, SessionLocal
from app import services
from app.models import (AuditLog, EquipmentFailure, FailureImpact,
                        FailureRevocation, Handover, QuarantineRecord, Sample,
                        TemperatureReading)
from app.services import ConflictError, DomainError

if os.path.exists("./test_revocation.db"):
    os.remove("./test_revocation.db")
Base.metadata.create_all(bind=engine)

t0 = datetime(2026, 2, 1, 0, 0, 0, tzinfo=timezone.utc)


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


db = SessionLocal()

# 样品 A：冷库A-01，一条在界读数，一条待确认交接（将被故障失效）
sA = services.register_sample(db, "RV-A", 2.0, 8.0, "冷库A-01", "tester")
sA.created_at = t0
db.flush()
hA = Handover(sample_id=sA.id, from_person="a", to_person="b",
              destination="冷库B-01", status="PENDING",
              created_at=t0 + timedelta(hours=1))
db.add(hA)
db.add(TemperatureReading(sample_id=sA.id, value=5.0, recorded_by="a",
                          is_out_of_range=False,
                          recorded_at=t0 + timedelta(hours=2)))

# 样品 B：冷库A-01，一条最终值越界读数（直接落库，样品仍为 ACTIVE，
# 随后由故障登记隔离），用于验证「最终更正值越界仍按原规则判定」
sB = services.register_sample(db, "RV-B", 2.0, 8.0, "冷库A-01", "tester")
sB.created_at = t0
db.flush()
db.add(TemperatureReading(sample_id=sB.id, value=9.0, recorded_by="a",
                          is_out_of_range=True,
                          recorded_at=t0 + timedelta(hours=2)))
db.commit()

# 故障 F-1：冷库A-01 01:00~03:00 → A/B 均未隔离：隔离 + A 的待确认交接失效
f1, created, imp1 = services.register_failure(
    db, "F-1", "冷库A-01", iso(t0 + timedelta(hours=1)),
    iso(t0 + timedelta(hours=3)), "压缩机故障", "op")
assert created and len(imp1) == 2
# 故障 F-2：冷库A-01 04:00~05:00 → A/B 已隔离：仅关联
f2, created, imp2 = services.register_failure(
    db, "F-2", "冷库A-01", iso(t0 + timedelta(hours=4)),
    iso(t0 + timedelta(hours=5)), "传感器漂移", "op")
assert created and len(imp2) == 2
assert all(i.was_quarantined for i in imp2)

win = {"start": iso(t0), "end": iso(t0 + timedelta(hours=10))}

# ---------- 撤销前：报告因 F-1/F-2 判不符合 ----------
db_pre = SessionLocal()
rep_pre = services.build_evidence_report(db_pre, sA.id, win["start"], win["end"])
assert rep_pre["verdict"] == "NON_COMPLIANT"
assert rep_pre["counts"]["failures"] == 2
assert rep_pre["counts"]["revoked_failures"] == 0
db_pre.close()

# ---------- 明确拒绝：空原因 / 空姓名 / 不存在的编号，且不留部分变更 ----------
for bad_args, msg in [
    (("F-1", "", "张三"), "撤销原因不能为空"),
    (("F-1", "   ", "张三"), "撤销原因不能为空"),
    (("F-1", "传感器误报", ""), "操作者姓名不能为空"),
    (("F-1", "传感器误报", "  "), "操作者姓名不能为空"),
    (("", "传感器误报", "张三"), "故障编号不能为空"),
]:
    try:
        services.revoke_failure(db, *bad_args)
        raise AssertionError(f"应拒绝：{bad_args}")
    except DomainError as e:
        assert e.status_code == 400 and e.message == msg, (e.message, msg)
try:
    services.revoke_failure(db, "F-NOPE", "传感器误报", "张三")
    raise AssertionError("不存在的编号应拒绝")
except DomainError as e:
    assert e.status_code == 404 and "不存在" in e.message
assert db.scalar(select(func.count()).select_from(FailureRevocation)) == 0
assert db.scalar(select(func.count()).select_from(AuditLog)
                 .where(AuditLog.event_type == "FAILURE_REVOKED")) == 0

# ---------- 服务层：撤销成功 + 幂等 + 冲突 ----------
version_before = db.get(Sample, sA.id).version
failure1, rev1, created1 = services.revoke_failure(db, "F-1", "传感器误报，已复核", "张三")
assert created1 and failure1.id == f1.id and rev1.reason == "传感器误报，已复核"
# 幂等：相同编号 + 原因 + 姓名 → 返回首次结果
failure1b, rev1b, created1b = services.revoke_failure(db, "F-1", "传感器误报，已复核", "张三")
assert not created1b and rev1b.id == rev1.id
assert db.scalar(select(func.count()).select_from(FailureRevocation)) == 1
# 冲突：同编号不同内容 → 409
for bad in [("F-1", "另一个原因", "张三"), ("F-1", "传感器误报，已复核", "李四")]:
    try:
        services.revoke_failure(db, *bad)
        raise AssertionError("内容不同应报冲突")
    except ConflictError as e:
        assert e.status_code == 409 and "冲突" in e.message
assert db.scalar(select(func.count()).select_from(FailureRevocation)) == 1

# ---------- 撤销不改变既有状态：隔离/交接/影响清单/版本均保持 ----------
sA_after = db.get(Sample, sA.id)
assert sA_after.status == "QUARANTINED"          # 不自动解除隔离
assert sA_after.version == version_before        # 撤销不变更样品版本
assert db.get(Handover, hA.id).status == "INVALIDATED"  # 不恢复交接
assert db.scalar(select(func.count()).select_from(FailureImpact)
                 .where(FailureImpact.failure_id == f1.id)) == 2  # 影响清单留存
assert db.scalar(select(func.count()).select_from(QuarantineRecord)
                 .where(QuarantineRecord.failure_id == f1.id)) == 2  # 隔离记录留存
# 每个受影响样品一条 FAILURE_REVOKED 审计（幂等撤销不重复写）
revoked_audits = db.scalars(select(AuditLog)
                            .where(AuditLog.event_type == "FAILURE_REVOKED")).all()
assert len(revoked_audits) == 2
assert {a.sample_id for a in revoked_audits} == {sA.id, sB.id}

# ---------- 撤销 F-1 后：A 的报告仍因 F-2（未撤销）不符合；B 因最终值越界不符合 ----------
db_r = SessionLocal()
rep_a = services.build_evidence_report(db_r, sA.id, win["start"], win["end"])
assert rep_a["verdict"] == "NON_COMPLIANT"
assert rep_a["counts"]["revoked_failures"] == 1
assert rep_a["counts"]["active_failures"] == 1
revoked_fr = [fr for fr in rep_a["failure_rows"] if fr["revocation"] is not None]
assert len(revoked_fr) == 1 and revoked_fr[0]["failure"].code == "F-1"
assert revoked_fr[0]["revocation"].operator == "张三"
assert any("撤销依据" in r for r in rep_a["reasons"])
rep_b = services.build_evidence_report(db_r, sB.id, win["start"], win["end"])
assert rep_b["verdict"] == "NON_COMPLIANT"  # 最终值越界不因故障撤销而改变
assert rep_b["counts"]["final_out_of_range"] == 1
db_r.close()

# ---------- 撤销 F-2 后：A 无越界读数且故障均已撤销 → 无法判定合规 ----------
services.revoke_failure(db, "F-2", "复核确认为误报", "李四")
db_r2 = SessionLocal()
rep_a2 = services.build_evidence_report(db_r2, sA.id, win["start"], win["end"])
assert rep_a2["verdict"] == "INCONCLUSIVE"
assert rep_a2["counts"]["revoked_failures"] == 2
assert rep_a2["counts"]["active_failures"] == 0
assert len(rep_a2["failure_rows"]) == 2  # 已撤销故障仍列出
db_r2.close()
# B 仍有最终值越界 → 不符合
db_r3 = SessionLocal()
rep_b2 = services.build_evidence_report(db_r3, sB.id, win["start"], win["end"])
assert rep_b2["verdict"] == "NON_COMPLIANT"
db_r3.close()

# ---------- 原故障登记幂等性不受撤销影响 ----------
f1_again, created_again, imp_again = services.register_failure(
    db, "F-1", "冷库A-01", iso(t0 + timedelta(hours=1)),
    iso(t0 + timedelta(hours=3)), "压缩机故障", "op")
assert not created_again and f1_again.id == f1.id and len(imp_again) == 2
assert db.scalar(select(func.count()).select_from(FailureRevocation)) == 2
db.close()

with TestClient(app) as client:
    # ---------- 页面：故障详情展示撤销记录 ----------
    html = client.get(f"/failures/{f1.id}").text
    for text in ("撤销记录（经核实为误报）", "张三", "传感器误报，已复核",
                 "不自动解除隔离", "已撤销（误报）"):
        assert text in html, f"故障详情缺少 {text!r}"
    # 已撤销的故障不再展示撤销表单
    assert 'action="/failures/revoke"' not in html

    # ---------- 页面：故障列表状态列与撤销表单 ----------
    html = client.get("/failures").text
    assert "已撤销（误报）" in html and 'action="/failures/revoke"' in html

    # ---------- 页面：样品详情关联故障区块展示撤销状态 ----------
    html = client.get(f"/samples/{sA.id}").text
    assert "已撤销（误报）" in html and "传感器误报，已复核" in html

    # ---------- 表单撤销：明确拒绝（空原因 / 空姓名 / 不存在编号）----------
    r = client.post("/failures/revoke",
                    data={"code": "F-1", "reason": "", "operator": "张三"})
    assert "撤销原因不能为空" in r.text
    r = client.post("/failures/revoke",
                    data={"code": "F-1", "reason": "x", "operator": ""})
    assert "操作者姓名不能为空" in r.text
    r = client.post("/failures/revoke",
                    data={"code": "F-NOPE", "reason": "x", "operator": "张三"})
    assert "不存在" in r.text
    # 表单幂等撤销：返回首次结果提示
    r = client.post("/failures/revoke",
                    data={"code": "F-1", "reason": "传感器误报，已复核", "operator": "张三"})
    assert "返回首次撤销结果" in r.text
    # 表单冲突：内容不同
    r = client.post("/failures/revoke",
                    data={"code": "F-1", "reason": "别的原因", "operator": "张三"})
    assert "冲突" in r.text

    # ---------- 表单完整撤销一个新故障 ----------
    db_f3 = SessionLocal()
    f3, created3, _ = services.register_failure(
        db_f3, "F-3", "冷库A-01", iso(t0 + timedelta(hours=6)),
        iso(t0 + timedelta(hours=7)), "短时断电", "op")
    assert created3
    db_f3.close()
    r = client.post("/failures/revoke",
                    data={"code": "F-3", "reason": "核实为计划内维护", "operator": "王五"})
    assert "已撤销（误报）" in r.text and "人工复核放行" in r.text

    # ---------- 证据报告 HTML：撤销列示与判定 ----------
    html = client.get(f"/samples/{sA.id}/evidence", params=win).text
    assert "结论：无法判定合规" in html          # 全部相交故障已撤销
    assert "已撤销（误报）" in html
    assert "撤销依据：传感器误报，已复核" in html
    assert "F-1" in html and "F-2" in html      # 已撤销故障仍列出
    html_b = client.get(f"/samples/{sB.id}/evidence", params=win).text
    assert "结论：不符合" in html_b              # 最终值越界仍判不符合

    # ---------- 证据报告 CSV：同内容 ----------
    csv_text = client.get(f"/samples/{sA.id}/evidence.csv",
                          params=win).content.decode("utf-8-sig")
    assert "已撤销（误报）" in csv_text
    assert "传感器误报，已复核" in csv_text and "张三" in csv_text
    assert "无法判定合规" in csv_text
    csv_b = client.get(f"/samples/{sB.id}/evidence.csv",
                       params=win).content.decode("utf-8-sig")
    assert "不符合" in csv_b

    # ---------- 审计导出含故障撤销事件 ----------
    audit_csv = client.get(f"/samples/{sA.id}/export.csv").content.decode("utf-8-sig")
    assert "故障撤销（误报）" in audit_csv

# ---------- 最终一致性：撤销后隔离/交接仍未被自动恢复 ----------
db2 = SessionLocal()
assert db2.get(Sample, sA.id).status == "QUARANTINED"
assert db2.get(Sample, sB.id).status == "QUARANTINED"
assert db2.get(Handover, hA.id).status == "INVALIDATED"
assert db2.scalar(select(func.count()).select_from(FailureRevocation)) == 3
# 每个故障对每个受影响样品恰好一条撤销审计
n_revoked = db2.scalar(select(func.count()).select_from(AuditLog)
                       .where(AuditLog.event_type == "FAILURE_REVOKED"))
assert n_revoked == 6, n_revoked  # F-1/F-2/F-3 × 样品 A、B
db2.close()

print("ALL FAILURE REVOCATION TESTS PASSED")
