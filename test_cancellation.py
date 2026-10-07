# -*- coding: utf-8 -*-
"""端到端验证已登记冷库故障的误报撤销（SQLite，FastAPI TestClient）。"""
import csv as csvmod
import io
import os
os.environ["DATABASE_URL"] = "sqlite:///./test_cancel.db"
os.environ["SEED_DEMO"] = "false"

from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient
from sqlalchemy import select

from app.main import app
from app.database import Base, engine, SessionLocal
from app import services
from app.models import (AuditLog, EquipmentFailure, FailureCancellation,
                        FailureImpact, Handover, QuarantineRecord, Sample,
                        TemperatureReading)

if os.path.exists("./test_cancel.db"):
    os.remove("./test_cancel.db")
Base.metadata.create_all(bind=engine)

t0 = datetime(2026, 2, 1, 0, 0, 0, tzinfo=timezone.utc)


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


db = SessionLocal()

# 样品 S1 登记于 冷库A-01，温区 [2,8]
s1 = services.register_sample(db, "C-001", 2.0, 8.0, "冷库A-01", "tester")
s1id = s1.id
s1.created_at = t0
db.flush()

# 样品 S2 登记于 冷库A-02（用于未撤销故障对照）
s2 = services.register_sample(db, "C-002", 2.0, 8.0, "冷库A-02", "tester")
s2id = s2.id
s2.created_at = t0
db.flush()

# S1 两条交接：01:30 发起待确认（应在故障登记时失效）；02:00 已确认 -> 冷库B-01
hp = Handover(sample_id=s1id, from_person="a", to_person="b", destination="冷库Z-99",
              status="PENDING", created_at=t0 + timedelta(hours=1, minutes=30))
hc = Handover(sample_id=s1id, from_person="a", to_person="b", destination="冷库B-01",
              status="CONFIRMED", created_at=t0 + timedelta(hours=1, minutes=50),
              decided_at=t0 + timedelta(hours=2))
db.add_all([hp, hc])
db.commit()

# 故障 F-ACTIVE：冷库A-01 [01:00,02:00)，与 S1 登记位置时段相交 -> 隔离 S1、失效 hp
f_a, created_a, imp_a = services.register_failure(
    db, "F-ACTIVE", "冷库A-01", iso(t0 + timedelta(hours=1)),
    iso(t0 + timedelta(hours=2)), "压缩机故障", "登记员甲")
assert created_a and len(imp_a) == 1
f_aid = f_a.id

# 故障 F-OTHER：冷库A-02 [01:00,02:00) 影响 S2，保持未撤销（对照）
f_o, created_o, imp_o = services.register_failure(
    db, "F-OTHER", "冷库A-02", iso(t0 + timedelta(hours=1)),
    iso(t0 + timedelta(hours=2)), "风机故障", "登记员乙")
assert created_o and len(imp_o) == 1
f_oid = f_o.id

# 故障 F-EMPTY：无样品库位 -> 空清单（撤销空清单故障不应写样品审计）
f_e, created_e, imp_e = services.register_failure(
    db, "F-EMPTY", "冷库X-00", iso(t0 + timedelta(hours=1)),
    iso(t0 + timedelta(hours=2)), "巡检误触", "登记员丙")
assert created_e and imp_e == []

snap = {
    "s1_status": db.get(Sample, s1id).status,
    "s1_version": db.get(Sample, s1id).version,
    "s1_location": db.get(Sample, s1id).current_location,
    "hp_status": db.get(Handover, hp.id).status,
    "hp_failure": db.get(Handover, hp.id).failure_id,
    "hc_status": db.get(Handover, hc.id).status,
    "impacts": [(i.id, i.sample_id, i.overlap_start, i.overlap_end, i.was_quarantined)
                for i in db.scalars(select(FailureImpact)
                                    .where(FailureImpact.failure_id == f_aid)).all()],
    "failure_reason": db.get(EquipmentFailure, f_aid).reason,
    "quarantines": [(q.id, q.status, q.reason) for q in db.scalars(
        select(QuarantineRecord).where(QuarantineRecord.failure_id == f_aid)).all()],
}
assert snap["s1_status"] == "QUARANTINED"
assert snap["s1_version"] == 2
assert snap["s1_location"] == "冷库A-01"   # 故障不改变位置
assert snap["hp_status"] == "INVALIDATED"
assert snap["hp_failure"] == f_aid
assert snap["hc_status"] == "CONFIRMED"
assert len(snap["quarantines"]) == 1
db.close()

# ================= 服务层：撤销校验与原子性 =================
db = SessionLocal()

# 不存在的编号 -> 404
try:
    services.cancel_failure(db, "F-NOPE", "误报", "复核员")
    raise AssertionError("应拒绝不存在的编号")
except services.DomainError as e:
    assert e.status_code == 404 and "不存在" in e.message

from sqlalchemy import func
# 空原因 / 空姓名 / 空编号 -> 400，且不留任何撤销记录
for kwargs in ({"code": "F-ACTIVE", "reason": "", "operator": "复核员"},
               {"code": "F-ACTIVE", "reason": "误报", "operator": "  "},
               {"code": "  ", "reason": "误报", "operator": "复核员"}):
    try:
        services.cancel_failure(db, **kwargs)
        raise AssertionError(f"应拒绝空字段：{kwargs}")
    except services.DomainError as e:
        assert e.status_code == 400, e.message
assert db.scalar(select(func.count()).select_from(FailureCancellation)) == 0

# 首次撤销 F-ACTIVE
failure, cancellation, created = services.cancel_failure(
    db, "F-ACTIVE", "现场复核为探头误报，设备正常", "复核员张三")
assert created and cancellation.failure_id == f_aid
cancel_id = cancellation.id
assert cancellation.reason == "现场复核为探头误报，设备正常"
assert cancellation.operator == "复核员张三"

# 幂等：相同编号/原因/操作者 -> 返回首次结果，不新增记录、不新增审计
f2, c2, created2 = services.cancel_failure(
    db, "F-ACTIVE", "现场复核为探头误报，设备正常", "复核员张三")
assert not created2 and c2.id == cancel_id
assert db.scalar(select(func.count())
                 .select_from(FailureCancellation)
                 .where(FailureCancellation.failure_id == f_aid)) == 1

# 冲突：原因不同 -> 409，原记录不改写
try:
    services.cancel_failure(db, "F-ACTIVE", "另一个撤销原因", "复核员张三")
    raise AssertionError("原因不同应报冲突")
except services.ConflictError as e:
    assert e.status_code == 409
# 冲突：操作者不同 -> 409
try:
    services.cancel_failure(db, "F-ACTIVE", "现场复核为探头误报，设备正常", "李四")
    raise AssertionError("操作者不同应报冲突")
except services.ConflictError:
    pass
c_after = db.get(FailureCancellation, cancel_id)
assert c_after.reason == "现场复核为探头误报，设备正常"
assert c_after.operator == "复核员张三"

# 撤销空清单故障：成功，但不产生任何样品审计
f_er, c_e, created_er = services.cancel_failure(db, "F-EMPTY", "巡检记录误触，无实际故障", "复核员王五")
assert created_er
assert db.scalar(select(func.count()).select_from(AuditLog)
                 .where(AuditLog.event_type == "FAULT_CANCELLED")) == 1  # 仅 S1 一条

# ================= 留存与不自动放行 =================
s1_after = db.get(Sample, s1id)
assert s1_after.status == "QUARANTINED", "撤销不得自动解除隔离"
assert s1_after.version == 2, "撤销不得改变样品版本"
assert s1_after.current_location == "冷库A-01", "撤销不得改变位置"
hp_after = db.get(Handover, hp.id)
assert hp_after.status == "INVALIDATED" and hp_after.failure_id == f_aid, "失效交接不得恢复"
hc_after = db.get(Handover, hc.id)
assert hc_after.status == "CONFIRMED"
imps_after = db.scalars(select(FailureImpact).where(FailureImpact.failure_id == f_aid)).all()
assert [(i.id, i.sample_id, services._as_utc(i.overlap_start),
         services._as_utc(i.overlap_end), i.was_quarantined)
        for i in imps_after] == snap["impacts"], "固定影响清单不得改写"
f_after = db.get(EquipmentFailure, f_aid)
assert f_after.reason == "压缩机故障" and f_after.code == "F-ACTIVE", "原故障登记不得改写"
qs_after = db.scalars(select(QuarantineRecord).where(QuarantineRecord.failure_id == f_aid)).all()
assert [(q.id, q.status, q.reason) for q in qs_after] == snap["quarantines"]
assert all(q.status == "ACTIVE" for q in qs_after)

# 撤销审计：逐受影响样品一条 FAULT_CANCELLED，明细含撤销依据
cancel_audits = db.scalars(select(AuditLog)
                           .where(AuditLog.event_type == "FAULT_CANCELLED",
                                  AuditLog.sample_id == s1id)).all()
assert len(cancel_audits) == 1
assert cancel_audits[0].actor == "复核员张三"
assert cancel_audits[0].detail["failure_code"] == "F-ACTIVE"
assert cancel_audits[0].detail["cancel_reason"] == "现场复核为探头误报，设备正常"
db.commit()
db.close()

# 人工复核放行仍然可用（撤销后仍须走原流程）
db = SessionLocal()
services.release_quarantine(db, s1id, "人工复核：故障系误报，样品全程温度合格，同意放行",
                            "复核主管赵六", 2)
assert db.get(Sample, s1id).status == "ACTIVE"
q_rel = db.scalars(select(QuarantineRecord)
                   .where(QuarantineRecord.sample_id == s1id)).all()
assert all(q.status == "RELEASED" for q in q_rel)
db.close()

# ================= 证据报告：列出已撤销故障但不再判不符合 =================
db = SessionLocal()
win = (iso(t0), iso(t0 + timedelta(hours=3)))

# S1：窗内与已撤销 F-ACTIVE 相交、读数全部在界 -> INCONCLUSIVE（不再仅因故障判不符合）
rep1 = services.build_evidence_report(db, s1id, *win)
assert len(rep1["failure_rows"]) == 1
fr1 = rep1["failure_rows"][0]
assert fr1["failure"].code == "F-ACTIVE"
assert fr1["cancelled"] is True
assert fr1["cancellation"].operator == "复核员张三"
assert rep1["verdict"] == "INCONCLUSIVE", rep1["verdict"]
assert rep1["counts"]["cancelled_failures"] == 1
assert rep1["counts"]["active_failures"] == 0
assert any("已被撤销" in r and "不再仅因该故障判为不符合" in r for r in rep1["reasons"])

# S2：相交故障 F-OTHER 未撤销 -> 仍为 NON_COMPLIANT
rep2 = services.build_evidence_report(db, s2id, *win)
assert rep2["verdict"] == "NON_COMPLIANT"
assert rep2["counts"]["active_failures"] == 1
assert rep2["counts"]["cancelled_failures"] == 0
assert rep2["failure_rows"][0]["cancelled"] is False
db.close()

# ================= HTTP 层 =================
with TestClient(app) as client:
    # 启动保持可用
    assert client.get("/health").status_code == 200
    assert client.get("/failures").status_code == 200

    # 故障详情展示撤销记录，且不再显示撤销表单
    detail = client.get(f"/failures/{f_aid}")
    assert detail.status_code == 200
    assert "已撤销（误报）" in detail.text
    assert "复核员张三" in detail.text
    assert "现场复核为探头误报，设备正常" in detail.text
    assert "/cancel" not in detail.text
    # 原故障、影响清单、隔离、失效交接仍展示
    assert "冷库A-01" in detail.text and "压缩机故障" in detail.text

    # 未撤销故障详情仍有撤销表单
    detail_o = client.get(f"/failures/{f_oid}")
    assert 'action="/failures/' in detail_o.text and "/cancel" in detail_o.text

    # 列表页显示撤销状态
    listing = client.get("/failures").text
    assert "已撤销（误报）" in listing and "F-ACTIVE" in listing

    # 重复撤销（相同内容）-> 303 回详情，消息说明返回首次结果
    r = client.post(f"/failures/{f_aid}/cancel", data={
        "code": "F-ACTIVE", "reason": "现场复核为探头误报，设备正常",
        "operator": "复核员张三"}, follow_redirects=False)
    assert r.status_code == 303
    r_follow = client.post(f"/failures/{f_aid}/cancel", data={
        "code": "F-ACTIVE", "reason": "现场复核为探头误报，设备正常",
        "operator": "复核员张三"})
    assert "已返回首次撤销结果" in r_follow.text

    # 内容不同 -> 冲突拒绝
    r_conf = client.post(f"/failures/{f_aid}/cancel", data={
        "code": "F-ACTIVE", "reason": "不一样的原因", "operator": "复核员张三"})
    assert "冲突" in r_conf.text
    r_conf2 = client.post(f"/failures/{f_aid}/cancel", data={
        "code": "F-ACTIVE", "reason": "现场复核为探头误报，设备正常", "operator": "王五"})
    assert "冲突" in r_conf2.text

    # 空原因 / 空姓名 -> 明确拒绝
    r_empty = client.post(f"/failures/{f_aid}/cancel", data={
        "code": "F-ACTIVE", "reason": "  ", "operator": "复核员张三"})
    assert "必填字段缺失" in r_empty.text and "撤销原因" in r_empty.text
    r_noop = client.post(f"/failures/{f_aid}/cancel", data={
        "code": "F-ACTIVE", "reason": "误报", "operator": ""})
    assert "必填字段缺失" in r_noop.text and "操作者" in r_noop.text

    # 不存在编号（用不存在的路径 ID）-> 拒绝
    r_404 = client.post("/failures/999999/cancel", data={
        "code": "F-GHOST", "reason": "误报", "operator": "张三"})
    assert r_404.status_code == 200 and "不存在" in r_404.text  # 重定向到列表并带错误消息
    # 路径与编号不一致 -> 拒绝
    r_mismatch = client.post(f"/failures/{f_aid}/cancel", data={
        "code": "F-OTHER", "reason": "误报", "operator": "张三"})
    assert "不一致" in r_mismatch.text

    # F-OTHER 经 HTTP 撤销：成功消息说明留存与不自动放行
    r_ok = client.post(f"/failures/{f_oid}/cancel", data={
        "code": "F-OTHER", "reason": "复核确认系通讯模块误报", "operator": "复核员李四"})
    assert "已标记为误报撤销" in r_ok.text
    assert "不自动解除隔离" in r_ok.text and "人工复核放行" in r_ok.text

    # 证据报告页面：S1 列已撤销故障及依据，结论不再是「不符合」
    html1 = client.get(f"/samples/{s1id}/evidence",
                       params={"start": win[0], "end": win[1]}).text
    assert "F-ACTIVE" in html1
    assert "现场复核为探头误报，设备正常" in html1
    assert "结论：无法判定合规" in html1
    assert "结论：不符合" not in html1
    assert "已被撤销" in html1
    # S2 撤销后再次查询 -> 也转为 INCONCLUSIVE
    html2 = client.get(f"/samples/{s2id}/evidence",
                       params={"start": win[0], "end": win[1]}).text
    assert "F-OTHER" in html2 and "通讯模块误报" in html2
    assert "结论：无法判定合规" in html2 and "结论：不符合" not in html2

    # CSV 同内容：撤销列与撤销依据齐全
    csv_resp = client.get(f"/samples/{s1id}/evidence.csv",
                          params={"start": win[0], "end": win[1]})
    assert csv_resp.status_code == 200 and "text/csv" in csv_resp.headers["content-type"]
    csv_text = csv_resp.content.decode("utf-8-sig")
    assert "F-ACTIVE" in csv_text
    assert "是（误报已撤销）" in csv_text
    assert "现场复核为探头误报，设备正常" in csv_text
    assert "复核员张三" in csv_text
    rows_csv = list(csvmod.reader(io.StringIO(csv_text)))
    verdict_row = next(r for r in rows_csv if r and r[0] == "结论")
    assert "无法判定合规" in verdict_row[1] and "不符合" not in verdict_row[1]
    fault_rows = [r for r in rows_csv if len(r) > 1 and r[1] == "F-ACTIVE"]
    assert fault_rows and fault_rows[0][8] == "是（误报已撤销）"

    # 样品详情页展示撤销状态，且仍提供人工解除入口说明
    sample_html = client.get(f"/samples/{s1id}").text
    assert "已撤销（误报）" in sample_html
    # S2 仍隔离 -> 解除表单仍在（撤销不自动解除）
    s2_html = client.get(f"/samples/{s2id}").text
    assert 'name="reason"' in s2_html and "解除隔离" in s2_html

    # 原故障登记查询保持可用：同编号重复登记仍幂等
    r_redup = client.post("/failures", data={
        "code": "F-ACTIVE", "location": "冷库A-01",
        "start_time": iso(t0 + timedelta(hours=1)),
        "end_time": iso(t0 + timedelta(hours=2)),
        "reason": "压缩机故障", "operator": "登记员甲"})
    assert r_redup.status_code == 200 and "已登记过" in r_redup.text

# ================= 最终更正值越界不因撤销而消失 =================
db = SessionLocal()
# 给 S1 补一条窗内越界读数（S1 已人工放行，越界会重新隔离）；显式指定历史时刻保证时间窗确定
bad = TemperatureReading(sample_id=s1id, value=99.0, recorded_by="记录员",
                         is_out_of_range=True,
                         recorded_at=t0 + timedelta(hours=2, minutes=30))
db.add(bad)
db.commit()
rep3 = services.build_evidence_report(db, s1id,
                                      iso(t0), iso(t0 + timedelta(hours=3)))
assert rep3["verdict"] == "NON_COMPLIANT", "最终值越界仍须判不符合"
assert rep3["counts"]["final_out_of_range"] >= 1
assert rep3["counts"]["cancelled_failures"] == 1
db.close()

print("ALL FAILURE CANCELLATION TESTS PASSED")
