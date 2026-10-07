# -*- coding: utf-8 -*-
"""并发验证：故障撤销与故障登记、读数更正、人工放行、证据报告同时进行。

SQLite 下写入经单写库锁串行（busy_timeout 等待），验证：
- 相同内容并发撤销只有一条撤销记录、一份 FAULT_CANCELLED 审计；
- 任何线程都不会留下部分变更或部分清单；
- 报告始终能在一致视图上生成；
- 撤销后仍可人工放行（仅一个放行成功），最终状态自洽。
"""
import os
import threading
os.environ["DATABASE_URL"] = "sqlite:///./test_conc_cancel.db"
os.environ["SEED_DEMO"] = "false"

from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from app.database import Base, SessionLocal, engine
from app import services
from app.models import (AuditLog, FailureCancellation, FailureImpact, Handover,
                        ReadingCorrection, Sample, TemperatureReading)

if os.path.exists("./test_conc_cancel.db"):
    os.remove("./test_conc_cancel.db")
Base.metadata.create_all(bind=engine)

t0 = datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc)
iso = lambda d: d.strftime("%Y-%m-%dT%H:%M:%S")

db = SessionLocal()
s = services.register_sample(db, "K-001", 2.0, 8.0, "冷库A-01", "setup")
sid = s.id
s.created_at = t0
db.commit()
f, _, imps = services.register_failure(
    db, "F-K", "冷库A-01", iso(t0 + timedelta(hours=1)),
    iso(t0 + timedelta(hours=2)), "压缩机故障", "甲")
fid = f.id
assert len(imps) == 1
# 登记时无待确认交接；直接插一条读数供并发更正使用
r = TemperatureReading(sample_id=sid, value=5.0, recorded_by="a",
                       is_out_of_range=False,
                       recorded_at=t0 + timedelta(minutes=30))
db.add(r)
db.commit()
rid = r.id
db.close()

errors: list[Exception] = []
N_THREADS = 10
barrier = threading.Barrier(N_THREADS)


def run(fn):
    def wrapper():
        try:
            barrier.wait()
            fn()
        except Exception as e:  # noqa
            errors.append(e)
    t = threading.Thread(target=wrapper)
    t.start()
    return t


def do_cancel_same():
    # 并发下只有一个线程真正插入，其余经唯一约束冲突路径回滚后按幂等返回首次结果
    x = SessionLocal()
    try:
        _, c, _ = services.cancel_failure(x, "F-K", "核实为探头误报", "复核员A")
        assert c.reason == "核实为探头误报"
    finally:
        x.close()


def do_register_same():
    x = SessionLocal()
    try:
        services.register_failure(
            x, "F-K", "冷库A-01", iso(t0 + timedelta(hours=1)),
            iso(t0 + timedelta(hours=2)), "压缩机故障", "甲")
    finally:
        x.close()


def do_correct():
    x = SessionLocal()
    try:
        services.correct_reading(x, rid, 5.1, "并发更正链", "更正员")
    finally:
        x.close()


def do_report():
    x = SessionLocal()
    try:
        rep = services.build_evidence_report(
            x, sid, iso(t0), iso(t0 + timedelta(hours=3)))
        assert rep["counts"]["failure_segments"] == len(rep["failure_rows"])
        assert rep["counts"]["readings"] == len(rep["rows"])
    finally:
        x.close()


def do_release():
    x = SessionLocal()
    try:
        # 压测中更正不增版本、撤销不动版本，v2 应可放行；并发下仅一个成功
        try:
            services.release_quarantine(
                x, sid, "人工复核：误报故障，放行", "主管", 2)
        except services.ConflictError:
            pass
    finally:
        x.close()


# 5 个同内容并发撤销（含首次插入竞争）+ 2 个同内容重复登记 + 更正/报告/放行各 1
threads = [run(do_cancel_same) for _ in range(5)]
threads += [run(do_register_same) for _ in range(2)]
threads += [run(do_correct), run(do_report), run(do_release)]
assert len(threads) == N_THREADS
for t in threads:
    t.join()

assert not errors, [repr(e) for e in errors]

# 并发结束后再验证冲突规则（确定性）：不同原因/操作者 -> 409；相同内容幂等
db = SessionLocal()
try:
    for reason, operator in (("另一个原因", "复核员A"),
                             ("核实为探头误报", "另一个人")):
        try:
            services.cancel_failure(db, "F-K", reason, operator)
            raise AssertionError("内容不同应报冲突")
        except services.ConflictError:
            pass
    _, c_same, created_same = services.cancel_failure(
        db, "F-K", "核实为探头误报", "复核员A")
    assert not created_same
finally:
    db.close()

# ---- 最终状态自洽 ----
db = SessionLocal()
cancels = db.scalars(select(FailureCancellation).where(
    FailureCancellation.failure_id == fid)).all()
assert len(cancels) == 1, len(cancels)
assert cancels[0].reason == "核实为探头误报" and cancels[0].operator == "复核员A"
cancel_audits = db.scalars(select(AuditLog).where(
    AuditLog.event_type == "FAULT_CANCELLED", AuditLog.sample_id == sid)).all()
assert len(cancel_audits) == 1, len(cancel_audits)
# 影响清单原样留存
final_imps = db.scalars(select(FailureImpact).where(
    FailureImpact.failure_id == fid)).all()
assert len(final_imps) == 1 and final_imps[0].id == imps[0].id
# 更正链完整
corrs = db.scalars(select(ReadingCorrection).where(
    ReadingCorrection.reading_id == rid)).all()
assert len(corrs) == 1
# 人工放行最终生效（撤销后仍须且仍可人工放行）
sample = db.get(Sample, sid)
assert sample.status == "ACTIVE", sample.status
assert sample.version == 3, sample.version
# 撤销后再生成报告：相交故障已撤销、读数最终值在界 -> INCONCLUSIVE
rep = services.build_evidence_report(
    db, sid, iso(t0), iso(t0 + timedelta(hours=3)))
assert rep["verdict"] == "INCONCLUSIVE"
assert rep["counts"]["cancelled_failures"] == 1
assert rep["counts"]["active_failures"] == 0
assert rep["rows"][0]["final_value"] == 5.1
assert rep["rows"][0]["final_out_of_range"] is False
db.close()

print("ALL CANCELLATION CONCURRENCY TESTS PASSED")
