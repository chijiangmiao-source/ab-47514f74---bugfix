"""One-shot acceptance service: build checks + unit tests + API/HTTP smoke.

Runs the build check (byte-compile) and the unit test suite, then exercises
the live HTTP API through the required scenarios:

  1. export keeps following the frozen rules snapshot after rules change
  2. crash recovery (converge a complete staged artifact; clean up a partial one)
  3. business-equivalent retransmission (first receipt, no second artifact)
     and conflict handling (different records or rules snapshot)
  4. two DIFFERENT export ids with equivalent content: each gets its own
     published file/digest/freeze info (pre-restart phase); after the app and
     workers are restarted, every result is re-checked (post-restart phase),
     including safe convergence of an already-affected aliased PUBLISHED row.

Phases are selected with VERIFY_PHASE=pre|post and share VERIFY_RUN so the two
invocations reference the same stable export ids. Exits 0 on success.
"""
import hashlib
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

API = os.environ.get("API_BASE", "http://localhost:8080").rstrip("/")
DATA_DIR = os.environ.get("DATA_DIR", "./data")
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUN = os.environ.get("VERIFY_RUN") or uuid.uuid4().hex[:6]
PHASE = os.environ.get("VERIFY_PHASE", "pre")

FAILURES = []


def check(name, condition, detail=""):
    mark = "PASS" if condition else "FAIL"
    line = "[%s] %s" % (mark, name)
    if not condition and detail:
        line += " -- " + detail
    print(line, flush=True)
    if not condition:
        FAILURES.append(name)


def step(title):
    print("\n=== %s ===" % title, flush=True)


# ------------------------------------------------------------------ helpers

def req(method, path, body=None):
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(API + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=15) as resp:
            return resp.status, resp.read(), dict(resp.headers)
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(), dict(exc.headers or {})


def as_json(raw):
    return json.loads(raw.decode("utf-8"))


def wait_for_stage(export_id, stage, timeout):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        status, raw, _ = req("GET", "/api/exports/" + export_id)
        if status == 200:
            last = as_json(raw)
            if last["stage"] == stage:
                return last
        time.sleep(1)
    return last


def published_files(export_id):
    directory = os.path.join(DATA_DIR, "artifacts", "published")
    if not os.path.isdir(directory):
        return []
    return [n for n in os.listdir(directory) if n == export_id + ".json"]


def tmp_files(export_id):
    directory = os.path.join(DATA_DIR, "artifacts", "tmp")
    if not os.path.isdir(directory):
        return []
    return [n for n in os.listdir(directory) if n.startswith(export_id + ".")]


def download(export_id):
    return req("GET", "/api/exports/%s/artifact" % export_id)


def eid(index):
    return "VFY%d-%s" % (index, RUN)


def get_detail(export_id):
    status, raw, _ = req("GET", "/api/exports/" + export_id)
    return as_json(raw) if status == 200 else None


def check_own_download(export_id, detail, label):
    """Download and assert the bytes are this export's OWN frozen artifact:
    file hash == recorded artifact digest, and the embedded export id / freeze
    time / input+rules digests all match the export's frozen row."""
    status, raw, _ = download(export_id)
    ok = check("%s: download 200" % label, status == 200, "HTTP %s %s" % (status, raw[:200]))
    if status != 200:
        return None
    check("%s: file hash equals recorded artifact digest" % label,
          hashlib.sha256(raw).hexdigest() == detail["artifact_digest"])
    doc = as_json(raw)
    check("%s: download carries its OWN export id and freeze time" % label,
          doc.get("export_id") == export_id
          and doc.get("received_at") == detail["received_at"],
          "embedded=%s/%s row=%s/%s" % (
              doc.get("export_id"), doc.get("received_at"), export_id, detail["received_at"]))
    check("%s: download matches its own frozen input/rules summary" % label,
          doc.get("input_digest") == detail["input_digest"]
          and doc.get("rules_digest") == detail["rules_digest"])
    return doc


def wait_own_download(export_id, timeout, label):
    """Wait for PUBLISHED then for a download that verifies as its own artifact
    (covers a legacy alias converging just before/after a restart)."""
    deadline = time.time() + timeout
    detail = wait_for_stage(export_id, "PUBLISHED", timeout)
    status = None
    while time.time() < deadline:
        detail = get_detail(export_id) or detail
        status, raw, _ = download(export_id)
        if status == 200:
            doc = as_json(raw)
            if (doc.get("export_id") == export_id
                    and doc.get("received_at") == (detail or {}).get("received_at")
                    and hashlib.sha256(raw).hexdigest() == detail["artifact_digest"]):
                return detail, doc
        time.sleep(0.5)
    check("%s: eventually serves its own verified artifact" % label, False,
          "last HTTP %s detail=%s" % (status, detail and detail.get("stage")))
    return detail, None


# ------------------------------------------------------------------ phases

def build_checks():
    step("构建检查：python -m compileall")
    proc = subprocess.run(
        [sys.executable, "-m", "compileall", "-q", "app", "verify", "tests"],
        cwd=REPO_ROOT, capture_output=True, text=True,
    )
    if proc.stdout:
        print(proc.stdout)
    if proc.stderr:
        print(proc.stderr)
    check("compileall app/verify/tests", proc.returncode == 0, proc.stderr.strip()[:400])


def unit_tests():
    step("代码测试：unittest discover")
    proc = subprocess.run(
        [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-t", ".", "-v"],
        cwd=REPO_ROOT, capture_output=True, text=True,
    )
    output = (proc.stdout + proc.stderr).strip()
    print("\n".join(output.splitlines()[-25:]))
    check("unit test suite", proc.returncode == 0, output[-400:])


def wait_for_api():
    step("等待 API 健康")
    deadline = time.time() + 90
    while time.time() < deadline:
        try:
            status, raw, _ = req("GET", "/healthz")
            if status == 200 and as_json(raw).get("ok"):
                check("health response", True)
                return
        except Exception:
            pass
        time.sleep(1)
    check("health response", False, "API did not become healthy in time")
    raise SystemExit(1)


def phase_pre():
    e1, e2, e3, e4, e5 = (eid(i) for i in range(1, 6))
    e6, e7, e8, e9 = (eid(i) for i in range(6, 10))
    recs = [
        {"ts": "2026-10-06T01:00:00Z", "lat": 31.230416, "lon": 121.473701, "depth_m": 42.51, "vessel_id": "HAICE-01"},
        {"ts": "2026-10-06T01:05:00Z", "lat": 31.231102, "lon": 121.480233, "depth_m": 43.04, "vessel_id": "HAICE-01"},
    ]
    rules_r1 = {"rules": [
        {"field": "depth_m", "action": "redact"},
        {"field": "vessel_id", "action": "hash", "length": 10},
    ]}
    rules_r2 = {"rules": [{"field": "lat", "action": "redact"}]}
    rules_r3 = {"rules": [
        {"field": "depth_m", "action": "redact"},
        {"field": "vessel_id", "action": "hash", "length": 10},
    ]}

    step("页面通过真实 API 轮询（页面可加载且引用 API）")
    status, raw, headers = req("GET", "/")
    check("operator page served", status == 200 and "text/html" in headers.get("Content-Type", ""))
    check("page polls real API", b"/api/exports" in raw and b"fetch(" in raw)

    step("设置规则 R1（遮蔽 depth_m / 散列 vessel_id）")
    status, raw, _ = req("PUT", "/api/rules", rules_r1)
    check("put rules R1", status == 200, "HTTP %s %s" % (status, raw[:200]))
    d1 = as_json(raw)["digest"]

    step("提交导出 E1 → 201 冻结裁决")
    status, raw, _ = req("POST", "/api/exports", {"export_id": e1, "records": recs})
    check("submit E1 -> 201", status == 201, "HTTP %s %s" % (status, raw[:300]))
    receipt1 = as_json(raw)
    check("E1 froze rules R1", receipt1.get("rules_digest") == d1)

    step("业务等价重传 E1（键序/空白不同）→ 200 首次回执")
    reordered = [dict(reversed(list(r.items()))) for r in recs]
    status, raw, _ = req("POST", "/api/exports", {"export_id": e1, "records": reordered})
    replay = as_json(raw)
    check("replay E1 -> 200", status == 200, "HTTP %s %s" % (status, raw[:300]))
    check("replay returns first receipt",
          replay.get("receipt_id") == receipt1["receipt_id"]
          and replay.get("received_at") == receipt1["received_at"]
          and replay.get("replay") is True)

    step("等待 E1 发布并校验工件")
    detail = wait_for_stage(e1, "PUBLISHED", 90)
    check("E1 published", detail is not None and detail["stage"] == "PUBLISHED",
          "last=%s" % (detail and detail.get("stage")))
    a1 = detail["artifact_digest"] if detail else None
    status, raw, _ = download(e1)
    check("download E1 -> 200", status == 200, "HTTP %s" % status)
    if status == 200:
        check("E1 digest matches download", hashlib.sha256(raw).hexdigest() == a1)
        doc = as_json(raw)
        check("E1 masked per R1 (depth redacted, vessel hashed, lat kept)",
              doc["records"][0]["depth_m"] == "***"
              and doc["records"][0]["vessel_id"] != "HAICE-01"
              and doc["records"][0]["lat"] == 31.230416)
        check("E1 artifact carries frozen digests",
              doc["rules_digest"] == d1 and doc["input_digest"] == receipt1["input_digest"])
    check("exactly one published artifact file for E1", len(published_files(e1)) == 1,
          str(published_files(e1)))

    step("值班员改规则 → R2（遮蔽 lat）")
    status, raw, _ = req("PUT", "/api/rules", rules_r2)
    check("put rules R2", status == 200)
    d2 = as_json(raw)["digest"]
    check("R2 digest differs", d2 != d1)

    step("规则变动后重传 E1（同记录）→ 409 冲突（规则快照不同）")
    status, raw, _ = req("POST", "/api/exports", {"export_id": e1, "records": recs})
    conflict = as_json(raw)
    check("replay under changed rules -> 409", status == 409, "HTTP %s" % status)
    check("conflict preserves original evidence",
          conflict.get("existing", {}).get("rules_digest") == d1
          and conflict.get("existing", {}).get("receipt_id") == receipt1["receipt_id"])

    step("不同记录重传 E1 → 409 冲突（记录不同）")
    changed = [dict(r, depth_m=99.9) for r in recs]
    status, raw, _ = req("POST", "/api/exports", {"export_id": e1, "records": changed})
    check("different records -> 409", status == 409, "HTTP %s" % status)

    detail = wait_for_stage(e1, "PUBLISHED", 5)
    check("E1 evidence untouched after conflicts",
          detail is not None and detail["artifact_digest"] == a1 and detail["stage"] == "PUBLISHED")

    step("规则改动后，E1 仍按冻结快照导出；新导出 E2 使用 R2")
    status, raw, _ = req("POST", "/api/exports", {"export_id": e2, "records": recs})
    check("submit E2 -> 201", status == 201)
    check("E2 froze rules R2", as_json(raw).get("rules_digest") == d2)
    detail2 = wait_for_stage(e2, "PUBLISHED", 90)
    check("E2 published", detail2 is not None and detail2["stage"] == "PUBLISHED")
    status, raw, _ = download(e2)
    if status == 200:
        doc = as_json(raw)
        check("E2 masked per R2 (lat redacted, depth kept)",
              doc["records"][0]["lat"] == "***" and doc["records"][0]["depth_m"] == 42.51)
    else:
        check("download E2 -> 200", False, "HTTP %s" % status)
    status, raw, _ = download(e1)
    check("E1 re-download unchanged after rule change",
          status == 200 and hashlib.sha256(raw).hexdigest() == a1)
    if status == 200:
        doc = as_json(raw)
        check("E1 still follows frozen R1 (depth redacted, lat kept)",
              doc["records"][0]["depth_m"] == "***" and doc["records"][0]["lat"] == 31.230416)

    step("崩溃恢复 A：暂存完整工件后进程退出 → 收敛到同一完整工件")
    status, raw, _ = req("POST", "/api/test/fault", {"export_id": e3, "mode": "crash_after_staged"})
    check("arm fault crash_after_staged", status == 202, "HTTP %s %s" % (status, raw[:200]))
    status, raw, _ = req("POST", "/api/exports", {"export_id": e3, "records": recs})
    check("submit E3 -> 201", status == 201)
    detail3 = wait_for_stage(e3, "PUBLISHED", 150)
    check("E3 published after crash+recovery", detail3 is not None and detail3["stage"] == "PUBLISHED",
          "last=%s" % (detail3 and detail3.get("stage")))
    if detail3:
        events = detail3.get("events", [])
        recovered = any("recovery" in (e.get("detail") or "") or "recover" in e.get("event", "")
                        for e in events)
        check("E3 journal shows recovery convergence", recovered,
              "events=%s" % [e["event"] for e in events])
        status, raw, _ = download(e3)
        check("E3 download verified", status == 200
              and hashlib.sha256(raw).hexdigest() == detail3["artifact_digest"])
        check("exactly one published artifact file for E3", len(published_files(e3)) == 1)
        check("no temp leftovers for E3", tmp_files(e3) == [], str(tmp_files(e3)))

    step("崩溃恢复 B：临时工件写一半进程退出 → 清理残缺工件并重处理")
    status, raw, _ = req("POST", "/api/test/fault", {"export_id": e4, "mode": "crash_partial_write"})
    check("arm fault crash_partial_write", status == 202, "HTTP %s" % status)
    status, raw, _ = req("POST", "/api/exports", {"export_id": e4, "records": recs})
    check("submit E4 -> 201", status == 201)
    detail4 = wait_for_stage(e4, "PUBLISHED", 150)
    check("E4 published after cleanup+requeue", detail4 is not None and detail4["stage"] == "PUBLISHED",
          "last=%s" % (detail4 and detail4.get("stage")))
    if detail4:
        events = [e["event"] for e in detail4.get("events", [])]
        check("E4 journal shows cleanup/requeue",
              "recovery_cleanup" in events or "requeued" in events, "events=%s" % events)
        check("E4 needed a retry", detail4["attempts"] >= 1)
        check("no temp leftovers for E4", tmp_files(e4) == [], str(tmp_files(e4)))
        status, raw, _ = download(e4)
        check("E4 download verified", status == 200
              and hashlib.sha256(raw).hexdigest() == detail4["artifact_digest"])

    step("下载接口不暴露未核验内容（崩溃窗口内只能 409，不能 200）")
    req("POST", "/api/test/fault", {"export_id": e5, "mode": "crash_partial_write"})
    status, raw, _ = req("POST", "/api/exports", {"export_id": e5, "records": recs})
    check("submit E5 -> 201", status == 201)
    saw_409 = False
    exposed = False
    deadline = time.time() + 150
    while time.time() < deadline:
        code, body, _ = download(e5)
        d = wait_for_stage(e5, "PUBLISHED", 1)  # stage probe after the download
        if code == 200:
            # 200 is legitimate only when the export is PUBLISHED (stage never
            # regresses, so a PUBLISHED probe after the 200 is conclusive).
            if d and d["stage"] == "PUBLISHED":
                break
            exposed = True
            break
        if code == 409:
            saw_409 = True
        time.sleep(0.4)
    check("unpublished content never served", not exposed)
    check("download refused while unverified (409 observed)", saw_409)

    step("发布后的业务等价重传：仍返回首次回执且不产生第二个工件")
    status, raw, _ = req("POST", "/api/exports", {"export_id": e2, "records": reordered})
    replay2 = as_json(raw)
    check("replay E2 -> 200 first receipt", status == 200 and replay2.get("replay") is True)
    detail2b = wait_for_stage(e2, "PUBLISHED", 5)
    check("E2 artifact digest unchanged by replay",
          detail2b is not None and detail2b["artifact_digest"] == detail2["artifact_digest"])
    check("exactly one published artifact file for E2", len(published_files(e2)) == 1)

    # ------------------------------------------------- two distinct export ids
    step("设置规则 R3，提交两份不同标识、业务等价的导出（E6 先发布，再提交 E7）")
    status, raw, _ = req("PUT", "/api/rules", rules_r3)
    check("put rules R3", status == 200, "HTTP %s" % status)
    d3 = as_json(raw)["digest"]

    status, raw, _ = req("POST", "/api/exports", {"export_id": e6, "records": recs})
    check("submit E6 -> 201", status == 201, "HTTP %s %s" % (status, raw[:200]))
    receipt6 = as_json(raw)
    check("E6 froze rules R3", receipt6.get("rules_digest") == d3)
    detail6 = wait_for_stage(e6, "PUBLISHED", 90)
    check("E6 published", detail6 is not None and detail6["stage"] == "PUBLISHED")
    check_own_download(e6, detail6, "E6")

    # keep the masking rules unchanged; business-equivalent payload only
    reordered_r3 = [dict(reversed(list(r.items()))) for r in recs]
    status, raw, _ = req("POST", "/api/exports", {"export_id": e7, "records": reordered_r3})
    check("submit E7 -> 201 (independent id, not a replay)", status == 201,
          "HTTP %s %s" % (status, raw[:200]))
    receipt7 = as_json(raw)
    check("E7 froze the same R3 snapshot", receipt7.get("rules_digest") == d3)
    check("E7 got its own first receipt/freeze",
          receipt7.get("receipt_id") != receipt6.get("receipt_id")
          and receipt7.get("received_at") != receipt6.get("received_at"))
    detail7 = wait_for_stage(e7, "PUBLISHED", 90)
    check("E7 published", detail7 is not None and detail7["stage"] == "PUBLISHED")
    doc7 = check_own_download(e7, detail7, "E7")

    step("两份导出工件彼此独立（文件、摘要、下载内容均属于自身冻结信息）")
    check("E6/E7 detail artifact summaries differ",
          detail6["artifact_digest"] != detail7["artifact_digest"],
          "%s vs %s" % (detail6["artifact_digest"], detail7["artifact_digest"]))
    check("separate published files for E6 and E7",
          len(published_files(e6)) == 1 and len(published_files(e7)) == 1,
          "%s %s" % (published_files(e6), published_files(e7)))
    p6 = os.path.join(DATA_DIR, "artifacts", "published", e6 + ".json")
    p7 = os.path.join(DATA_DIR, "artifacts", "published", e7 + ".json")
    with open(p6, "rb") as fh6, open(p7, "rb") as fh7:
        b6, b7 = fh6.read(), fh7.read()
    check("published file bytes differ and embed own id",
          hashlib.sha256(b6).hexdigest() == detail6["artifact_digest"]
          and hashlib.sha256(b7).hexdigest() == detail7["artifact_digest"]
          and as_json(b6)["export_id"] == e6 and as_json(b7)["export_id"] == e7)
    check("E7 masked content is business-equivalent to E6",
          doc7 is not None and doc7["records"][0]["depth_m"] == "***"
          and doc7["records"][0]["lat"] == 31.230416)

    # --------------------------------------- already-affected (legacy aliases)
    step("受影响导出 E8（旧版别名）：下载接口不得暴露 E6 内容，并安全收敛为自身工件")
    status, _, _ = req("POST", "/api/test/fault",
                       {"export_id": e8, "mode": "legacy_alias", "target_export_id": e6})
    check("arm fault legacy_alias E8->E6", status == 202, "HTTP %s" % status)
    status, raw, _ = req("POST", "/api/exports", {"export_id": e8, "records": recs})
    check("submit E8 -> 201", status == 201)
    detail8 = wait_for_stage(e8, "PUBLISHED", 90)
    check("E8 shows PUBLISHED (affected aliased state)",
          detail8 is not None and detail8["stage"] == "PUBLISHED")
    check("E8 was recorded against E6's artifact summary (the bug)",
          detail8 is not None and detail8["artifact_digest"] == detail6["artifact_digest"])
    # The download must never return E6's bytes; the server converges E8 to its
    # own artifact and serves that instead.
    _, doc8 = wait_own_download(e8, 60, "E8")
    check("E8 download never exposes E6 identity",
          doc8 is not None and doc8.get("export_id") == e8
          and doc8.get("received_at") != detail6["received_at"])
    detail8b = get_detail(e8)
    check("E8 converged summary now differs from E6 and has its own file",
          detail8b["artifact_digest"] != detail6["artifact_digest"]
          and len(published_files(e8)) == 1)
    events8 = [e["event"] for e in (detail8b or {}).get("events", [])]
    check("E8 convergence recorded as evidence", "artifact_converged" in events8, str(events8))

    step("受影响导出 E9：重启前保持别名已发布状态（不触发下载），留待重启收敛")
    status, _, _ = req("POST", "/api/test/fault",
                       {"export_id": e9, "mode": "legacy_alias", "target_export_id": e6})
    check("arm fault legacy_alias E9->E6", status == 202, "HTTP %s" % status)
    status, raw, _ = req("POST", "/api/exports", {"export_id": e9, "records": recs})
    check("submit E9 -> 201", status == 201)
    detail9 = wait_for_stage(e9, "PUBLISHED", 90)
    check("E9 shows PUBLISHED while aliased to E6",
          detail9 is not None and detail9["stage"] == "PUBLISHED"
          and detail9["artifact_digest"] == detail6["artifact_digest"],
          "digest=%s" % (detail9 and detail9.get("artifact_digest")))
    check("E9 has no own published file before restart", not os.path.exists(
        os.path.join(DATA_DIR, "artifacts", "published", e9 + ".json")))

    step("终态检查：无残缺临时工件残留")
    leftovers = []
    for i in range(1, 10):
        leftovers.extend(tmp_files(eid(i)))
    check("no temp artifacts left behind", leftovers == [], str(leftovers))


def phase_post():
    """After app + workers restart: every result must still be independently
    correct, including E9 (left aliased pre-restart) converged at startup."""
    ids = [eid(i) for i in range(1, 10)]
    step("重启后：全部导出仍为 PUBLISHED，且各自下载自身冻结工件")
    status, raw, _ = req("GET", "/api/exports")
    check("list exports after restart", status == 200, "HTTP %s" % status)
    rows = {x["export_id"]: x for x in as_json(raw)["exports"]} if status == 200 else {}
    for export_id in ids:
        check("%s present and PUBLISHED after restart" % export_id,
              rows.get(export_id, {}).get("stage") == "PUBLISHED",
              "row=%s" % rows.get(export_id))

    details = {}
    for export_id in ids:
        detail = wait_for_stage(export_id, "PUBLISHED", 30)
        details[export_id] = detail
        check_own_download(export_id, detail, "%s post-restart" % export_id)
        check("%s exactly one own published file after restart" % export_id,
              len(published_files(export_id)) == 1, str(published_files(export_id)))

    step("重启后：E6/E7 工件依旧相互独立")
    d6, d7 = details[eid(6)], details[eid(7)]
    check("E6/E7 summaries remain distinct",
          d6["artifact_digest"] != d7["artifact_digest"]
          and d6["receipt_id"] != d7["receipt_id"]
          and d6["received_at"] != d7["received_at"])

    step("重启后：E9 已在启动阶段安全收敛为自身可校验工件（无阶段倒退）")
    d9 = details[eid(9)]
    d6 = details[eid(6)]
    check("E9 converged away from E6 summary",
          d9["artifact_digest"] != d6["artifact_digest"])
    check("E9 terminal stage preserved", d9["stage"] == "PUBLISHED" and d9["published_at"] is not None)
    p9 = os.path.join(DATA_DIR, "artifacts", "published", eid(9) + ".json")
    check("E9 own published file exists and verifies", os.path.exists(p9))
    if os.path.exists(p9):
        with open(p9, "rb") as fh:
            check("E9 file hash equals its summary",
                  hashlib.sha256(fh.read()).hexdigest() == d9["artifact_digest"])
    full9 = get_detail(eid(9))
    events9 = full9.get("events", [])
    check("E9 journal records the convergence (startup/download)",
          any(e.get("event") == "artifact_converged" for e in events9),
          "events=%s" % [e["event"] for e in events9])

    step("重启后：E8 收敛结果稳定；E6 原始工件从未被改动")
    d8 = details[eid(8)]
    check("E8 stays on its own converged artifact",
          d8["artifact_digest"] != d6["artifact_digest"])
    check_own_download(eid(8), d8, "E8 stable")
    p6 = os.path.join(DATA_DIR, "artifacts", "published", eid(6) + ".json")
    if os.path.exists(p6):
        with open(p6, "rb") as fh:
            b6 = fh.read()
        check("E6 original file untouched (hash + own identity preserved)",
              hashlib.sha256(b6).hexdigest() == d6["artifact_digest"]
              and as_json(b6)["export_id"] == eid(6))
    else:
        check("E6 original file untouched (file present)", False)

    step("重启后回归：规则冻结差异（E1 用 R1、E2 用 R2）与恢复证据仍成立")
    _, b1, _ = download(eid(1))
    _, b2, _ = download(eid(2))
    c1, c2 = as_json(b1), as_json(b2)
    check("E1 still redacts depth/keeps lat; E2 redacts lat after restart",
          c1["records"][0]["depth_m"] == "***" and c1["records"][0]["lat"] == 31.230416
          and c2["records"][0]["lat"] == "***")
    check("E1/E2 frozen rules snapshots differ",
          c1["rules_digest"] != c2["rules_digest"])
    full3 = get_detail(eid(3)) or {}
    check("E3 recovery evidence retained",
          any("recovery" in (e.get("detail") or "") or "recover" in e.get("event", "")
              for e in full3.get("events", [])))
    check("E4 retry counter retained", details[eid(4)]["attempts"] >= 1)


def main():
    print("verify: one-shot acceptance run %s phase=%s against %s" % (RUN, PHASE, API), flush=True)
    if PHASE == "pre":
        build_checks()
        unit_tests()
    elif PHASE != "post":
        print("unknown VERIFY_PHASE=%r (expected pre|post)" % PHASE, flush=True)
        return 2
    wait_for_api()
    if PHASE == "pre":
        phase_pre()
    else:
        phase_post()
    print("\n==============================================")
    if FAILURES:
        print("verify[%s]: FAILED (%d): %s" % (PHASE, len(FAILURES), ", ".join(FAILURES)), flush=True)
        return 1
    print("verify[%s]: ALL CHECKS PASSED" % PHASE, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
