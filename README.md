# 航迹导出服务（track-export）

海洋测绘队向协作方导出航迹前的遮蔽导出服务。值班员在页面上编辑字段遮蔽规则、
提交带稳定导出标识的少量 JSON 记录，并轮询查看处理阶段、冻结规则摘要与已发布
工件摘要。纯 Python 3.11 标准库实现（无第三方依赖）。

## 架构

```
┌────────────┐   HTTP    ┌─────────────────────────────┐
│  值班员页面 │ ───────▶ │ app (python -m app.server)   │
│ (真实API轮询)│ ◀─────── │  /api/rules /api/exports ... │
└────────────┘           └──────────┬──────────────────┘
                                    │ SQLite (WAL) + 工件目录（共享卷）
                         ┌──────────┴──────────────────┐
                         │ worker ×2 (app.worker)       │
                         │ 租约 → 暂存 → 核验 → 原子发布 │
                         │ 崩溃恢复：收敛 / 清理重排队   │
                         └─────────────────────────────┘
```

- **同一持久化裁决**：`POST /api/exports` 在单个 `IMMEDIATE` 事务中读取当前规则并
  冻结「规范化输入 + 规则快照 + 决策摘要 + 日志」。此后改动当前规则不影响该标识的导出。
- **标识独立**：即使业务记录与冻结规则快照完全相同，**不同的稳定导出标识也是彼此独立
  的裁决**——各有自己的首次回执、冻结时间、发布文件与工件摘要。下载内容内嵌该标识自身的
  冻结字段（`export_id` / `received_at` / `input_digest` / `rules_digest`），下载接口在
  哈希核验之外再做身份绑定校验，绝不会投递另一标识的工件。
- **幂等与冲突**：同一标识的业务等价重传（键序/空白/整型浮点/Unicode 组合差异）返回
  首次回执（HTTP 200，`replay: true`），不产生第二个工件；记录或规则快照不同则返回
  HTTP 409 并保留原有证据（原裁决行不被修改，冲突尝试记入日志）。
- **历史别名收敛**：旧版本曾把「不同标识、等价决策」复用到首个已发布工件。这类已处于
  `PUBLISHED` 的别名行在进程启动（app/worker）及下载时被安全收敛：由各自冻结记录重算并
  发布自身可校验文件、订正指针、保留旧证据（旧工件行置 `aborted` 并记
  `artifact_converged` 日志）。阶段始终停留在终态，绝不倒退。
- **租约**：worker 必须持有有效租约（带 fencing token）才能处理；发布前复查租约。
  租约过期后可被其他 worker 接管，fencing 递增使旧持有者失效。
- **工件管线**：临时文件（fsync）→ 摘要登记 → 重读核验（含身份绑定）→ `link(2)` 原子
  发布（不可覆盖）→ 事务内标记 `PUBLISHED`。下载接口只投递摘要与身份都核验通过的工件。
- **崩溃恢复**：worker 启动扫描未完成导出——暂存工件完整（摘要与日志、
  确定性重算一致）则**收敛**到同一工件发布；否则**清理**残缺工件并重排队；孤儿临时
  文件定期清扫。`PUBLISHED` 为终态，任何路径都不能使其倒退。
- **唯一发布**：租约串行化 + `artifacts` 表部分唯一索引（每导出仅一条 published）
  + 不可覆盖链接 + 阶段 CAS，四重保证两个 worker 并行时同一导出只发布一次。

## 快速开始（Docker Compose）

```sh
./scripts/verify.sh        # 构建、启动 app+2×worker、运行一次性 verify、以退出码报告
```

或手动：

```sh
APP_PORT=8080 docker compose up -d --build app worker   # 可配置宿主端口
curl localhost:8080/healthz                              # 健康响应
RUN=$(python3 -c 'import uuid;print(uuid.uuid4().hex[:6])')
docker compose run --rm -e VERIFY_PHASE=pre  -e VERIFY_RUN="$RUN" verify   # 重启前
docker compose restart app worker                        # 重启服务与后台进程
docker compose run --rm -e VERIFY_PHASE=post -e VERIFY_RUN="$RUN" verify   # 重启后
docker compose down -v                                   # 重置全部状态
```

页面：<http://localhost:8080/>（每 2 秒轮询真实 API）。

## verify 验收内容（分两阶段，中间真实重启服务）

`scripts/verify.sh` 与 `scripts/smoke-local.sh` 先运行 **pre** 阶段，随后重启
app 与两个 worker，再对同一批稳定导出标识运行 **post** 阶段（两阶段共享
`VERIFY_RUN`）。

1. **构建检查**：`python -m compileall app verify tests`
2. **代码测试**：`python -m unittest discover -s tests`（53 个用例：规范化、遮蔽、
   裁决/幂等/冲突、阶段单调、租约 fencing、恢复收敛/清理、双 worker 竞态、
   不同标识工件独立、下载身份绑定、历史别名安全收敛）
3. **pre 阶段 API/HTTP 冒烟（真实接口）**：
   - 规则改动后，已冻结导出仍按冻结快照导出（E1 用 R1、E2 用 R2，互不影响）
   - 崩溃恢复：暂存完整后崩溃 → 收敛到同一完整工件；写一半崩溃 → 清理残缺并重处理
   - 业务等价重传 → 首次回执且无第二个工件；记录/规则快照不同 → 409 且证据保留
   - 下载接口在崩溃窗口内只返回 409，绝不暴露未核验内容
   - **两份不同标识、业务等价的提交（E6/E7，遮蔽规则保持不变）**：均发布；各自下载
     内容与工件摘要匹配其自身冻结信息（标识、冻结时间、输入/规则摘要）；发布文件彼此
     独立、摘要不同
   - **已受影响导出**：E8 在下载时即时收敛、E9 留待重启——均不得暴露他人内容
4. **重启（app + workers）后的 post 阶段**：
   - 全部导出仍为 `PUBLISHED`，各自只下载自身冻结工件，各自独立文件
   - E9 已在启动阶段安全收敛为自身可校验工件（终态不倒退、证据保留）；E6 原始工件
     从未被改动；E8 收敛结果稳定
   - 回归规则冻结差异、崩溃恢复证据与重试计数

## 本地开发（无 Docker）

```sh
python3 -m unittest discover -s tests -t .     # 单元测试
./scripts/smoke-local.sh                        # 完整本地冒烟（server + 2 worker + verify）
```

## API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/healthz` | 健康响应 |
| GET/PUT | `/api/rules` | 查看 / 替换当前遮蔽规则（版本+摘要） |
| POST | `/api/exports` | 提交 `{export_id, records}` → 201 / 200(replay) / 409(conflict) |
| GET | `/api/exports` | 列表：阶段、冻结规则摘要、输入摘要、工件摘要 |
| GET | `/api/exports/{id}` | 详情 + 处理日志 + 当前租约 |
| GET | `/api/exports/{id}/artifact` | 下载已发布工件（摘要核验，否则 409/410/500） |
| POST | `/api/test/fault` | 故障注入（仅 `TEST_HOOKS=1`）：`crash_partial_write` / `crash_after_staged`；或 `mode=legacy_alias, target_export_id=...` 复现旧版跨标识别名以验证安全收敛 |

## 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `APP_PORT` | `8080` | Compose 宿主端口（`APP_PORT=9090 docker compose up`） |
| `PORT` | `8080` | 容器内监听端口 |
| `DATA_DIR` | `./data` | SQLite 与工件根目录 |
| `LEASE_TTL_SECONDS` | `10` | 租约时长（崩溃接管延迟的上界） |
| `POLL_INTERVAL_SECONDS` | `0.5` | worker 轮询间隔 |
| `TEST_HOOKS` | 关 | 置 `1` 开启故障注入端点（验收用，生产应关闭） |

## 遮蔽规则示例

```json
{"rules": [
  {"field": "depth_m",   "action": "redact", "replacement": "***"},
  {"field": "lat",       "action": "round",  "precision": 2},
  {"field": "vessel_id", "action": "hash",   "length": 12},
  {"field": "note",      "action": "drop"}
]}
```
