# 实体键桥

> 遵守 `ops/底座生成前必读_效率与技术栈.md`。FastAPI + Vue 3 + SQLite，交付须 docker compose 一键起；库用 SQLite，不挂外置 Postgres。  
> **禁止同构换皮**：本仓主链路、表结构、接口与页面均独立，不得与其它仓共用同一套 `/api/items` 壳。

## 骨架一句话

实体键映射生效后入站先重映再进正式区，冲突进队列，映射版本与正式区对账

## 基本信息

| 项 | 值 |
|----|-----|
| 产品名（首页） | 实体键桥 |
| 目录 | `07-keyremap` |
| 领域建议 | 数仓与数据湖 |
| 接口 | http://127.0.0.1:10939 |
| 库 | `data/app.db` |
| 账号 | admin / 123456 |

## 为何能出多题

一对多/多对一、生效窗、旧键只读、重映重放

## 避开

别做成 Feed 冲突归并；正式区不是 staging 换皮

## 互斥切口候选

- 冲突拒收整批 vs 进冲突区后重放
- 废弃旧键只读保留 vs 重写正式键
- 生效窗原子切换 vs 双窗按事件时裁

## 启动（一键）

`powershell
cd projects/07-keyremap
docker compose up -d --build
`

浏览器：http://127.0.0.1:10939/  
健康检查：GET http://127.0.0.1:10939/api/health

本机开发（可选）：

`powershell
python -m pip install -r requirements.txt
python -m uvicorn app:app --app-dir backend --host 127.0.0.1 --port 10604
python -m pytest -q
`

## 批重映与 cursor（甲路径：强制游标）

跳号修复口径见 `ops/跳号修复说明.md`（2026-11-20 锁定，甲乙互斥，本次取甲）。续跑必须携带上次 cursor，缺/错整笔拒绝（400）、正式零增；同 batch 两路推进须占批锁，后到只记争用（409）；完成旗只认应收键数。

| 接口 | 方法 | 说明 |
|----|----|----|
| `/api/batch/seed` | POST | 入夹具批次（batch_id、limit、items） |
| `/api/batch/remap` | POST | 单窗推进，body 须带 `resume_cursor`（首批 0） |
| `/api/batch/cursor` | GET | cursor 账末值与历史 |
| `/api/batch/receipt` | GET | 批回执 |
| `/api/batch/proof` | GET | 互证页：五处同态判定（cursor 账/正式实体/skip 审计/回执/互证页） |

本地夹具：`fixtures/inbound_batch.json`（SQLite，不挂外置库）。红测：`tests/test_cursor_skip.py`。
