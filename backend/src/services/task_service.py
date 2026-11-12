"""采集任务与调度模拟服务（算法 B 的落库封装）。

配额冻结与运行护栏：
- 创建任务时把站点配额（qps/crawl-delay/并发/优先级/robots）冻结进 quota_snapshot，
  调度模拟只读冻结快照；后续站点修改只影响新任务，快照永不被回写。
- 已确认结果（finished）的任务重跑 → 409，调度结果与抓取记录都不可覆盖。
- 同站点任务互斥：按域 advisory lock 串行化运行尝试，running 冲突 → 409 并记录
  last_block_reason，调度看板可展示阻止原因。
- enabled 是安全开关（kill-switch），始终按当前值判定，不进快照。
"""
from __future__ import annotations

import json

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.constants import error_codes as codes
from src.constants.enums import FetchOrigin, TaskStatus, UrlStatus
from src.constants.limits import DEFAULT_SIM_MAX_SEQUENCE, DEFAULT_SIM_SEED, DEFAULT_SIM_WINDOW_S
from src.constants.roles import ROLE_ADMIN, ROLE_ENGINEER
from src.models.crawl_task import CrawlTask
from src.models.fetch_record import FetchRecord
from src.models.frontier_url import FrontierUrl
from src.models.seed_site import SeedSite
from src.models.user import User
from src.services import audit_service, scheduler
from src.utils.errors import AppError, not_found
from src.utils.timeutil import to_iso, utcnow

# 创建时冻结的站点字段（调度结果的全部站点侧输入；enabled 除外——安全开关按当前值）
FROZEN_QUOTA_FIELDS: tuple[str, ...] = (
    "qps_quota",
    "crawl_delay_s",
    "max_concurrency",
    "priority",
    "robots_status",
    "robots_rules",
)


def _parse_json(raw: str | None, fallback):
    if not raw:
        return fallback
    try:
        return json.loads(raw)
    except ValueError:
        return fallback


def build_quota_snapshot(sites: list[SeedSite]) -> dict:
    """创建任务时冻结的站点配额快照（按域名字典序，可复现）。"""
    snapshot: dict[str, dict] = {}
    for site in sorted(sites, key=lambda item: item.domain):
        snapshot[site.domain] = {
            "qps_quota": site.qps_quota,
            "crawl_delay_s": site.crawl_delay_s,
            "max_concurrency": site.max_concurrency,
            "priority": site.priority,
            "robots_status": site.robots_status,
            "robots_rules": _parse_json(site.robots_rules, {"disallow": [], "allow": []}),
        }
    return snapshot


def configs_from_snapshot(
    snapshot: dict, domains: list[str], sites_by_domain: dict[str, SeedSite]
) -> list[scheduler.SiteConfig]:
    """用冻结快照构建调度器站点配置；快照缺失的字段（旧数据）回退当前站点值。"""
    configs: list[scheduler.SiteConfig] = []
    for domain in sorted(domains):
        site = sites_by_domain.get(domain)
        if site is None:
            continue
        frozen = snapshot.get(domain)
        frozen = frozen if isinstance(frozen, dict) else {}
        rules = frozen.get("robots_rules")
        if rules is None:
            rules_text = site.robots_rules
        elif isinstance(rules, dict):
            rules_text = json.dumps(rules, ensure_ascii=False)
        else:
            rules_text = str(rules)
        configs.append(
            scheduler.SiteConfig(
                domain=domain,
                name=site.name,
                qps_quota=float(frozen.get("qps_quota", site.qps_quota)),
                crawl_delay_s=float(frozen.get("crawl_delay_s", site.crawl_delay_s)),
                max_concurrency=int(frozen.get("max_concurrency", site.max_concurrency)),
                priority=int(frozen.get("priority", site.priority)),
                robots_status=str(frozen.get("robots_status", site.robots_status)),
                robots_rules=rules_text,
                enabled=site.enabled,
            )
        )
    return configs


def _quota_diff(snapshot: dict, domains: list[str], sites_by_domain: dict[str, SeedSite]) -> list[dict]:
    """冻结配额 vs 当前站点配置的逐项差异（看板「配置漂移」展示用）。"""
    diffs: list[dict] = []
    for domain in sorted(domains):
        site = sites_by_domain.get(domain)
        if site is None:
            diffs.append({"domain": domain, "field": "site", "frozen": "已冻结", "current": "站点已删除"})
            continue
        frozen = snapshot.get(domain)
        if not isinstance(frozen, dict) or not frozen:
            continue
        for field in FROZEN_QUOTA_FIELDS:
            if field not in frozen:
                continue  # 旧快照未冻结该字段，不参与漂移判定
            frozen_value = frozen[field]
            current = getattr(site, field)
            if field == "robots_rules":
                current = _parse_json(site.robots_rules, {})
            elif field in ("qps_quota", "crawl_delay_s"):
                frozen_value = float(frozen_value)
                current = float(current)
            elif field in ("max_concurrency", "priority"):
                frozen_value = int(frozen_value)
                current = int(current)
            if frozen_value != current:
                diffs.append(
                    {"domain": domain, "field": field, "frozen": frozen_value, "current": current}
                )
    return diffs


def _sites_by_domain(db: Session, domains: list[str] | None = None) -> dict[str, SeedSite]:
    query = db.query(SeedSite)
    if domains:
        query = query.filter(SeedSite.domain.in_(domains))
    return {site.domain: site for site in query.all()}


def serialize_task(
    task: CrawlTask,
    *,
    include_result: bool = False,
    sites_by_domain: dict[str, SeedSite] | None = None,
) -> dict:
    result = _parse_json(task.scheduler_result, None)
    snapshot = _parse_json(task.quota_snapshot, {})
    domains = _parse_json(task.site_ids, [])
    payload = {
        "id": str(task.id),
        "name": task.name,
        "site_domains": domains,
        "concurrency": task.concurrency,
        "quota_snapshot": snapshot,
        "quota_frozen": bool(snapshot),
        "frozen_at": to_iso(task.created_at),
        "quota_diff": (
            _quota_diff(snapshot, domains, sites_by_domain) if sites_by_domain is not None else []
        ),
        "block_reason": task.last_block_reason or "",
        "blocked_at": to_iso(task.last_blocked_at),
        "status": task.status,
        "seed": task.seed,
        "window_s": task.window_s,
        "max_sequence": task.max_sequence,
        "scheduled_count": task.scheduled_count,
        "created_by": task.created_by,
        "started_at": to_iso(task.started_at),
        "finished_at": to_iso(task.finished_at),
        "created_at": to_iso(task.created_at),
        "has_result": result is not None,
        "result_summary": (
            {
                "sequence_length": result.get("sequence_length"),
                "rate_limited_count": result.get("rate_limited_count"),
                "starvation_count": len(result.get("starvation_boosts", [])),
                "elapsed_s": result.get("elapsed_s"),
                "digest": result.get("digest"),
            }
            if result
            else None
        ),
    }
    if include_result:
        payload["scheduler_result"] = result
    return payload


def list_tasks(db: Session) -> dict:
    rows = db.query(CrawlTask).order_by(CrawlTask.created_at.asc()).all()
    sites_by_domain = _sites_by_domain(db)
    items = [serialize_task(row, sites_by_domain=sites_by_domain) for row in rows]
    summary = {
        "total": len(items),
        "queued": sum(1 for item in items if item["status"] == TaskStatus.QUEUED.value),
        "running": sum(1 for item in items if item["status"] == TaskStatus.RUNNING.value),
        "paused": sum(1 for item in items if item["status"] == TaskStatus.PAUSED.value),
        "finished": sum(1 for item in items if item["status"] == TaskStatus.FINISHED.value),
        "scheduled_total": sum(item["scheduled_count"] for item in items),
    }
    return {"items": items, "summary": summary}


def get_task(db: Session, task_id: str) -> CrawlTask:
    task = db.get(CrawlTask, task_id)
    if task is None:
        raise not_found("CrawlTask", task_id)
    return task


def get_task_detail(db: Session, task_id: str) -> dict:
    task = get_task(db, task_id)
    domains = _parse_json(task.site_ids, [])
    return serialize_task(task, include_result=True, sites_by_domain=_sites_by_domain(db, domains))


def create_task(db: Session, payload: dict, *, actor: User, ip=None, trace_id=None) -> dict:
    if actor.role not in (ROLE_ADMIN, ROLE_ENGINEER):
        raise AppError(codes.FORBIDDEN, "当前角色没有创建任务的权限", f"role={actor.role}")
    name = str(payload.get("name") or "").strip()
    if not name:
        raise AppError(codes.VALIDATION_ERROR, "任务名称不能为空")
    exists = db.query(CrawlTask).filter(CrawlTask.name == name).one_or_none()
    if exists is not None:
        raise AppError(codes.CONFLICT, "任务名称已存在", f"name={name}")
    domains = payload.get("site_domains") or []
    if not isinstance(domains, list) or not domains:
        raise AppError(codes.VALIDATION_ERROR, "至少选择 1 个站点")
    sites = db.query(SeedSite).filter(SeedSite.domain.in_(domains)).all()
    found = {site.domain for site in sites}
    missing = [domain for domain in domains if domain not in found]
    if missing:
        raise AppError(codes.VALIDATION_ERROR, "站点不存在", f"missing={missing}")
    concurrency = int(payload.get("concurrency") or 3)
    if not 1 <= concurrency <= 10:
        raise AppError(codes.VALIDATION_ERROR, "并发需在 1~10 之间")
    window_s = float(payload.get("window_s") or DEFAULT_SIM_WINDOW_S)
    if not 1 <= window_s <= 120:
        raise AppError(codes.VALIDATION_ERROR, "调度窗口需在 1~120 秒之间")
    max_sequence = int(payload.get("max_sequence") or DEFAULT_SIM_MAX_SEQUENCE)
    if not 1 <= max_sequence <= 200:
        raise AppError(codes.VALIDATION_ERROR, "序列上限需在 1~200 之间")
    seed = int(payload.get("seed") or DEFAULT_SIM_SEED)
    # 创建时冻结站点配额：此后站点修改只影响新任务
    quota_snapshot = build_quota_snapshot(sites)
    task = CrawlTask(
        name=name,
        site_ids=json.dumps(sorted(found), ensure_ascii=False),
        concurrency=concurrency,
        quota_snapshot=json.dumps(quota_snapshot, ensure_ascii=False),
        status=TaskStatus.QUEUED.value,
        seed=seed,
        window_s=window_s,
        max_sequence=max_sequence,
        created_by=actor.email,
    )
    db.add(task)
    db.commit()
    db.refresh(task)
    audit_service.record(
        db,
        action="task.create",
        entity_type="CrawlTask",
        entity_id=task.id,
        actor=actor,
        payload={"name": task.name, "sites": domains, "window_s": window_s, "max_sequence": max_sequence},
        ip=ip,
        trace_id=trace_id,
    )
    return serialize_task(task, include_result=True, sites_by_domain=_sites_by_domain(db, domains))


def _queued_urls(db: Session, domains: list[str]) -> list[scheduler.UrlItem]:
    sites = {site.domain: site for site in db.query(SeedSite).filter(SeedSite.domain.in_(domains)).all()}
    rows = (
        db.query(FrontierUrl)
        .filter(
            FrontierUrl.site_id.in_([site.id for site in sites.values()]),
            FrontierUrl.is_duplicate.is_(False),
            FrontierUrl.status == UrlStatus.QUEUED.value,
        )
        .order_by(FrontierUrl.raw_url.asc())
        .all()
    )
    items: list[scheduler.UrlItem] = []
    for row in rows:
        site = next((site for site in sites.values() if site.id == row.site_id), None)
        if site is None:
            continue
        items.append(
            scheduler.UrlItem(
                url_id=str(row.id),
                domain=site.domain,
                raw_url=row.raw_url,
                canonical_url=row.canonical_url,
                depth=row.depth,
                priority_score=float(site.priority),
                wait_seconds=float(row.wait_seconds),
            )
        )
    return items


def _block_run(
    db: Session,
    task: CrawlTask,
    reason: str,
    *,
    actor: User | None = None,
    extra: dict | None = None,
    ip=None,
    trace_id=None,
) -> AppError:
    """运行被阻止：持久化阻止原因（看板展示）+ 审计 + 返回 409。"""
    task.last_block_reason = reason
    task.last_blocked_at = utcnow()
    db.add(task)
    db.commit()
    audit_service.record(
        db,
        action="task.simulate_blocked",
        entity_type="CrawlTask",
        entity_id=task.id,
        actor=actor,
        payload={"reason": reason, **(extra or {})},
        ip=ip,
        trace_id=trace_id,
    )
    return AppError(codes.CONFLICT, reason, extra=extra)


def simulate_task(db: Session, task_id: str, *, actor: User, ip=None, trace_id=None) -> dict:
    if actor.role not in (ROLE_ADMIN, ROLE_ENGINEER):
        raise AppError(codes.FORBIDDEN, "当前角色没有运行调度模拟的权限", f"role={actor.role}")
    task = get_task(db, task_id)
    domains = _parse_json(task.site_ids, [])
    # 同站点运行串行化：按域（字典序，避免死锁）取事务级 advisory lock；
    # 并发的同站点运行在此排队，拿到锁后经 running 检查得到确定性阻止原因。
    for domain in sorted(domains):
        db.execute(text("SELECT pg_advisory_xact_lock(hashtext(:domain))"), {"domain": domain})
    # 行锁防同任务并发重跑；populate_existing 强制用行内最新值覆盖会话缓存，
    # 否则并发事务已提交的 running/finished 状态会被 identity map 旧值掩盖
    task = (
        db.query(CrawlTask)
        .filter(CrawlTask.id == task.id)
        .with_for_update()
        .populate_existing()
        .one()
    )

    if task.status == TaskStatus.FINISHED.value:
        raise _block_run(
            db,
            task,
            "任务结果已确认，不能重复运行（冻结配额、调度结果与抓取记录保持不变）",
            actor=actor,
            extra={"reason": "task_already_confirmed", "scheduled_count": task.scheduled_count},
            ip=ip,
            trace_id=trace_id,
        )
    if task.status == TaskStatus.RUNNING.value:
        raise _block_run(
            db,
            task,
            "任务正在运行中，不能并发重跑",
            actor=actor,
            extra={"reason": "task_already_running"},
            ip=ip,
            trace_id=trace_id,
        )

    running = (
        db.query(CrawlTask)
        .filter(CrawlTask.status == TaskStatus.RUNNING.value, CrawlTask.id != task.id)
        .all()
    )
    domain_set = set(domains)
    conflicts = [row for row in running if domain_set & set(_parse_json(row.site_ids, []))]
    if conflicts:
        blocked_by = [
            {
                "task_id": str(row.id),
                "name": row.name,
                "domains": sorted(domain_set & set(_parse_json(row.site_ids, []))),
            }
            for row in conflicts
        ]
        shared = sorted({domain for item in blocked_by for domain in item["domains"]})
        raise _block_run(
            db,
            task,
            f"站点 {', '.join(shared)} 有任务「{conflicts[0].name}」正在运行，同站点配额不能并发叠加",
            actor=actor,
            extra={"reason": "site_run_conflict", "blocked_by": blocked_by},
            ip=ip,
            trace_id=trace_id,
        )

    sites_by_domain = _sites_by_domain(db, domains)
    disabled = sorted(
        domain
        for domain in domains
        if sites_by_domain.get(domain) is not None and not sites_by_domain[domain].enabled
    )
    if disabled:
        raise _block_run(
            db,
            task,
            f"站点已停用：{', '.join(disabled)}（enabled 为安全开关，按当前值判定）",
            actor=actor,
            extra={"reason": "site_disabled", "domains": disabled},
            ip=ip,
            trace_id=trace_id,
        )

    urls = _queued_urls(db, domains)
    if not urls:
        raise _block_run(
            db,
            task,
            "任务站点下没有排队中的前沿 URL",
            actor=actor,
            extra={"reason": "no_queued_urls"},
            ip=ip,
            trace_id=trace_id,
        )

    now = utcnow()
    task.status = TaskStatus.RUNNING.value
    task.started_at = now
    task.last_block_reason = ""
    task.last_blocked_at = None
    db.add(task)
    db.commit()  # 让其他会话看到 running：同站点任务此时会被互斥阻止

    try:
        # 调度只使用创建时冻结的配额快照；快照本身永不被回写
        snapshot = _parse_json(task.quota_snapshot, {})
        configs = configs_from_snapshot(snapshot, domains, sites_by_domain)
        result = scheduler.simulate(
            configs,
            urls,
            task_name=task.name,
            window_s=task.window_s,
            max_sequence=task.max_sequence,
            seed=task.seed,
            now=now,
        )
        task.status = TaskStatus.FINISHED.value
        task.scheduled_count = int(result["sequence_length"])
        task.scheduler_result = json.dumps(result, ensure_ascii=False)
        task.finished_at = utcnow()
        db.add(task)
        # 抓取记录只写一次：已存在本任务模拟记录时不删不改，保证历史不可替换
        existing_records = (
            db.query(FetchRecord)
            .filter(FetchRecord.task_id == task.id, FetchRecord.origin == FetchOrigin.SIMULATION.value)
            .count()
        )
        if existing_records == 0:
            for item in result["sequence"]:
                db.add(
                    FetchRecord(
                        task_id=task.id,
                        url_id=item["url_id"],
                        domain=item["domain"],
                        status_code=item["status_code"],
                        latency_ms=item["latency_ms"],
                        bytes=1024 + (item["seq"] * 37) % 4096,
                        was_duplicate=False,
                        rate_limited=item["rate_limited"],
                        backoff_applied_s=item["backoff_applied_s"],
                        origin=FetchOrigin.SIMULATION.value,
                        fetched_at=now,
                    )
                )
        db.commit()
    except Exception as exc:
        db.rollback()
        task.status = TaskStatus.QUEUED.value
        task.last_block_reason = f"运行失败已回滚为待调度：{exc}"
        task.last_blocked_at = utcnow()
        db.add(task)
        db.commit()
        raise AppError(codes.INTERNAL_ERROR, "调度模拟执行失败", str(exc))

    db.refresh(task)
    audit_service.record(
        db,
        action="task.simulate",
        entity_type="CrawlTask",
        entity_id=task.id,
        actor=actor,
        payload={
            "sequence_length": result["sequence_length"],
            "rate_limited": result["rate_limited_count"],
            "starvation": len(result["starvation_boosts"]),
            "digest": result["digest"],
            "quota": "frozen_snapshot",
        },
        ip=ip,
        trace_id=trace_id,
    )
    return serialize_task(task, include_result=True, sites_by_domain=sites_by_domain)
