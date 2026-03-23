"""500.com 球队页历史比赛同步 CLI。"""

from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from jczq_assistant.sfc500_team_history import DEFAULT_TEAM_ID_END
from jczq_assistant.sfc500_team_history import DEFAULT_TEAM_FIXTURE_RECORDS
from jczq_assistant.sfc500_team_history import DEFAULT_LIVE_RECENT_SYNC_DAYS
from jczq_assistant.sfc500_team_history import SFC500_TEAM_HISTORY_DATABASE_PATH
from jczq_assistant.sfc500_team_history import fetch_and_save_team
from jczq_assistant.sfc500_team_history import fetch_and_save_live_matches
from jczq_assistant.sfc500_team_history import get_sfc500_team_history_overview
from jczq_assistant.sfc500_team_history import init_sfc500_team_history_db
from jczq_assistant.sfc500_team_history import refresh_known_teams
from jczq_assistant.sfc500_team_history import scan_team_range
from jczq_assistant.sfc500_team_history import sync_recent_live_matches


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="抓取 500.com 球队页历史比赛")
    parser.add_argument(
        "--db-path",
        default=str(SFC500_TEAM_HISTORY_DATABASE_PATH),
        help="SQLite 文件路径，默认 data/sfc500_team_history.sqlite3",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    fetch_team_parser = subparsers.add_parser("fetch-team", help="抓取并写入一个球队")
    fetch_team_parser.add_argument("--team-id", type=int, required=True)
    fetch_team_parser.add_argument("--print-limit", type=int, default=3)
    fetch_team_parser.add_argument(
        "--records",
        type=int,
        default=DEFAULT_TEAM_FIXTURE_RECORDS,
        help="抓取最近多少场，可用 10/30/50/100，默认 100",
    )

    scan_range_parser = subparsers.add_parser("scan-range", help="按球队 ID 区间扫描")
    scan_range_parser.add_argument("--start-team-id", type=int, default=1)
    scan_range_parser.add_argument("--end-team-id", type=int, default=DEFAULT_TEAM_ID_END)
    scan_range_parser.add_argument("--sleep-seconds", type=float, default=0.0)
    scan_range_parser.add_argument(
        "--records",
        type=int,
        default=DEFAULT_TEAM_FIXTURE_RECORDS,
        help="抓取最近多少场，可用 10/30/50/100，默认 100",
    )
    scan_range_parser.add_argument(
        "--no-skip-checked",
        action="store_true",
        help="默认跳过已检查过的 team_id；传入后会强制重扫",
    )
    scan_range_parser.add_argument(
        "--progress-every",
        type=int,
        default=100,
        help="每处理多少个 team_id 打印一次进度，默认 100；传 0 表示只打印开始和结束",
    )

    refresh_parser = subparsers.add_parser("refresh-known", help="刷新已发现球队的最近 100 场")
    refresh_parser.add_argument("--team-id", type=int, action="append", default=[])
    refresh_parser.add_argument("--limit", type=int)
    refresh_parser.add_argument("--offset", type=int, default=0)
    refresh_parser.add_argument("--sleep-seconds", type=float, default=0.0)
    refresh_parser.add_argument(
        "--records",
        type=int,
        default=DEFAULT_TEAM_FIXTURE_RECORDS,
        help="抓取最近多少场，可用 10/30/50/100，默认 100",
    )
    refresh_parser.add_argument(
        "--progress-every",
        type=int,
        default=100,
        help="每刷新多少支球队打印一次进度，默认 100；传 0 表示只打印开始和结束",
    )

    live_date_parser = subparsers.add_parser(
        "sync-live-date",
        help="抓取 live.500.com 某一天的完场比赛并写入球队大库",
    )
    live_date_parser.add_argument("--date", required=True, help="日期，格式 YYYY-MM-DD")
    live_date_parser.add_argument("--print-limit", type=int, default=3)

    live_recent_parser = subparsers.add_parser(
        "sync-live-recent",
        help="抓取 live.500.com 最近几天的完场比赛并增量更新球队大库",
    )
    live_recent_parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_LIVE_RECENT_SYNC_DAYS,
        help="最近多少天，默认 3，最多 7",
    )

    subparsers.add_parser("overview", help="查看球队大库概览")
    return parser


def _progress_logger(event: dict[str, object], *, every: int) -> None:
    stage = str(event.get("stage") or "")
    current_index = int(event.get("current_index") or 0)
    total_windows = int(event.get("total_windows") or 0)

    should_print = stage in {"start", "finish", "team_error"}
    if stage in {"team_done", "team_skipped"} and every > 0 and current_index % every == 0:
        should_print = True

    if not should_print:
        return

    payload = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "stage": stage,
        "current_index": current_index,
        "total_windows": total_windows,
        "team_id": event.get("team_id"),
        "team_name": event.get("team_name"),
        "rows_fetched": event.get("rows_fetched"),
        "rows_inserted": event.get("rows_inserted"),
        "is_valid": event.get("is_valid"),
        "message": event.get("message"),
    }
    print(json.dumps(payload, ensure_ascii=False), flush=True)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    db_path = Path(args.db_path)
    init_sfc500_team_history_db(db_path)

    if args.command == "fetch-team":
        summary = fetch_and_save_team(args.team_id, db_path=db_path, records=args.records)
        output = dict(summary)
        matches = list(output.pop("matches", []) or [])
        output["sample_matches"] = matches[: max(args.print_limit, 0)]
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0

    if args.command == "scan-range":
        progress_callback = lambda event: _progress_logger(  # noqa: E731
            event,
            every=max(int(args.progress_every), 0),
        )
        summary = scan_team_range(
            args.start_team_id,
            args.end_team_id,
            db_path=db_path,
            sleep_seconds=args.sleep_seconds,
            skip_checked=not args.no_skip_checked,
            records=args.records,
            progress_callback=progress_callback,
        )
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    if args.command == "refresh-known":
        progress_callback = lambda event: _progress_logger(  # noqa: E731
            event,
            every=max(int(args.progress_every), 0),
        )
        summary = refresh_known_teams(
            db_path=db_path,
            team_ids=list(args.team_id),
            limit=args.limit,
            offset=args.offset,
            sleep_seconds=args.sleep_seconds,
            records=args.records,
            progress_callback=progress_callback,
        )
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    if args.command == "sync-live-date":
        expect_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        summary = fetch_and_save_live_matches(expect_date, db_path=db_path)
        output = dict(summary)
        matches = list(output.pop("matches", []) or [])
        output["sample_matches"] = matches[: max(args.print_limit, 0)]
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0

    if args.command == "sync-live-recent":
        progress_callback = lambda event: _progress_logger(  # noqa: E731
            event,
            every=1,
        )
        summary = sync_recent_live_matches(
            days=args.days,
            db_path=db_path,
            progress_callback=progress_callback,
        )
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    if args.command == "overview":
        summary = get_sfc500_team_history_overview(db_path)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    parser.error(f"未识别的命令: {args.command}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
