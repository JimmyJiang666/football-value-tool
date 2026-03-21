"""500.com 胜负彩历史赔率与赛果同步 CLI。"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
import sys
import time


PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from jczq_assistant.sfc500_history import fetch_and_save_expect
from jczq_assistant.sfc500_history import init_sfc500_db
from jczq_assistant.sfc500_history import SFC500_DATABASE_PATH
from jczq_assistant.sfc500_history import sync_recent_history
from jczq_assistant.sfc500_history import sync_year
from jczq_assistant.sfc500_history import sync_year_range


def build_parser() -> argparse.ArgumentParser:
    """构建命令行参数。"""

    parser = argparse.ArgumentParser(
        description="抓取 500.com 胜负彩历史赔率与赛果"
    )
    parser.add_argument(
        "--db-path",
        default=str(SFC500_DATABASE_PATH),
        help="SQLite 文件路径，默认写入 data/sfc500_history.sqlite3",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="日志级别",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    fetch_expect_parser = subparsers.add_parser(
        "fetch-expect",
        help="抓取单个期次并写入 SQLite",
    )
    fetch_expect_parser.add_argument("--expect", required=True, help="期次，如 25013")
    fetch_expect_parser.add_argument(
        "--only-settled",
        action="store_true",
        help="只保留已开奖比赛",
    )
    fetch_expect_parser.add_argument(
        "--print-limit",
        type=int,
        default=5,
        help="终端打印前几条结构化记录",
    )

    sync_year_parser = subparsers.add_parser(
        "sync-year",
        help="顺序扫描某一年的期次并写入 SQLite",
    )
    sync_year_parser.add_argument("--year", type=int, required=True, help="年份，如 2026")
    sync_year_parser.add_argument(
        "--start-period",
        type=int,
        default=1,
        help="从第几期开始扫描，默认 1",
    )
    sync_year_parser.add_argument(
        "--end-period",
        type=int,
        default=399,
        help="扫描到第几期结束，默认 399",
    )
    sync_year_parser.add_argument(
        "--stop-after-empty",
        type=int,
        default=10,
        help="在已发现有效期次后，连续多少个空期次就停止，默认 10",
    )
    sync_year_parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.0,
        help="每个期次请求后的休眠秒数，默认 0",
    )
    sync_year_parser.add_argument(
        "--print-limit",
        type=int,
        default=5,
        help="终端打印前几条结构化记录",
    )
    sync_year_parser.add_argument(
        "--only-settled",
        action="store_true",
        help="只保留已开奖比赛",
    )

    sync_years_parser = subparsers.add_parser(
        "sync-years",
        help="按年份区间顺序回填",
    )
    sync_years_parser.add_argument("--start-year", type=int, required=True, help="起始年份")
    sync_years_parser.add_argument("--end-year", type=int, required=True, help="结束年份")
    sync_years_parser.add_argument(
        "--first-year-start-period",
        type=int,
        default=1,
        help="起始年份从第几期开始扫描，默认 1",
    )
    sync_years_parser.add_argument(
        "--last-year-end-period",
        type=int,
        default=399,
        help="结束年份扫描到第几期，默认 399",
    )
    sync_years_parser.add_argument(
        "--stop-after-empty",
        type=int,
        default=10,
        help="在已发现有效期次后，连续多少个空期次就停止，默认 10",
    )
    sync_years_parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.0,
        help="每个期次请求后的休眠秒数，默认 0",
    )
    sync_years_parser.add_argument(
        "--print-limit",
        type=int,
        default=5,
        help="终端打印前几条结构化记录",
    )
    sync_years_parser.add_argument(
        "--only-settled",
        action="store_true",
        help="只保留已开奖比赛",
    )

    sync_recent_parser = subparsers.add_parser(
        "sync-recent",
        help="同步最近 N 天附近的期次",
    )
    sync_recent_parser.add_argument("--days", type=int, required=True, help="天数，如 7/14/30")
    sync_recent_parser.add_argument(
        "--only-settled",
        action="store_true",
        help="只保留已开奖比赛",
    )
    sync_recent_parser.add_argument(
        "--print-limit",
        type=int,
        default=5,
        help="终端打印前几条结构化记录",
    )

    return parser


def _print_summary(
    summary: dict,
    *,
    headline: str,
    print_limit: int,
) -> None:
    """统一打印摘要。"""

    print(headline)
    print(
        json.dumps(
            summary.get("sample_matches", [])[:print_limit],
            ensure_ascii=False,
            indent=2,
        )
    )


def main(argv: list[str] | None = None) -> int:
    """CLI 入口。"""

    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(levelname)s:%(name)s:%(message)s",
    )

    db_path = Path(args.db_path)
    init_sfc500_db(db_path)
    started_at = time.perf_counter()

    if args.command == "fetch-expect":
        summary = fetch_and_save_expect(
            args.expect,
            db_path=db_path,
            only_settled=args.only_settled,
        )
        elapsed_seconds = time.perf_counter() - started_at
        print(
            f"expect={summary['expect']} rows_fetched={summary['rows_fetched']} "
            f"rows_inserted={summary['rows_inserted']} db_path={db_path} "
            f"elapsed_seconds={elapsed_seconds:.2f}"
        )
        print(
            json.dumps(
                summary["matches"][: args.print_limit],
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    if args.command == "sync-year":
        summary = sync_year(
            args.year,
            db_path=db_path,
            start_period=args.start_period,
            end_period=args.end_period,
            stop_after_empty=args.stop_after_empty,
            sleep_seconds=args.sleep_seconds,
            only_settled=args.only_settled,
        )
        elapsed_seconds = time.perf_counter() - started_at
        _print_summary(
            summary,
            headline=(
                f"year={summary['year']} rows_fetched={summary['rows_fetched']} "
                f"rows_inserted={summary['rows_inserted']} valid_expects={summary['valid_expects']} "
                f"scanned_expects={summary['scanned_expects']} db_path={summary['db_path']} "
                f"elapsed_seconds={elapsed_seconds:.2f}"
            ),
            print_limit=args.print_limit,
        )
        return 0

    if args.command == "sync-years":
        summary = sync_year_range(
            args.start_year,
            args.end_year,
            db_path=db_path,
            first_year_start_period=args.first_year_start_period,
            last_year_end_period=args.last_year_end_period,
            stop_after_empty=args.stop_after_empty,
            sleep_seconds=args.sleep_seconds,
            only_settled=args.only_settled,
        )
        elapsed_seconds = time.perf_counter() - started_at
        _print_summary(
            summary,
            headline=(
                f"start_year={summary['start_year']} end_year={summary['end_year']} "
                f"rows_fetched={summary['rows_fetched']} rows_inserted={summary['rows_inserted']} "
                f"valid_expects={summary['valid_expects']} scanned_expects={summary['scanned_expects']} "
                f"db_path={summary['db_path']} elapsed_seconds={elapsed_seconds:.2f}"
            ),
            print_limit=args.print_limit,
        )
        return 0

    if args.command == "sync-recent":
        summary = sync_recent_history(
            args.days,
            db_path=db_path,
            only_settled=args.only_settled,
        )
        elapsed_seconds = time.perf_counter() - started_at
        _print_summary(
            summary,
            headline=(
                f"days={summary['days']} status={summary['status']} "
                f"rows_fetched={summary['rows_fetched']} rows_inserted={summary['rows_inserted']} "
                f"valid_expects={summary['valid_expects']} scanned_expects={summary['scanned_expects']} "
                f"db_path={summary['db_path']} elapsed_seconds={elapsed_seconds:.2f}"
            ),
            print_limit=args.print_limit,
        )
        return 0

    parser.error("未识别的命令")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
