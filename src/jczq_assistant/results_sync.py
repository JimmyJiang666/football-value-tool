"""历史赛果回填与增量同步 CLI。

当前只做 MVP：
1. backfill: 按日期区间切窗回填历史赛果
2. sync_recent: 同步最近 N 天赛果

实现原则：
- 每个时间窗口独立抓取、独立写库
- 单个窗口失败时继续后面的窗口，避免整次任务白跑
- 最终把总结果写入 sync_runs
"""

from __future__ import annotations

import argparse
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from datetime import timedelta
import logging
import sys

from jczq_assistant.db import (
    count_results_in_range,
    create_sync_run,
    finish_sync_run,
    init_db,
    save_results_raw_with_stats,
)
from jczq_assistant.history_results import (
    fetch_history_results_by_date_range,
    fetch_history_results_by_date_range_with_meta,
    get_default_results_query_params,
)


logger = logging.getLogger(__name__)
ProgressCallback = Callable[[dict], None]


@dataclass
class SyncSummary:
    """同步任务结果摘要。"""

    run_id: int
    sync_type: str
    start_date: str
    end_date: str
    status: str
    rows_fetched: int
    rows_inserted: int
    window_count: int
    skipped_windows: int
    stop_reason: str | None
    stop_date: str | None
    error_messages: list[str]


def backfill_results(
    start_date_str: str,
    end_date_str: str,
    window: str = "week",
    skip_existing: bool = False,
    progress_callback: ProgressCallback | None = None,
) -> SyncSummary:
    """按日期区间回填历史赛果。"""

    start_date_value, end_date_value = _parse_date_range(start_date_str, end_date_str)
    init_db()

    run_id = create_sync_run(
        sync_type="backfill",
        start_date=start_date_value.isoformat(),
        end_date=end_date_value.isoformat(),
    )
    return _run_results_sync(
        run_id=run_id,
        sync_type="backfill",
        start_date_value=start_date_value,
        end_date_value=end_date_value,
        window=window,
        skip_existing=skip_existing,
        progress_callback=progress_callback,
    )


def sync_recent_results(
    days: int = 7,
    window: str = "day",
    skip_existing: bool = False,
    progress_callback: ProgressCallback | None = None,
) -> SyncSummary:
    """同步最近 N 天的历史赛果。"""

    if days <= 0:
        raise ValueError("days 必须大于 0。")

    default_query_params = get_default_results_query_params()
    end_date_value = date.fromisoformat(default_query_params["endTime"])
    start_date_value = end_date_value - timedelta(days=days - 1)
    init_db()

    run_id = create_sync_run(
        sync_type="sync_recent",
        start_date=start_date_value.isoformat(),
        end_date=end_date_value.isoformat(),
    )
    return _run_results_sync(
        run_id=run_id,
        sync_type="sync_recent",
        start_date_value=start_date_value,
        end_date_value=end_date_value,
        window=window,
        skip_existing=skip_existing,
        progress_callback=progress_callback,
    )


def backfill_until_empty(
    start_date_str: str | None = None,
    skip_existing: bool = False,
    max_days: int | None = None,
    progress_callback: ProgressCallback | None = None,
) -> SyncSummary:
    """从较新的日期开始按天回溯，直到遇到“有比赛但无赛果”的日期才停止。

    说明：
    - 默认起点不是本机“今天”，而是足彩网结果页当前默认 endTime
    - 只有“页面里有比赛行，但这些比赛都没有赛果”时才视为停止信号
    - 单纯当天没比赛不会停止，会继续向前回溯
    - 这个策略仍然是实用型启发式，用于识别站点更老历史区间的可抓取边界
    """

    if max_days is not None and max_days <= 0:
        raise ValueError("max_days 必须大于 0。")

    if start_date_str:
        start_date_value = date.fromisoformat(start_date_str)
    else:
        default_query_params = get_default_results_query_params()
        start_date_value = date.fromisoformat(default_query_params["endTime"])

    init_db()
    run_id = create_sync_run(
        sync_type="backfill_until_empty",
        start_date=start_date_value.isoformat(),
        end_date=start_date_value.isoformat(),
    )
    return _run_backfill_until_empty(
        run_id=run_id,
        start_date_value=start_date_value,
        skip_existing=skip_existing,
        max_days=max_days,
        progress_callback=progress_callback,
    )


def _run_results_sync(
    run_id: int,
    sync_type: str,
    start_date_value: date,
    end_date_value: date,
    window: str,
    skip_existing: bool,
    progress_callback: ProgressCallback | None,
) -> SyncSummary:
    """执行分窗口历史赛果同步。"""

    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(message)s",
        )

    rows_fetched = 0
    rows_inserted = 0
    skipped_windows = 0
    error_messages: list[str] = []
    windows = list(iter_date_windows(start_date_value, end_date_value, window))

    logger.info(
        "Starting %s run_id=%s range=%s..%s window=%s windows=%s",
        sync_type,
        run_id,
        start_date_value,
        end_date_value,
        window,
        len(windows),
    )
    _emit_progress(
        progress_callback,
        stage="start",
        sync_type=sync_type,
        current_index=0,
        total_windows=len(windows),
        message=f"开始同步，共 {len(windows)} 个窗口。",
    )

    for index, (window_start, window_end) in enumerate(windows, start=1):
        logger.info("Syncing window %s -> %s", window_start, window_end)
        _emit_progress(
            progress_callback,
            stage="window_start",
            sync_type=sync_type,
            current_index=index,
            total_windows=len(windows),
            window_start=window_start.isoformat(),
            window_end=window_end.isoformat(),
            message=f"正在处理 {window_start} -> {window_end}",
        )
        try:
            if skip_existing:
                existing_count = count_results_in_range(
                    start_date=window_start.isoformat(),
                    end_date=window_end.isoformat(),
                )
                if existing_count > 0:
                    skipped_windows += 1
                    logger.info(
                        (
                            "Skipping window %s -> %s because local results already exist "
                            "count=%s"
                        ),
                        window_start,
                        window_end,
                        existing_count,
                    )
                    _emit_progress(
                        progress_callback,
                        stage="window_skipped",
                        sync_type=sync_type,
                        current_index=index,
                        total_windows=len(windows),
                        window_start=window_start.isoformat(),
                        window_end=window_end.isoformat(),
                        skipped_windows=skipped_windows,
                        existing_count=existing_count,
                        message=(
                            f"跳过 {window_start} -> {window_end}，"
                            f"本地已有 {existing_count} 条。"
                        ),
                    )
                    continue

            results = fetch_history_results_by_date_range(
                start_date=window_start.isoformat(),
                end_date=window_end.isoformat(),
            )
            stats = save_results_raw_with_stats(results)
            rows_fetched += stats["processed"]
            rows_inserted += stats["inserted"]
            logger.info(
                "Window completed %s -> %s fetched=%s inserted=%s",
                window_start,
                window_end,
                stats["processed"],
                stats["inserted"],
            )
            _emit_progress(
                progress_callback,
                stage="window_complete",
                sync_type=sync_type,
                current_index=index,
                total_windows=len(windows),
                window_start=window_start.isoformat(),
                window_end=window_end.isoformat(),
                rows_fetched=rows_fetched,
                rows_inserted=rows_inserted,
                skipped_windows=skipped_windows,
                message=(
                    f"完成 {window_start} -> {window_end}，"
                    f"抓取 {stats['processed']}，新增 {stats['inserted']}。"
                ),
            )
        except Exception as exc:
            message = f"{window_start} -> {window_end}: {exc}"
            logger.exception("Window failed %s", message)
            error_messages.append(message)
            _emit_progress(
                progress_callback,
                stage="window_error",
                sync_type=sync_type,
                current_index=index,
                total_windows=len(windows),
                window_start=window_start.isoformat(),
                window_end=window_end.isoformat(),
                error=message,
                message=f"窗口失败：{message}",
            )

    status = _determine_sync_status(
        rows_fetched=rows_fetched,
        error_messages=error_messages,
    )
    error_message_text = "\n".join(error_messages) if error_messages else None
    finish_sync_run(
        run_id=run_id,
        status=status,
        rows_fetched=rows_fetched,
        rows_inserted=rows_inserted,
        error_message=error_message_text,
        skipped_windows=skipped_windows,
    )

    summary = SyncSummary(
        run_id=run_id,
        sync_type=sync_type,
        start_date=start_date_value.isoformat(),
        end_date=end_date_value.isoformat(),
        status=status,
        rows_fetched=rows_fetched,
        rows_inserted=rows_inserted,
        window_count=len(windows),
        skipped_windows=skipped_windows,
        stop_reason=None,
        stop_date=None,
        error_messages=error_messages,
    )
    logger.info(
        "Finished %s run_id=%s status=%s fetched=%s inserted=%s errors=%s",
        sync_type,
        run_id,
        status,
        rows_fetched,
        rows_inserted,
        len(error_messages),
    )
    _emit_progress(
        progress_callback,
        stage="finish",
        sync_type=sync_type,
        current_index=len(windows),
        total_windows=len(windows),
        status=status,
        rows_fetched=rows_fetched,
        rows_inserted=rows_inserted,
        skipped_windows=skipped_windows,
        message=f"同步完成，状态：{status}。",
    )
    return summary


def _run_backfill_until_empty(
    run_id: int,
    start_date_value: date,
    skip_existing: bool,
    max_days: int | None,
    progress_callback: ProgressCallback | None,
) -> SyncSummary:
    """执行按天向前回溯，直到“有比赛但无赛果”才停止的同步任务。"""

    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(message)s",
        )

    rows_fetched = 0
    rows_inserted = 0
    skipped_windows = 0
    window_count = 0
    error_messages: list[str] = []
    stop_reason: str | None = None
    stop_date: str | None = None
    current = start_date_value

    logger.info(
        "Starting backfill_until_empty run_id=%s start_date=%s skip_existing=%s max_days=%s",
        run_id,
        start_date_value,
        skip_existing,
        max_days,
    )
    _emit_progress(
        progress_callback,
        stage="start",
        sync_type="backfill_until_empty",
        current_index=0,
        total_windows=max_days,
        message="开始向前回溯同步。",
    )

    while True:
        if max_days is not None and window_count >= max_days:
            stop_reason = "max_days"
            stop_date = (current + timedelta(days=1)).isoformat()
            logger.info("Stopping because max_days=%s reached", max_days)
            _emit_progress(
                progress_callback,
                stage="stop",
                sync_type="backfill_until_empty",
                current_index=window_count,
                total_windows=max_days,
                stop_reason=stop_reason,
                stop_date=stop_date,
                message=f"达到最大回溯天数，停止于 {stop_date}。",
            )
            break

        window_count += 1
        logger.info("Syncing window %s -> %s", current, current)
        _emit_progress(
            progress_callback,
            stage="window_start",
            sync_type="backfill_until_empty",
            current_index=window_count,
            total_windows=max_days,
            window_start=current.isoformat(),
            window_end=current.isoformat(),
            message=f"正在处理 {current}。",
        )

        try:
            if skip_existing:
                existing_count = count_results_in_range(
                    start_date=current.isoformat(),
                    end_date=current.isoformat(),
                )
                if existing_count > 0:
                    skipped_windows += 1
                    logger.info(
                        "Skipping window %s -> %s because local results already exist count=%s",
                        current,
                        current,
                        existing_count,
                    )
                    _emit_progress(
                        progress_callback,
                        stage="window_skipped",
                        sync_type="backfill_until_empty",
                        current_index=window_count,
                        total_windows=max_days,
                        window_start=current.isoformat(),
                        window_end=current.isoformat(),
                        skipped_windows=skipped_windows,
                        existing_count=existing_count,
                        message=f"跳过 {current}，本地已有 {existing_count} 条。",
                    )
                    current -= timedelta(days=1)
                    continue

            fetch_result = fetch_history_results_by_date_range_with_meta(
                start_date=current.isoformat(),
                end_date=current.isoformat(),
            )
            if fetch_result.raw_row_count > 0 and not fetch_result.results:
                stop_reason = "records_without_results"
                stop_date = current.isoformat()
                logger.info(
                    (
                        "Stopping because %s returned %s match records but all settled "
                        "result columns were empty"
                    ),
                    current,
                    fetch_result.raw_row_count,
                )
                _emit_progress(
                    progress_callback,
                    stage="stop",
                    sync_type="backfill_until_empty",
                    current_index=window_count,
                    total_windows=max_days,
                    stop_reason=stop_reason,
                    stop_date=stop_date,
                    raw_row_count=fetch_result.raw_row_count,
                    message=(
                        f"{current} 有 {fetch_result.raw_row_count} 场比赛记录，"
                        "但赛果列全空，停止回溯。"
                    ),
                )
                break

            if fetch_result.raw_row_count == 0:
                logger.info(
                    "No match records found on %s, continuing to previous day",
                    current,
                )
                _emit_progress(
                    progress_callback,
                    stage="window_no_matches",
                    sync_type="backfill_until_empty",
                    current_index=window_count,
                    total_windows=max_days,
                    window_start=current.isoformat(),
                    window_end=current.isoformat(),
                    message=f"{current} 没有比赛记录，继续向前回溯。",
                )
                current -= timedelta(days=1)
                continue

            stats = save_results_raw_with_stats(fetch_result.results)
            rows_fetched += stats["processed"]
            rows_inserted += stats["inserted"]
            logger.info(
                "Window completed %s -> %s raw_rows=%s fetched=%s inserted=%s",
                current,
                current,
                fetch_result.raw_row_count,
                stats["processed"],
                stats["inserted"],
            )
            _emit_progress(
                progress_callback,
                stage="window_complete",
                sync_type="backfill_until_empty",
                current_index=window_count,
                total_windows=max_days,
                window_start=current.isoformat(),
                window_end=current.isoformat(),
                raw_row_count=fetch_result.raw_row_count,
                rows_fetched=rows_fetched,
                rows_inserted=rows_inserted,
                skipped_windows=skipped_windows,
                message=(
                    f"完成 {current}，页面记录 {fetch_result.raw_row_count}，"
                    f"抓取 {stats['processed']}，新增 {stats['inserted']}。"
                ),
            )
        except Exception as exc:
            message = f"{current} -> {current}: {exc}"
            logger.exception("Window failed %s", message)
            error_messages.append(message)
            _emit_progress(
                progress_callback,
                stage="window_error",
                sync_type="backfill_until_empty",
                current_index=window_count,
                total_windows=max_days,
                window_start=current.isoformat(),
                window_end=current.isoformat(),
                error=message,
                message=f"窗口失败：{message}",
            )

        current -= timedelta(days=1)

    status = _determine_sync_status(
        rows_fetched=rows_fetched,
        error_messages=error_messages,
    )
    error_message_text = "\n".join(error_messages) if error_messages else None
    final_end_date = stop_date or (current + timedelta(days=1)).isoformat()
    finish_sync_run(
        run_id=run_id,
        status=status,
        start_date=start_date_value.isoformat(),
        end_date=final_end_date,
        rows_fetched=rows_fetched,
        rows_inserted=rows_inserted,
        error_message=error_message_text,
        skipped_windows=skipped_windows,
        stop_reason=stop_reason,
        stop_date=stop_date,
    )

    summary = SyncSummary(
        run_id=run_id,
        sync_type="backfill_until_empty",
        start_date=start_date_value.isoformat(),
        end_date=final_end_date,
        status=status,
        rows_fetched=rows_fetched,
        rows_inserted=rows_inserted,
        window_count=window_count,
        skipped_windows=skipped_windows,
        stop_reason=stop_reason,
        stop_date=stop_date,
        error_messages=error_messages,
    )
    logger.info(
        (
            "Finished backfill_until_empty run_id=%s status=%s fetched=%s "
            "inserted=%s skipped=%s stop_reason=%s stop_date=%s"
        ),
        run_id,
        status,
        rows_fetched,
        rows_inserted,
        skipped_windows,
        stop_reason,
        stop_date,
    )
    _emit_progress(
        progress_callback,
        stage="finish",
        sync_type="backfill_until_empty",
        current_index=window_count,
        total_windows=max_days,
        status=status,
        rows_fetched=rows_fetched,
        rows_inserted=rows_inserted,
        skipped_windows=skipped_windows,
        stop_reason=stop_reason,
        stop_date=stop_date,
        message=f"同步完成，状态：{status}。",
    )
    return summary


def _emit_progress(
    progress_callback: ProgressCallback | None,
    **payload,
) -> None:
    """向调用方发送同步进度事件。"""

    if progress_callback is None:
        return
    progress_callback(payload)


def iter_date_windows(start_date_value: date, end_date_value: date, window: str):
    """按 day/week/month 生成日期窗口。

    假设：
    - day: 每天一个窗口
    - week: 从 start_date 开始按连续 7 天切窗
    - month: 按自然月边界切窗
    """

    current = start_date_value

    while current <= end_date_value:
        if window == "day":
            window_end = current
        elif window == "week":
            window_end = min(current + timedelta(days=6), end_date_value)
        elif window == "month":
            next_month = _first_day_of_next_month(current)
            window_end = min(next_month - timedelta(days=1), end_date_value)
        else:
            raise ValueError("window 必须是 day / week / month。")

        yield current, window_end
        current = window_end + timedelta(days=1)


def _first_day_of_next_month(value: date) -> date:
    """返回下个月的第一天。"""

    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


def _parse_date_range(start_date_str: str, end_date_str: str) -> tuple[date, date]:
    """解析并校验日期区间。"""

    start_date_value = date.fromisoformat(start_date_str)
    end_date_value = date.fromisoformat(end_date_str)
    if start_date_value > end_date_value:
        raise ValueError("开始日期不能晚于结束日期。")
    return start_date_value, end_date_value


def _determine_sync_status(rows_fetched: int, error_messages: list[str]) -> str:
    """根据抓取结果生成任务状态。"""

    if error_messages and rows_fetched == 0:
        return "failed"
    if error_messages:
        return "partial_failed"
    return "success"


def build_parser() -> argparse.ArgumentParser:
    """构造命令行参数解析器。"""

    parser = argparse.ArgumentParser(description="竞彩足球历史赛果回填与同步工具")
    subparsers = parser.add_subparsers(dest="command", required=True)

    backfill_parser = subparsers.add_parser("backfill", help="按日期区间回填历史赛果")
    backfill_parser.add_argument("--start-date", required=True, help="开始日期，格式 YYYY-MM-DD")
    backfill_parser.add_argument("--end-date", required=True, help="结束日期，格式 YYYY-MM-DD")
    backfill_parser.add_argument(
        "--window",
        choices=["day", "week", "month"],
        default="week",
        help="切窗粒度，默认 week",
    )
    backfill_parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="如果窗口内本地已存在赛果，则直接跳过该窗口",
    )

    recent_parser = subparsers.add_parser("sync-recent", help="同步最近 N 天赛果")
    recent_parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="同步最近多少天，默认 7",
    )
    recent_parser.add_argument(
        "--window",
        choices=["day", "week", "month"],
        default="day",
        help="切窗粒度，默认 day",
    )
    recent_parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="如果窗口内本地已存在赛果，则直接跳过该窗口",
    )

    until_empty_parser = subparsers.add_parser(
        "backfill-until-empty",
        help="从较新的日期开始逐天回溯，直到遇到空日停止",
    )
    until_empty_parser.add_argument(
        "--start-date",
        help="起始日期，格式 YYYY-MM-DD；默认使用足彩网结果页当前默认 endTime",
    )
    until_empty_parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="如果某一天本地已存在赛果，则直接跳过该天",
    )
    until_empty_parser.add_argument(
        "--max-days",
        type=int,
        help="最多回溯多少天，用于限制运行时长",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI 主入口。"""

    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "backfill":
            summary = backfill_results(
                start_date_str=args.start_date,
                end_date_str=args.end_date,
                window=args.window,
                skip_existing=args.skip_existing,
            )
        elif args.command == "sync-recent":
            summary = sync_recent_results(
                days=args.days,
                window=args.window,
                skip_existing=args.skip_existing,
            )
        else:
            summary = backfill_until_empty(
                start_date_str=args.start_date,
                skip_existing=args.skip_existing,
                max_days=args.max_days,
            )
    except Exception as exc:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
        logger.exception("Sync command failed: %s", exc)
        return 1

    print(
        f"run_id={summary.run_id} "
        f"status={summary.status} "
        f"rows_fetched={summary.rows_fetched} "
        f"rows_inserted={summary.rows_inserted} "
        f"windows={summary.window_count} "
        f"skipped_windows={summary.skipped_windows}"
    )
    if summary.error_messages:
        print("errors:")
        for message in summary.error_messages:
            print(f"- {message}")
    if summary.stop_reason:
        print(f"stop_reason={summary.stop_reason} stop_date={summary.stop_date}")

    return 0 if summary.status == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
