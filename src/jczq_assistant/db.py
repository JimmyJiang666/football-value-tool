"""SQLite 相关逻辑。

当前只负责：
1. 创建数据库文件
2. 初始化 matches_raw 表
3. 写入抓取到的原始比赛数据
4. 初始化并写入 results_raw 表
5. 记录赛果同步任务日志
"""

from collections.abc import Iterable
from datetime import date
from datetime import datetime
from datetime import timedelta
import sqlite3
from pathlib import Path

from jczq_assistant.config import DATABASE_PATH, DATA_DIR


def get_connection() -> sqlite3.Connection:
    """返回 SQLite 连接。

    在连接前先确保 data 目录存在，避免首次启动时报错。
    """

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DATABASE_PATH)


def _ensure_matches_raw_columns(connection: sqlite3.Connection) -> None:
    """为已有数据库补齐当前版本需要的字段。

    由于项目已经初始化过一次，这里做一个很轻量的迁移，
    避免用户删库重建。
    """

    existing_columns = {
        row[1] for row in connection.execute("PRAGMA table_info(matches_raw)")
    }

    if "analysis_url" not in existing_columns:
        connection.execute("ALTER TABLE matches_raw ADD COLUMN analysis_url TEXT")


def _ensure_results_raw_columns(connection: sqlite3.Connection) -> None:
    """为已有 results_raw 表补齐字段。"""

    existing_columns = {
        row[1] for row in connection.execute("PRAGMA table_info(results_raw)")
    }

    required_columns = {
        "source_match_id": "TEXT",
        "match_no": "TEXT",
        "league": "TEXT",
        "match_time": "TEXT",
        "home_team": "TEXT",
        "away_team": "TEXT",
        "final_score": "TEXT",
        "spf_result": "TEXT",
        "handicap": "TEXT",
        "handicap_result": "TEXT",
        "correct_score_result": "TEXT",
        "total_goals_result": "TEXT",
        "half_full_result": "TEXT",
        "source_url": "TEXT",
        "fetched_at": "TEXT",
    }

    for column_name, column_type in required_columns.items():
        if column_name not in existing_columns:
            connection.execute(
                f"ALTER TABLE results_raw ADD COLUMN {column_name} {column_type}"
            )


def _ensure_sync_runs_columns(connection: sqlite3.Connection) -> None:
    """为已有 sync_runs 表补齐字段。"""

    existing_columns = {
        row[1] for row in connection.execute("PRAGMA table_info(sync_runs)")
    }

    required_columns = {
        "skipped_windows": "INTEGER NOT NULL DEFAULT 0",
        "stop_reason": "TEXT",
        "stop_date": "TEXT",
    }

    for column_name, column_type in required_columns.items():
        if column_name not in existing_columns:
            connection.execute(
                f"ALTER TABLE sync_runs ADD COLUMN {column_name} {column_type}"
            )


def init_db() -> Path:
    """初始化数据库表。

    这里只创建一张最基础的原始比赛表，后续抓取到的数据可以先落到这里。
    """

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS matches_raw (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        match_no TEXT NOT NULL,
        league TEXT NOT NULL,
        kickoff_time TEXT NOT NULL,
        home_team TEXT NOT NULL,
        away_team TEXT NOT NULL,
        home_win_odds REAL,
        draw_odds REAL,
        away_win_odds REAL,
        source_url TEXT,
        analysis_url TEXT,
        fetched_date TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """

    create_results_table_sql = """
    CREATE TABLE IF NOT EXISTS results_raw (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_match_id TEXT NOT NULL,
        match_no TEXT,
        league TEXT,
        match_time TEXT,
        home_team TEXT,
        away_team TEXT,
        final_score TEXT,
        spf_result TEXT,
        handicap TEXT,
        handicap_result TEXT,
        correct_score_result TEXT,
        total_goals_result TEXT,
        half_full_result TEXT,
        source_url TEXT,
        fetched_at TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """

    create_results_index_sql = """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_results_raw_source_match_id
    ON results_raw(source_match_id);
    """

    create_sync_runs_table_sql = """
    CREATE TABLE IF NOT EXISTS sync_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sync_type TEXT NOT NULL,
        start_date TEXT NOT NULL,
        end_date TEXT NOT NULL,
        status TEXT NOT NULL,
        rows_fetched INTEGER NOT NULL DEFAULT 0,
        rows_inserted INTEGER NOT NULL DEFAULT 0,
        skipped_windows INTEGER NOT NULL DEFAULT 0,
        started_at TEXT NOT NULL,
        finished_at TEXT,
        error_message TEXT,
        stop_reason TEXT,
        stop_date TEXT
    );
    """

    with get_connection() as connection:
        connection.execute(create_table_sql)
        _ensure_matches_raw_columns(connection)
        connection.execute(create_results_table_sql)
        _ensure_results_raw_columns(connection)
        connection.execute(create_results_index_sql)
        connection.execute(create_sync_runs_table_sql)
        _ensure_sync_runs_columns(connection)
        connection.commit()

    return DATABASE_PATH


def save_matches_raw(matches: Iterable[dict]) -> int:
    """把抓取结果写入 matches_raw。

    这里不做复杂去重策略，只在同一个 fetched_date 下按 match_no 覆盖写入，
    这样重复点击抓取按钮时不会不断堆积当天的重复记录。
    """

    match_list = list(matches)
    if not match_list:
        return 0

    fetched_date = match_list[0].get("fetched_date")
    match_nos = [match["match_no"] for match in match_list if match.get("match_no")]

    insert_sql = """
    INSERT INTO matches_raw (
        match_no,
        league,
        kickoff_time,
        home_team,
        away_team,
        home_win_odds,
        draw_odds,
        away_win_odds,
        source_url,
        analysis_url,
        fetched_date
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    values = [
        (
            match.get("match_no"),
            match.get("league"),
            match.get("kickoff_time"),
            match.get("home_team"),
            match.get("away_team"),
            match.get("home_win_odds"),
            match.get("draw_odds"),
            match.get("away_win_odds"),
            match.get("source_url"),
            match.get("analysis_url"),
            match.get("fetched_date"),
        )
        for match in match_list
    ]

    with get_connection() as connection:
        if fetched_date and match_nos:
            placeholders = ", ".join("?" for _ in match_nos)
            delete_sql = (
                f"DELETE FROM matches_raw "
                f"WHERE fetched_date = ? AND match_no IN ({placeholders})"
            )
            connection.execute(delete_sql, [fetched_date, *match_nos])

        connection.executemany(insert_sql, values)
        connection.commit()

    return len(values)


def save_results_raw(results: Iterable[dict]) -> int:
    """把历史开奖结果写入 results_raw。

    使用 source_match_id 做幂等写入。
    重复运行时更新字段和最新抓取时间，不重复插入。
    """

    stats = save_results_raw_with_stats(results)
    return stats["processed"]


def save_results_raw_with_stats(results: Iterable[dict]) -> dict[str, int]:
    """把历史开奖结果写入 results_raw，并返回处理统计。"""

    result_list = list(results)
    if not result_list:
        return {"processed": 0, "inserted": 0}

    upsert_sql = """
    INSERT INTO results_raw (
        source_match_id,
        match_no,
        league,
        match_time,
        home_team,
        away_team,
        final_score,
        spf_result,
        handicap,
        handicap_result,
        correct_score_result,
        total_goals_result,
        half_full_result,
        source_url,
        fetched_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(source_match_id) DO UPDATE SET
        match_no = excluded.match_no,
        league = excluded.league,
        match_time = excluded.match_time,
        home_team = excluded.home_team,
        away_team = excluded.away_team,
        final_score = excluded.final_score,
        spf_result = excluded.spf_result,
        handicap = excluded.handicap,
        handicap_result = excluded.handicap_result,
        correct_score_result = excluded.correct_score_result,
        total_goals_result = excluded.total_goals_result,
        half_full_result = excluded.half_full_result,
        source_url = excluded.source_url,
        fetched_at = excluded.fetched_at
    """

    values = [
        (
            result.get("source_match_id"),
            result.get("match_no"),
            result.get("league"),
            result.get("match_time"),
            result.get("home_team"),
            result.get("away_team"),
            result.get("final_score"),
            result.get("spf_result"),
            result.get("handicap"),
            result.get("handicap_result"),
            result.get("correct_score_result"),
            result.get("total_goals_result"),
            result.get("half_full_result"),
            result.get("source_url"),
            result.get("fetched_at"),
        )
        for result in result_list
    ]

    with get_connection() as connection:
        inserted_count = _count_new_result_ids(connection, result_list)
        connection.executemany(upsert_sql, values)
        connection.commit()

    return {
        "processed": len(values),
        "inserted": inserted_count,
    }


def _count_new_result_ids(connection: sqlite3.Connection, results: list[dict]) -> int:
    """统计本次写入中真正新增的赛果条数。"""

    source_ids = {
        result["source_match_id"]
        for result in results
        if result.get("source_match_id")
    }
    if not source_ids:
        return 0

    existing_ids: set[str] = set()
    sorted_ids = sorted(source_ids)
    batch_size = 500

    for index in range(0, len(sorted_ids), batch_size):
        batch = sorted_ids[index : index + batch_size]
        placeholders = ", ".join("?" for _ in batch)
        rows = connection.execute(
            f"SELECT source_match_id FROM results_raw WHERE source_match_id IN ({placeholders})",
            batch,
        ).fetchall()
        existing_ids.update(row[0] for row in rows)

    return len(source_ids - existing_ids)


def create_sync_run(sync_type: str, start_date: str, end_date: str) -> int:
    """创建一条同步任务日志，初始状态为 running。"""

    started_at = datetime.now().isoformat(timespec="seconds")
    insert_sql = """
    INSERT INTO sync_runs (
        sync_type,
        start_date,
        end_date,
        status,
        rows_fetched,
        rows_inserted,
        started_at
    ) VALUES (?, ?, ?, 'running', 0, 0, ?)
    """

    with get_connection() as connection:
        cursor = connection.execute(insert_sql, (sync_type, start_date, end_date, started_at))
        connection.commit()
        return int(cursor.lastrowid)


def finish_sync_run(
    run_id: int,
    status: str,
    rows_fetched: int,
    rows_inserted: int,
    error_message: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    skipped_windows: int | None = None,
    stop_reason: str | None = None,
    stop_date: str | None = None,
) -> None:
    """更新同步任务日志的最终结果。"""

    finished_at = datetime.now().isoformat(timespec="seconds")
    update_sql = """
    UPDATE sync_runs
    SET status = ?,
        start_date = COALESCE(?, start_date),
        end_date = COALESCE(?, end_date),
        rows_fetched = ?,
        rows_inserted = ?,
        skipped_windows = COALESCE(?, skipped_windows),
        finished_at = ?,
        error_message = ?,
        stop_reason = ?,
        stop_date = ?
    WHERE id = ?
    """

    with get_connection() as connection:
        connection.execute(
            update_sql,
            (
                status,
                start_date,
                end_date,
                rows_fetched,
                rows_inserted,
                skipped_windows,
                finished_at,
                error_message,
                stop_reason,
                stop_date,
                run_id,
            ),
        )
        connection.commit()


def count_results_in_range(start_date: str, end_date: str) -> int:
    """统计某个日期区间内已有多少条历史赛果。

    这里用于 CLI 的快速跳过优化：
    - 如果一个窗口已经存在历史赛果，就可以选择直接跳过抓取
    - 这是启发式判断，不保证窗口已经“完整回填”
    - 因此更适合 day 粒度，week/month 使用时要接受可能跳过部分窗口
    """

    start_date_value = date.fromisoformat(start_date)
    end_date_value = date.fromisoformat(end_date)
    next_day = end_date_value + timedelta(days=1)

    query_sql = """
    SELECT COUNT(*)
    FROM results_raw
    WHERE match_time >= ?
      AND match_time < ?
    """

    with get_connection() as connection:
        row = connection.execute(
            query_sql,
            (
                start_date_value.isoformat(),
                next_day.isoformat(),
            ),
        ).fetchone()

    return int(row[0]) if row else 0


def get_recent_sync_runs(limit: int = 10) -> list[dict]:
    """读取最近几次历史赛果同步记录。"""

    query_sql = """
    SELECT
        id,
        sync_type,
        start_date,
        end_date,
        status,
        rows_fetched,
        rows_inserted,
        skipped_windows,
        stop_reason,
        stop_date,
        started_at,
        finished_at,
        error_message
    FROM sync_runs
    ORDER BY id DESC
    LIMIT ?
    """

    with get_connection() as connection:
        cursor = connection.execute(query_sql, (limit,))
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()

    return [
        dict(zip(columns, row, strict=False))
        for row in rows
    ]
