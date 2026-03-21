"""SQLite 相关逻辑。

当前只负责：
1. 创建数据库文件
2. 初始化 matches_raw 表
3. 写入抓取到的原始比赛数据
"""

import sqlite3
from pathlib import Path
from typing import Iterable

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

    with get_connection() as connection:
        connection.execute(create_table_sql)
        _ensure_matches_raw_columns(connection)
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
