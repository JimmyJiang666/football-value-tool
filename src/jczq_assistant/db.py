"""SQLite 相关逻辑。

当前只负责：
1. 创建数据库文件
2. 初始化 matches_raw 表
"""

import sqlite3
from pathlib import Path

from jczq_assistant.config import DATABASE_PATH, DATA_DIR


def get_connection() -> sqlite3.Connection:
    """返回 SQLite 连接。

    在连接前先确保 data 目录存在，避免首次启动时报错。
    """

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DATABASE_PATH)


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
        fetched_date TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """

    with get_connection() as connection:
        connection.execute(create_table_sql)
        connection.commit()

    return DATABASE_PATH
