"""500.com 胜负彩历史赔率与赛果抓取、落库和查询。"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from datetime import datetime
from datetime import timedelta
import logging
from pathlib import Path
import sqlite3
import time
from typing import Any

from bs4 import BeautifulSoup
import requests

from jczq_assistant.config import DATA_DIR, REQUEST_TIMEOUT_SECONDS, REQUEST_USER_AGENT


logger = logging.getLogger(__name__)

SFC500_BASE_URL = "https://trade.500.com/sfc/"
SFC500_DATABASE_PATH = DATA_DIR / "sfc500_history.sqlite3"
LEGACY_SFC500_DATABASE_PATH = DATA_DIR / "sfc500_history_settled.sqlite3"
DEFAULT_RETRIES = 2
ProgressCallback = Callable[[dict[str, Any]], None]


def get_sfc500_connection(db_path: Path | None = None) -> sqlite3.Connection:
    """返回 500.com 历史库连接。"""

    target_path = db_path or SFC500_DATABASE_PATH
    target_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(target_path)
    connection.row_factory = sqlite3.Row
    return connection


def init_sfc500_db(db_path: Path | None = None) -> Path:
    """初始化 500.com 历史赔率数据库。"""

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS sfc500_matches_raw (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        expect TEXT NOT NULL,
        match_no INTEGER NOT NULL,
        competition TEXT,
        match_time TEXT,
        match_time_raw TEXT,
        home_team TEXT,
        away_team TEXT,
        final_score TEXT,
        spf_result TEXT,
        spf_result_code TEXT,
        is_settled INTEGER NOT NULL DEFAULT 0,
        avg_win_odds REAL,
        avg_draw_odds REAL,
        avg_lose_odds REAL,
        avg_win_prob REAL,
        avg_draw_prob REAL,
        avg_lose_prob REAL,
        asian_home_odds REAL,
        asian_line TEXT,
        asian_away_odds REAL,
        kelly_win REAL,
        kelly_draw REAL,
        kelly_lose REAL,
        analysis_url TEXT,
        asian_url TEXT,
        euro_url TEXT,
        source_url TEXT NOT NULL,
        fetched_at TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """

    create_unique_index_sql = """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_sfc500_matches_expect_match_no
    ON sfc500_matches_raw(expect, match_no);
    """

    create_match_time_index_sql = """
    CREATE INDEX IF NOT EXISTS idx_sfc500_matches_match_time
    ON sfc500_matches_raw(match_time);
    """

    create_competition_index_sql = """
    CREATE INDEX IF NOT EXISTS idx_sfc500_matches_competition
    ON sfc500_matches_raw(competition);
    """

    create_home_team_index_sql = """
    CREATE INDEX IF NOT EXISTS idx_sfc500_matches_home_team
    ON sfc500_matches_raw(home_team);
    """

    create_away_team_index_sql = """
    CREATE INDEX IF NOT EXISTS idx_sfc500_matches_away_team
    ON sfc500_matches_raw(away_team);
    """

    target_path = db_path or SFC500_DATABASE_PATH
    with get_sfc500_connection(target_path) as connection:
        connection.execute(create_table_sql)
        _ensure_sfc500_columns(connection)
        connection.execute(create_unique_index_sql)
        connection.execute(create_match_time_index_sql)
        connection.execute(create_competition_index_sql)
        connection.execute(create_home_team_index_sql)
        connection.execute(create_away_team_index_sql)
        connection.commit()

    return target_path


def _ensure_sfc500_columns(connection: sqlite3.Connection) -> None:
    """为已有表补齐当前版本需要的字段。"""

    existing_columns = {
        row[1] for row in connection.execute("PRAGMA table_info(sfc500_matches_raw)")
    }

    required_columns = {
        "is_settled": "INTEGER NOT NULL DEFAULT 0",
    }

    for column_name, column_type in required_columns.items():
        if column_name not in existing_columns:
            connection.execute(
                f"ALTER TABLE sfc500_matches_raw ADD COLUMN {column_name} {column_type}"
            )

    # 旧库没有 is_settled 字段时，会用默认值 0 补列。
    # 这里顺手根据已有比分和赛果把历史记录修正成真实状态。
    connection.execute(
        """
        UPDATE sfc500_matches_raw
        SET is_settled = CASE
            WHEN COALESCE(final_score, '') <> ''
             AND COALESCE(spf_result, '') <> '' THEN 1
            ELSE 0
        END
        WHERE is_settled IS NULL
           OR is_settled NOT IN (0, 1)
           OR (
                is_settled = 0
                AND COALESCE(final_score, '') <> ''
                AND COALESCE(spf_result, '') <> ''
           )
           OR (
                is_settled = 1
                AND (
                    COALESCE(final_score, '') = ''
                    OR COALESCE(spf_result, '') = ''
                )
           )
        """
    )


def build_issue_url(expect: str) -> str:
    """构造指定期次的页面 URL。"""

    return f"{SFC500_BASE_URL}?expect={expect}"


def infer_year_from_expect(expect: str) -> int:
    """从期次前两位推断年份。"""

    if len(expect) < 2 or not expect[:2].isdigit():
        raise ValueError(f"无法从期次推断年份: {expect}")

    return 2000 + int(expect[:2])


def build_full_match_time(expect: str, match_time_raw: str | None) -> str | None:
    """把页面里的 `01-24 01:45` 补成完整年份时间。"""

    if not match_time_raw:
        return None

    year = infer_year_from_expect(expect)
    normalized = f"{year}-{match_time_raw.strip()}"

    try:
        parsed = datetime.strptime(normalized, "%Y-%m-%d %H:%M")
    except ValueError:
        logger.warning("无法解析比赛时间 expect=%s match_time_raw=%s", expect, match_time_raw)
        return None

    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def _parse_float_triplet(raw_value: str | None) -> tuple[float | None, float | None, float | None]:
    """把 `3.37,3.45,2.11` 解析成三元浮点数。"""

    if not raw_value:
        return None, None, None

    parts = [part.strip() for part in raw_value.split(",")]
    if len(parts) < 3:
        return None, None, None

    return _to_float(parts[0]), _to_float(parts[1]), _to_float(parts[2])


def _parse_asian_triplet(raw_value: str | None) -> tuple[float | None, str | None, float | None]:
    """解析 data-asian 字段。"""

    if not raw_value:
        return None, None, None

    parts = [part.strip() for part in raw_value.split(",")]
    if len(parts) < 3:
        return None, None, None

    return _to_float(parts[0]), parts[1] or None, _to_float(parts[2])


def _to_float(raw_value: str | None) -> float | None:
    """把页面里的数字安全转成 float。"""

    if raw_value is None:
        return None

    normalized = raw_value.strip()
    if not normalized or normalized in {"-", "--", "- - -"}:
        return None

    try:
        return float(normalized)
    except ValueError:
        return None


def _build_session() -> requests.Session:
    """创建带基础请求头的 Session。"""

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": REQUEST_USER_AGENT,
            "Referer": SFC500_BASE_URL,
        }
    )
    return session


def fetch_issue_html(
    expect: str,
    *,
    session: requests.Session | None = None,
    timeout: int = REQUEST_TIMEOUT_SECONDS,
    retries: int = DEFAULT_RETRIES,
) -> str:
    """抓取单个期次的 HTML。"""

    source_url = build_issue_url(expect)
    active_session = session or _build_session()
    last_error: Exception | None = None

    for attempt in range(1, retries + 2):
        try:
            response = active_session.get(source_url, timeout=timeout)
            response.raise_for_status()
            response.encoding = response.apparent_encoding or response.encoding
            logger.info("Fetched expect=%s status=%s", expect, response.status_code)
            return response.text
        except requests.RequestException as exc:
            last_error = exc
            logger.warning(
                "Fetch failed expect=%s attempt=%s/%s error=%s",
                expect,
                attempt,
                retries + 1,
                exc,
            )
            if attempt <= retries:
                time.sleep(0.8 * attempt)

    raise RuntimeError(f"抓取 500.com 期次失败 expect={expect}") from last_error


def parse_issue_page(html: str, expect: str) -> list[dict[str, Any]]:
    """解析单个期次页面。"""

    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("#vsTable tr.bet-tb-tr")
    parsed_rows: list[dict[str, Any]] = []
    fetched_at = datetime.now().isoformat(timespec="seconds")
    source_url = build_issue_url(expect)

    for row in rows:
        match_no_text = row.select_one("td.td-no")
        competition_link = row.select_one("td.td-evt a")
        match_time_cell = row.select_one("td.td-endtime")
        home_team_link = row.select_one("td.td-team span.team-l a")
        away_team_link = row.select_one("td.td-team span.team-r a")
        score_link = row.select_one("td.td-team i.team-vs a")

        if not match_no_text or not home_team_link or not away_team_link:
            continue

        avg_win_odds, avg_draw_odds, avg_lose_odds = _parse_float_triplet(
            row.get("data-bjpl")
        )
        avg_win_prob, avg_draw_prob, avg_lose_prob = _parse_float_triplet(
            row.get("data-pjgl")
        )
        asian_home_odds, asian_line, asian_away_odds = _parse_asian_triplet(
            row.get("data-asian")
        )
        kelly_win, kelly_draw, kelly_lose = _parse_float_triplet(row.get("data-kl"))

        spf_result_code = None
        spf_result = None
        selected_result = row.select_one("td.td-betbtn .betbtn-ok")
        if selected_result and selected_result.has_attr("data-opt"):
            spf_result_code = selected_result["data-opt"].strip()
            spf_result = {"3": "胜", "1": "平", "0": "负"}.get(spf_result_code)

        analysis_url = None
        asian_url = None
        euro_url = None
        for link in row.select("td.td-data a"):
            link_text = link.get_text(strip=True)
            link_href = link.get("href")
            if link_text == "析":
                analysis_url = link_href
            elif link_text == "亚":
                asian_url = link_href
            elif link_text == "欧":
                euro_url = link_href

        match_time_raw = match_time_cell.get_text(strip=True) if match_time_cell else None
        final_score = score_link.get_text(strip=True) if score_link else None
        is_settled = bool(final_score and spf_result)

        parsed_rows.append(
            {
                "expect": expect,
                "match_no": int(match_no_text.get_text(strip=True)),
                "competition": competition_link.get_text(strip=True)
                if competition_link
                else None,
                "match_time": build_full_match_time(expect, match_time_raw),
                "match_time_raw": match_time_raw,
                "home_team": home_team_link.get_text(strip=True),
                "away_team": away_team_link.get_text(strip=True),
                "final_score": final_score,
                "spf_result": spf_result,
                "spf_result_code": spf_result_code,
                "is_settled": int(is_settled),
                "avg_win_odds": avg_win_odds,
                "avg_draw_odds": avg_draw_odds,
                "avg_lose_odds": avg_lose_odds,
                "avg_win_prob": avg_win_prob,
                "avg_draw_prob": avg_draw_prob,
                "avg_lose_prob": avg_lose_prob,
                "asian_home_odds": asian_home_odds,
                "asian_line": asian_line,
                "asian_away_odds": asian_away_odds,
                "kelly_win": kelly_win,
                "kelly_draw": kelly_draw,
                "kelly_lose": kelly_lose,
                "analysis_url": analysis_url,
                "asian_url": asian_url,
                "euro_url": euro_url,
                "source_url": source_url,
                "fetched_at": fetched_at,
            }
        )

    logger.info("Parsed expect=%s matches=%s", expect, len(parsed_rows))
    return parsed_rows


def fetch_issue_matches(
    expect: str,
    *,
    session: requests.Session | None = None,
    timeout: int = REQUEST_TIMEOUT_SECONDS,
    retries: int = DEFAULT_RETRIES,
    only_settled: bool = False,
) -> list[dict[str, Any]]:
    """抓取并解析单个期次。"""

    html = fetch_issue_html(expect, session=session, timeout=timeout, retries=retries)
    matches = parse_issue_page(html, expect)

    if only_settled:
        matches = [match for match in matches if match.get("is_settled")]

    return matches


def _count_new_expect_match_keys(
    connection: sqlite3.Connection,
    matches: list[dict[str, Any]],
) -> int:
    """统计本次真正新增了多少 `(expect, match_no)` 记录。"""

    keys = sorted(
        {
            (str(match.get("expect")), int(match.get("match_no")))
            for match in matches
            if match.get("expect") and match.get("match_no") is not None
        }
    )
    if not keys:
        return 0

    existing_keys: set[tuple[str, int]] = set()
    batch_size = 200

    for index in range(0, len(keys), batch_size):
        batch = keys[index : index + batch_size]
        conditions = " OR ".join("(expect = ? AND match_no = ?)" for _ in batch)
        params: list[Any] = []
        for expect, match_no in batch:
            params.extend([expect, match_no])
        rows = connection.execute(
            f"SELECT expect, match_no FROM sfc500_matches_raw WHERE {conditions}",
            params,
        ).fetchall()
        existing_keys.update((str(row["expect"]), int(row["match_no"])) for row in rows)

    return len(set(keys) - existing_keys)


def save_issue_matches(
    matches: list[dict[str, Any]],
    *,
    db_path: Path | None = None,
) -> int:
    """把一个期次的比赛写入数据库。"""

    if not matches:
        return 0

    target_path = db_path or SFC500_DATABASE_PATH
    init_sfc500_db(target_path)

    upsert_sql = """
    INSERT INTO sfc500_matches_raw (
        expect,
        match_no,
        competition,
        match_time,
        match_time_raw,
        home_team,
        away_team,
        final_score,
        spf_result,
        spf_result_code,
        is_settled,
        avg_win_odds,
        avg_draw_odds,
        avg_lose_odds,
        avg_win_prob,
        avg_draw_prob,
        avg_lose_prob,
        asian_home_odds,
        asian_line,
        asian_away_odds,
        kelly_win,
        kelly_draw,
        kelly_lose,
        analysis_url,
        asian_url,
        euro_url,
        source_url,
        fetched_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(expect, match_no) DO UPDATE SET
        competition = excluded.competition,
        match_time = excluded.match_time,
        match_time_raw = excluded.match_time_raw,
        home_team = excluded.home_team,
        away_team = excluded.away_team,
        final_score = excluded.final_score,
        spf_result = excluded.spf_result,
        spf_result_code = excluded.spf_result_code,
        is_settled = excluded.is_settled,
        avg_win_odds = excluded.avg_win_odds,
        avg_draw_odds = excluded.avg_draw_odds,
        avg_lose_odds = excluded.avg_lose_odds,
        avg_win_prob = excluded.avg_win_prob,
        avg_draw_prob = excluded.avg_draw_prob,
        avg_lose_prob = excluded.avg_lose_prob,
        asian_home_odds = excluded.asian_home_odds,
        asian_line = excluded.asian_line,
        asian_away_odds = excluded.asian_away_odds,
        kelly_win = excluded.kelly_win,
        kelly_draw = excluded.kelly_draw,
        kelly_lose = excluded.kelly_lose,
        analysis_url = excluded.analysis_url,
        asian_url = excluded.asian_url,
        euro_url = excluded.euro_url,
        source_url = excluded.source_url,
        fetched_at = excluded.fetched_at
    """

    values = [
        (
            match.get("expect"),
            match.get("match_no"),
            match.get("competition"),
            match.get("match_time"),
            match.get("match_time_raw"),
            match.get("home_team"),
            match.get("away_team"),
            match.get("final_score"),
            match.get("spf_result"),
            match.get("spf_result_code"),
            match.get("is_settled"),
            match.get("avg_win_odds"),
            match.get("avg_draw_odds"),
            match.get("avg_lose_odds"),
            match.get("avg_win_prob"),
            match.get("avg_draw_prob"),
            match.get("avg_lose_prob"),
            match.get("asian_home_odds"),
            match.get("asian_line"),
            match.get("asian_away_odds"),
            match.get("kelly_win"),
            match.get("kelly_draw"),
            match.get("kelly_lose"),
            match.get("analysis_url"),
            match.get("asian_url"),
            match.get("euro_url"),
            match.get("source_url"),
            match.get("fetched_at"),
        )
        for match in matches
    ]

    with get_sfc500_connection(target_path) as connection:
        inserted_rows = _count_new_expect_match_keys(connection, matches)
        connection.executemany(upsert_sql, values)
        connection.commit()

    return inserted_rows


def fetch_and_save_expect(
    expect: str,
    *,
    db_path: Path | None = None,
    session: requests.Session | None = None,
    timeout: int = REQUEST_TIMEOUT_SECONDS,
    retries: int = DEFAULT_RETRIES,
    only_settled: bool = False,
) -> dict[str, Any]:
    """抓取并写入单个期次，返回摘要。"""

    matches = fetch_issue_matches(
        expect,
        session=session,
        timeout=timeout,
        retries=retries,
        only_settled=only_settled,
    )
    inserted_rows = save_issue_matches(matches, db_path=db_path)

    return {
        "expect": expect,
        "rows_fetched": len(matches),
        "rows_inserted": inserted_rows,
        "matches": matches,
    }


def _emit_progress(progress_callback: ProgressCallback | None, **event: Any) -> None:
    """向上层发送同步进度。"""

    if progress_callback is None:
        return
    progress_callback(event)


def sync_year(
    year: int,
    *,
    db_path: Path | None = None,
    start_period: int = 1,
    end_period: int = 399,
    stop_after_empty: int = 10,
    sleep_seconds: float = 0.0,
    timeout: int = REQUEST_TIMEOUT_SECONDS,
    retries: int = DEFAULT_RETRIES,
    only_settled: bool = False,
    progress_callback: ProgressCallback | None = None,
) -> dict[str, Any]:
    """顺序扫描某一年的期次并落库。"""

    prefix = f"{year % 100:02d}"
    target_path = db_path or SFC500_DATABASE_PATH
    init_sfc500_db(target_path)

    fetched_rows_total = 0
    inserted_rows_total = 0
    valid_expect_count = 0
    scanned_expect_count = 0
    empty_streak = 0
    found_any_valid_expect = False
    sample_matches: list[dict[str, Any]] = []

    session = _build_session()

    _emit_progress(
        progress_callback,
        stage="start",
        year=year,
        current_index=0,
        total_windows=end_period - start_period + 1,
        message=f"开始同步 {year} 年，期次范围 {prefix}{start_period:03d} -> {prefix}{end_period:03d}",
    )

    for period in range(start_period, end_period + 1):
        expect = f"{prefix}{period:03d}"
        scanned_expect_count += 1
        _emit_progress(
            progress_callback,
            stage="expect_start",
            year=year,
            expect=expect,
            current_index=scanned_expect_count,
            total_windows=end_period - start_period + 1,
            message=f"正在抓取期次 {expect}",
        )

        matches = fetch_issue_matches(
            expect,
            session=session,
            timeout=timeout,
            retries=retries,
            only_settled=only_settled,
        )

        if matches:
            found_any_valid_expect = True
            valid_expect_count += 1
            empty_streak = 0
            inserted_rows = save_issue_matches(matches, db_path=target_path)
            fetched_rows_total += len(matches)
            inserted_rows_total += inserted_rows

            if len(sample_matches) < 5:
                remaining_slots = 5 - len(sample_matches)
                sample_matches.extend(matches[:remaining_slots])

            logger.info(
                "Saved expect=%s rows_fetched=%s rows_inserted=%s",
                expect,
                len(matches),
                inserted_rows,
            )
            _emit_progress(
                progress_callback,
                stage="expect_done",
                year=year,
                expect=expect,
                current_index=scanned_expect_count,
                total_windows=end_period - start_period + 1,
                rows_fetched=len(matches),
                rows_inserted=inserted_rows,
                message=f"期次 {expect} 完成，抓取 {len(matches)} 条，新增 {inserted_rows} 条。",
            )
        else:
            logger.info("Empty expect=%s", expect)
            _emit_progress(
                progress_callback,
                stage="expect_empty",
                year=year,
                expect=expect,
                current_index=scanned_expect_count,
                total_windows=end_period - start_period + 1,
                message=f"期次 {expect} 没有可落库比赛。",
            )
            if found_any_valid_expect:
                empty_streak += 1
                if empty_streak >= stop_after_empty:
                    logger.info(
                        "Stopping year sync year=%s at expect=%s because empty_streak=%s",
                        year,
                        expect,
                        empty_streak,
                    )
                    break

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    summary = {
        "year": year,
        "db_path": str(target_path),
        "rows_fetched": fetched_rows_total,
        "rows_inserted": inserted_rows_total,
        "valid_expects": valid_expect_count,
        "scanned_expects": scanned_expect_count,
        "sample_matches": sample_matches,
        "start_period": start_period,
        "end_period": end_period,
    }
    _emit_progress(
        progress_callback,
        stage="finish",
        current_index=scanned_expect_count,
        total_windows=end_period - start_period + 1,
        message=(
            f"{year} 年同步完成，扫描 {scanned_expect_count} 个期次，"
            f"抓取 {fetched_rows_total} 条，新增 {inserted_rows_total} 条。"
        ),
        **summary,
    )
    return summary


def sync_year_range(
    start_year: int,
    end_year: int,
    *,
    db_path: Path | None = None,
    first_year_start_period: int = 1,
    last_year_end_period: int = 399,
    stop_after_empty: int = 10,
    sleep_seconds: float = 0.0,
    timeout: int = REQUEST_TIMEOUT_SECONDS,
    retries: int = DEFAULT_RETRIES,
    only_settled: bool = False,
) -> dict[str, Any]:
    """按年份区间顺序回填。"""

    if end_year < start_year:
        raise ValueError("end_year 不能早于 start_year。")

    target_path = db_path or SFC500_DATABASE_PATH
    init_sfc500_db(target_path)

    year_summaries: list[dict[str, Any]] = []
    total_rows_fetched = 0
    total_rows_inserted = 0
    total_valid_expects = 0
    total_scanned_expects = 0

    for year in range(start_year, end_year + 1):
        start_period = first_year_start_period if year == start_year else 1
        end_period = last_year_end_period if year == end_year else 399
        summary = sync_year(
            year,
            db_path=target_path,
            start_period=start_period,
            end_period=end_period,
            stop_after_empty=stop_after_empty,
            sleep_seconds=sleep_seconds,
            timeout=timeout,
            retries=retries,
            only_settled=only_settled,
        )
        year_summaries.append(summary)
        total_rows_fetched += int(summary["rows_fetched"])
        total_rows_inserted += int(summary["rows_inserted"])
        total_valid_expects += int(summary["valid_expects"])
        total_scanned_expects += int(summary["scanned_expects"])

    return {
        "start_year": start_year,
        "end_year": end_year,
        "db_path": str(target_path),
        "rows_fetched": total_rows_fetched,
        "rows_inserted": total_rows_inserted,
        "valid_expects": total_valid_expects,
        "scanned_expects": total_scanned_expects,
        "year_summaries": year_summaries,
        "sample_matches": [
            match
            for summary in year_summaries
            for match in summary.get("sample_matches", [])
        ][:5],
    }


def _get_recent_expect_anchors(
    days: int,
    *,
    db_path: Path | None = None,
) -> list[int]:
    """从本地库估算最近同步应覆盖的期次锚点。"""

    target_path = db_path or SFC500_DATABASE_PATH
    if not target_path.exists():
        return []

    today = datetime.now().date()
    lower_bound = (today - timedelta(days=days + 7)).isoformat() + " 00:00:00"
    upper_bound = (today + timedelta(days=7)).isoformat() + " 23:59:59"

    with get_sfc500_connection(target_path) as connection:
        expect_rows = connection.execute(
            """
            SELECT DISTINCT CAST(expect AS INTEGER) AS expect_num
            FROM sfc500_matches_raw
            WHERE match_time >= ?
              AND match_time <= ?
            ORDER BY expect_num
            """,
            (lower_bound, upper_bound),
        ).fetchall()
        max_expect_row = connection.execute(
            "SELECT MAX(CAST(expect AS INTEGER)) AS max_expect FROM sfc500_matches_raw"
        ).fetchone()

    anchors = [
        int(row["expect_num"])
        for row in expect_rows
        if row["expect_num"] is not None
    ]
    if max_expect_row and max_expect_row["max_expect"] is not None:
        anchors.append(int(max_expect_row["max_expect"]))

    return sorted(set(anchors))


def _expand_expect_candidates(
    anchor_expects: list[int],
    *,
    before: int,
    after: int,
) -> list[str]:
    """围绕锚点期次扩展出一组候选期次。"""

    candidate_values: set[int] = set()

    for anchor in anchor_expects:
        for delta in range(-before, after + 1):
            candidate = anchor + delta
            if candidate <= 0:
                continue
            candidate_values.add(candidate)

    return [f"{value:05d}" for value in sorted(candidate_values, reverse=True)]


def sync_recent_history(
    days: int,
    *,
    db_path: Path | None = None,
    timeout: int = REQUEST_TIMEOUT_SECONDS,
    retries: int = DEFAULT_RETRIES,
    only_settled: bool = False,
    progress_callback: ProgressCallback | None = None,
) -> dict[str, Any]:
    """按最近天数做一轮 500.com 历史赔率增量同步。"""

    if days <= 0:
        raise ValueError("days 必须大于 0。")

    target_path = db_path or SFC500_DATABASE_PATH
    init_sfc500_db(target_path)

    anchor_expects = _get_recent_expect_anchors(days, db_path=target_path)
    if not anchor_expects:
        current_year_prefix = int(datetime.now().strftime("%y"))
        anchor_expects = [current_year_prefix * 1000 + 50]

    before = max(4, min(12, days // 3 + 4))
    after = max(8, min(18, days // 2 + 6))
    candidate_expects = _expand_expect_candidates(
        anchor_expects,
        before=before,
        after=after,
    )

    session = _build_session()
    rows_fetched_total = 0
    rows_inserted_total = 0
    valid_expect_count = 0
    errors: list[str] = []
    sample_matches: list[dict[str, Any]] = []

    _emit_progress(
        progress_callback,
        stage="start",
        days=days,
        current_index=0,
        total_windows=len(candidate_expects),
        message=f"开始同步最近 {days} 天，候选期次 {len(candidate_expects)} 个。",
    )

    for index, expect in enumerate(candidate_expects, start=1):
        _emit_progress(
            progress_callback,
            stage="expect_start",
            days=days,
            expect=expect,
            current_index=index,
            total_windows=len(candidate_expects),
            message=f"正在抓取期次 {expect}",
        )
        try:
            matches = fetch_issue_matches(
                expect,
                session=session,
                timeout=timeout,
                retries=retries,
                only_settled=only_settled,
            )
            inserted_rows = save_issue_matches(matches, db_path=target_path)
            rows_fetched_total += len(matches)
            rows_inserted_total += inserted_rows

            if matches:
                valid_expect_count += 1
                if len(sample_matches) < 5:
                    remaining_slots = 5 - len(sample_matches)
                    sample_matches.extend(matches[:remaining_slots])

            _emit_progress(
                progress_callback,
                stage="expect_done",
                days=days,
                expect=expect,
                current_index=index,
                total_windows=len(candidate_expects),
                rows_fetched=len(matches),
                rows_inserted=inserted_rows,
                message=f"期次 {expect} 完成，抓取 {len(matches)} 条，新增 {inserted_rows} 条。",
            )
        except Exception as exc:
            error_message = f"期次 {expect} 同步失败：{exc}"
            logger.exception(error_message)
            errors.append(error_message)
            _emit_progress(
                progress_callback,
                stage="expect_error",
                days=days,
                expect=expect,
                current_index=index,
                total_windows=len(candidate_expects),
                message=error_message,
            )

    summary = {
        "days": days,
        "db_path": str(target_path),
        "rows_fetched": rows_fetched_total,
        "rows_inserted": rows_inserted_total,
        "valid_expects": valid_expect_count,
        "scanned_expects": len(candidate_expects),
        "sample_matches": sample_matches,
        "candidate_expects": candidate_expects,
        "errors": errors,
        "status": "success" if not errors else "partial_success",
    }

    _emit_progress(
        progress_callback,
        stage="finish",
        days=days,
        current_index=len(candidate_expects),
        total_windows=len(candidate_expects),
        message=(
            f"最近 {days} 天同步完成，扫描 {len(candidate_expects)} 个期次，"
            f"抓取 {rows_fetched_total} 条，新增 {rows_inserted_total} 条。"
        ),
        **summary,
    )
    return summary


def get_sfc500_history_overview(db_path: Path | None = None) -> dict[str, Any]:
    """返回历史赔率库的总体概览。"""

    target_path = db_path or SFC500_DATABASE_PATH
    init_sfc500_db(target_path)

    with get_sfc500_connection(target_path) as connection:
        row = connection.execute(
            """
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT expect) AS expect_count,
                COUNT(DISTINCT competition) AS competition_count,
                SUM(CASE WHEN is_settled = 1 THEN 1 ELSE 0 END) AS settled_count,
                MIN(match_time) AS min_match_time,
                MAX(match_time) AS max_match_time
            FROM sfc500_matches_raw
            """
        ).fetchone()

    if row is None:
        return {
            "row_count": 0,
            "expect_count": 0,
            "competition_count": 0,
            "settled_count": 0,
            "min_match_time": None,
            "max_match_time": None,
        }

    return {
        "row_count": int(row["row_count"] or 0),
        "expect_count": int(row["expect_count"] or 0),
        "competition_count": int(row["competition_count"] or 0),
        "settled_count": int(row["settled_count"] or 0),
        "min_match_time": row["min_match_time"],
        "max_match_time": row["max_match_time"],
    }


def get_sfc500_filter_options(db_path: Path | None = None) -> dict[str, list[str]]:
    """返回历史赔率筛选项。"""

    target_path = db_path or SFC500_DATABASE_PATH
    init_sfc500_db(target_path)

    with get_sfc500_connection(target_path) as connection:
        competition_rows = connection.execute(
            """
            SELECT DISTINCT competition
            FROM sfc500_matches_raw
            WHERE competition IS NOT NULL AND competition <> ''
            ORDER BY competition
            """
        ).fetchall()
        team_rows = connection.execute(
            """
            SELECT team
            FROM (
                SELECT home_team AS team FROM sfc500_matches_raw
                UNION
                SELECT away_team AS team FROM sfc500_matches_raw
            )
            WHERE team IS NOT NULL AND team <> ''
            ORDER BY team
            """
        ).fetchall()

    return {
        "competitions": [str(row["competition"]) for row in competition_rows],
        "teams": [str(row["team"]) for row in team_rows],
    }


def query_sfc500_matches(
    *,
    db_path: Path | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    competitions: list[str] | None = None,
    teams: list[str] | None = None,
    team_keyword: str | None = None,
    expect: str | None = None,
    settled_only: bool | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    """按条件查询 500.com 历史赔率与赛果。"""

    target_path = db_path or SFC500_DATABASE_PATH
    init_sfc500_db(target_path)

    where_clauses: list[str] = []
    params: list[Any] = []

    if start_date:
        where_clauses.append("match_time >= ?")
        params.append(f"{start_date} 00:00:00")

    if end_date:
        next_day = date.fromisoformat(end_date) + timedelta(days=1)
        where_clauses.append("match_time < ?")
        params.append(f"{next_day.isoformat()} 00:00:00")

    if competitions:
        placeholders = ", ".join("?" for _ in competitions)
        where_clauses.append(f"competition IN ({placeholders})")
        params.extend(competitions)

    if teams:
        placeholders = ", ".join("?" for _ in teams)
        where_clauses.append(
            f"(home_team IN ({placeholders}) OR away_team IN ({placeholders}))"
        )
        params.extend(teams)
        params.extend(teams)

    normalized_team_keyword = (team_keyword or "").strip()
    if normalized_team_keyword:
        where_clauses.append("(home_team LIKE ? OR away_team LIKE ?)")
        like_value = f"%{normalized_team_keyword}%"
        params.extend([like_value, like_value])

    normalized_expect = (expect or "").strip()
    if normalized_expect:
        where_clauses.append("expect = ?")
        params.append(normalized_expect)

    if settled_only is True:
        where_clauses.append("is_settled = 1")
    elif settled_only is False:
        where_clauses.append("is_settled = 0")

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    count_sql = f"""
    SELECT COUNT(*) AS total_count
    FROM sfc500_matches_raw
    {where_sql}
    """

    query_sql = f"""
    SELECT
        expect,
        match_no,
        competition,
        match_time,
        home_team,
        away_team,
        final_score,
        spf_result,
        is_settled,
        avg_win_odds,
        avg_draw_odds,
        avg_lose_odds,
        avg_win_prob,
        avg_draw_prob,
        avg_lose_prob,
        asian_home_odds,
        asian_line,
        asian_away_odds,
        kelly_win,
        kelly_draw,
        kelly_lose,
        analysis_url,
        asian_url,
        euro_url,
        source_url,
        fetched_at
    FROM sfc500_matches_raw
    {where_sql}
    ORDER BY match_time DESC, expect DESC, match_no ASC
    LIMIT ?
    """

    with get_sfc500_connection(target_path) as connection:
        count_row = connection.execute(count_sql, params).fetchone()
        rows = connection.execute(query_sql, [*params, limit]).fetchall()

    return {
        "rows": [dict(row) for row in rows],
        "total_count": int(count_row["total_count"] or 0) if count_row else 0,
    }
