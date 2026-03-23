"""500.com 球队页历史比赛抓取、落库和概览。"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from datetime import datetime
from datetime import timedelta
import json
import logging
from pathlib import Path
import random
import re
import sqlite3
import time
from typing import Any
from urllib.parse import quote

from bs4 import BeautifulSoup
import requests

from jczq_assistant.config import (
    APP_READ_ONLY,
    DATA_DIR,
    REQUEST_TIMEOUT_SECONDS,
    REQUEST_USER_AGENT,
    SFC500_TEAM_HISTORY_SNAPSHOT_URL,
)
from jczq_assistant.snapshot_bootstrap import ensure_sqlite_snapshot


logger = logging.getLogger(__name__)

SFC500_TEAM_HISTORY_BASE_URL = "https://liansai.500.com/team/"
SFC500_TEAM_HISTORY_AJAX_FIXTURE_URL = "https://liansai.500.com/index.php?c=teams&a=ajax_fixture"
SFC500_LIVE_SCORE_URL = "https://live.500.com/"
SFC500_TEAM_HISTORY_DATABASE_PATH = DATA_DIR / "sfc500_team_history.sqlite3"
DEFAULT_TEAM_ID_END = 26400
DEFAULT_TEAM_SCAN_RETRIES = 2
DEFAULT_TEAM_FIXTURE_RECORDS = 100
DEFAULT_LIVE_RECENT_SYNC_DAYS = 3
MAX_LIVE_RECENT_SYNC_DAYS = 7
LIVE_MATCH_STATUS_LABELS = {
    "0": "未开场",
    "1": "上半场",
    "2": "中场",
    "3": "下半场",
    "4": "已完场",
}
DEFAULT_LIVE_RECOMMENDATION_STATUS_CODES = {"0", "1", "2", "3"}
ProgressCallback = Callable[[dict[str, Any]], None]
TRANSIENT_HTTP_STATUS_CODES = {429, 500, 502, 503, 504}


def get_sfc500_team_history_connection(
    db_path: Path | None = None,
) -> sqlite3.Connection:
    """返回球队大库连接。"""

    target_path = db_path or SFC500_TEAM_HISTORY_DATABASE_PATH
    if APP_READ_ONLY:
        ensure_sfc500_team_history_db_available(target_path)
        encoded_path = quote(str(target_path.resolve()), safe="/")
        connection = sqlite3.connect(f"file:{encoded_path}?mode=ro", uri=True)
    else:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(target_path)
    connection.row_factory = sqlite3.Row
    return connection


def ensure_sfc500_team_history_db_available(db_path: Path | None = None) -> Path:
    """根据当前模式确保球队大库可用。"""

    target_path = db_path or SFC500_TEAM_HISTORY_DATABASE_PATH
    if APP_READ_ONLY:
        ensure_sqlite_snapshot(
            target_path=target_path,
            snapshot_url=SFC500_TEAM_HISTORY_SNAPSHOT_URL,
        )
        if not target_path.exists():
            raise FileNotFoundError(
                f"只读模式下未找到球队大库: {target_path}。"
                "请提供 data/sfc500_team_history.sqlite3，"
                "或在部署 secrets 里设置 SFC500_TEAM_HISTORY_SNAPSHOT_URL。"
            )
        return target_path
    return init_sfc500_team_history_db(target_path)


def is_sfc500_team_history_db_available(db_path: Path | None = None) -> bool:
    """判断球队大库当前是否可用。"""

    try:
        ensure_sfc500_team_history_db_available(db_path)
    except FileNotFoundError:
        return False
    return True


def init_sfc500_team_history_db(db_path: Path | None = None) -> Path:
    """初始化球队大库。"""

    create_teams_sql = """
    CREATE TABLE IF NOT EXISTS sfc500_teams (
        team_id INTEGER PRIMARY KEY,
        team_name TEXT,
        source_url TEXT NOT NULL,
        teamfixture_url TEXT NOT NULL,
        page_title TEXT,
        last_team_fetch_at TEXT,
        last_fixture_fetch_at TEXT,
        last_rows_fetched INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """

    create_scan_state_sql = """
    CREATE TABLE IF NOT EXISTS sfc500_team_scan_state (
        team_id INTEGER PRIMARY KEY,
        is_valid INTEGER NOT NULL DEFAULT 0,
        team_name TEXT,
        last_checked_at TEXT NOT NULL,
        notes TEXT
    );
    """

    create_matches_sql = """
    CREATE TABLE IF NOT EXISTS sfc500_team_matches_raw (
        fixture_id INTEGER PRIMARY KEY,
        season_id INTEGER,
        match_id INTEGER,
        match_time TEXT,
        match_date TEXT,
        competition TEXT,
        competition_full_name TEXT,
        competition_url TEXT,
        home_team_id INTEGER,
        away_team_id INTEGER,
        home_team TEXT,
        away_team TEXT,
        home_team_canonical TEXT,
        away_team_canonical TEXT,
        home_score INTEGER,
        away_score INTEGER,
        home_ht_score INTEGER,
        away_ht_score INTEGER,
        final_score TEXT,
        half_time_score TEXT,
        spf_result TEXT,
        spf_result_code TEXT,
        is_settled INTEGER NOT NULL DEFAULT 0,
        avg_win_odds REAL,
        avg_draw_odds REAL,
        avg_lose_odds REAL,
        asian_handicap_line TEXT,
        asian_handicap_name TEXT,
        asian_home_odds REAL,
        asian_away_odds REAL,
        pan_result TEXT,
        over_under_result TEXT,
        analysis_url TEXT,
        source_team_id INTEGER,
        source_url TEXT NOT NULL,
        fetched_at TEXT NOT NULL,
        raw_payload TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """

    create_indexes = [
        """
        CREATE INDEX IF NOT EXISTS idx_sfc500_team_matches_match_time
        ON sfc500_team_matches_raw(match_time);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_sfc500_team_matches_competition
        ON sfc500_team_matches_raw(competition);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_sfc500_team_matches_home_team
        ON sfc500_team_matches_raw(home_team);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_sfc500_team_matches_away_team
        ON sfc500_team_matches_raw(away_team);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_sfc500_team_matches_home_team_id
        ON sfc500_team_matches_raw(home_team_id);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_sfc500_team_matches_away_team_id
        ON sfc500_team_matches_raw(away_team_id);
        """,
    ]

    target_path = db_path or SFC500_TEAM_HISTORY_DATABASE_PATH
    with get_sfc500_team_history_connection(target_path) as connection:
        connection.execute(create_teams_sql)
        connection.execute(create_scan_state_sql)
        connection.execute(create_matches_sql)
        for index_sql in create_indexes:
            connection.execute(index_sql)
        connection.commit()

    return target_path


def build_team_home_url(team_id: int) -> str:
    return f"{SFC500_TEAM_HISTORY_BASE_URL}{team_id}/"


def build_team_fixture_url(team_id: int) -> str:
    return f"{SFC500_TEAM_HISTORY_BASE_URL}{team_id}/teamfixture/"


def build_live_score_url(expect_date: date | None = None) -> str:
    if expect_date is None:
        return SFC500_LIVE_SCORE_URL
    return f"{SFC500_LIVE_SCORE_URL}?e={expect_date.isoformat()}"


def _build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": REQUEST_USER_AGENT,
            "Referer": SFC500_TEAM_HISTORY_BASE_URL,
        }
    )
    return session


def _fetch_html(
    url: str,
    *,
    session: requests.Session | None = None,
    timeout: int = REQUEST_TIMEOUT_SECONDS,
    retries: int = DEFAULT_TEAM_SCAN_RETRIES,
) -> str:
    """抓取并按站点编码解码 HTML。"""

    active_session = session or _build_session()
    last_error: Exception | None = None

    for attempt in range(1, retries + 2):
        try:
            response = active_session.get(url, timeout=timeout)
            response.raise_for_status()
            return response.content.decode("gb18030", errors="ignore")
        except requests.RequestException as exc:
            last_error = exc
            logger.warning("Fetch failed url=%s attempt=%s error=%s", url, attempt, exc)
            if attempt <= retries:
                time.sleep(_compute_retry_delay(exc, attempt))

    raise RuntimeError(f"抓取页面失败: {url}") from last_error


def _fetch_team_fixture_json(
    team_id: int,
    *,
    session: requests.Session | None = None,
    timeout: int = REQUEST_TIMEOUT_SECONDS,
    retries: int = DEFAULT_TEAM_SCAN_RETRIES,
    records: int = DEFAULT_TEAM_FIXTURE_RECORDS,
    hoa: int = 0,
) -> dict[str, Any]:
    """抓取球队赛程 AJAX 数据。"""

    active_session = session or _build_session()
    last_error: Exception | None = None
    target_records = records if records in {10, 30, 50, 100} else DEFAULT_TEAM_FIXTURE_RECORDS
    params = {"tid": team_id, "records": target_records, "hoa": hoa}
    headers = {"Referer": build_team_fixture_url(team_id)}

    for attempt in range(1, retries + 2):
        try:
            response = active_session.get(
                SFC500_TEAM_HISTORY_AJAX_FIXTURE_URL,
                params=params,
                headers=headers,
                timeout=timeout,
            )
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            logger.warning(
                "Fetch ajax fixture failed team_id=%s records=%s attempt=%s error=%s",
                team_id,
                target_records,
                attempt,
                exc,
            )
            if attempt <= retries:
                time.sleep(_compute_retry_delay(exc, attempt))

    raise RuntimeError(
        f"抓取球队赛程 AJAX 失败: team_id={team_id} records={target_records}"
    ) from last_error


def _parse_title_text(html: str) -> str:
    matched = re.search(r"<title>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
    if not matched:
        return ""
    return matched.group(1).strip()


def _extract_status_code(exc: Exception) -> int | None:
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    return int(status_code) if isinstance(status_code, int) else None


def _is_transient_exception(exc: Exception) -> bool:
    status_code = _extract_status_code(exc)
    if status_code in TRANSIENT_HTTP_STATUS_CODES:
        return True
    return isinstance(
        exc,
        (
            requests.Timeout,
            requests.ConnectionError,
            requests.exceptions.ChunkedEncodingError,
        ),
    )


def _compute_retry_delay(exc: Exception, attempt: int) -> float:
    status_code = _extract_status_code(exc)
    if status_code in {429, 503}:
        return min(30.0, 5.0 * attempt + random.uniform(0.2, 1.0))
    if status_code in {500, 502, 504}:
        return min(20.0, 2.5 * attempt + random.uniform(0.2, 0.8))
    if _is_transient_exception(exc):
        return min(12.0, 1.5 * attempt + random.uniform(0.1, 0.5))
    return 0.8 * attempt


def _extract_team_name_from_title(title: str) -> str | None:
    normalized = title.strip().replace(" - 500彩票网", "")
    for marker in ("赛程表_", "赛程_"):
        if marker in normalized:
            team_name = normalized.split(marker, 1)[0].strip()
            if team_name:
                return team_name
    return None


def parse_team_home_page(html: str, team_id: int) -> dict[str, Any] | None:
    """解析球队主页，返回标准队名。"""

    title = _parse_title_text(html)
    team_name = _extract_team_name_from_title(title)
    if not team_name:
        return None

    fetched_at = datetime.now().isoformat(timespec="seconds")
    return {
        "team_id": team_id,
        "team_name": team_name,
        "source_url": build_team_home_url(team_id),
        "teamfixture_url": build_team_fixture_url(team_id),
        "page_title": title,
        "last_team_fetch_at": fetched_at,
    }


def _normalize_absolute_url(url: str | None) -> str | None:
    if not url:
        return None
    if url.startswith("//"):
        return f"https:{url}"
    return url


def _normalize_analysis_url(url: str | None) -> str | None:
    if not url:
        return None
    if url.startswith("//"):
        return f"https:{url}"
    return url


def _build_spf_result(home_score: int | None, away_score: int | None) -> tuple[str | None, str | None]:
    if home_score is None or away_score is None:
        return None, None
    if home_score > away_score:
        return "胜", "3"
    if home_score == away_score:
        return "平", "1"
    return "负", "0"


def parse_team_fixture_payload(
    payload: dict[str, Any],
    *,
    source_team_id: int,
    source_team_name: str | None,
) -> dict[str, Any]:
    """解析球队赛程 AJAX 返回。"""

    fetched_at = datetime.now().isoformat(timespec="seconds")
    raw_matches = list(payload.get("list") or [])
    matches: list[dict[str, Any]] = []
    discovered_teams: dict[int, str] = {}
    source_name_counts: dict[str, int] = {}

    for item in raw_matches:
        fixture_id = _to_optional_int(item.get("FIXTUREID"))
        if fixture_id is None or fixture_id <= 0:
            continue

        home_team_id = _to_optional_int(item.get("HOMETEAMID"))
        away_team_id = _to_optional_int(item.get("AWAYTEAMID"))
        home_team = str(item.get("HOMETEAMSXNAME") or "").strip() or None
        away_team = str(item.get("AWAYTEAMSXNAME") or "").strip() or None
        if home_team_id == source_team_id and source_team_name:
            home_team = source_team_name
        if away_team_id == source_team_id and source_team_name:
            away_team = source_team_name

        if home_team_id == source_team_id and home_team:
            source_name_counts[home_team] = source_name_counts.get(home_team, 0) + 1
        if away_team_id == source_team_id and away_team:
            source_name_counts[away_team] = source_name_counts.get(away_team, 0) + 1

        if home_team_id and home_team:
            discovered_teams[home_team_id] = home_team
        if away_team_id and away_team:
            discovered_teams[away_team_id] = away_team

        home_score = _to_optional_int(item.get("HOMESCORE"))
        away_score = _to_optional_int(item.get("AWAYSCORE"))
        home_ht_score = _to_optional_int(item.get("HOMEHTSCORE"))
        away_ht_score = _to_optional_int(item.get("AWAYHTSCORE"))
        spf_result, spf_result_code = _build_spf_result(home_score, away_score)
        is_settled = int(home_score is not None and away_score is not None)
        match_time = _normalize_match_time(str(item.get("VSDATE") or ""))
        match_date = str(item.get("MATCHDATE") or "").strip() or None
        season_id = _to_optional_int(item.get("SEASONID"))

        raw_payload = item.get("h_str")
        if raw_payload:
            try:
                payload_json = json.loads(raw_payload)
            except json.JSONDecodeError:
                payload_json = item
        else:
            payload_json = item

        matches.append(
            {
                "fixture_id": fixture_id,
                "season_id": season_id,
                "match_id": _to_optional_int(item.get("MATCHID")),
                "match_time": match_time,
                "match_date": match_date,
                "competition": str(item.get("SIMPLEGBNAME") or "").strip() or None,
                "competition_full_name": str(item.get("MATCHGBNAME") or "").strip() or None,
                "competition_url": (
                    f"https://liansai.500.com/zuqiu-{season_id}/" if season_id else None
                ),
                "home_team_id": home_team_id,
                "away_team_id": away_team_id,
                "home_team": home_team,
                "away_team": away_team,
                "home_team_canonical": home_team,
                "away_team_canonical": away_team,
                "home_score": home_score,
                "away_score": away_score,
                "home_ht_score": home_ht_score,
                "away_ht_score": away_ht_score,
                "final_score": _format_score(home_score, away_score),
                "half_time_score": _format_score(home_ht_score, away_ht_score),
                "spf_result": spf_result,
                "spf_result_code": spf_result_code,
                "is_settled": is_settled,
                "avg_win_odds": _to_optional_float(item.get("WIN")),
                "avg_draw_odds": _to_optional_float(item.get("DRAW")),
                "avg_lose_odds": _to_optional_float(item.get("LOST")),
                "asian_handicap_line": str(item.get("HANDICAPLINE") or "").strip() or None,
                "asian_handicap_name": str(item.get("HANDICAPLINENAME") or "").strip() or None,
                "asian_home_odds": _to_optional_float(item.get("HOMEMONEYLINE")),
                "asian_away_odds": _to_optional_float(item.get("AWAYMONEYLINE")),
                "pan_result": str(item.get("PAN") or "").strip() or None,
                "over_under_result": str(item.get("BS") or "").strip() or None,
                "analysis_url": _normalize_analysis_url(
                    f"https://odds.500.com/fenxi/shuju-{fixture_id}.shtml"
                ),
                "source_team_id": source_team_id,
                "source_url": build_team_fixture_url(source_team_id),
                "fetched_at": fetched_at,
                "raw_payload": json.dumps(payload_json, ensure_ascii=False, sort_keys=True),
            }
        )

    return {
        "matches": matches,
        "discovered_teams": discovered_teams,
        "rows_fetched": len(matches),
        "source_team_name": (
            max(source_name_counts.items(), key=lambda item: (item[1], len(item[0])))[0]
            if source_name_counts
            else None
        ),
    }


def parse_team_fixture_page(
    html: str,
    *,
    source_team_id: int,
    source_team_name: str | None,
) -> dict[str, Any]:
    """解析球队近期 100 场页面。"""

    soup = BeautifulSoup(html, "html.parser")
    title = _parse_title_text(html)
    team_name_from_title = _extract_team_name_from_title(title)
    row_nodes = soup.select("tr[id][data]")
    fetched_at = datetime.now().isoformat(timespec="seconds")

    team_record = None
    if team_name_from_title:
        team_record = {
            "team_id": source_team_id,
            "team_name": team_name_from_title,
            "source_url": build_team_home_url(source_team_id),
            "teamfixture_url": build_team_fixture_url(source_team_id),
            "page_title": title,
            "last_fixture_fetch_at": fetched_at,
            "last_rows_fetched": 0,
        }

    matches: list[dict[str, Any]] = []
    discovered_teams: dict[int, str] = {}
    source_name_counts: dict[str, int] = {}

    for row in row_nodes:
        raw_payload = row.get("data")
        if not raw_payload:
            continue
        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError:
            continue

        fixture_id = int(payload.get("FIXTUREID") or 0)
        if fixture_id <= 0:
            continue

        links = row.select("a")
        competition_url = None
        analysis_url = None
        team_links: list[tuple[int, str]] = []
        for link in links:
            href = link.get("href")
            text = link.get_text(" ", strip=True)
            if not href:
                continue
            if "/zuqiu-" in href and competition_url is None:
                competition_url = href
            if "/team/" in href:
                matched = re.search(r"/team/(\d+)/", href)
                if matched:
                    team_links.append((int(matched.group(1)), text))
            if "fenxi/shuju-" in href:
                analysis_url = _normalize_analysis_url(href)

        home_team_id = int(payload.get("HOMETEAMID") or 0) or None
        away_team_id = int(payload.get("AWAYTEAMID") or 0) or None
        home_team = next((name for team_id, name in team_links if team_id == home_team_id), None)
        away_team = next((name for team_id, name in team_links if team_id == away_team_id), None)

        if home_team_id and home_team:
            discovered_teams[home_team_id] = home_team
        if away_team_id and away_team:
            discovered_teams[away_team_id] = away_team
        if home_team_id == source_team_id and home_team:
            source_name_counts[home_team] = source_name_counts.get(home_team, 0) + 1
        if away_team_id == source_team_id and away_team:
            source_name_counts[away_team] = source_name_counts.get(away_team, 0) + 1

        home_score = _to_optional_int(payload.get("HOMESCORE"))
        away_score = _to_optional_int(payload.get("AWAYSCORE"))
        home_ht_score = _to_optional_int(payload.get("HOMEHTSCORE"))
        away_ht_score = _to_optional_int(payload.get("AWAYHTSCORE"))
        spf_result, spf_result_code = _build_spf_result(home_score, away_score)
        is_settled = int(home_score is not None and away_score is not None)
        match_time = _normalize_match_time(str(payload.get("VSDATE") or ""))
        match_date = str(payload.get("MATCHDATE") or "") or None

        matches.append(
            {
                "fixture_id": fixture_id,
                "season_id": _to_optional_int(payload.get("SEASONID")),
                "match_id": _to_optional_int(payload.get("MATCHID")),
                "match_time": match_time,
                "match_date": match_date,
                "competition": str(payload.get("SIMPLEGBNAME") or "").strip() or None,
                "competition_full_name": str(payload.get("MATCHGBNAME") or "").strip() or None,
                "competition_url": competition_url,
                "home_team_id": home_team_id,
                "away_team_id": away_team_id,
                "home_team": home_team or str(payload.get("HOMETEAMSXNAME") or "").strip() or None,
                "away_team": away_team or str(payload.get("AWAYTEAMSXNAME") or "").strip() or None,
                "home_team_canonical": home_team or str(payload.get("HOMETEAMSXNAME") or "").strip() or None,
                "away_team_canonical": away_team or str(payload.get("AWAYTEAMSXNAME") or "").strip() or None,
                "home_score": home_score,
                "away_score": away_score,
                "home_ht_score": home_ht_score,
                "away_ht_score": away_ht_score,
                "final_score": _format_score(home_score, away_score),
                "half_time_score": _format_score(home_ht_score, away_ht_score),
                "spf_result": spf_result,
                "spf_result_code": spf_result_code,
                "is_settled": is_settled,
                "avg_win_odds": _to_optional_float(payload.get("WIN")),
                "avg_draw_odds": _to_optional_float(payload.get("DRAW")),
                "avg_lose_odds": _to_optional_float(payload.get("LOST")),
                "asian_handicap_line": str(payload.get("HANDICAPLINE") or "").strip() or None,
                "asian_handicap_name": str(payload.get("HANDICAPLINENAME") or "").strip() or None,
                "asian_home_odds": _to_optional_float(payload.get("HOMEMONEYLINE")),
                "asian_away_odds": _to_optional_float(payload.get("AWAYMONEYLINE")),
                "pan_result": str(payload.get("PAN") or "").strip() or None,
                "over_under_result": str(payload.get("BS") or "").strip() or None,
                "analysis_url": analysis_url,
                "source_team_id": source_team_id,
                "source_url": build_team_fixture_url(source_team_id),
                "fetched_at": fetched_at,
                "raw_payload": json.dumps(payload, ensure_ascii=False, sort_keys=True),
            }
        )

    if team_record is not None:
        team_record["last_rows_fetched"] = len(matches)
    inferred_source_team_name = (
        max(source_name_counts.items(), key=lambda item: (item[1], len(item[0])))[0]
        if source_name_counts
        else None
    )

    return {
        "team": team_record,
        "matches": matches,
        "discovered_teams": discovered_teams,
        "source_team_name": inferred_source_team_name,
        "rows_fetched": len(matches),
    }


def _extract_live_odds_map(html: str) -> dict[str, Any]:
    matched = re.search(
        r"var\s+liveOddsList\s*=\s*(\{.*?\})\s*;\s*</script>",
        html,
        flags=re.DOTALL,
    )
    if not matched:
        return {}
    try:
        payload = json.loads(matched.group(1))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _extract_live_page_date(html: str) -> date | None:
    matched = re.search(r"window\.live_json_path\s*=\s*'jczq/(\d{8})'", html)
    if matched:
        return datetime.strptime(matched.group(1), "%Y%m%d").date()
    matched = re.search(r'<option value="(\d{4}-\d{2}-\d{2})"', html)
    if matched:
        return datetime.strptime(matched.group(1), "%Y-%m-%d").date()
    return None


def _parse_live_score_cell(score_cell: BeautifulSoup) -> tuple[int | None, int | None, str | None]:
    anchors = score_cell.find_all("a")
    if len(anchors) < 3:
        return None, None, None
    home_score = _to_optional_int(anchors[0].get_text(" ", strip=True))
    handicap_name = anchors[1].get_text(" ", strip=True) or None
    away_score = _to_optional_int(anchors[2].get_text(" ", strip=True))
    return home_score, away_score, handicap_name


def _parse_live_half_time_score(raw_value: str) -> tuple[int | None, int | None, str | None]:
    normalized = raw_value.strip().replace("：", "-")
    matched = re.search(r"(\d+)\s*-\s*(\d+)", normalized)
    if not matched:
        return None, None, None
    home_ht_score = int(matched.group(1))
    away_ht_score = int(matched.group(2))
    return home_ht_score, away_ht_score, f"{home_ht_score}:{away_ht_score}"


def _parse_live_match_time(raw_value: str, expect_date: date) -> str | None:
    normalized = raw_value.strip()
    matched = re.search(r"(\d{2})-(\d{2})\s+(\d{2}):(\d{2})", normalized)
    if not matched:
        return None
    month, day, hour, minute = map(int, matched.groups())
    match_time = datetime(expect_date.year, month, day, hour, minute)
    return match_time.strftime("%Y-%m-%d %H:%M:%S")


def parse_live_score_page(
    html: str,
    *,
    expect_date: date,
    allowed_statuses: set[str] | None = None,
) -> dict[str, Any]:
    """解析 live.500.com 指定日期页面中的比赛。"""

    soup = BeautifulSoup(html, "html.parser")
    odds_map = _extract_live_odds_map(html)
    fetched_at = datetime.now().isoformat(timespec="seconds")
    matches: list[dict[str, Any]] = []
    discovered_teams: dict[int, str] = {}
    effective_statuses = allowed_statuses or {"4"}

    for row in soup.select("#table_match tbody tr[id]"):
        fixture_id = _to_optional_int(row.get("fid"))
        if fixture_id is None or fixture_id <= 0:
            matched = re.search(r"^a(\d+)$", str(row.get("id") or ""))
            if matched:
                fixture_id = int(matched.group(1))
        if fixture_id is None or fixture_id <= 0:
            continue

        status_value = str(row.get("status") or "").strip()
        if status_value not in effective_statuses:
            continue

        cells = row.find_all("td")
        if len(cells) < 13:
            continue

        competition_link = cells[1].find("a", href=True)
        competition_url = _normalize_absolute_url(
            competition_link.get("href") if competition_link else None
        )
        competition = cells[1].get_text(" ", strip=True) or None
        season_id = None
        if competition_url:
            matched = re.search(r"/zuqiu-(\d+)/", competition_url)
            if matched:
                season_id = int(matched.group(1))

        home_link = cells[5].find("a", href=re.compile(r"/team/\d+/"))
        away_link = cells[7].find("a", href=re.compile(r"/team/\d+/"))
        if home_link is None or away_link is None:
            continue

        home_href = str(home_link.get("href") or "")
        away_href = str(away_link.get("href") or "")
        home_team_id_match = re.search(r"/team/(\d+)/", home_href)
        away_team_id_match = re.search(r"/team/(\d+)/", away_href)
        home_team_id = int(home_team_id_match.group(1)) if home_team_id_match else None
        away_team_id = int(away_team_id_match.group(1)) if away_team_id_match else None
        home_team = home_link.get_text(" ", strip=True) or None
        away_team = away_link.get_text(" ", strip=True) or None
        if home_team_id and home_team:
            discovered_teams[home_team_id] = home_team
        if away_team_id and away_team:
            discovered_teams[away_team_id] = away_team

        match_time = _parse_live_match_time(cells[3].get_text(" ", strip=True), expect_date)
        home_score, away_score, asian_handicap_name = _parse_live_score_cell(cells[6])
        half_time_text = cells[8].get_text(" ", strip=True)
        home_ht_score, away_ht_score, half_time_score = _parse_live_half_time_score(
            half_time_text
        )
        spf_result, spf_result_code = _build_spf_result(home_score, away_score)

        odds_values = odds_map.get(str(fixture_id), {}).get("0") or []
        if not odds_values:
            odds_values = [
                span.get_text(" ", strip=True)
                for span in cells[9].find_all("span")
            ]
        avg_win_odds = _to_optional_float(odds_values[0] if len(odds_values) > 0 else None)
        avg_draw_odds = _to_optional_float(odds_values[1] if len(odds_values) > 1 else None)
        avg_lose_odds = _to_optional_float(odds_values[2] if len(odds_values) > 2 else None)

        analysis_url = None
        for link in cells[12].find_all("a", href=True):
            href = _normalize_absolute_url(link.get("href"))
            if href and "fenxi/shuju-" in href:
                analysis_url = href
                break

        matches.append(
            {
                "fixture_id": fixture_id,
                "season_id": season_id,
                "match_id": None,
                "match_time": match_time,
                "match_date": expect_date.isoformat(),
                "competition": competition,
                "competition_full_name": competition,
                "competition_url": competition_url,
                "home_team_id": home_team_id,
                "away_team_id": away_team_id,
                "home_team": home_team,
                "away_team": away_team,
                "home_team_canonical": home_team,
                "away_team_canonical": away_team,
                "home_score": home_score,
                "away_score": away_score,
                "home_ht_score": home_ht_score,
                "away_ht_score": away_ht_score,
                "final_score": _format_score(home_score, away_score),
                "half_time_score": half_time_score,
                "spf_result": spf_result,
                "spf_result_code": spf_result_code,
                "is_settled": 1 if status_value == "4" else 0,
                "status_code": status_value,
                "status_label": LIVE_MATCH_STATUS_LABELS.get(status_value, status_value),
                "avg_win_odds": avg_win_odds,
                "avg_draw_odds": avg_draw_odds,
                "avg_lose_odds": avg_lose_odds,
                "asian_handicap_line": asian_handicap_name,
                "asian_handicap_name": asian_handicap_name,
                "asian_home_odds": None,
                "asian_away_odds": None,
                "pan_result": None,
                "over_under_result": None,
                "analysis_url": analysis_url,
                "source_team_id": None,
                "source_url": build_live_score_url(expect_date),
                "fetched_at": fetched_at,
                "raw_payload": json.dumps(
                    {
                        "fixture_id": fixture_id,
                        "status": status_value,
                        "expect_date": expect_date.isoformat(),
                        "odds": odds_map.get(str(fixture_id), {}),
                        "row": str(row),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            }
        )

    return {
        "matches": matches,
        "discovered_teams": discovered_teams,
        "rows_fetched": len(matches),
    }


def fetch_live_matches_snapshot(
    *,
    expect_date: date | None = None,
    allowed_statuses: set[str] | None = None,
    session: requests.Session | None = None,
    timeout: int = REQUEST_TIMEOUT_SECONDS,
    retries: int = DEFAULT_TEAM_SCAN_RETRIES,
) -> dict[str, Any]:
    """抓取 live.500.com 某一天的比赛快照，支持未开场/进行中筛选。"""

    active_session = session or _build_session()
    target_url = build_live_score_url(expect_date)
    html = _fetch_html(
        target_url,
        session=active_session,
        timeout=timeout,
        retries=retries,
    )
    effective_date = _extract_live_page_date(html) or expect_date or datetime.now().date()
    parsed = parse_live_score_page(
        html,
        expect_date=effective_date,
        allowed_statuses=allowed_statuses,
    )
    return {
        "expect_date": effective_date.isoformat(),
        "source_url": build_live_score_url(effective_date),
        **parsed,
    }


def _normalize_match_time(raw_value: str) -> str | None:
    normalized = raw_value.strip()
    if not normalized:
        return None
    try:
        return datetime.fromisoformat(normalized).strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return normalized or None


def _format_score(home_score: int | None, away_score: int | None) -> str | None:
    if home_score is None or away_score is None:
        return None
    return f"{home_score}:{away_score}"


def _to_optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _upsert_scan_state(
    connection: sqlite3.Connection,
    *,
    team_id: int,
    is_valid: bool,
    team_name: str | None,
    notes: str | None = None,
) -> None:
    connection.execute(
        """
        INSERT INTO sfc500_team_scan_state (
            team_id,
            is_valid,
            team_name,
            last_checked_at,
            notes
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(team_id) DO UPDATE SET
            is_valid = excluded.is_valid,
            team_name = excluded.team_name,
            last_checked_at = excluded.last_checked_at,
            notes = excluded.notes
        """,
        (
            team_id,
            int(is_valid),
            team_name,
            datetime.now().isoformat(timespec="seconds"),
            notes,
        ),
    )


def _should_skip_checked_state(row: sqlite3.Row) -> bool:
    is_valid = int(row["is_valid"] or 0)
    notes = str(row["notes"] or "").strip()
    if is_valid == 1:
        return True
    return notes == "invalid_team_page"


def _upsert_discovered_teams(
    connection: sqlite3.Connection,
    discovered_teams: dict[int, str],
) -> None:
    for discovered_team_id, discovered_team_name in discovered_teams.items():
        connection.execute(
            """
            INSERT INTO sfc500_teams (
                team_id,
                team_name,
                source_url,
                teamfixture_url
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(team_id) DO UPDATE SET
                team_name = COALESCE(sfc500_teams.team_name, excluded.team_name),
                source_url = excluded.source_url,
                teamfixture_url = excluded.teamfixture_url,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                discovered_team_id,
                discovered_team_name,
                build_team_home_url(discovered_team_id),
                build_team_fixture_url(discovered_team_id),
            ),
        )


def _upsert_match_rows(
    connection: sqlite3.Connection,
    matches: list[dict[str, Any]],
) -> set[int]:
    match_upsert_sql = """
    INSERT INTO sfc500_team_matches_raw (
        fixture_id,
        season_id,
        match_id,
        match_time,
        match_date,
        competition,
        competition_full_name,
        competition_url,
        home_team_id,
        away_team_id,
        home_team,
        away_team,
        home_team_canonical,
        away_team_canonical,
        home_score,
        away_score,
        home_ht_score,
        away_ht_score,
        final_score,
        half_time_score,
        spf_result,
        spf_result_code,
        is_settled,
        avg_win_odds,
        avg_draw_odds,
        avg_lose_odds,
        asian_handicap_line,
        asian_handicap_name,
        asian_home_odds,
        asian_away_odds,
        pan_result,
        over_under_result,
        analysis_url,
        source_team_id,
        source_url,
        fetched_at,
        raw_payload
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(fixture_id) DO UPDATE SET
        season_id = excluded.season_id,
        match_id = excluded.match_id,
        match_time = excluded.match_time,
        match_date = excluded.match_date,
        competition = excluded.competition,
        competition_full_name = excluded.competition_full_name,
        competition_url = excluded.competition_url,
        home_team_id = excluded.home_team_id,
        away_team_id = excluded.away_team_id,
        home_team = excluded.home_team,
        away_team = excluded.away_team,
        home_team_canonical = excluded.home_team_canonical,
        away_team_canonical = excluded.away_team_canonical,
        home_score = excluded.home_score,
        away_score = excluded.away_score,
        home_ht_score = excluded.home_ht_score,
        away_ht_score = excluded.away_ht_score,
        final_score = excluded.final_score,
        half_time_score = excluded.half_time_score,
        spf_result = excluded.spf_result,
        spf_result_code = excluded.spf_result_code,
        is_settled = excluded.is_settled,
        avg_win_odds = excluded.avg_win_odds,
        avg_draw_odds = excluded.avg_draw_odds,
        avg_lose_odds = excluded.avg_lose_odds,
        asian_handicap_line = excluded.asian_handicap_line,
        asian_handicap_name = excluded.asian_handicap_name,
        asian_home_odds = excluded.asian_home_odds,
        asian_away_odds = excluded.asian_away_odds,
        pan_result = excluded.pan_result,
        over_under_result = excluded.over_under_result,
        analysis_url = excluded.analysis_url,
        source_team_id = COALESCE(sfc500_team_matches_raw.source_team_id, excluded.source_team_id),
        source_url = excluded.source_url,
        fetched_at = excluded.fetched_at,
        raw_payload = excluded.raw_payload,
        updated_at = CURRENT_TIMESTAMP
    """

    existing_fixture_ids: set[int] = set()
    if not matches:
        return existing_fixture_ids

    placeholders = ", ".join("?" for _ in matches)
    rows = connection.execute(
        f"""
        SELECT fixture_id
        FROM sfc500_team_matches_raw
        WHERE fixture_id IN ({placeholders})
        """,
        [int(match["fixture_id"]) for match in matches],
    ).fetchall()
    existing_fixture_ids = {int(row["fixture_id"]) for row in rows}

    connection.executemany(
        match_upsert_sql,
        [
            (
                match.get("fixture_id"),
                match.get("season_id"),
                match.get("match_id"),
                match.get("match_time"),
                match.get("match_date"),
                match.get("competition"),
                match.get("competition_full_name"),
                match.get("competition_url"),
                match.get("home_team_id"),
                match.get("away_team_id"),
                match.get("home_team"),
                match.get("away_team"),
                match.get("home_team_canonical"),
                match.get("away_team_canonical"),
                match.get("home_score"),
                match.get("away_score"),
                match.get("home_ht_score"),
                match.get("away_ht_score"),
                match.get("final_score"),
                match.get("half_time_score"),
                match.get("spf_result"),
                match.get("spf_result_code"),
                match.get("is_settled"),
                match.get("avg_win_odds"),
                match.get("avg_draw_odds"),
                match.get("avg_lose_odds"),
                match.get("asian_handicap_line"),
                match.get("asian_handicap_name"),
                match.get("asian_home_odds"),
                match.get("asian_away_odds"),
                match.get("pan_result"),
                match.get("over_under_result"),
                match.get("analysis_url"),
                match.get("source_team_id"),
                match.get("source_url"),
                match.get("fetched_at"),
                match.get("raw_payload"),
            )
            for match in matches
        ],
    )
    return existing_fixture_ids


def save_team_fixture_snapshot(
    *,
    team_id: int,
    team_record: dict[str, Any] | None,
    matches: list[dict[str, Any]],
    discovered_teams: dict[int, str],
    db_path: Path | None = None,
) -> dict[str, Any]:
    """把球队主页和最近比赛快照写入大库。"""

    target_path = db_path or SFC500_TEAM_HISTORY_DATABASE_PATH
    init_sfc500_team_history_db(target_path)

    team_upsert_sql = """
    INSERT INTO sfc500_teams (
        team_id,
        team_name,
        source_url,
        teamfixture_url,
        page_title,
        last_team_fetch_at,
        last_fixture_fetch_at,
        last_rows_fetched
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(team_id) DO UPDATE SET
        team_name = COALESCE(excluded.team_name, sfc500_teams.team_name),
        source_url = excluded.source_url,
        teamfixture_url = excluded.teamfixture_url,
        page_title = COALESCE(excluded.page_title, sfc500_teams.page_title),
        last_team_fetch_at = COALESCE(excluded.last_team_fetch_at, sfc500_teams.last_team_fetch_at),
        last_fixture_fetch_at = COALESCE(excluded.last_fixture_fetch_at, sfc500_teams.last_fixture_fetch_at),
        last_rows_fetched = excluded.last_rows_fetched,
        updated_at = CURRENT_TIMESTAMP
    """

    with get_sfc500_team_history_connection(target_path) as connection:
        if team_record is not None:
            connection.execute(
                team_upsert_sql,
                (
                    team_record.get("team_id"),
                    team_record.get("team_name"),
                    team_record.get("source_url"),
                    team_record.get("teamfixture_url"),
                    team_record.get("page_title"),
                    team_record.get("last_team_fetch_at"),
                    team_record.get("last_fixture_fetch_at"),
                    team_record.get("last_rows_fetched"),
                ),
            )
            _upsert_scan_state(
                connection,
                team_id=team_id,
                is_valid=True,
                team_name=str(team_record.get("team_name") or ""),
            )
        else:
            _upsert_scan_state(
                connection,
                team_id=team_id,
                is_valid=False,
                team_name=None,
                notes="invalid_team_page",
            )

        _upsert_discovered_teams(connection, discovered_teams)
        existing_fixture_ids = _upsert_match_rows(connection, matches)
        connection.commit()

    return {
        "rows_fetched": len(matches),
        "rows_inserted": len(matches) - len(existing_fixture_ids),
    }


def save_live_matches_snapshot(
    *,
    matches: list[dict[str, Any]],
    discovered_teams: dict[int, str],
    db_path: Path | None = None,
) -> dict[str, Any]:
    """把 live.500.com 完场页抓到的比赛写入球队大库。"""

    target_path = db_path or SFC500_TEAM_HISTORY_DATABASE_PATH
    init_sfc500_team_history_db(target_path)

    with get_sfc500_team_history_connection(target_path) as connection:
        _upsert_discovered_teams(connection, discovered_teams)
        existing_fixture_ids = _upsert_match_rows(connection, matches)
        connection.commit()

    return {
        "rows_fetched": len(matches),
        "rows_inserted": len(matches) - len(existing_fixture_ids),
    }


def fetch_and_save_team(
    team_id: int,
    *,
    db_path: Path | None = None,
    session: requests.Session | None = None,
    timeout: int = REQUEST_TIMEOUT_SECONDS,
    retries: int = DEFAULT_TEAM_SCAN_RETRIES,
    records: int = DEFAULT_TEAM_FIXTURE_RECORDS,
) -> dict[str, Any]:
    """抓取并写入一个球队主页和近期 100 场。"""

    home_title = ""
    team_record: dict[str, Any] | None = None
    team_name_hint = ""
    parsed: dict[str, Any] = {"matches": [], "discovered_teams": {}, "rows_fetched": 0}
    try:
        fixture_payload = _fetch_team_fixture_json(
            team_id,
            session=session,
            timeout=timeout,
            retries=retries,
            records=records,
        )
        parsed = parse_team_fixture_payload(
            fixture_payload,
            source_team_id=team_id,
            source_team_name=team_name_hint,
        )
    except Exception:
        home_html = _fetch_html(
            build_team_home_url(team_id),
            session=session,
            timeout=timeout,
            retries=retries,
        )
        team_record = parse_team_home_page(home_html, team_id)
        home_title = _parse_title_text(home_html)
        team_name_hint = str((team_record or {}).get("team_name") or "")
        fixture_html = _fetch_html(
            build_team_fixture_url(team_id),
            session=session,
            timeout=timeout,
            retries=retries,
        )
        parsed = parse_team_fixture_page(
            fixture_html,
            source_team_id=team_id,
            source_team_name=team_name_hint,
        )

    inferred_source_team_name = str(parsed.get("source_team_name") or "").strip() or None
    rows_fetched = int(
        parsed.get("rows_fetched")
        or len(list(parsed.get("matches") or []))
    )
    if team_record is None and (rows_fetched <= 0 or inferred_source_team_name is None):
        home_html = _fetch_html(
            build_team_home_url(team_id),
            session=session,
            timeout=timeout,
            retries=retries,
        )
        team_record = parse_team_home_page(home_html, team_id)
        home_title = _parse_title_text(home_html)
        team_name_hint = str((team_record or {}).get("team_name") or "")

    merged_team_record = dict(team_record or {})
    if not merged_team_record and inferred_source_team_name:
        fetched_at = datetime.now().isoformat(timespec="seconds")
        merged_team_record = {
            "team_id": team_id,
            "team_name": inferred_source_team_name,
            "source_url": build_team_home_url(team_id),
            "teamfixture_url": build_team_fixture_url(team_id),
            "page_title": home_title or None,
            "last_team_fetch_at": fetched_at,
        }

    if merged_team_record:
        merged_team_record["last_fixture_fetch_at"] = datetime.now().isoformat(
            timespec="seconds"
        )
        merged_team_record["last_rows_fetched"] = rows_fetched

    save_summary = save_team_fixture_snapshot(
        team_id=team_id,
        team_record=merged_team_record or None,
        matches=list(parsed.get("matches") or []),
        discovered_teams=dict(parsed.get("discovered_teams") or {}),
        db_path=db_path,
    )
    is_valid = bool(merged_team_record or rows_fetched > 0)
    return {
        "team_id": team_id,
        "team_name": (
            (merged_team_record or {}).get("team_name")
            or inferred_source_team_name
        ),
        "rows_fetched": save_summary["rows_fetched"],
        "rows_inserted": save_summary["rows_inserted"],
        "is_valid": is_valid,
        "matches": parsed.get("matches") or [],
    }


def fetch_and_save_live_matches(
    expect_date: date,
    *,
    db_path: Path | None = None,
    session: requests.Session | None = None,
    timeout: int = REQUEST_TIMEOUT_SECONDS,
    retries: int = DEFAULT_TEAM_SCAN_RETRIES,
) -> dict[str, Any]:
    """抓取 live.500.com 指定日期的完场比赛并写入球队大库。"""

    live_html = _fetch_html(
        build_live_score_url(expect_date),
        session=session,
        timeout=timeout,
        retries=retries,
    )
    parsed = parse_live_score_page(live_html, expect_date=expect_date)
    save_summary = save_live_matches_snapshot(
        matches=list(parsed.get("matches") or []),
        discovered_teams=dict(parsed.get("discovered_teams") or {}),
        db_path=db_path,
    )
    return {
        "expect_date": expect_date.isoformat(),
        "rows_fetched": save_summary["rows_fetched"],
        "rows_inserted": save_summary["rows_inserted"],
        "matches": parsed.get("matches") or [],
    }


def _emit_progress(progress_callback: ProgressCallback | None, **event: Any) -> None:
    if progress_callback is None:
        return
    progress_callback(event)


def scan_team_range(
    start_team_id: int,
    end_team_id: int,
    *,
    db_path: Path | None = None,
    timeout: int = REQUEST_TIMEOUT_SECONDS,
    retries: int = DEFAULT_TEAM_SCAN_RETRIES,
    sleep_seconds: float = 0.0,
    skip_checked: bool = True,
    progress_callback: ProgressCallback | None = None,
    records: int = DEFAULT_TEAM_FIXTURE_RECORDS,
) -> dict[str, Any]:
    """扫描球队 ID 区间，发现有效球队并抓最近 100 场。"""

    if end_team_id < start_team_id:
        raise ValueError("end_team_id 不能早于 start_team_id。")

    target_path = db_path or SFC500_TEAM_HISTORY_DATABASE_PATH
    init_sfc500_team_history_db(target_path)
    session = _build_session()

    checked_team_ids: set[int] = set()
    if skip_checked:
        with get_sfc500_team_history_connection(target_path) as connection:
            rows = connection.execute(
                "SELECT team_id, is_valid, notes FROM sfc500_team_scan_state"
            ).fetchall()
            checked_team_ids = {
                int(row["team_id"]) for row in rows if _should_skip_checked_state(row)
            }

    rows_fetched_total = 0
    rows_inserted_total = 0
    valid_team_count = 0
    scanned_team_count = 0
    sample_matches: list[dict[str, Any]] = []
    errors: list[str] = []
    consecutive_transient_errors = 0
    total_windows = end_team_id - start_team_id + 1

    _emit_progress(
        progress_callback,
        stage="start",
        start_team_id=start_team_id,
        end_team_id=end_team_id,
        current_index=0,
        total_windows=total_windows,
        message=f"开始扫描球队 ID {start_team_id} -> {end_team_id}",
    )

    for index, team_id in enumerate(range(start_team_id, end_team_id + 1), start=1):
        scanned_team_count += 1
        if skip_checked and team_id in checked_team_ids:
            _emit_progress(
                progress_callback,
                stage="team_skipped",
                team_id=team_id,
                current_index=index,
                total_windows=total_windows,
                message=f"球队 {team_id} 已检查，跳过。",
            )
            continue

        _emit_progress(
            progress_callback,
            stage="team_start",
            team_id=team_id,
            current_index=index,
            total_windows=total_windows,
            message=f"正在抓取球队 {team_id}",
        )
        try:
            summary = fetch_and_save_team(
                team_id,
                db_path=target_path,
                session=session,
                timeout=timeout,
                retries=retries,
                records=records,
            )
            rows_fetched_total += int(summary["rows_fetched"])
            rows_inserted_total += int(summary["rows_inserted"])
            if summary["is_valid"]:
                valid_team_count += 1
                if len(sample_matches) < 5:
                    remaining = 5 - len(sample_matches)
                    sample_matches.extend((summary.get("matches") or [])[:remaining])
            consecutive_transient_errors = 0
            _emit_progress(
                progress_callback,
                stage="team_done",
                team_id=team_id,
                current_index=index,
                total_windows=total_windows,
                rows_fetched=int(summary["rows_fetched"]),
                rows_inserted=int(summary["rows_inserted"]),
                is_valid=bool(summary["is_valid"]),
                team_name=summary.get("team_name"),
                message=(
                    f"球队 {team_id} 完成，"
                    f"{'有效' if summary['is_valid'] else '无效'}，"
                    f"抓取 {summary['rows_fetched']} 场，新增 {summary['rows_inserted']} 场。"
                ),
            )
        except Exception as exc:
            is_transient_error = _is_transient_exception(exc)
            error_message = f"球队 {team_id} 扫描失败：{exc}"
            logger.exception(error_message)
            errors.append(error_message)
            with get_sfc500_team_history_connection(target_path) as connection:
                _upsert_scan_state(
                    connection,
                    team_id=team_id,
                    is_valid=False,
                    team_name=None,
                    notes=(
                        f"transient_fetch_error: {exc}"
                        if is_transient_error
                        else str(exc)
                    ),
                )
                connection.commit()
            _emit_progress(
                progress_callback,
                stage="team_error",
                team_id=team_id,
                current_index=index,
                total_windows=total_windows,
                message=error_message,
            )
            if is_transient_error:
                consecutive_transient_errors += 1
                if consecutive_transient_errors >= 3:
                    cooldown_seconds = min(60.0, 10.0 * consecutive_transient_errors)
                    logger.warning(
                        "Encountered %s consecutive transient errors, cooling down %.1f seconds",
                        consecutive_transient_errors,
                        cooldown_seconds,
                    )
                    _emit_progress(
                        progress_callback,
                        stage="cooldown",
                        team_id=team_id,
                        current_index=index,
                        total_windows=total_windows,
                        message=(
                            f"连续 {consecutive_transient_errors} 次临时错误，"
                            f"冷却 {cooldown_seconds:.1f} 秒后继续。"
                        ),
                    )
                    time.sleep(cooldown_seconds)
            else:
                consecutive_transient_errors = 0

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    summary = {
        "db_path": str(target_path),
        "start_team_id": start_team_id,
        "end_team_id": end_team_id,
        "scanned_team_count": scanned_team_count,
        "valid_team_count": valid_team_count,
        "rows_fetched": rows_fetched_total,
        "rows_inserted": rows_inserted_total,
        "errors": errors,
        "sample_matches": sample_matches,
    }
    _emit_progress(
        progress_callback,
        stage="finish",
        current_index=total_windows,
        total_windows=total_windows,
        message=(
            f"扫描完成，共检查 {scanned_team_count} 个 ID，"
            f"发现 {valid_team_count} 支有效球队，抓取 {rows_fetched_total} 场。"
        ),
        **summary,
    )
    return summary


def refresh_known_teams(
    *,
    db_path: Path | None = None,
    team_ids: list[int] | None = None,
    limit: int | None = None,
    offset: int = 0,
    timeout: int = REQUEST_TIMEOUT_SECONDS,
    retries: int = DEFAULT_TEAM_SCAN_RETRIES,
    sleep_seconds: float = 0.0,
    progress_callback: ProgressCallback | None = None,
    records: int = DEFAULT_TEAM_FIXTURE_RECORDS,
) -> dict[str, Any]:
    """刷新已发现球队的最近 100 场。"""

    target_path = db_path or SFC500_TEAM_HISTORY_DATABASE_PATH
    init_sfc500_team_history_db(target_path)

    with get_sfc500_team_history_connection(target_path) as connection:
        if team_ids:
            placeholders = ", ".join("?" for _ in team_ids)
            rows = connection.execute(
                f"""
                SELECT team_id
                FROM sfc500_teams
                WHERE team_id IN ({placeholders})
                ORDER BY team_id
                """,
                team_ids,
            ).fetchall()
        else:
            query = """
            SELECT team_id
            FROM sfc500_teams
            ORDER BY team_id
            """
            params: list[Any] = []
            if limit is not None:
                query += " LIMIT ? OFFSET ?"
                params.extend([limit, offset])
            rows = connection.execute(query, params).fetchall()

    refresh_ids = [int(row["team_id"]) for row in rows]
    session = _build_session()
    rows_fetched_total = 0
    rows_inserted_total = 0
    sample_matches: list[dict[str, Any]] = []
    errors: list[str] = []

    _emit_progress(
        progress_callback,
        stage="start",
        current_index=0,
        total_windows=len(refresh_ids),
        message=f"开始刷新 {len(refresh_ids)} 支已发现球队。",
    )

    for index, team_id in enumerate(refresh_ids, start=1):
        try:
            summary = fetch_and_save_team(
                team_id,
                db_path=target_path,
                session=session,
                timeout=timeout,
                retries=retries,
                records=records,
            )
            rows_fetched_total += int(summary["rows_fetched"])
            rows_inserted_total += int(summary["rows_inserted"])
            if len(sample_matches) < 5:
                remaining = 5 - len(sample_matches)
                sample_matches.extend((summary.get("matches") or [])[:remaining])
            _emit_progress(
                progress_callback,
                stage="team_done",
                team_id=team_id,
                current_index=index,
                total_windows=len(refresh_ids),
                rows_fetched=int(summary["rows_fetched"]),
                rows_inserted=int(summary["rows_inserted"]),
                team_name=summary.get("team_name"),
                message=f"球队 {team_id} 刷新完成，抓取 {summary['rows_fetched']} 场。",
            )
        except Exception as exc:
            error_message = f"球队 {team_id} 刷新失败：{exc}"
            logger.exception(error_message)
            errors.append(error_message)
            _emit_progress(
                progress_callback,
                stage="team_error",
                team_id=team_id,
                current_index=index,
                total_windows=len(refresh_ids),
                message=error_message,
            )

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    return {
        "db_path": str(target_path),
        "team_count": len(refresh_ids),
        "rows_fetched": rows_fetched_total,
        "rows_inserted": rows_inserted_total,
        "errors": errors,
        "sample_matches": sample_matches,
    }


def sync_recent_live_matches(
    days: int = DEFAULT_LIVE_RECENT_SYNC_DAYS,
    *,
    db_path: Path | None = None,
    timeout: int = REQUEST_TIMEOUT_SECONDS,
    retries: int = DEFAULT_TEAM_SCAN_RETRIES,
    progress_callback: ProgressCallback | None = None,
) -> dict[str, Any]:
    """使用 live.500.com 最近几天的完场列表增量更新球队大库。"""

    if days <= 0:
        raise ValueError("days 必须大于 0。")
    if days > MAX_LIVE_RECENT_SYNC_DAYS:
        raise ValueError(f"live 增量同步最多支持最近 {MAX_LIVE_RECENT_SYNC_DAYS} 天。")

    target_path = db_path or SFC500_TEAM_HISTORY_DATABASE_PATH
    init_sfc500_team_history_db(target_path)
    session = _build_session()
    live_index_html = _fetch_html(
        build_live_score_url(),
        session=session,
        timeout=timeout,
        retries=retries,
    )
    end_date = _extract_live_page_date(live_index_html) or datetime.now().date()
    start_date = end_date - timedelta(days=days - 1)
    dates = [start_date + timedelta(days=offset) for offset in range(days)]
    rows_fetched_total = 0
    rows_inserted_total = 0
    errors: list[str] = []
    sample_matches: list[dict[str, Any]] = []

    _emit_progress(
        progress_callback,
        stage="start",
        current_index=0,
        total_windows=len(dates),
        message=f"开始用 live.500.com 同步最近 {days} 天完场比赛。",
    )

    for index, expect_date in enumerate(dates, start=1):
        _emit_progress(
            progress_callback,
            stage="date_start",
            current_index=index,
            total_windows=len(dates),
            expect_date=expect_date.isoformat(),
            message=f"正在同步 {expect_date.isoformat()} 的完场比赛。",
        )
        try:
            summary = fetch_and_save_live_matches(
                expect_date,
                db_path=target_path,
                session=session,
                timeout=timeout,
                retries=retries,
            )
            rows_fetched_total += int(summary["rows_fetched"])
            rows_inserted_total += int(summary["rows_inserted"])
            if len(sample_matches) < 5:
                remaining = 5 - len(sample_matches)
                sample_matches.extend((summary.get("matches") or [])[:remaining])
            _emit_progress(
                progress_callback,
                stage="date_done",
                current_index=index,
                total_windows=len(dates),
                expect_date=expect_date.isoformat(),
                rows_fetched=int(summary["rows_fetched"]),
                rows_inserted=int(summary["rows_inserted"]),
                message=(
                    f"{expect_date.isoformat()} 同步完成，"
                    f"抓取 {summary['rows_fetched']} 场，新增 {summary['rows_inserted']} 场。"
                ),
            )
        except Exception as exc:
            error_message = f"{expect_date.isoformat()} live 同步失败：{exc}"
            logger.exception(error_message)
            errors.append(error_message)
            _emit_progress(
                progress_callback,
                stage="date_error",
                current_index=index,
                total_windows=len(dates),
                expect_date=expect_date.isoformat(),
                message=error_message,
            )

    summary = {
        "db_path": str(target_path),
        "days": days,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "date_count": len(dates),
        "rows_fetched": rows_fetched_total,
        "rows_inserted": rows_inserted_total,
        "errors": errors,
        "sample_matches": sample_matches,
    }
    _emit_progress(
        progress_callback,
        stage="finish",
        current_index=len(dates),
        total_windows=len(dates),
        message=(
            f"live 增量同步完成，最近 {days} 天共抓取 {rows_fetched_total} 场，"
            f"新增 {rows_inserted_total} 场。"
        ),
        **summary,
    )
    return summary


def get_sfc500_team_history_overview(db_path: Path | None = None) -> dict[str, Any]:
    """返回球队大库的总体概览。"""

    target_path = db_path or SFC500_TEAM_HISTORY_DATABASE_PATH
    ensure_sfc500_team_history_db_available(target_path)

    with get_sfc500_team_history_connection(target_path) as connection:
        row = connection.execute(
            """
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT competition) AS competition_count,
                COUNT(DISTINCT home_team_id) + COUNT(DISTINCT away_team_id) AS raw_team_count,
                SUM(CASE WHEN is_settled = 1 THEN 1 ELSE 0 END) AS settled_count,
                MIN(match_time) AS min_match_time,
                MAX(match_time) AS max_match_time
            FROM sfc500_team_matches_raw
            """
        ).fetchone()
        team_count_row = connection.execute(
            "SELECT COUNT(*) AS team_count FROM sfc500_teams"
        ).fetchone()

    return {
        "row_count": int((row or {})["row_count"] or 0),
        "expect_count": int((team_count_row or {})["team_count"] or 0),
        "competition_count": int((row or {})["competition_count"] or 0),
        "settled_count": int((row or {})["settled_count"] or 0),
        "min_match_time": (row or {})["min_match_time"] if row else None,
        "max_match_time": (row or {})["max_match_time"] if row else None,
        "team_count": int((team_count_row or {})["team_count"] or 0),
    }


def get_sfc500_team_filter_options(db_path: Path | None = None) -> dict[str, list[str]]:
    """返回球队大库的筛选项。"""

    target_path = db_path or SFC500_TEAM_HISTORY_DATABASE_PATH
    ensure_sfc500_team_history_db_available(target_path)

    with get_sfc500_team_history_connection(target_path) as connection:
        competition_rows = connection.execute(
            """
            SELECT DISTINCT competition
            FROM sfc500_team_matches_raw
            WHERE competition IS NOT NULL AND competition <> ''
            ORDER BY competition
            """
        ).fetchall()
        team_rows = connection.execute(
            """
            SELECT team
            FROM (
                SELECT COALESCE(home_team_canonical, home_team) AS team FROM sfc500_team_matches_raw
                UNION
                SELECT COALESCE(away_team_canonical, away_team) AS team FROM sfc500_team_matches_raw
            )
            WHERE team IS NOT NULL AND team <> ''
            ORDER BY team
            """
        ).fetchall()

    return {
        "competitions": [str(row["competition"]) for row in competition_rows],
        "teams": [str(row["team"]) for row in team_rows],
    }


def query_sfc500_team_matches(
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
    """按统一结构查询球队大库比赛，供页面展示复用。"""

    target_path = db_path or SFC500_TEAM_HISTORY_DATABASE_PATH
    ensure_sfc500_team_history_db_available(target_path)

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
            (
                f"(home_team_canonical IN ({placeholders}) "
                f"OR away_team_canonical IN ({placeholders}) "
                f"OR home_team IN ({placeholders}) "
                f"OR away_team IN ({placeholders}))"
            )
        )
        params.extend(teams)
        params.extend(teams)
        params.extend(teams)
        params.extend(teams)

    normalized_team_keyword = (team_keyword or "").strip()
    if normalized_team_keyword:
        where_clauses.append(
            """
            (
                home_team LIKE ?
                OR away_team LIKE ?
                OR home_team_canonical LIKE ?
                OR away_team_canonical LIKE ?
            )
            """
        )
        like_value = f"%{normalized_team_keyword}%"
        params.extend([like_value, like_value, like_value, like_value])

    normalized_expect = (expect or "").strip()
    if normalized_expect:
        where_clauses.append("CAST(fixture_id AS TEXT) = ?")
        params.append(normalized_expect)

    if settled_only is True:
        where_clauses.append("is_settled = 1")
    elif settled_only is False:
        where_clauses.append("is_settled = 0")

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    normalized_prob_sql = """
    CASE
        WHEN avg_win_odds > 0 AND avg_draw_odds > 0 AND avg_lose_odds > 0 THEN
            (1.0 / avg_win_odds) / (
                (1.0 / avg_win_odds) + (1.0 / avg_draw_odds) + (1.0 / avg_lose_odds)
            )
    END
    """
    normalized_draw_prob_sql = """
    CASE
        WHEN avg_win_odds > 0 AND avg_draw_odds > 0 AND avg_lose_odds > 0 THEN
            (1.0 / avg_draw_odds) / (
                (1.0 / avg_win_odds) + (1.0 / avg_draw_odds) + (1.0 / avg_lose_odds)
            )
    END
    """
    normalized_lose_prob_sql = """
    CASE
        WHEN avg_win_odds > 0 AND avg_draw_odds > 0 AND avg_lose_odds > 0 THEN
            (1.0 / avg_lose_odds) / (
                (1.0 / avg_win_odds) + (1.0 / avg_draw_odds) + (1.0 / avg_lose_odds)
            )
    END
    """

    count_sql = f"""
    SELECT COUNT(*) AS total_count
    FROM sfc500_team_matches_raw
    {where_sql}
    """

    query_sql = f"""
    SELECT
        CAST(fixture_id AS TEXT) AS expect,
        1 AS match_no,
        competition,
        match_time,
        home_team,
        away_team,
        home_team_canonical,
        away_team_canonical,
        final_score,
        spf_result,
        is_settled,
        avg_win_odds,
        avg_draw_odds,
        avg_lose_odds,
        {normalized_prob_sql} AS avg_win_prob,
        {normalized_draw_prob_sql} AS avg_draw_prob,
        {normalized_lose_prob_sql} AS avg_lose_prob,
        asian_home_odds,
        asian_handicap_line AS asian_line,
        asian_away_odds,
        NULL AS kelly_win,
        NULL AS kelly_draw,
        NULL AS kelly_lose,
        analysis_url,
        NULL AS asian_url,
        NULL AS euro_url,
        source_url,
        fetched_at
    FROM sfc500_team_matches_raw
    {where_sql}
    ORDER BY match_time DESC, fixture_id DESC
    LIMIT ?
    """

    with get_sfc500_team_history_connection(target_path) as connection:
        count_row = connection.execute(count_sql, params).fetchone()
        rows = connection.execute(query_sql, [*params, limit]).fetchall()

    return {
        "rows": [dict(row) for row in rows],
        "total_count": int(count_row["total_count"] or 0) if count_row else 0,
    }
