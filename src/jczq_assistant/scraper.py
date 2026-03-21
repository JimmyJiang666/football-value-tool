"""竞彩足球今日比赛抓取逻辑。

当前实现目标：
1. 优先抓取 cp.zgzcw.com 的竞彩足球公开列表页
2. 如果主页面解析失败，则回退到 live.zgzcw.com 的公开比分页
3. 只抓取首页展示需要的基础字段

这里先做 MVP，不引入 Playwright，也不做过多抽象。
"""

from __future__ import annotations

from datetime import date
from datetime import datetime
import logging
import re
import time

import requests
from bs4 import BeautifulSoup
from requests import Response, Session
from requests.exceptions import RequestException

from jczq_assistant.config import (
    JCZQ_FALLBACK_LIVE_URL,
    JCZQ_MATCH_LIST_URL,
    REQUEST_RETRIES,
    REQUEST_TIMEOUT_SECONDS,
    REQUEST_USER_AGENT,
)


logger = logging.getLogger(__name__)
WEEKDAY_LABELS = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]


def fetch_today_matches(
    timeout: int = REQUEST_TIMEOUT_SECONDS,
    retries: int = REQUEST_RETRIES,
) -> list[dict]:
    """抓取今日竞彩足球比赛列表。

    返回值为内部统一字段列表，后续可直接用于：
    1. 页面展示
    2. SQLite 落库
    """

    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO)

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": REQUEST_USER_AGENT,
            "Referer": JCZQ_MATCH_LIST_URL,
        }
    )

    sources = [
        (JCZQ_MATCH_LIST_URL, _parse_cp_match_list),
        (JCZQ_FALLBACK_LIVE_URL, _parse_live_match_list),
    ]
    errors: list[str] = []

    for source_url, parser in sources:
        try:
            html = _request_html(
                session=session,
                url=source_url,
                timeout=timeout,
                retries=retries,
            )
            matches = parser(html)
            if not matches:
                raise RuntimeError("页面返回成功，但没有解析到任何比赛。")

            fetched_date = _extract_source_date(source_url, html)
            if not fetched_date:
                fetched_date = datetime.now().date().isoformat()

            for match in matches:
                match["source_url"] = source_url
                match["fetched_date"] = fetched_date

            logger.info("Scraped %s matches from %s", len(matches), source_url)
            return matches
        except Exception as exc:
            logger.exception("Failed to scrape matches from %s", source_url)
            errors.append(f"{source_url}: {exc}")

    raise RuntimeError("；".join(errors))


def _request_html(session: Session, url: str, timeout: int, retries: int) -> str:
    """带超时、重试和日志的基础请求函数。"""

    last_error: Exception | None = None
    total_attempts = retries + 1

    for attempt in range(1, total_attempts + 1):
        try:
            logger.info("Fetching %s (attempt %s/%s)", url, attempt, total_attempts)
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            return _decode_response(response)
        except RequestException as exc:
            last_error = exc
            logger.warning(
                "Request failed for %s on attempt %s/%s: %s",
                url,
                attempt,
                total_attempts,
                exc,
            )
            if attempt < total_attempts:
                time.sleep(1)

    raise RuntimeError(f"请求失败：{last_error}")


def _decode_response(response: Response) -> str:
    """尽量用 requests 推断出的编码解码网页。"""

    if response.encoding:
        return response.text

    response.encoding = response.apparent_encoding or "utf-8"
    return response.text


def _parse_cp_match_list(html: str) -> list[dict]:
    """解析 cp.zgzcw.com 的竞彩足球胜平负/让球列表页。"""

    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("tr[id^='tr_'][mn]")
    matches: list[dict] = []
    issue_date = _extract_cp_issue_date(html)
    issue_label = _date_to_weekday_label(issue_date) if issue_date else None

    for row in rows:
        match_no = row.get("mn")
        if issue_label and match_no and not match_no.startswith(issue_label):
            continue

        odds = _extract_cp_sp_odds(row)
        matches.append(
            {
                "match_no": match_no,
                "league": _extract_cp_league(row),
                "kickoff_time": _extract_cp_kickoff_time(row),
                "home_team": _extract_text(row.select_one("td.wh-4 a")),
                "away_team": _extract_text(row.select_one("td.wh-6 a")),
                "home_win_odds": odds[0],
                "draw_odds": odds[1],
                "away_win_odds": odds[2],
                "analysis_url": _extract_cp_analysis_url(row),
            }
        )

    return _clean_matches(matches)


def _parse_live_match_list(html: str) -> list[dict]:
    """解析 live.zgzcw.com/jz/ 的公开比分页。"""

    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("tr.matchTr")
    matches: list[dict] = []
    page_date = _extract_live_page_date(html)

    for row in rows:
        kickoff_time = _extract_live_kickoff_time(row)
        if page_date and kickoff_time and not kickoff_time.startswith(page_date):
            continue

        odds = _extract_live_sp_odds(row)
        matches.append(
            {
                "match_no": _extract_text(row.select_one("td")),
                "league": _extract_text(row.select_one("td.matchType span")),
                "kickoff_time": kickoff_time,
                "home_team": _extract_text(row.select_one("span.sptr a")),
                "away_team": _extract_text(row.select_one("span.sptl a")),
                "home_win_odds": odds[0],
                "draw_odds": odds[1],
                "away_win_odds": odds[2],
                "analysis_url": _extract_live_analysis_url(row),
            }
        )

    return _clean_matches(matches)


def _extract_cp_league(row) -> str:
    """提取 cp 页面中的联赛名称。"""

    league_cell = row.select_one("td.wh-2")
    if not league_cell:
        return ""
    return league_cell.get("title") or _extract_text(league_cell)


def _extract_cp_kickoff_time(row) -> str:
    """从 cp 页面隐藏字段中提取完整比赛时间。"""

    for span in row.select("td.wh-3 span"):
        title = span.get("title", "")
        if title.startswith("比赛时间:"):
            return title.replace("比赛时间:", "", 1).strip()
    return ""


def _extract_cp_sp_odds(row) -> tuple[float | None, float | None, float | None]:
    """提取 cp 页面中的胜平负赔率。"""

    anchors = row.select("div.tz-area.frq a.weisai")
    values = [_to_float(anchor.get_text(strip=True)) for anchor in anchors[:3]]
    return _pad_odds(values)


def _extract_cp_analysis_url(row) -> str | None:
    """根据 cp 页面里的 newPlayId 构造分析页链接。"""

    analysis_cell = row.select_one("td.wh-10")
    if not analysis_cell:
        return None

    new_play_id = analysis_cell.get("newplayid")
    if not new_play_id:
        return None

    return f"https://fenxi.zgzcw.com/{new_play_id}/bfyc"


def _extract_source_date(source_url: str, html: str) -> str | None:
    """从源页面中提取当前抓取对应的日期。"""

    if source_url == JCZQ_MATCH_LIST_URL:
        return _extract_cp_issue_date(html)

    if source_url == JCZQ_FALLBACK_LIVE_URL:
        return _extract_live_page_date(html)

    return None


def _extract_cp_issue_date(html: str) -> str | None:
    """提取 cp 页面当前期次日期，例如 2026-03-21。"""

    match = re.search(r'issue:"(\d{4}-\d{2}-\d{2})"', html)
    if not match:
        return None
    return match.group(1)


def _extract_live_kickoff_time(row) -> str:
    """提取 live 页面中的完整开赛时间。"""

    match_date_cell = row.select_one("td.matchDate")
    if not match_date_cell:
        return ""
    return match_date_cell.get("date") or _extract_text(match_date_cell)


def _extract_live_page_date(html: str) -> str | None:
    """提取 live 页面当前展示的服务器日期。"""

    match = re.search(r'id="serverDate"[^>]*value="(\d{4}-\d{2}-\d{2})', html)
    if not match:
        return None
    return match.group(1)


def _extract_live_sp_odds(row) -> tuple[float | None, float | None, float | None]:
    """优先提取 live 页面中的胜平负 SP，拿不到时退回欧赔。"""

    sp_spans = row.select("td.oddMatch div.jcsp span")
    if not sp_spans:
        sp_spans = row.select("td.oddMatch div.oupei span")

    values = [_to_float(span.get_text(strip=True)) for span in sp_spans[:3]]
    return _pad_odds(values)


def _extract_live_analysis_url(row) -> str | None:
    """提取 live 页面里的分析页链接。"""

    analysis_link = row.select_one("td.fc a[href*='/bfyc']")
    if not analysis_link:
        return None
    return analysis_link.get("href")


def _clean_matches(matches: list[dict]) -> list[dict]:
    """过滤掉明显不完整的记录。"""

    cleaned = []
    for match in matches:
        if not match.get("match_no"):
            continue
        if not match.get("home_team") or not match.get("away_team"):
            continue
        cleaned.append(match)
    return cleaned


def _extract_text(element) -> str:
    """安全提取标签文本。"""

    if element is None:
        return ""
    return element.get_text(" ", strip=True)


def _to_float(value: str) -> float | None:
    """把赔率文本安全转换成浮点数。"""

    if not value:
        return None

    normalized = value.strip().replace("\xa0", "")
    if normalized in {"-", "--", "VS"}:
        return None

    try:
        return float(normalized)
    except ValueError:
        return None


def _pad_odds(values: list[float | None]) -> tuple[float | None, float | None, float | None]:
    """保证赔率返回固定三列，避免解析不完整时页面报错。"""

    padded = list(values[:3])
    while len(padded) < 3:
        padded.append(None)
    return padded[0], padded[1], padded[2]


def _date_to_weekday_label(value: str | None) -> str | None:
    """把 YYYY-MM-DD 转成足彩网比赛编号前缀，例如 周六。"""

    if not value:
        return None

    weekday_index = date.fromisoformat(value).weekday()
    return WEEKDAY_LABELS[weekday_index]
