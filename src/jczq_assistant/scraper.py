"""500.com 当前在售竞彩足球比赛抓取逻辑。

当前实现目标：
1. 使用 requests + BeautifulSoup 解析 trade.500.com/jczq/
2. 只抓取首页展示需要的基础字段
3. 展示胜平负实时赔率，保留分析页链接

页面结构要点：
- 比赛行直接在 HTML 里输出，无需额外 JS 请求
- 行级属性里已包含 matchnum / matchdate / matchtime / league / home / away
- 胜平负赔率使用 data-type='nspf'
- 让球胜平负赔率使用 data-type='spf'，当前首页不展示这一组
"""

from __future__ import annotations

from datetime import datetime
import logging
import time

import requests
from bs4 import BeautifulSoup
from requests import Response, Session
from requests.exceptions import RequestException

from jczq_assistant.config import (
    JCZQ_MATCH_LIST_URL,
    REQUEST_RETRIES,
    REQUEST_TIMEOUT_SECONDS,
    REQUEST_USER_AGENT,
)


logger = logging.getLogger(__name__)


def fetch_today_matches(
    timeout: int = REQUEST_TIMEOUT_SECONDS,
    retries: int = REQUEST_RETRIES,
) -> list[dict]:
    """抓取 500.com 当前在售竞彩足球比赛列表。

    虽然函数名仍保留 `today_matches`，但当前页面口径更接近“当前在售比赛”：
    - 当天已结束比赛通常会被隐藏
    - 后续一段时间内已开售的比赛也会一起展示
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

    html = _request_html(
        session=session,
        url=JCZQ_MATCH_LIST_URL,
        timeout=timeout,
        retries=retries,
    )
    matches = _parse_500_match_list(html)
    if not matches:
        raise RuntimeError("500.com 页面返回成功，但没有解析到当前在售比赛。")

    fetched_date = datetime.now().date().isoformat()
    for match in matches:
        match["source_url"] = JCZQ_MATCH_LIST_URL
        match["fetched_date"] = fetched_date

    logger.info("Scraped %s current-sale matches from %s", len(matches), JCZQ_MATCH_LIST_URL)
    return matches


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

    raise RuntimeError(f"请求失败：{last_error}") from last_error


def _decode_response(response: Response) -> str:
    """尽量使用站点返回编码解码页面。"""

    if response.encoding:
        return response.text

    response.encoding = response.apparent_encoding or "utf-8"
    return response.text


def _parse_500_match_list(html: str) -> list[dict]:
    """解析 500.com 当前在售竞彩列表。"""

    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("tr.bet-tb-tr[data-matchnum]")
    matches: list[dict] = []

    for row in rows:
        if _is_hidden_or_ended(row):
            continue

        odds = _extract_nspf_odds(row)
        matches.append(
            {
                "match_no": row.get("data-matchnum") or _extract_text(row.select_one("td.td-no")),
                "league": row.get("data-simpleleague")
                or _extract_team_name(row.select_one("td.td-evt a")),
                "kickoff_time": _build_kickoff_time(
                    row.get("data-matchdate"),
                    row.get("data-matchtime"),
                ),
                "home_team": row.get("data-homesxname")
                or _extract_team_name(row.select_one("span.team-l a")),
                "away_team": row.get("data-awaysxname")
                or _extract_team_name(row.select_one("span.team-r a")),
                "home_win_odds": odds[0],
                "draw_odds": odds[1],
                "away_win_odds": odds[2],
                "analysis_url": _extract_analysis_url(row),
            }
        )

    # 统一按开赛时间排序，页面展示更稳定。
    matches.sort(key=lambda item: (item.get("kickoff_time") or "", item.get("match_no") or ""))
    return matches


def _is_hidden_or_ended(row) -> bool:
    """过滤已结束或页面默认隐藏的比赛。"""

    if row.get("data-isend") == "1":
        return True

    style = (row.get("style") or "").replace(" ", "").lower()
    if "display:none" in style:
        return True

    return False


def _build_kickoff_time(match_date: str | None, match_time: str | None) -> str:
    """把行内日期时间拼成完整比赛时间。"""

    if not match_date or not match_time:
        return ""
    return f"{match_date.strip()} {match_time.strip()}"


def _extract_nspf_odds(row) -> tuple[float | None, float | None, float | None]:
    """提取胜平负赔率。

    500.com 页面中：
    - `nspf` 对应胜平负
    - `spf` 对应让球胜平负
    """

    buttons = row.select(".itm-rangB1 p.betbtn[data-type='nspf']")
    values = [_to_float(button.get("data-sp") or button.get_text(strip=True)) for button in buttons[:3]]
    return _pad_odds(values)


def _extract_analysis_url(row) -> str | None:
    """提取分析页链接。"""

    analysis_link = row.select_one("td.td-data a")
    if analysis_link is None:
        return None
    return analysis_link.get("href")


def _extract_team_name(element) -> str:
    """优先读取 title，其次读取文本。"""

    if element is None:
        return ""
    return element.get("title") or element.get_text(" ", strip=True)


def _extract_text(element) -> str:
    """安全提取标签文本。"""

    if element is None:
        return ""
    return element.get_text(" ", strip=True)


def _to_float(raw_value: str | None) -> float | None:
    """把页面文本安全转成浮点数。"""

    if raw_value is None:
        return None

    normalized = raw_value.strip()
    if not normalized or normalized in {"-", "--", "- - -"}:
        return None

    try:
        return float(normalized)
    except ValueError:
        return None


def _pad_odds(values: list[float | None]) -> tuple[float | None, float | None, float | None]:
    """把赔率列表补齐成固定三元组。"""

    padded = values[:3]
    while len(padded) < 3:
        padded.append(None)
    return padded[0], padded[1], padded[2]
