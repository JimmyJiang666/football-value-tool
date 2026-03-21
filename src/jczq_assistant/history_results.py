"""竞彩足球历史开奖结果抓取逻辑。

当前实现目标：
1. 使用 requests + BeautifulSoup 抓取足彩网公开开奖结果页
2. 支持默认最近结果抓取，也支持按自定义日期区间抓取
3. 支持简单分页、超时、重试、日志和 User-Agent
4. 过滤未开奖的空行，只保留已经有结果的比赛

关于查询参数的假设：
- 当前公开页为 GET /dc/getKaijiangFootBall.action
- 表单参数使用 startTime / endTime / league
- 翻页参数使用 jumpPage
如果目标站点后续改表单字段名，这一层需要一起调整。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import time as datetime_time
from datetime import timedelta
import logging
import time

import requests
from bs4 import BeautifulSoup
from requests import Response, Session
from requests.exceptions import RequestException

from jczq_assistant.config import (
    HISTORY_RESULTS_MAX_PAGES,
    JCZQ_RESULTS_URL,
    REQUEST_RETRIES,
    REQUEST_TIMEOUT_SECONDS,
    REQUEST_USER_AGENT,
)


logger = logging.getLogger(__name__)


@dataclass
class HistoryResultsFetchResult:
    """历史赛果抓取结果和页面元信息。"""

    results: list[dict]
    raw_row_count: int


def get_default_results_query_params(
    timeout: int = REQUEST_TIMEOUT_SECONDS,
    retries: int = REQUEST_RETRIES,
) -> dict[str, str]:
    """读取足彩网结果页当前默认查询参数。

    主要用于 sync_recent，避免直接依赖本机时区的“今天”。
    """

    session = _create_session()
    html = _request_html(
        session=session,
        url=JCZQ_RESULTS_URL,
        timeout=timeout,
        retries=retries,
    )
    return _extract_query_params(html)


def fetch_history_results(
    timeout: int = REQUEST_TIMEOUT_SECONDS,
    retries: int = REQUEST_RETRIES,
    max_pages: int = HISTORY_RESULTS_MAX_PAGES,
) -> list[dict]:
    """抓取竞彩足球历史开奖结果。

    当前使用站点默认日期范围，并抓取前几页历史数据。
    """

    session = _create_session()

    first_html = _request_html(
        session=session,
        url=JCZQ_RESULTS_URL,
        timeout=timeout,
        retries=retries,
    )
    query_params = _extract_query_params(first_html)
    fetch_result = _fetch_results_with_params(
        session=session,
        query_params=query_params,
        timeout=timeout,
        retries=retries,
        max_pages=max_pages,
        first_html=first_html,
    )
    return fetch_result.results

def fetch_history_results_by_date_range(
    start_date: str,
    end_date: str,
    league: str = "",
    timeout: int = REQUEST_TIMEOUT_SECONDS,
    retries: int = REQUEST_RETRIES,
    max_pages: int | None = None,
) -> list[dict]:
    """按指定日期区间抓取历史开奖结果。

    start_date 和 end_date 使用 YYYY-MM-DD。
    这里假设足彩网查询接口支持通过 GET 参数按日期范围筛选。
    """

    session = _create_session()
    query_params = _build_query_params(
        start_date=start_date,
        end_date=end_date,
        league=league,
    )
    fetch_result = _fetch_results_with_params(
        session=session,
        query_params=query_params,
        timeout=timeout,
        retries=retries,
        max_pages=max_pages,
    )
    return fetch_result.results


def fetch_history_results_by_date_range_with_meta(
    start_date: str,
    end_date: str,
    league: str = "",
    timeout: int = REQUEST_TIMEOUT_SECONDS,
    retries: int = REQUEST_RETRIES,
    max_pages: int | None = None,
) -> HistoryResultsFetchResult:
    """按指定日期区间抓取历史开奖结果，并返回页面元信息。"""

    session = _create_session()
    query_params = _build_query_params(
        start_date=start_date,
        end_date=end_date,
        league=league,
    )
    return _fetch_results_with_params(
        session=session,
        query_params=query_params,
        timeout=timeout,
        retries=retries,
        max_pages=max_pages,
    )


def _create_session() -> Session:
    """创建带基础请求头的会话。"""

    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO)

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": REQUEST_USER_AGENT,
            "Referer": JCZQ_RESULTS_URL,
        }
    )
    return session


def _fetch_results_with_params(
    session: Session,
    query_params: dict[str, str],
    timeout: int,
    retries: int,
    max_pages: int | None = None,
    first_html: str | None = None,
) -> HistoryResultsFetchResult:
    """按查询参数抓取多页历史开奖结果。"""

    results_by_id: dict[str, dict] = {}
    total_raw_row_count = 0
    page_number = 1

    while True:
        if page_number == 1 and first_html is not None:
            html = first_html
        else:
            params = {**query_params, "jumpPage": page_number}
            html = _request_html(
                session=session,
                url=JCZQ_RESULTS_URL,
                timeout=timeout,
                retries=retries,
                params=params,
            )

        page_results = _parse_results_page(
            html=html,
            start_date=query_params["startTime"],
            end_date=query_params["endTime"],
        )
        raw_row_count = _count_result_rows(html)
        total_raw_row_count += raw_row_count
        logger.info("Parsed %s settled results from page %s", len(page_results), page_number)
        if not page_results:
            if raw_row_count > 0:
                logger.warning(
                    (
                        "Page %s contains %s result rows, but settled columns are empty. "
                        "This usually means the public HTML for this date range does not "
                        "expose historical outcomes directly."
                    ),
                    page_number,
                    raw_row_count,
                )

        for result in page_results:
            results_by_id[result["source_match_id"]] = result

        if max_pages is not None and page_number >= max_pages:
            break

        if not _has_next_page(html):
            break

        page_number += 1

    fetched_at = datetime.now().isoformat(timespec="seconds")
    results = sorted(
        results_by_id.values(),
        key=lambda item: item.get("match_time", ""),
        reverse=True,
    )
    for result in results:
        result["source_url"] = JCZQ_RESULTS_URL
        result["fetched_at"] = fetched_at

    logger.info(
        "Scraped %s historical results from %s for params=%s raw_rows=%s",
        len(results),
        JCZQ_RESULTS_URL,
        query_params,
        total_raw_row_count,
    )
    return HistoryResultsFetchResult(
        results=results,
        raw_row_count=total_raw_row_count,
    )


def _request_html(
    session: Session,
    url: str,
    timeout: int,
    retries: int,
    params: dict | None = None,
) -> str:
    """带超时、重试和日志的基础请求。"""

    last_error: Exception | None = None
    total_attempts = retries + 1

    for attempt in range(1, total_attempts + 1):
        try:
            logger.info(
                "Fetching %s (attempt %s/%s) params=%s",
                url,
                attempt,
                total_attempts,
                params,
            )
            response = session.get(url, params=params, timeout=timeout)
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
    """尽量使用站点返回的编码解码页面。"""

    if response.encoding:
        return response.text

    response.encoding = response.apparent_encoding or "utf-8"
    return response.text


def _extract_query_params(html: str) -> dict[str, str]:
    """提取页面默认查询参数，供后续翻页复用。"""

    soup = BeautifulSoup(html, "html.parser")
    start_time = _extract_input_value(soup, "startTime")
    end_time = _extract_input_value(soup, "endTime")

    league = ""
    league_option = soup.select_one("select[name='league'] option[selected]")
    if league_option:
        league = league_option.get("value", "")

    if not start_time or not end_time:
        raise RuntimeError("无法从页面中提取历史结果查询日期范围。")

    return {
        "startTime": start_time,
        "endTime": end_time,
        "league": league,
    }


def _build_query_params(start_date: str, end_date: str, league: str = "") -> dict[str, str]:
    """构造结果页查询参数。"""

    return {
        "startTime": start_date,
        "endTime": end_date,
        "league": league,
    }


def _has_next_page(html: str) -> bool:
    """判断当前结果页是否还有下一页。

    足彩网当前分页结构会在最后一页移除“下一页”链接，
    因此这里按链接文本判断是否继续翻页。
    """

    soup = BeautifulSoup(html, "html.parser")
    for link in soup.select("#pageNavigator .page-left a"):
        if link.get_text(strip=True) == "下一页":
            return True
    return False


def _parse_results_page(
    html: str,
    start_date: str,
    end_date: str,
) -> list[dict]:
    """解析单页历史开奖结果。"""

    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("div.jczq-kjing tbody tr")
    results: list[dict] = []

    for row in rows:
        result = _parse_result_row(row, start_date=start_date, end_date=end_date)
        if result is None:
            continue
        results.append(result)

    return results


def _count_result_rows(html: str) -> int:
    """统计页面中原始结果行数量，用于辅助诊断解析为 0 的原因。"""

    soup = BeautifulSoup(html, "html.parser")
    return len(soup.select("div.jczq-kjing tbody tr"))


def _parse_result_row(row, start_date: str, end_date: str) -> dict | None:
    """解析单行开奖结果。"""

    columns = row.find_all("td")
    if len(columns) < 12:
        return None

    match_time = _resolve_full_match_time(
        partial_time=_extract_text(columns[2]),
        start_date_str=start_date,
        end_date_str=end_date,
    )
    final_score = _extract_final_score(_extract_text(columns[4]))
    spf_result = _extract_text(columns[6])

    # 历史开奖结果页第一页会混入未开奖场次，这里直接跳过。
    if not final_score and not spf_result:
        return None

    source_match_id = columns[2].get("tid") or _build_fallback_match_id(
        match_no=_extract_text(columns[0]),
        match_time=match_time,
        home_team=_extract_text(columns[3]),
        away_team=_extract_text(columns[5]),
    )

    return {
        "source_match_id": source_match_id,
        "match_no": _extract_text(columns[0]),
        "league": _extract_text(columns[1]),
        "match_time": match_time,
        "home_team": _extract_text(columns[3]),
        "away_team": _extract_text(columns[5]),
        "final_score": final_score,
        "spf_result": spf_result,
        "handicap": _extract_text(columns[7]),
        "handicap_result": _extract_text(columns[8]),
        "correct_score_result": _extract_text(columns[9]),
        "total_goals_result": _extract_text(columns[10]),
        "half_full_result": _extract_text(columns[11]),
    }


def _extract_input_value(soup: BeautifulSoup, input_id: str) -> str:
    """提取 input 的当前值。"""

    input_element = soup.select_one(f"input#{input_id}")
    if input_element is None:
        return ""
    return input_element.get("value", "").strip()


def _resolve_full_match_time(
    partial_time: str,
    start_date_str: str,
    end_date_str: str,
) -> str:
    """把页面中的 MM-DD HH:MM 补全年份。"""

    if not partial_time:
        return ""

    try:
        parsed_partial = datetime.strptime(partial_time, "%m-%d %H:%M")
        start_date_value = date.fromisoformat(start_date_str)
        end_date_value = date.fromisoformat(end_date_str)
    except ValueError:
        return partial_time

    candidate_years = {
        start_date_value.year - 1,
        start_date_value.year,
        end_date_value.year,
        end_date_value.year + 1,
    }
    start_bound = datetime.combine(start_date_value, datetime_time.min) - timedelta(days=1)
    end_bound = datetime.combine(end_date_value, datetime_time.max) + timedelta(days=1)

    candidates = []
    for year in sorted(candidate_years):
        try:
            candidate = parsed_partial.replace(year=year)
        except ValueError:
            continue
        candidates.append(candidate)
        if start_bound <= candidate <= end_bound:
            return candidate.strftime("%Y-%m-%d %H:%M")

    if not candidates:
        return partial_time

    reference = datetime.combine(end_date_value, datetime_time.max)
    nearest = min(candidates, key=lambda item: abs(item - reference))
    return nearest.strftime("%Y-%m-%d %H:%M")


def _extract_final_score(score_text: str) -> str:
    """提取最终比分，不包含半场比分。"""

    if not score_text:
        return ""

    parts = score_text.split()
    if not parts:
        return ""

    return parts[0]


def _build_fallback_match_id(
    match_no: str,
    match_time: str,
    home_team: str,
    away_team: str,
) -> str:
    """当页面没有 tid 时生成退化唯一键。"""

    return f"{match_no}|{match_time}|{home_team}|{away_team}"


def _extract_text(element) -> str:
    """安全提取标签文本。"""

    if element is None:
        return ""
    return element.get_text(" ", strip=True)
