"""zgzcw 当前竞彩候选池抓取。"""

from __future__ import annotations

from datetime import date
from datetime import datetime
import re
import time
from typing import Any

from bs4 import BeautifulSoup
import requests

from jczq_assistant.config import REQUEST_TIMEOUT_SECONDS
from jczq_assistant.config import REQUEST_USER_AGENT


ZGZCW_JCMINI_BASE_URL = (
    "https://cp.zgzcw.com/lottery/jchtplayvsForJsp.action?lotteryId=47&type=jcmini"
)
ZGZCW_ANALYSIS_BASE_URL = "https://fenxi.zgzcw.com/"
DEFAULT_RETRIES = 2


def _build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": REQUEST_USER_AGENT,
            "Referer": ZGZCW_JCMINI_BASE_URL,
        }
    )
    return session


def _build_issue_url(issue: str | None = None) -> str:
    if not issue:
        return ZGZCW_JCMINI_BASE_URL
    return f"{ZGZCW_JCMINI_BASE_URL}&issue={issue}"


def _fetch_issue_html(
    issue: str | None = None,
    *,
    session: requests.Session | None = None,
    timeout: int = REQUEST_TIMEOUT_SECONDS,
    retries: int = DEFAULT_RETRIES,
) -> str:
    active_session = session or _build_session()
    source_url = _build_issue_url(issue)
    last_error: Exception | None = None

    for attempt in range(1, retries + 2):
        try:
            response = active_session.get(source_url, timeout=timeout)
            response.raise_for_status()
            response.encoding = response.apparent_encoding or response.encoding or "utf-8"
            return response.text
        except requests.RequestException as exc:
            last_error = exc
            if attempt <= retries:
                time.sleep(0.8 * attempt)

    raise RuntimeError(f"抓取 zgzcw 当前候选池失败: {source_url}") from last_error


def _safe_text(node) -> str:
    return node.get_text(" ", strip=True) if node is not None else ""


def _to_float(raw_value: str | None) -> float | None:
    if raw_value is None:
        return None
    normalized = raw_value.strip()
    if not normalized or normalized in {"-", "--", "- - -"}:
        return None
    try:
        return float(normalized)
    except ValueError:
        return None


def _extract_timestamp_from_title(raw_title: str | None, prefix: str) -> str | None:
    title_text = str(raw_title or "").strip()
    if not title_text.startswith(prefix):
        return None
    timestamp = title_text.removeprefix(prefix).strip()
    try:
        return datetime.fromisoformat(timestamp).strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return timestamp or None


def _extract_team_id_from_href(raw_href: str | None) -> int | None:
    matched = re.search(r"/team/\d+/(\d+)", str(raw_href or ""))
    if matched is None:
        return None
    return int(matched.group(1))


def parse_zgzcw_issue_options(
    html: str,
    *,
    today: date | None = None,
) -> tuple[list[dict[str, str]], str | None]:
    """提取当前可切换的 issue 下拉框。"""

    soup = BeautifulSoup(html, "html.parser")
    selected_issue: str | None = None
    all_issues: list[str] = []
    seen_issues: set[str] = set()
    today_value = (today or date.today()).isoformat()

    for option in soup.select("#selectissue option"):
        issue = str(option.get("value") or "").strip()
        if not issue or issue in seen_issues:
            continue
        if option.has_attr("selected"):
            selected_issue = issue
        all_issues.append(issue)
        seen_issues.add(issue)

    candidate_issues = [
        issue
        for issue in all_issues
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", issue) and issue >= today_value
    ]
    if selected_issue and selected_issue not in candidate_issues and selected_issue in seen_issues:
        candidate_issues = [selected_issue, *candidate_issues]
    if not candidate_issues:
        candidate_issues = all_issues[: min(3, len(all_issues))]

    candidate_issue_set = set(candidate_issues)
    issue_options: list[dict[str, str]] = []
    for issue in all_issues:
        if issue not in candidate_issue_set:
            continue
        label = issue
        if issue == selected_issue:
            label = f"{issue}（默认）"
        elif issue == today_value:
            label = f"{issue}（今日）"
        issue_options.append({"issue": issue, "label": label})

    default_issue = (
        selected_issue
        if selected_issue and selected_issue in candidate_issue_set
        else issue_options[0]["issue"]
        if issue_options
        else None
    )
    return issue_options, default_issue


def parse_zgzcw_issue_page(html: str, issue: str) -> list[dict[str, Any]]:
    """解析 zgzcw 某个 issue 的当前在售比赛。"""

    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("tr.beginBet, tr.endBet")
    parsed_rows: list[dict[str, Any]] = []
    issue_url = _build_issue_url(issue)
    fetched_at = datetime.now().isoformat(timespec="seconds")

    for row in rows:
        row_classes = set(row.get("class") or [])
        is_open_for_sale = "beginBet" in row_classes
        if not is_open_for_sale:
            continue

        match_no_node = row.select_one(".wh-1 i")
        home_team_link = row.select_one(".wh-4 a")
        away_team_link = row.select_one(".wh-6 a")
        standard_odds_links = row.select(".wh-8 .tz-area.frq a.weisai")
        if not standard_odds_links:
            standard_odds_links = row.select(".wh-8 .tz-area[pid='49'] a.weisai")
        match_time_node = row.select_one(".wh-3 span[title*='比赛时间']")
        deadline_node = row.select_one(".wh-3 span[title*='截期时间']")
        action_cell = row.select_one(".wh-10")
        handicap_node = row.select_one(".wh-8 .tz-area-2 .rq")

        if not match_no_node or not home_team_link or not away_team_link:
            continue

        match_no = int(match_no_node.get_text(strip=True))
        newplayid = str((action_cell.get("newplayid") if action_cell else "") or "").strip()
        fixture_id = (
            int(newplayid)
            if newplayid.isdigit()
            else int(f"{issue.replace('-', '')}{match_no:03d}")
        )
        score_text = _safe_text(row.select_one(".wh-5"))
        home_score = None
        away_score = None
        score_match = re.search(r"(\d+)\s*[:\-]\s*(\d+)", score_text)
        if score_match is not None:
            home_score = int(score_match.group(1))
            away_score = int(score_match.group(2))

        avg_win_odds = _to_float(_safe_text(standard_odds_links[0]) if len(standard_odds_links) > 0 else None)
        avg_draw_odds = _to_float(_safe_text(standard_odds_links[1]) if len(standard_odds_links) > 1 else None)
        avg_lose_odds = _to_float(_safe_text(standard_odds_links[2]) if len(standard_odds_links) > 2 else None)
        analysis_url = (
            f"{ZGZCW_ANALYSIS_BASE_URL}{newplayid}/bjop" if newplayid else None
        )
        match_time = _extract_timestamp_from_title(
            match_time_node.get("title") if match_time_node else None,
            "比赛时间:",
        )
        deadline_time = _extract_timestamp_from_title(
            deadline_node.get("title") if deadline_node else None,
            "截期时间:",
        )

        parsed_rows.append(
            {
                "fixture_id": fixture_id,
                "expect": issue,
                "match_no": match_no,
                "season_id": None,
                "match_id": fixture_id,
                "match_time": match_time or deadline_time,
                "match_date": str(match_time or deadline_time or "").split(" ")[0] or issue,
                "competition": (row.get("m") or "").strip() or _safe_text(row.select_one(".wh-2")),
                "competition_full_name": (row.get("m") or "").strip() or _safe_text(row.select_one(".wh-2")),
                "competition_url": (
                    row.select_one(".wh-2 a").get("href") if row.select_one(".wh-2 a") else None
                ),
                "home_team_id": _extract_team_id_from_href(home_team_link.get("href")),
                "away_team_id": _extract_team_id_from_href(away_team_link.get("href")),
                "home_team": _safe_text(home_team_link),
                "away_team": _safe_text(away_team_link),
                "home_team_canonical": _safe_text(home_team_link),
                "away_team_canonical": _safe_text(away_team_link),
                "home_score": home_score,
                "away_score": away_score,
                "home_ht_score": None,
                "away_ht_score": None,
                "final_score": score_text if score_match is not None else None,
                "half_time_score": None,
                "spf_result": "",
                "spf_result_code": "",
                "is_settled": 0,
                "status_code": "0",
                "status_label": "未开场",
                "avg_win_odds": avg_win_odds,
                "avg_draw_odds": avg_draw_odds,
                "avg_lose_odds": avg_lose_odds,
                "analysis_url": analysis_url,
                "asian_handicap_line": _safe_text(handicap_node) or None,
                "source_team_id": None,
                "source_url": issue_url,
                "fetched_at": fetched_at,
                "raw_payload": None,
            }
        )

    parsed_rows.sort(
        key=lambda row: (
            str(row.get("match_time") or ""),
            int(row.get("fixture_id") or 0),
        )
    )
    return parsed_rows


def fetch_zgzcw_live_issue_snapshot(
    issue: str | None = None,
    *,
    session: requests.Session | None = None,
    timeout: int = REQUEST_TIMEOUT_SECONDS,
    retries: int = DEFAULT_RETRIES,
) -> dict[str, Any]:
    """抓取 zgzcw 当前在售竞彩候选池。"""

    active_session = session or _build_session()
    landing_html = _fetch_issue_html(
        None,
        session=active_session,
        timeout=timeout,
        retries=retries,
    )
    issue_options, default_issue = parse_zgzcw_issue_options(landing_html)
    if not issue_options:
        raise RuntimeError("未能从 zgzcw 页面识别当前在售 issue")

    issue_values = {option["issue"] for option in issue_options}
    selected_issue = issue if issue in issue_values else default_issue
    if not selected_issue:
        raise RuntimeError("未能确定 zgzcw 当前候选池 issue")

    selected_label = next(
        option["label"] for option in issue_options if option["issue"] == selected_issue
    )
    if selected_issue == default_issue:
        issue_html = landing_html
    else:
        issue_html = _fetch_issue_html(
            selected_issue,
            session=active_session,
            timeout=timeout,
            retries=retries,
        )

    matches = parse_zgzcw_issue_page(issue_html, selected_issue)
    return {
        "expect_date": selected_issue,
        "source_url": ZGZCW_JCMINI_BASE_URL,
        "issue_url": _build_issue_url(selected_issue),
        "source_label": "cp.zgzcw.com jcmini 当前在售",
        "source_key": "zgzcw",
        "selected_issue": selected_issue,
        "issue_label": selected_label,
        "issue_options": issue_options,
        "matches": matches,
        "rows_fetched": len(matches),
    }
