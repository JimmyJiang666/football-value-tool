"""最小验证脚本：检查足彩网近期 issue 和 bjop 历史赔率页是否仍可访问。

目标：
1. 验证列表页 issue=YYYY-MM-DD 是否还能返回比赛行
2. 验证比赛详情页 /{newplayid}/bjop 是否还能拿到开赔 / 即时赔
3. 验证 bjop 页里是否仍然存在公司历史链接 zhishu

说明：
- 这是独立验证脚本，不接现有主程序
- 默认读取当前页面下拉框里的 issue，并额外检查 2025-01-01
- 输出 JSON 到 /tmp，便于后续继续分析
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from dataclasses import asdict
from dataclasses import dataclass
import json
from pathlib import Path
import re
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


LIST_BASE_URL = "https://cp.zgzcw.com/lottery/jchtplayvsForJsp.action?lotteryId=47&type=jcmini"
BJOP_BASE_URL = "https://fenxi.zgzcw.com/"
DEFAULT_OUTPUT_PATH = Path("/tmp/zgzcw_odds_history_verify.json")
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
}


@dataclass
class MatchRow:
    """列表页比赛行。"""

    issue: str
    list_url: str
    newplayid: str
    competition: str
    match_start_time: str
    home_team: str
    away_team: str
    score_source: str


@dataclass
class MatchVerification:
    """单场比赛赔率页验证结果。"""

    issue: str
    newplayid: str
    home_team: str
    away_team: str
    bjop_url: str
    detail_ok: bool
    detail_match_date: str
    company_rows: int
    parsed_rate_rows: int
    zhishu_links: int
    sample_rates: list[dict[str, str]]
    error: str


def fetch_html(url: str, timeout: int = 20, retries: int = 2) -> str:
    """带简单重试的 HTML 请求。"""

    last_error: Exception | None = None
    total_attempts = retries + 1

    for _attempt in range(total_attempts):
        try:
            response = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
            response.raise_for_status()
            response.encoding = response.apparent_encoding or "utf-8"
            return response.text
        except requests.RequestException as exc:
            last_error = exc

    raise RuntimeError(f"请求失败: {url} | {last_error}")


def get_current_issue_options() -> list[str]:
    """读取当前页面下拉框里的 issue 值。"""

    html = fetch_html(LIST_BASE_URL)
    soup = BeautifulSoup(html, "html.parser")
    issues = []

    for option in soup.select("#selectissue option"):
        value = option.get("value", "").strip()
        if value:
            issues.append(value)

    # 去重但保留顺序。
    return list(dict.fromkeys(issues))


def fetch_issue_matches(issue: str) -> dict[str, Any]:
    """抓取某个 issue 的比赛列表。"""

    url = f"{LIST_BASE_URL}&issue={issue}"
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select(".beginBet, .endBet")

    match_rows: list[MatchRow] = []
    for row in rows:
        data_cell = row.select_one(".wh-10")
        if data_cell is None:
            continue

        newplayid = (data_cell.get("newplayid") or "").strip()
        if not newplayid:
            continue

        match_rows.append(
            MatchRow(
                issue=issue,
                list_url=url,
                newplayid=newplayid,
                competition=(row.get("m") or "").strip(),
                match_start_time=(row.get("t") or "").strip(),
                home_team=_safe_select_text(row, ".wh-4 a"),
                away_team=_safe_select_text(row, ".wh-6 a"),
                score_source=_safe_select_text(row, ".wh-5"),
            )
        )

    response_json = _extract_response_json(html)
    return {
        "issue": issue,
        "url": url,
        "list_rows": len(match_rows),
        "sum_match": response_json.get("sumMatch"),
        "is_history": response_json.get("isHistory"),
        "matches": match_rows,
    }


def verify_bjop(match: MatchRow) -> MatchVerification:
    """验证单场比赛的 bjop 页是否还能拿到赔率数据。"""

    bjop_url = urljoin(BJOP_BASE_URL, f"{match.newplayid}/bjop")

    try:
        html = fetch_html(bjop_url)
        if "Access Verification" in html:
            return MatchVerification(
                issue=match.issue,
                newplayid=match.newplayid,
                home_team=match.home_team,
                away_team=match.away_team,
                bjop_url=bjop_url,
                detail_ok=False,
                detail_match_date="",
                company_rows=0,
                parsed_rate_rows=0,
                zhishu_links=0,
                sample_rates=[],
                error="access_verification",
            )

        soup = BeautifulSoup(html, "html.parser")

        detail_match_date = _safe_select_text(soup, ".bfyc-duizhen-r .date span")
        rate_rows = soup.select(".tr-hr")
        sample_rates = []

        for row in rate_rows[:3]:
            columns = row.select("td")
            if len(columns) < 8:
                continue

            sample_rates.append(
                {
                    "company": columns[1].get_text(" ", strip=True),
                    "begin_win": columns[2].get("data") or columns[2].get_text(strip=True),
                    "begin_draw": columns[3].get("data") or columns[3].get_text(strip=True),
                    "begin_lost": columns[4].get("data") or columns[4].get_text(strip=True),
                    "latest_win": columns[5].get("data") or columns[5].get_text(strip=True),
                    "latest_draw": columns[6].get("data") or columns[6].get_text(strip=True),
                    "latest_lost": columns[7].get("data") or columns[7].get_text(strip=True),
                }
            )

        zhishu_links = len(soup.select('a[href*="/bjop/zhishu?company_id="]'))
        return MatchVerification(
            issue=match.issue,
            newplayid=match.newplayid,
            home_team=match.home_team,
            away_team=match.away_team,
            bjop_url=bjop_url,
            detail_ok=bool(detail_match_date or rate_rows),
            detail_match_date=detail_match_date,
            company_rows=len(rate_rows),
            parsed_rate_rows=len(sample_rates),
            zhishu_links=zhishu_links,
            sample_rates=sample_rates,
            error="",
        )
    except Exception as exc:
        return MatchVerification(
            issue=match.issue,
            newplayid=match.newplayid,
            home_team=match.home_team,
            away_team=match.away_team,
            bjop_url=bjop_url,
            detail_ok=False,
            detail_match_date="",
            company_rows=0,
            parsed_rate_rows=0,
            zhishu_links=0,
            sample_rates=[],
            error=str(exc),
        )


def _safe_select_text(node, selector: str) -> str:
    """安全读取文本。"""

    element = node.select_one(selector)
    if element is None:
        return ""
    return element.get_text(" ", strip=True)


def _extract_response_json(html: str) -> dict[str, Any]:
    """从页面里的 responseJson textarea 提取简单元信息。"""

    match = re.search(r"<textarea id=\"responseJson\"[^>]*>(.*?)</textarea>", html, re.S)
    if not match:
        return {}

    content = match.group(1)
    issue_match = re.search(r'issue:"([^"]+)"', content)
    sum_match = re.search(r"sumMatch:(\d+)", content)
    is_history = re.search(r"isHistory:(true|false)", content)

    return {
        "issue": issue_match.group(1) if issue_match else None,
        "sumMatch": int(sum_match.group(1)) if sum_match else None,
        "isHistory": (is_history.group(1) == "true") if is_history else None,
    }


def summarize_issue_verification(
    issue_result: dict[str, Any],
    verification_results: list[MatchVerification],
) -> dict[str, Any]:
    """汇总某个 issue 的验证情况。"""

    detail_ok_count = sum(1 for item in verification_results if item.detail_ok)
    with_rates_count = sum(1 for item in verification_results if item.parsed_rate_rows > 0)
    with_zhishu_count = sum(1 for item in verification_results if item.zhishu_links > 0)

    return {
        "issue": issue_result["issue"],
        "list_url": issue_result["url"],
        "list_rows": issue_result["list_rows"],
        "sum_match": issue_result["sum_match"],
        "is_history": issue_result["is_history"],
        "detail_ok_count": detail_ok_count,
        "with_rates_count": with_rates_count,
        "with_zhishu_count": with_zhishu_count,
        "sample_matches": [
            asdict(item)
            for item in verification_results[:5]
        ],
    }


def run_verification(
    issues: list[str],
    max_workers: int,
    max_matches_per_issue: int | None,
) -> dict[str, Any]:
    """执行完整验证。"""

    issue_results = []

    for issue in issues:
        issue_result = fetch_issue_matches(issue)
        matches: list[MatchRow] = issue_result["matches"]
        if max_matches_per_issue is not None:
            matches = matches[:max_matches_per_issue]
            issue_result["matches"] = matches
            issue_result["list_rows"] = len(matches)
        verification_results: list[MatchVerification] = []

        if matches:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(verify_bjop, match) for match in matches]
                for future in as_completed(futures):
                    verification_results.append(future.result())

            verification_results.sort(key=lambda item: (item.home_team, item.away_team, item.newplayid))

        issue_results.append(
            {
                "issue_summary": summarize_issue_verification(issue_result, verification_results),
                "matches": [asdict(match) for match in matches],
                "verifications": [asdict(result) for result in verification_results],
            }
        )

    return {
        "issues_tested": issues,
        "issue_results": issue_results,
    }


def print_summary(report: dict[str, Any]) -> None:
    """打印终端摘要。"""

    print(f"issues_tested={report['issues_tested']}")
    for item in report["issue_results"]:
        summary = item["issue_summary"]
        print(
            "issue={issue} list_rows={list_rows} sum_match={sum_match} "
            "detail_ok={detail_ok_count} with_rates={with_rates_count} "
            "with_zhishu={with_zhishu_count}".format(**summary)
        )

    print("sample_matches:")
    for item in report["issue_results"]:
        summary = item["issue_summary"]
        for sample in summary["sample_matches"][:2]:
            print(
                f"- issue={summary['issue']} "
                f"{sample['home_team']} vs {sample['away_team']} "
                f"detail_ok={sample['detail_ok']} "
                f"company_rows={sample['company_rows']} "
                f"parsed_rate_rows={sample['parsed_rate_rows']} "
                f"zhishu_links={sample['zhishu_links']}"
            )


def build_parser() -> argparse.ArgumentParser:
    """构造 CLI 参数。"""

    parser = argparse.ArgumentParser(description="验证足彩网历史赔率与 bjop 页面是否仍可访问")
    parser.add_argument(
        "--extra-issue",
        action="append",
        default=["2025-01-01"],
        help="额外验证的 issue，可传多次；默认会检查 2025-01-01",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=6,
        help="并发验证 bjop 详情页的 worker 数，默认 6",
    )
    parser.add_argument(
        "--max-matches-per-issue",
        type=int,
        help="每个 issue 最多验证多少场比赛，用于降低触发风控的概率",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT_PATH),
        help="JSON 输出文件路径",
    )
    return parser


def main() -> int:
    """CLI 主入口。"""

    args = build_parser().parse_args()
    current_issues = get_current_issue_options()
    issues = list(dict.fromkeys([*current_issues, *args.extra_issue]))

    report = run_verification(
        issues=issues,
        max_workers=args.max_workers,
        max_matches_per_issue=args.max_matches_per_issue,
    )
    output_path = Path(args.output)
    output_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print_summary(report)
    print(f"json_saved={output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
