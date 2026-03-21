"""验证官方中国体彩网竞彩足球赛果页的数据加载方式。

当前目标：
1. 用 requests 直接请求页面，判断是否被拦截、是否是静态 HTML
2. 用 Playwright 模拟浏览器访问页面，抓取真实网络请求
3. 如果页面可访问，尽量判断数据是静态直出还是 JS 异步加载

注意：
- 这是独立验证脚本，不接入现有主程序
- 默认输出保存到 /tmp/lottery_gov_cn_verify
- Playwright 是可选依赖；未安装时只运行 requests 验证
- 如果当前网络环境被站点风控，这个脚本会明确输出“被拦截”
"""

from __future__ import annotations

import argparse
from dataclasses import asdict
from dataclasses import dataclass
import json
from pathlib import Path
import re
import subprocess
import sys
from typing import Any

import requests


DEFAULT_URL = "https://www.lottery.gov.cn/jc/zqsgkj/"
DEFAULT_OUTPUT_DIR = Path("/tmp/lottery_gov_cn_verify")
EDGEONE_BLOCK_MARKERS = (
    "请求已被站点的安全策略拦截",
    "Restricted Access",
    "Tencent Cloud EdgeOne",
)


@dataclass
class RequestCheckResult:
    """requests 方式的验证结果。"""

    ok: bool
    status_code: int | None
    final_url: str
    content_type: str
    server: str
    blocked_by_edgeone: bool
    needs_js_render: bool
    likely_reason: str
    saved_html_path: str


@dataclass
class PlaywrightCheckResult:
    """Playwright 方式的验证结果。"""

    executed: bool
    ok: bool
    goto_status: int | None
    final_url: str
    title: str
    blocked_by_edgeone: bool
    likely_reason: str
    saved_html_path: str
    saved_network_path: str
    candidate_api_requests: list[dict[str, Any]]


def run_requests_check(url: str, output_dir: Path) -> RequestCheckResult:
    """执行 requests 直连验证。"""

    output_dir.mkdir(parents=True, exist_ok=True)
    html_path = output_dir / "requests_response.html"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,image/apng,*/*;q=0.8"
        ),
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Upgrade-Insecure-Requests": "1",
        "Referer": "https://www.lottery.gov.cn/",
    }

    try:
        response = requests.get(url, headers=headers, timeout=20)
        html_path.write_text(response.text, encoding="utf-8")
    except requests.RequestException as exc:
        return RequestCheckResult(
            ok=False,
            status_code=None,
            final_url=url,
            content_type="",
            server="",
            blocked_by_edgeone=False,
            needs_js_render=False,
            likely_reason=f"network_error: {exc}",
            saved_html_path=str(html_path),
        )

    blocked = is_edgeone_block(
        status_code=response.status_code,
        html=response.text,
        server=response.headers.get("server", ""),
    )
    likely_reason = infer_failure_reason(
        status_code=response.status_code,
        blocked_by_edgeone=blocked,
        html=response.text,
    )

    return RequestCheckResult(
        ok=response.ok and not blocked,
        status_code=response.status_code,
        final_url=response.url,
        content_type=response.headers.get("content-type", ""),
        server=response.headers.get("server", ""),
        blocked_by_edgeone=blocked,
        needs_js_render=False,
        likely_reason=likely_reason,
        saved_html_path=str(html_path),
    )


def run_playwright_check(
    url: str,
    output_dir: Path,
    headed: bool = False,
    manual_seconds: int = 0,
) -> PlaywrightCheckResult:
    """执行 Playwright 浏览器级验证。"""

    output_dir.mkdir(parents=True, exist_ok=True)
    html_path = output_dir / "playwright_response.html"
    network_path = output_dir / "playwright_network.json"

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return PlaywrightCheckResult(
            executed=False,
            ok=False,
            goto_status=None,
            final_url=url,
            title="",
            blocked_by_edgeone=False,
            likely_reason="playwright_not_installed",
            saved_html_path=str(html_path),
            saved_network_path=str(network_path),
            candidate_api_requests=[],
        )

    inline_code = f"""
from pathlib import Path
import json
from playwright.sync_api import sync_playwright

events = []
result = {{
    "goto_status": None,
    "final_url": {url!r},
    "title": "",
    "error": "",
}}

with sync_playwright() as p:
    try:
        browser = p.chromium.launch(headless={str(not headed)})
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            locale="zh-CN",
            viewport={{"width": 1440, "height": 900}},
        )
        page = context.new_page()
        page.on(
            "request",
            lambda req: events.append({{
                "event": "request",
                "resource_type": req.resource_type,
                "method": req.method,
                "url": req.url,
                "headers": req.headers,
            }}),
        )
        page.on(
            "response",
            lambda resp: events.append({{
                "event": "response",
                "status": resp.status,
                "url": resp.url,
                "headers": resp.headers,
            }}),
        )
        response = page.goto({url!r}, wait_until="networkidle", timeout=45000)
        if {manual_seconds} > 0:
            page.wait_for_timeout({manual_seconds} * 1000)
        html = page.content()
        Path({str(html_path)!r}).write_text(html, encoding="utf-8")
        Path({str(network_path)!r}).write_text(
            json.dumps(events, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        result["goto_status"] = response.status if response else None
        result["final_url"] = page.url
        result["title"] = page.title()
        browser.close()
    except Exception as exc:
        Path({str(network_path)!r}).write_text(
            json.dumps(events, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        result["error"] = str(exc)

print(json.dumps(result, ensure_ascii=False))
"""

    process = subprocess.run(
        [sys.executable, "-c", inline_code],
        capture_output=True,
        text=True,
        timeout=90,
    )

    stdout = process.stdout.strip()
    stderr = process.stderr.strip()

    if not stdout:
        return PlaywrightCheckResult(
            executed=True,
            ok=False,
            goto_status=None,
            final_url=url,
            title="",
            blocked_by_edgeone=False,
            likely_reason=f"playwright_error: {stderr or 'empty_stdout'}",
            saved_html_path=str(html_path),
            saved_network_path=str(network_path),
            candidate_api_requests=[],
        )

    payload = json.loads(stdout)
    if payload.get("error"):
        return PlaywrightCheckResult(
            executed=True,
            ok=False,
            goto_status=payload.get("goto_status"),
            final_url=payload.get("final_url", url),
            title=payload.get("title", ""),
            blocked_by_edgeone=False,
            likely_reason=f"playwright_error: {payload['error']}",
            saved_html_path=str(html_path),
            saved_network_path=str(network_path),
            candidate_api_requests=load_candidate_requests(network_path),
        )

    html = html_path.read_text(encoding="utf-8") if html_path.exists() else ""
    goto_status = payload.get("goto_status")
    title = payload.get("title", "")
    final_url = payload.get("final_url", url)

    blocked = is_edgeone_block(
        status_code=goto_status,
        html=html,
        server="",
    )
    likely_reason = infer_failure_reason(
        status_code=goto_status,
        blocked_by_edgeone=blocked,
        html=html,
    )

    return PlaywrightCheckResult(
        executed=True,
        ok=bool(goto_status and 200 <= goto_status < 400 and not blocked),
        goto_status=goto_status,
        final_url=final_url,
        title=title,
        blocked_by_edgeone=blocked,
        likely_reason=likely_reason,
        saved_html_path=str(html_path),
        saved_network_path=str(network_path),
        candidate_api_requests=load_candidate_requests(network_path),
    )


def is_edgeone_block(status_code: int | None, html: str, server: str) -> bool:
    """判断是否被腾讯 EdgeOne 安全页拦截。"""

    if status_code == 567:
        return True
    if "TencentEdgeOne" in server:
        return True
    return any(marker in html for marker in EDGEONE_BLOCK_MARKERS)


def infer_failure_reason(
    status_code: int | None,
    blocked_by_edgeone: bool,
    html: str,
) -> str:
    """给出较粗粒度的失败原因。"""

    if blocked_by_edgeone:
        return "blocked_by_site_security_or_ip_policy"
    if status_code in (401, 403):
        return "forbidden_maybe_cookie_or_policy"
    if status_code and status_code >= 500:
        return "server_side_error_or_waf"
    if looks_like_js_shell(html):
        return "page_shell_detected_may_need_js_render"
    return "unknown"


def looks_like_js_shell(html: str) -> bool:
    """粗略判断是否像前端壳页。"""

    scripts = len(re.findall(r"<script", html, flags=re.IGNORECASE))
    table_rows = len(re.findall(r"<tr", html, flags=re.IGNORECASE))
    return scripts > 3 and table_rows == 0


def extract_candidate_api_requests(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """从浏览器网络日志中提取可能的数据接口。"""

    candidates: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    for event in events:
        if event.get("event") != "request":
            continue

        resource_type = event.get("resource_type")
        url = event.get("url", "")
        method = event.get("method", "")

        if resource_type not in {"xhr", "fetch", "document"}:
            continue

        key = (method, url)
        if key in seen:
            continue
        seen.add(key)

        candidates.append(
            {
                "resource_type": resource_type,
                "method": method,
                "url": url,
            }
        )

    return candidates


def load_candidate_requests(network_path: Path) -> list[dict[str, Any]]:
    """从保存的网络日志中读取候选接口。"""

    if not network_path.exists():
        return []
    events = json.loads(network_path.read_text(encoding="utf-8"))
    return extract_candidate_api_requests(events)


def print_summary(
    request_result: RequestCheckResult,
    playwright_result: PlaywrightCheckResult,
) -> None:
    """打印终端摘要，便于快速判断。"""

    print("== requests ==")
    print(json.dumps(asdict(request_result), ensure_ascii=False, indent=2))
    print()
    print("== playwright ==")
    print(json.dumps(asdict(playwright_result), ensure_ascii=False, indent=2))

    if request_result.blocked_by_edgeone and playwright_result.blocked_by_edgeone:
        print()
        print("Conclusion: both requests and Playwright were blocked before the business page loaded.")
        print("This environment cannot observe the real data-loading path of the official page.")


def build_parser() -> argparse.ArgumentParser:
    """构造 CLI 参数。"""

    parser = argparse.ArgumentParser(description="验证中国体彩网竞彩足球赛果页的数据加载方式")
    parser.add_argument("--url", default=DEFAULT_URL, help="待验证页面 URL")
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="保存 HTML 和网络日志的目录",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="以有界面浏览器运行 Playwright，便于手动观察",
    )
    parser.add_argument(
        "--manual-seconds",
        type=int,
        default=0,
        help="页面打开后额外等待多少秒，便于手动操作日期筛选并抓网络请求",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI 主入口。"""

    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)

    request_result = run_requests_check(args.url, output_dir)
    playwright_result = run_playwright_check(
        url=args.url,
        output_dir=output_dir,
        headed=args.headed,
        manual_seconds=args.manual_seconds,
    )
    print_summary(request_result, playwright_result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
