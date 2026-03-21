"""Streamlit 页面逻辑。"""

from datetime import datetime
from datetime import timedelta

import pandas as pd
import streamlit as st

from jczq_assistant.config import APP_TITLE, SOURCE_SITE_URL
from jczq_assistant.db import init_db, save_matches_raw
from jczq_assistant.mock_data import get_mock_matches
from jczq_assistant.scraper import fetch_today_matches
from jczq_assistant.sfc500_history import (
    SFC500_DATABASE_PATH,
    get_sfc500_filter_options,
    get_sfc500_history_overview,
    init_sfc500_db,
    query_sfc500_matches,
    sync_recent_history,
)


DISPLAY_COLUMNS = [
    "比赛编号",
    "联赛",
    "开赛时间",
    "主队",
    "客队",
    "主胜赔率",
    "平局赔率",
    "客胜赔率",
]

RECENT_SYNC_OPTIONS = {
    "最近 7 天": 7,
    "最近 14 天": 14,
    "最近 30 天": 30,
}

SFC500_HISTORY_COLUMNS = [
    "期次",
    "场次",
    "联赛",
    "比赛时间",
    "主队",
    "客队",
    "比分",
    "赛果",
    "已开奖",
    "主胜均赔",
    "平局均赔",
    "客胜均赔",
    "主胜概率",
    "平局概率",
    "客胜概率",
    "亚盘主队",
    "亚盘盘口",
    "亚盘客队",
    "凯利主胜",
    "凯利平局",
    "凯利客胜",
]


def build_display_dataframe(matches: list[dict]) -> pd.DataFrame:
    """把抓取到的内部字段转换成页面展示字段。"""

    rows = [
        {
            "比赛编号": match.get("match_no"),
            "联赛": match.get("league"),
            "开赛时间": match.get("kickoff_time"),
            "主队": match.get("home_team"),
            "客队": match.get("away_team"),
            "主胜赔率": match.get("home_win_odds"),
            "平局赔率": match.get("draw_odds"),
            "客胜赔率": match.get("away_win_odds"),
        }
        for match in matches
    ]

    return pd.DataFrame(rows, columns=DISPLAY_COLUMNS)


def build_sfc500_history_dataframe(matches: list[dict]) -> pd.DataFrame:
    """把 500.com 历史赔率记录转成表格。"""

    rows = [
        {
            "期次": match.get("expect"),
            "场次": match.get("match_no"),
            "联赛": match.get("competition"),
            "比赛时间": match.get("match_time"),
            "主队": match.get("home_team"),
            "客队": match.get("away_team"),
            "比分": match.get("final_score"),
            "赛果": match.get("spf_result"),
            "已开奖": "是" if match.get("is_settled") else "否",
            "主胜均赔": match.get("avg_win_odds"),
            "平局均赔": match.get("avg_draw_odds"),
            "客胜均赔": match.get("avg_lose_odds"),
            "主胜概率": match.get("avg_win_prob"),
            "平局概率": match.get("avg_draw_prob"),
            "客胜概率": match.get("avg_lose_prob"),
            "亚盘主队": match.get("asian_home_odds"),
            "亚盘盘口": match.get("asian_line"),
            "亚盘客队": match.get("asian_away_odds"),
            "凯利主胜": match.get("kelly_win"),
            "凯利平局": match.get("kelly_draw"),
            "凯利客胜": match.get("kelly_lose"),
        }
        for match in matches
    ]
    return pd.DataFrame(rows, columns=SFC500_HISTORY_COLUMNS)


def render_recent_sync_summary(summary: dict) -> None:
    """渲染最近一次 500.com 历史同步摘要。"""

    st.caption(f"最近一次同步写入：{summary.get('db_path')}")
    metric_columns = st.columns(4)
    metric_columns[0].metric("days", summary.get("days"))
    metric_columns[1].metric("status", summary.get("status") or "-")
    metric_columns[2].metric("rows_fetched", summary.get("rows_fetched") or 0)
    metric_columns[3].metric("rows_inserted", summary.get("rows_inserted") or 0)

    extra_columns = st.columns(2)
    extra_columns[0].metric("valid_expects", summary.get("valid_expects") or 0)
    extra_columns[1].metric("scanned_expects", summary.get("scanned_expects") or 0)

    if summary.get("errors"):
        st.error("部分期次同步失败：\n\n" + "\n".join(summary["errors"]))

    sample_df = build_sfc500_history_dataframe(summary.get("sample_matches", []))
    if not sample_df.empty:
        st.caption("样本预览")
        st.dataframe(sample_df, use_container_width=True, hide_index=True)


def _estimate_progress_ratio(event: dict) -> float:
    """根据同步事件估算进度条比例。"""

    if event.get("stage") == "finish":
        return 1.0

    current_index = event.get("current_index", 0)
    total_windows = event.get("total_windows") or 0
    if total_windows <= 0:
        return 0.02

    return min(max(current_index / total_windows, 0.0), 1.0)


def _resolve_settled_filter(label: str) -> bool | None:
    """把页面文案映射成查询参数。"""

    if label == "仅已开奖":
        return True
    if label == "仅未开奖":
        return False
    return None


def main() -> None:
    """渲染首页。"""

    st.set_page_config(page_title=APP_TITLE, layout="wide")

    db_path = init_db()
    init_sfc500_db()

    if "matches_df" not in st.session_state:
        st.session_state["matches_df"] = get_mock_matches()
    if "data_source_label" not in st.session_state:
        st.session_state["data_source_label"] = "Mock 数据"
    if "sfc500_recent_sync_summary" not in st.session_state:
        st.session_state["sfc500_recent_sync_summary"] = None

    st.title(APP_TITLE)
    st.caption(f"当前优先数据源：{SOURCE_SITE_URL}")

    button_clicked = st.button("抓取今日比赛", type="primary")
    if button_clicked:
        with st.spinner("正在抓取今日比赛，请稍候..."):
            try:
                matches = fetch_today_matches()
                display_df = build_display_dataframe(matches)

                if display_df.empty:
                    raise RuntimeError("页面返回成功，但没有解析到比赛数据。")

                saved_count = save_matches_raw(matches)
                st.session_state["matches_df"] = display_df
                st.session_state["data_source_label"] = matches[0]["source_url"]

                st.success(f"抓取成功，共获取 {len(matches)} 场比赛，已写入 {saved_count} 条记录。")
            except Exception as exc:
                st.session_state["matches_df"] = get_mock_matches()
                st.session_state["data_source_label"] = "Mock 数据（抓取失败后回退）"
                st.error(f"抓取失败：{exc}")

    st.subheader("当前在售比赛列表")
    st.caption(f"当前数据源：{st.session_state['data_source_label']}")
    st.dataframe(
        st.session_state["matches_df"],
        use_container_width=True,
        hide_index=True,
    )

    st.divider()

    st.subheader("500.com 历史赔率同步")
    st.caption(f"主库：{SFC500_DATABASE_PATH}")

    sync_control_col1, sync_control_col2 = st.columns([1, 2])
    selected_sync_label = sync_control_col1.selectbox(
        "同步范围",
        options=list(RECENT_SYNC_OPTIONS.keys()),
        index=0,
    )
    sync_button_clicked = sync_control_col2.button(
        "同步 500.com 历史赔率",
        key="sync_sfc500_recent_history",
        type="primary",
    )

    sync_status_placeholder = st.empty()
    sync_progress_placeholder = st.empty()

    if sync_button_clicked:
        progress_bar = sync_progress_placeholder.progress(0.0)

        def on_sync_progress(event: dict) -> None:
            ratio = _estimate_progress_ratio(event)
            progress_bar.progress(ratio)

            message = event.get("message", "正在同步...")
            stage = event.get("stage")

            if stage == "expect_error":
                sync_status_placeholder.error(message)
            elif stage == "finish":
                if event.get("status") == "success":
                    sync_status_placeholder.success(message)
                else:
                    sync_status_placeholder.warning(message)
            else:
                sync_status_placeholder.info(message)

        days = RECENT_SYNC_OPTIONS[selected_sync_label]
        with st.spinner(f"正在同步最近 {days} 天相关期次，请稍候..."):
            try:
                summary = sync_recent_history(
                    days=days,
                    progress_callback=on_sync_progress,
                )
                st.session_state["sfc500_recent_sync_summary"] = summary
                sync_progress_placeholder.progress(1.0)
            except Exception as exc:
                st.session_state["sfc500_recent_sync_summary"] = None
                sync_status_placeholder.error(f"同步失败：{exc}")
                sync_progress_placeholder.empty()
                st.error(f"同步失败：{exc}")

    if st.session_state["sfc500_recent_sync_summary"]:
        render_recent_sync_summary(st.session_state["sfc500_recent_sync_summary"])
    else:
        st.info("尚未执行页面内的 500.com 历史赔率同步。")

    st.divider()

    st.subheader("500.com 历史赔率与赛果")
    overview = get_sfc500_history_overview()

    overview_col1, overview_col2, overview_col3, overview_col4 = st.columns(4)
    overview_col1.metric("records", overview.get("row_count") or 0)
    overview_col2.metric("expects", overview.get("expect_count") or 0)
    overview_col3.metric("settled", overview.get("settled_count") or 0)
    overview_col4.metric("competitions", overview.get("competition_count") or 0)

    min_match_time = overview.get("min_match_time")
    max_match_time = overview.get("max_match_time")
    if min_match_time and max_match_time:
        st.caption(f"覆盖时间：{min_match_time} -> {max_match_time}")
    else:
        st.caption("当前历史库还没有可展示的数据。")

    filter_options = get_sfc500_filter_options()
    today = datetime.now().date()
    default_end_date = today
    if max_match_time:
        default_end_date = min(today, datetime.fromisoformat(max_match_time).date())

    default_start_date = default_end_date - timedelta(days=30)
    if min_match_time:
        min_date = datetime.fromisoformat(min_match_time).date()
        if default_start_date < min_date:
            default_start_date = min_date

    filter_col1, filter_col2, filter_col3 = st.columns(3)
    start_date = filter_col1.date_input(
        "开始日期",
        value=default_start_date,
        key="history_start_date",
    )
    end_date = filter_col2.date_input(
        "结束日期",
        value=default_end_date,
        key="history_end_date",
    )
    result_limit = filter_col3.selectbox(
        "展示条数",
        options=[100, 200, 500, 1000],
        index=1,
    )

    filter_col4, filter_col5 = st.columns(2)
    selected_competitions = filter_col4.multiselect(
        "联赛",
        options=filter_options["competitions"],
        default=[],
    )
    selected_teams = filter_col5.multiselect(
        "球队",
        options=filter_options["teams"],
        default=[],
    )

    filter_col6, filter_col7, filter_col8 = st.columns(3)
    team_keyword = filter_col6.text_input("球队关键词", value="")
    expect_value = filter_col7.text_input("期次", value="")
    settled_filter_label = filter_col8.selectbox(
        "赛果状态",
        options=["全部", "仅已开奖", "仅未开奖"],
        index=0,
    )

    if start_date > end_date:
        st.error("开始日期不能晚于结束日期。")
    else:
        query_result = query_sfc500_matches(
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            competitions=selected_competitions,
            teams=selected_teams,
            team_keyword=team_keyword,
            expect=expect_value,
            settled_only=_resolve_settled_filter(settled_filter_label),
            limit=result_limit,
        )

        history_df = build_sfc500_history_dataframe(query_result["rows"])
        st.caption(
            f"匹配到 {query_result['total_count']} 条记录，当前展示前 {min(len(history_df), result_limit)} 条。"
        )

        if history_df.empty:
            st.info("当前筛选条件下没有记录。")
        else:
            st.dataframe(
                history_df,
                use_container_width=True,
                hide_index=True,
            )

    st.info(
        f"SQLite 已初始化：{db_path}\n\n"
        f"500.com 历史主库：{SFC500_DATABASE_PATH}"
    )
