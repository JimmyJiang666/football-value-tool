"""Streamlit 页面逻辑。"""

import pandas as pd
import streamlit as st

from jczq_assistant.config import APP_TITLE, SOURCE_SITE_URL
from jczq_assistant.db import init_db, save_matches_raw
from jczq_assistant.mock_data import get_mock_matches
from jczq_assistant.scraper import fetch_today_matches


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


def main() -> None:
    """渲染首页。

    当前阶段目标：
    1. 页面可以本地直接打开
    2. 点击按钮后抓取今日比赛
    3. 抓取失败时回退到 mock 数据
    4. 启动时自动初始化 SQLite
    """

    st.set_page_config(page_title=APP_TITLE, layout="wide")

    # 启动页面时顺手确保数据库和基础表已经就绪。
    db_path = init_db()

    # 默认仍然使用 mock 数据，抓取失败时也回到这里。
    if "matches_df" not in st.session_state:
        st.session_state["matches_df"] = get_mock_matches()
    if "data_source_label" not in st.session_state:
        st.session_state["data_source_label"] = "Mock 数据"

    st.title(APP_TITLE)
    st.caption("当前版本：抓取今日竞彩足球比赛列表 MVP")

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

    st.subheader("今日比赛列表")
    st.caption(f"当前数据源：{st.session_state['data_source_label']}")
    st.dataframe(
        st.session_state["matches_df"],
        use_container_width=True,
        hide_index=True,
    )

    # 先把当前原型状态放在页面上，方便后续开发时确认环境是否正常。
    st.info(
        f"SQLite 已初始化：{db_path}\n\n"
        f"预留数据源：{SOURCE_SITE_URL}"
    )
