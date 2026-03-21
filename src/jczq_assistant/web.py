"""Streamlit 页面逻辑。"""

import streamlit as st

from jczq_assistant.config import APP_TITLE, SOURCE_SITE_URL
from jczq_assistant.db import init_db
from jczq_assistant.mock_data import get_mock_matches


def main() -> None:
    """渲染首页。

    当前阶段目标：
    1. 页面可以本地直接打开
    2. 展示一个占位按钮
    3. 展示一张 mock 比赛表格
    4. 启动时自动初始化 SQLite
    """

    st.set_page_config(page_title=APP_TITLE, layout="wide")

    # 启动页面时顺手确保数据库和基础表已经就绪。
    db_path = init_db()

    st.title(APP_TITLE)
    st.caption("第一阶段：项目骨架 + 本地网页首页")

    button_clicked = st.button("抓取今日比赛（暂未实现）", type="primary")
    if button_clicked:
        st.warning("抓取逻辑将在下一阶段实现，当前展示的是 mock 数据。")

    st.subheader("今日比赛列表")
    st.dataframe(get_mock_matches(), use_container_width=True, hide_index=True)

    # 先把当前原型状态放在页面上，方便后续开发时确认环境是否正常。
    st.info(
        f"SQLite 已初始化：{db_path}\n\n"
        f"预留数据源：{SOURCE_SITE_URL}"
    )
