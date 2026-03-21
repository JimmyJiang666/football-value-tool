"""项目级基础配置。

当前阶段只保留最少的配置项，避免过早抽象。
"""

from pathlib import Path


# 项目根目录：.../football-value-tool
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# 本地数据目录，用于存放 SQLite 数据库等文件。
DATA_DIR = PROJECT_ROOT / "data"

# SQLite 数据库文件路径。
DATABASE_PATH = DATA_DIR / "jczq.sqlite3"

# Streamlit 页面标题。
APP_TITLE = "竞彩足球推荐助手"

# 未来真实抓取时可复用的数据源地址。
SOURCE_SITE_URL = "https://cp.zgzcw.com/"
