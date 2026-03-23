"""项目级基础配置。

当前阶段只保留最少的配置项，避免过早抽象。
"""

import os
from pathlib import Path


# 项目根目录：.../football-value-tool
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# 本地数据目录，用于存放 SQLite 数据库等文件。
DATA_DIR = PROJECT_ROOT / "data"

# Streamlit 页面标题。
APP_TITLE = "赌神"

# 部署只读 demo 模式：隐藏写库入口，并强制数据库只读连接。
APP_READ_ONLY = os.getenv("APP_READ_ONLY", "").strip().lower() in {"1", "true", "yes", "on"}

# 当前优先使用的公开数据源地址。
SOURCE_SITE_URL = "https://trade.500.com/"

# 抓取请求的基础配置。
REQUEST_TIMEOUT_SECONDS = 15
REQUEST_RETRIES = 2
REQUEST_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)
