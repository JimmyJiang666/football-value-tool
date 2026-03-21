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

# 竞彩足球胜平负/让球公开列表页。
JCZQ_MATCH_LIST_URL = (
    "https://cp.zgzcw.com/lottery/jchtplayvsForJsp.action?lotteryId=47&type=jcmini"
)

# 当主页面结构变化或暂时不可用时，使用同站点的公开比分页兜底。
JCZQ_FALLBACK_LIVE_URL = "https://live.zgzcw.com/jz/"

# 抓取请求的基础配置。
REQUEST_TIMEOUT_SECONDS = 15
REQUEST_RETRIES = 2
REQUEST_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)
