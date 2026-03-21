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

# 当前优先使用的公开数据源地址。
SOURCE_SITE_URL = "https://trade.500.com/"

# 500.com 当前在售竞彩足球页面。
JCZQ_MATCH_LIST_URL = "https://trade.500.com/jczq/"

# 当主页面结构变化或暂时不可用时，使用同站点的公开比分页兜底。
JCZQ_FALLBACK_LIVE_URL = "https://live.zgzcw.com/jz/"

# 竞彩足球历史开奖结果页。
JCZQ_RESULTS_URL = "https://cp.zgzcw.com/dc/getKaijiangFootBall.action"

# 抓取请求的基础配置。
REQUEST_TIMEOUT_SECONDS = 15
REQUEST_RETRIES = 2
REQUEST_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

# 历史赛果抓取的最大页数，当前只做 MVP，避免一次抓太多。
HISTORY_RESULTS_MAX_PAGES = 5
