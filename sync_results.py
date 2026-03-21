"""历史赛果同步 CLI 入口。

执行示例：
    python sync_results.py backfill --start-date 2025-01-01 --end-date 2025-03-31
    python sync_results.py sync-recent --days 7
"""

from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from jczq_assistant.results_sync import main


if __name__ == "__main__":
    raise SystemExit(main())
