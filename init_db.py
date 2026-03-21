"""SQLite 初始化入口。

执行方式：
    python3 init_db.py
"""

from pathlib import Path
import sys


# 让根目录脚本可以直接导入 src 下的项目代码。
PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from jczq_assistant.db import init_db


if __name__ == "__main__":
    db_path = init_db()
    print(f"SQLite database initialized: {db_path}")
