"""Streamlit 启动入口。

根目录保留一个很薄的入口文件，方便直接执行：
    streamlit run app.py
实际业务代码统一放在 src 目录下。
"""

from pathlib import Path
import sys


# 把 src 目录加入 Python 搜索路径，保证本地直接运行时可以导入项目代码。
PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from jczq_assistant.web import main


if __name__ == "__main__":
    main()
