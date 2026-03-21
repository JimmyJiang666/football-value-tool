# 竞彩足球推荐助手

一个本地运行的个人工具原型，当前阶段只实现：

- `Streamlit` 首页
- `SQLite` 数据库初始化
- 今日比赛表格的 mock 数据展示

后续可以在这个骨架上继续增加：

1. 抓取今日竞彩足球比赛
2. 抓取历史开奖结果
3. 做简单推荐和解释
4. 做提醒

## 技术栈

- Python
- Streamlit
- SQLite

## 项目结构

```text
football-value-tool/
├── app.py
├── init_db.py
├── README.md
├── requirements.txt
├── data/
│   └── .gitkeep
└── src/
    └── jczq_assistant/
        ├── __init__.py
        ├── config.py
        ├── db.py
        ├── mock_data.py
        └── web.py
```

## 安装

建议使用虚拟环境：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 初始化数据库

首次运行前，可以手动初始化 SQLite：

```bash
python3 init_db.py
```

这会在 `data/jczq.sqlite3` 创建数据库文件，并初始化 `matches_raw` 表。

说明：首页启动时也会自动执行一次初始化，所以这一步是可选的。

## 启动本地网页

```bash
streamlit run app.py
```

启动后，终端会输出本地访问地址，通常是：

```text
http://localhost:8501
```

## 当前首页内容

- 页面标题：`竞彩足球推荐助手`
- 按钮：`抓取今日比赛（暂未实现）`
- 表格：使用 mock 数据展示今日比赛列表

## 下一步建议

下一阶段可以优先补这两块：

1. 增加真实页面抓取模块，把今日比赛写入 `matches_raw`
2. 增加历史开奖结果表，并建立最基础的数据清洗流程
