# 500.com 历史赔率助手

一个本地运行的 `Streamlit + SQLite` 工具，当前只保留 500.com 胜负彩历史赔率与赛果主链路。

## 主要能力

- 同步 500.com 历史赔率与赛果到 `data/sfc500_history.sqlite3`
- 页面内按日期、联赛、球队、期次筛选历史数据
- 保存原始队名，同时写入标准化队名字段
- 维护球队别名映射表 `team_name_aliases`

## 启动

```bash
streamlit run app.py
```

## 常用命令

同步最近 30 天相关期次：

```bash
python sync_sfc500_history.py sync-recent --days 30
```

回填球队标准名：

```bash
python team_name_tools.py backfill
```

查看待人工确认的别名候选：

```bash
python team_name_tools.py list-candidates --limit 20
```

手工确认一条别名：

```bash
python team_name_tools.py add-alias --alias 曼联 --canonical 曼彻斯特联
```

运行一次最小回测：

```bash
python run_backtest.py --strategy lowest_odds_fixed --start-date 2025-01-01 --end-date 2025-03-31 --stake 10
```
