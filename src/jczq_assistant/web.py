"""Streamlit 页面逻辑。"""

from datetime import date
from datetime import datetime
from datetime import timedelta

import pandas as pd
import streamlit as st

from jczq_assistant.backtest import BacktestConfig
from jczq_assistant.backtest import BacktestEngine
from jczq_assistant.backtest import DEFAULT_LOOKBACK_DAYS
from jczq_assistant.backtest import DEFAULT_VALUE_MODE
from jczq_assistant.backtest import SQLiteBacktestDataSource
from jczq_assistant.backtest import build_strategy
from jczq_assistant.backtest import get_default_selection_thresholds
from jczq_assistant.config import APP_READ_ONLY, APP_TITLE, SOURCE_SITE_URL
from jczq_assistant.sfc500_history import (
    SFC500_DATABASE_PATH,
    ensure_sfc500_db_available,
    get_sfc500_filter_options,
    get_sfc500_connection,
    get_sfc500_history_overview,
    init_sfc500_db,
    query_sfc500_matches,
    sync_recent_history,
)
from jczq_assistant.team_names import (
    TeamTableSpec,
    apply_manual_team_name_alias,
    apply_team_name_candidate_unification,
    clean_team_name,
    delete_team_name_review_decision,
    find_team_alias_candidates,
    list_team_name_aliases,
    list_team_name_review_decisions,
    skip_team_name_candidate,
)


RECENT_SYNC_OPTIONS = {
    "最近 7 天": 7,
    "最近 14 天": 14,
    "最近 30 天": 30,
}

SFC500_HISTORY_COLUMNS = [
    "期次",
    "场次",
    "联赛",
    "比赛时间",
    "主队",
    "客队",
    "比分",
    "赛果",
    "已开奖",
    "主胜均赔",
    "平局均赔",
    "客胜均赔",
    "主胜概率",
    "平局概率",
    "客胜概率",
    "亚盘主队",
    "亚盘盘口",
    "亚盘客队",
    "凯利主胜",
    "凯利平局",
    "凯利客胜",
]
TEAM_NAME_TABLE_SPEC = TeamTableSpec(table_name="sfc500_matches_raw")
APP_PAGES = ["历史数据", "回测", "数据库维护"]
READ_ONLY_APP_PAGES = ["历史数据", "回测"]
BACKTEST_STRATEGY_OPTIONS = {
    "最低赔率单关": {
        "strategy_name": "lowest_odds_fixed",
        "mode": "single",
    },
    "历史水位匹配价值投注": {
        "strategy_name": "historical_odds_value",
        "mode": "value_match",
    },
    "最低赔率串关": {
        "strategy_name": "lowest_odds_parlay",
        "mode": "parlay",
    },
}
BACKTEST_DAILY_LIMIT_OPTIONS: list[str | int] = ["不限制", *list(range(0, 15))]
BACKTEST_PARLAY_OPTIONS = list(range(2, 15))
BACKTEST_HISTORY_MATCH_COUNT_OPTIONS = [50, 100, 150, 200, 300, 500]
BACKTEST_LOOKBACK_OPTIONS: list[str | int] = ["全部历史", 180, 365, 730, 1095]
BACKTEST_DATE_PRESET_OPTIONS = [
    "最近 7 天",
    "最近 30 天",
    "最近 90 天",
    "最近 180 天",
    "全部历史",
    "自定义",
]
BACKTEST_WEIGHTING_MODE_OPTIONS = {
    "等权": "equal",
    "距离反比加权": "inverse_distance",
}
BACKTEST_VALUE_MODE_OPTIONS = {
    "概率差": "probability_diff",
    "期望收益": "expected_value",
}
BACKTEST_STAKING_MODE_OPTIONS = {
    "固定投注": "fixed",
    "Kelly 资金管理": "fractional_kelly",
}
BACKTEST_WEIGHTING_MODE_LABELS = {
    value: label for label, value in BACKTEST_WEIGHTING_MODE_OPTIONS.items()
}
BACKTEST_VALUE_MODE_LABELS = {
    value: label for label, value in BACKTEST_VALUE_MODE_OPTIONS.items()
}
BACKTEST_STAKING_MODE_LABELS = {
    value: label for label, value in BACKTEST_STAKING_MODE_OPTIONS.items()
}
BACKTEST_SKIP_REASON_LABELS = {
    "missing_odds": "赔率缺失",
    "match_not_settled": "比赛未开奖",
    "outside_daily_limit": "超出当日下注场数限制",
    "outside_parlay_selection": "未进入当日串关",
    "insufficient_parlay_candidates": "当日可串场次不足",
    "insufficient_history_matches": "历史匹配样本不足",
    "no_positive_edge": "未达到下注阈值",
    "strategy_no_bet": "策略未下注",
    "missing_result_code": "缺少赛果编码",
    "invalid_stake": "下注金额无效",
    "invalid_selection": "下注选项无效",
    "missing_selected_odds": "所选赔率缺失",
    "bankroll_depleted": "资金已耗尽",
    "non_positive_kelly": "Kelly 建议仓位不为正",
    "empty_ticket": "空串关票",
    "duplicate_ticket_leg": "同票重复比赛",
}


def _resolve_default_date_range(overview: dict) -> tuple[date, date]:
    """基于历史库覆盖时间生成默认日期区间。"""

    min_match_time = overview.get("min_match_time")
    max_match_time = overview.get("max_match_time")
    today = datetime.now().date()
    default_end_date = today
    if max_match_time:
        default_end_date = min(today, datetime.fromisoformat(max_match_time).date())

    default_start_date = default_end_date - timedelta(days=30)
    if min_match_time:
        min_date = datetime.fromisoformat(min_match_time).date()
        if default_start_date < min_date:
            default_start_date = min_date

    return default_start_date, default_end_date


def _resolve_date_bounds(overview: dict) -> tuple[date, date]:
    """返回历史库可回测日期边界。"""

    min_match_time = overview.get("min_match_time")
    max_match_time = overview.get("max_match_time")
    today = datetime.now().date()
    min_date = today - timedelta(days=30)
    max_date = today

    if min_match_time:
        min_date = datetime.fromisoformat(min_match_time).date()
    if max_match_time:
        max_date = min(today, datetime.fromisoformat(max_match_time).date())

    return min_date, max_date


def _resolve_preset_date_range(
    preset: str,
    *,
    min_date: date,
    max_date: date,
) -> tuple[date, date]:
    """把时间预设转成起止日期。"""

    if preset == "最近 7 天":
        start_date = max(min_date, max_date - timedelta(days=6))
        return start_date, max_date
    if preset == "最近 30 天":
        start_date = max(min_date, max_date - timedelta(days=30))
        return start_date, max_date
    if preset == "最近 90 天":
        start_date = max(min_date, max_date - timedelta(days=90))
        return start_date, max_date
    if preset == "最近 180 天":
        start_date = max(min_date, max_date - timedelta(days=180))
        return start_date, max_date
    if preset == "全部历史":
        return min_date, max_date
    return st.session_state.get("backtest_start_date", min_date), st.session_state.get(
        "backtest_end_date",
        max_date,
    )


def _format_daily_limit_option(option: str | int) -> str:
    """格式化单关每日下注场数选项。"""

    if option == "不限制":
        return "不限制"
    return f"{option} 场"


def _resolve_daily_limit_value(option: str | int) -> int | None:
    """把页面选项转换成回测配置值。"""

    if option == "不限制":
        return None
    return int(option)


def _format_lookback_option(option: str | int) -> str:
    """格式化历史回看窗口选项。"""

    if option == "全部历史":
        return "全部历史"
    return f"最近 {option} 天"


def _resolve_lookback_value(option: str | int) -> int | None:
    """把历史回看窗口选项转换成回测配置值。"""

    if option == "全部历史":
        return None
    return int(option)


def _format_lookback_label(lookback_days: int | None) -> str:
    """把历史回看窗口值格式化成页面文案。"""

    if lookback_days is None:
        return "全部历史"
    return f"最近 {lookback_days} 天"


def _format_weighting_mode_label(weighting_mode: str) -> str:
    """把样本加权模式转换成页面文案。"""

    return BACKTEST_WEIGHTING_MODE_LABELS.get(weighting_mode, weighting_mode)


def _format_value_mode_label(value_mode: str) -> str:
    """把 value 计算方式转换成页面文案。"""

    return BACKTEST_VALUE_MODE_LABELS.get(value_mode, value_mode)


def _format_staking_mode_label(staking_mode: str) -> str:
    """把投注模式转换成页面文案。"""

    return BACKTEST_STAKING_MODE_LABELS.get(staking_mode, staking_mode)


def _resolve_value_mode_score_label(value_mode: str) -> str:
    """返回当前 value 模式下的分数名称。"""

    if value_mode == "expected_value":
        return "EV"
    return "概率差"


def _resolve_value_mode_threshold_defaults(value_mode: str) -> dict[str, float]:
    """返回当前 value 模式下的默认阈值。"""

    defaults = get_default_selection_thresholds(value_mode)
    return {
        "home_win": float(defaults["home_win"]),
        "draw": float(defaults["draw"]),
        "away_win": float(defaults["away_win"]),
    }


def _format_threshold_meaning(value_mode: str, threshold: float) -> str:
    """格式化阈值数值对应的含义。"""

    if value_mode == "expected_value":
        return f"{threshold:.3f} 表示期望收益率至少 {threshold:.1%}"
    return f"{threshold:.3f} 表示模型概率至少高于庄家概率 {threshold:.1%}"


def _is_parlay_strategy(strategy_name: str) -> bool:
    """判断是否为串关策略。"""

    return strategy_name == "lowest_odds_parlay"


def _is_value_strategy(strategy_name: str) -> bool:
    """判断是否为历史匹配价值策略。"""

    return strategy_name == "historical_odds_value"


def _format_seconds_brief(value: float | None) -> str:
    """把秒数转成简洁文案。"""

    if value is None:
        return "-"
    total_seconds = max(int(round(value)), 0)
    minutes, seconds = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    if minutes > 0:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def build_team_name_candidate_dataframe(candidate: dict) -> pd.DataFrame:
    """把球队名候选组转成表格。"""

    rows = [
        {
            "候选名称": variant.get("team_name"),
            "当前标准名": variant.get("canonical_name") or "-",
            "记录数": variant.get("row_count"),
        }
        for variant in candidate.get("variants", [])
    ]
    return pd.DataFrame(rows, columns=["候选名称", "当前标准名", "记录数"])


def build_team_name_alias_dataframe(rows: list[dict]) -> pd.DataFrame:
    """把已确认的球队别名映射转成表格。"""

    return pd.DataFrame(
        rows,
        columns=["alias_name", "canonical_name", "source", "confidence", "updated_at"],
    )


def _build_candidate_canonical_options(candidate: dict) -> list[str]:
    """为一个候选组构造可选标准名。"""

    options: list[str] = []
    seen: set[str] = set()

    for canonical_name in candidate.get("canonical_names", []):
        cleaned = clean_team_name(canonical_name)
        if cleaned and cleaned not in seen:
            options.append(cleaned)
            seen.add(cleaned)

    sorted_variants = sorted(
        candidate.get("variants", []),
        key=lambda item: (-int(item.get("row_count") or 0), str(item.get("team_name") or "")),
    )
    for variant in sorted_variants:
        for field_name in ("team_name", "canonical_name"):
            cleaned = clean_team_name(variant.get(field_name))
            if cleaned and cleaned not in seen:
                options.append(cleaned)
                seen.add(cleaned)

    return options


def build_sfc500_history_dataframe(matches: list[dict]) -> pd.DataFrame:
    """把 500.com 历史赔率记录转成表格。"""

    rows = [
        {
            "期次": match.get("expect"),
            "场次": match.get("match_no"),
            "联赛": match.get("competition"),
            "比赛时间": match.get("match_time"),
            "主队": match.get("home_team_canonical") or match.get("home_team"),
            "客队": match.get("away_team_canonical") or match.get("away_team"),
            "比分": match.get("final_score"),
            "赛果": match.get("spf_result"),
            "已开奖": "是" if match.get("is_settled") else "否",
            "主胜均赔": match.get("avg_win_odds"),
            "平局均赔": match.get("avg_draw_odds"),
            "客胜均赔": match.get("avg_lose_odds"),
            "主胜概率": match.get("avg_win_prob"),
            "平局概率": match.get("avg_draw_prob"),
            "客胜概率": match.get("avg_lose_prob"),
            "亚盘主队": match.get("asian_home_odds"),
            "亚盘盘口": match.get("asian_line"),
            "亚盘客队": match.get("asian_away_odds"),
            "凯利主胜": match.get("kelly_win"),
            "凯利平局": match.get("kelly_draw"),
            "凯利客胜": match.get("kelly_lose"),
        }
        for match in matches
    ]
    return pd.DataFrame(rows, columns=SFC500_HISTORY_COLUMNS)


def _format_backtest_skip_reason(reason: str) -> str:
    """把回测跳过原因转成页面文案。"""

    return BACKTEST_SKIP_REASON_LABELS.get(reason, reason)


def build_backtest_bets_dataframe(result) -> pd.DataFrame:
    """把回测下注明细转成表格。"""

    rows = [
        {
            "期次": bet.expect,
            "场次": bet.match_no,
            "联赛": bet.competition,
            "比赛时间": bet.match_time,
            "主队": bet.home_team,
            "客队": bet.away_team,
            "下注项": bet.selection_label,
            "赛果": bet.result_label,
            "赔率": bet.odds,
            "投注额": bet.stake,
            "返还": bet.payout,
            "盈亏": bet.pnl,
            "命中": "是" if bet.won else "否",
            "模型概率": bet.model_probability,
            "庄家概率": bet.bookmaker_probability,
            "value": bet.edge,
            "历史样本": bet.sample_size,
            "原因": bet.reason,
        }
        for bet in result.bets
    ]
    return pd.DataFrame(
        rows,
        columns=[
            "期次",
            "场次",
            "联赛",
            "比赛时间",
            "主队",
            "客队",
            "下注项",
            "赛果",
            "赔率",
            "投注额",
            "返还",
            "盈亏",
            "命中",
            "模型概率",
            "庄家概率",
            "value",
            "历史样本",
            "原因",
        ],
    )


def build_backtest_tickets_dataframe(result) -> pd.DataFrame:
    """把串关票据明细转成表格。"""

    rows = [
        {
            "日期": ticket.trade_date,
            "票号": ticket.ticket_no,
            "串关类型": ticket.ticket_type,
            "串关场数": ticket.legs_count,
            "比赛": ticket.matches_summary,
            "选择": ticket.selections_summary,
            "合成赔率": ticket.combined_odds,
            "投注额": ticket.stake,
            "返还": ticket.payout,
            "盈亏": ticket.pnl,
            "命中": "是" if ticket.won else "否",
            "原因": ticket.reason,
        }
        for ticket in result.tickets
    ]
    return pd.DataFrame(
        rows,
        columns=[
            "日期",
            "票号",
            "串关类型",
            "串关场数",
            "比赛",
            "选择",
            "合成赔率",
            "投注额",
            "返还",
            "盈亏",
            "命中",
            "原因",
        ],
    )


def build_backtest_daily_dataframe(result) -> pd.DataFrame:
    """把每日回测结果转成表格。"""

    rows = [
        {
            "日期": row.trade_date,
            "纳入比赛": row.matches_considered,
            "下注场数": row.bets_placed,
            "跳过场数": row.skipped_matches,
            "总投注": row.total_stake,
            "总返还": row.total_return,
            "当日盈亏": row.pnl,
            "累计盈亏": row.cumulative_pnl,
            "回撤": row.drawdown,
        }
        for row in result.daily_results
    ]
    return pd.DataFrame(
        rows,
        columns=[
            "日期",
            "纳入比赛",
            "下注场数",
            "跳过场数",
            "总投注",
            "总返还",
            "当日盈亏",
            "累计盈亏",
            "回撤",
        ],
    )


def build_backtest_competition_dataframe(result) -> pd.DataFrame:
    """把按联赛汇总的回测结果转成表格。"""

    rows = [
        {
            "联赛": row.competition or "-",
            "下注场数": row.bets_placed,
            "总投注": row.total_stake,
            "总返还": row.total_return,
            "盈亏": row.pnl,
            "ROI": row.roi,
            "命中率": row.win_rate,
            "平均赔率": row.average_odds,
        }
        for row in result.competition_summaries
    ]
    return pd.DataFrame(
        rows,
        columns=["联赛", "下注场数", "总投注", "总返还", "盈亏", "ROI", "命中率", "平均赔率"],
    )


def build_backtest_skipped_dataframe(result) -> pd.DataFrame:
    """把跳过比赛明细转成表格。"""

    rows = [
        {
            "期次": row.expect,
            "场次": row.match_no,
            "联赛": row.competition,
            "比赛时间": row.match_time,
            "主队": row.home_team,
            "客队": row.away_team,
            "原因": _format_backtest_skip_reason(row.reason),
            "原因编码": row.reason,
        }
        for row in result.skipped_matches
    ]
    return pd.DataFrame(
        rows,
        columns=["期次", "场次", "联赛", "比赛时间", "主队", "客队", "原因", "原因编码"],
    )


def build_value_strategy_top_edge_dataframe(
    result,
    *,
    limit: int = 10,
    score_label: str = "value",
) -> pd.DataFrame:
    """构造 value 策略的高 edge 下注样本表。"""

    sorted_bets = sorted(
        result.bets,
        key=lambda bet: (
            -(float(bet.edge or 0.0)),
            -(float(bet.model_probability or 0.0)),
            bet.match_time,
        ),
    )
    rows = [
        {
            "比赛时间": bet.match_time,
            "联赛": bet.competition,
            "主队": bet.home_team,
            "客队": bet.away_team,
            "下注项": bet.selection_label,
            "模型概率": bet.model_probability,
            "庄家概率": bet.bookmaker_probability,
            score_label: bet.edge,
            "样本数": bet.sample_size,
            "赔率": bet.odds,
            "投注额": bet.stake,
            "盈亏": bet.pnl,
        }
        for bet in sorted_bets[:limit]
    ]
    return pd.DataFrame(
        rows,
        columns=[
            "比赛时间",
            "联赛",
            "主队",
            "客队",
            "下注项",
            "模型概率",
            "庄家概率",
            score_label,
            "样本数",
            "赔率",
            "投注额",
            "盈亏",
        ],
    )


def build_value_strategy_pnl_extremes_dataframe(
    result,
    *,
    limit: int = 3,
    direction: str,
    score_label: str = "value",
) -> pd.DataFrame:
    """构造 value 策略里盈亏极值下注样本表。"""

    reverse = direction == "profit"
    sorted_bets = sorted(
        result.bets,
        key=lambda bet: (
            float(bet.pnl),
            float(bet.stake),
            bet.match_time,
        ),
        reverse=reverse,
    )
    rows = [
        {
            "比赛时间": bet.match_time,
            "联赛": bet.competition,
            "主队": bet.home_team,
            "客队": bet.away_team,
            "下注项": bet.selection_label,
            "赔率": bet.odds,
            "投注额": bet.stake,
            score_label: bet.edge,
            "盈亏": bet.pnl,
        }
        for bet in sorted_bets[:limit]
    ]
    return pd.DataFrame(
        rows,
        columns=[
            "比赛时间",
            "联赛",
            "主队",
            "客队",
            "下注项",
            "赔率",
            "投注额",
            score_label,
            "盈亏",
        ],
    )


def render_value_strategy_explanation_card(result, detail_limit: int) -> None:
    """渲染历史水位匹配价值投注的解释卡。"""

    if not result.bets:
        return

    diagnostics = result.diagnostics
    value_mode = str(diagnostics.get("value_mode") or DEFAULT_VALUE_MODE)
    weighting_mode = str(diagnostics.get("weighting_mode") or "inverse_distance")
    lookback_days = diagnostics.get("lookback_days")
    staking_mode = str(diagnostics.get("staking_mode") or "fixed")
    value_mode_label = _format_value_mode_label(value_mode)
    weighting_mode_label = _format_weighting_mode_label(weighting_mode)
    staking_mode_label = _format_staking_mode_label(staking_mode)
    score_label = _resolve_value_mode_score_label(value_mode)
    average_edge = sum(float(bet.edge or 0.0) for bet in result.bets) / len(result.bets)
    average_sample_size = sum(float(bet.sample_size or 0.0) for bet in result.bets) / len(result.bets)
    max_edge = max(float(bet.edge or 0.0) for bet in result.bets)
    positive_edge_bets = sum(1 for bet in result.bets if float(bet.edge or 0.0) > 0.0)
    home_threshold = float(diagnostics.get("min_edge_home_win") or 0.0)
    draw_threshold = float(diagnostics.get("min_edge_draw") or 0.0)
    away_threshold = float(diagnostics.get("min_edge_away_win") or 0.0)

    card = st.container(border=True)
    with card:
        st.markdown("#### 策略解释卡")
        st.caption(
            "这套策略不是直接买最低赔率，而是先把当前胜平负赔率转成庄家概率，"
            "再去历史里找最相近的赔率结构样本。"
        )
        if value_mode == "expected_value":
            st.markdown(
                "`模型概率 = 历史相似样本的加权结果频率`  |  "
                "`庄家概率 = 当前胜平负赔率归一化隐含概率`  |  "
                "`EV = 模型概率 x 当前赔率 - 1`"
            )
        else:
            st.markdown(
                "`模型概率 = 历史相似样本的加权结果频率`  |  "
                "`庄家概率 = 当前胜平负赔率归一化隐含概率`  |  "
                "`value = 模型概率 - 庄家概率`"
            )

        metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
        metric_col1.metric(f"平均{score_label}", f"{average_edge:.2%}")
        metric_col2.metric(f"最大{score_label}", f"{max_edge:.2%}")
        metric_col3.metric("平均样本数", f"{average_sample_size:.1f}")
        metric_col4.metric("达标注单", positive_edge_bets)

        st.caption(
            f"当前使用：{_format_lookback_label(lookback_days)}、{weighting_mode_label}、"
            f"{value_mode_label}、{staking_mode_label}。"
        )
        if staking_mode == "fractional_kelly":
            st.caption(
                "资金管理：初始资金 "
                f"{float(diagnostics.get('initial_bankroll') or 0.0):.2f}，"
                f"期末资金 {float(diagnostics.get('ending_bankroll') or 0.0):.2f}，"
                f"Kelly 折扣 {float(diagnostics.get('kelly_fraction') or 0.0):.2f}，"
                f"单场最大仓位 {float(diagnostics.get('max_stake_pct') or 0.0):.1%}。"
            )
        st.caption(
            "主胜 / 平局 / 客胜阈值分别是 "
            f"{home_threshold:.3f} / {draw_threshold:.3f} / {away_threshold:.3f}；"
            f"只有 {score_label} 超过对应阈值才会下注。"
        )
        st.caption(
            "阈值含义：主胜 "
            f"{_format_threshold_meaning(value_mode, home_threshold)}；平局 "
            f"{_format_threshold_meaning(value_mode, draw_threshold)}；客胜 "
            f"{_format_threshold_meaning(value_mode, away_threshold)}。"
        )
        st.caption(
            f"如果当天符合条件的比赛太多，会按 {score_label} 从高到低保留。"
        )

        top_edge_df = build_value_strategy_top_edge_dataframe(
            result,
            limit=min(detail_limit, 10),
            score_label=score_label,
        )
        if not top_edge_df.empty:
            st.caption("高分下注样本")
            st.dataframe(top_edge_df, use_container_width=True, hide_index=True)

        profit_col, loss_col = st.columns(2)
        top_profit_df = build_value_strategy_pnl_extremes_dataframe(
            result,
            limit=3,
            direction="profit",
            score_label=score_label,
        )
        top_loss_df = build_value_strategy_pnl_extremes_dataframe(
            result,
            limit=3,
            direction="loss",
            score_label=score_label,
        )
        with profit_col:
            st.caption("赚得最多 3 条")
            if top_profit_df.empty:
                st.info("当前没有可展示的盈利注单。")
            else:
                st.dataframe(top_profit_df, use_container_width=True, hide_index=True)
        with loss_col:
            st.caption("亏得最多 3 条")
            if top_loss_df.empty:
                st.info("当前没有可展示的亏损注单。")
            else:
                st.dataframe(top_loss_df, use_container_width=True, hide_index=True)


def render_backtest_pnl_chart(result) -> None:
    """渲染带 0 轴虚线的累计盈亏曲线。"""

    daily_df = build_backtest_daily_dataframe(result)
    if daily_df.empty:
        st.info("当前没有可绘制的回测曲线。")
        return

    chart_df = pd.DataFrame(
        {
            "date": daily_df["日期"],
            "cumulative_pnl": daily_df["累计盈亏"],
            "daily_pnl": daily_df["当日盈亏"],
        }
    )
    max_abs_value = max(
        1.0,
        float(chart_df["cumulative_pnl"].abs().max() or 0.0),
    )
    domain_limit = round(max_abs_value * 1.15, 4)

    chart_spec = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "data": {"values": chart_df.to_dict(orient="records")},
        "height": 320,
        "layer": [
            {
                "mark": {
                    "type": "rule",
                    "color": "#94a3b8",
                    "strokeDash": [6, 6],
                    "strokeWidth": 1.5,
                },
                "encoding": {
                    "y": {
                        "datum": 0,
                        "type": "quantitative",
                        "scale": {"domain": [-domain_limit, domain_limit]},
                        "axis": {"title": "累计盈亏"},
                    }
                },
            },
            {
                "mark": {
                    "type": "area",
                    "color": "#14b8a6",
                    "opacity": 0.14,
                    "line": {"color": "#0f766e", "strokeWidth": 3},
                },
                "encoding": {
                    "x": {
                        "field": "date",
                        "type": "temporal",
                        "axis": {"title": None, "labelAngle": 0},
                    },
                    "y": {
                        "field": "cumulative_pnl",
                        "type": "quantitative",
                        "scale": {"domain": [-domain_limit, domain_limit]},
                        "axis": {"title": "累计盈亏"},
                    },
                    "y2": {"datum": 0},
                    "tooltip": [
                        {"field": "date", "type": "temporal", "title": "日期"},
                        {"field": "cumulative_pnl", "type": "quantitative", "title": "累计盈亏"},
                        {"field": "daily_pnl", "type": "quantitative", "title": "当日盈亏"},
                    ],
                },
            },
            {
                "transform": [
                    {
                        "calculate": "datum.cumulative_pnl >= 0 ? '盈利' : '亏损'",
                        "as": "status",
                    }
                ],
                "mark": {
                    "type": "point",
                    "filled": True,
                    "size": 40,
                    "strokeWidth": 1,
                },
                "encoding": {
                    "x": {"field": "date", "type": "temporal"},
                    "y": {
                        "field": "cumulative_pnl",
                        "type": "quantitative",
                        "scale": {"domain": [-domain_limit, domain_limit]},
                    },
                    "color": {
                        "field": "status",
                        "type": "nominal",
                        "legend": None,
                        "scale": {
                            "domain": ["盈利", "亏损"],
                            "range": ["#0f766e", "#dc2626"],
                        },
                    },
                },
            },
        ],
        "config": {
            "view": {"stroke": None},
            "axis": {"gridColor": "#e2e8f0", "labelColor": "#475569", "titleColor": "#334155"},
            "background": "#ffffff",
        },
    }

    st.vega_lite_chart(spec=chart_spec, use_container_width=True)


def render_recent_sync_summary(summary: dict) -> None:
    """渲染最近一次 500.com 历史同步摘要。"""

    st.caption(f"最近一次同步写入：{summary.get('db_path')}")
    metric_columns = st.columns(4)
    metric_columns[0].metric("days", summary.get("days"))
    metric_columns[1].metric("status", summary.get("status") or "-")
    metric_columns[2].metric("rows_fetched", summary.get("rows_fetched") or 0)
    metric_columns[3].metric("rows_inserted", summary.get("rows_inserted") or 0)

    extra_columns = st.columns(2)
    extra_columns[0].metric("valid_expects", summary.get("valid_expects") or 0)
    extra_columns[1].metric("scanned_expects", summary.get("scanned_expects") or 0)

    if summary.get("errors"):
        st.error("部分期次同步失败：\n\n" + "\n".join(summary["errors"]))

    sample_df = build_sfc500_history_dataframe(summary.get("sample_matches", []))
    if not sample_df.empty:
        st.caption("样本预览")
        st.dataframe(sample_df, use_container_width=True, hide_index=True)


def _estimate_progress_ratio(event: dict) -> float:
    """根据同步事件估算进度条比例。"""

    if event.get("stage") == "finish":
        return 1.0

    current_index = event.get("current_index", 0)
    total_windows = event.get("total_windows") or 0
    if total_windows <= 0:
        return 0.02

    return min(max(current_index / total_windows, 0.0), 1.0)


def _resolve_settled_filter(label: str) -> bool | None:
    """把页面文案映射成查询参数。"""

    if label == "仅已开奖":
        return True
    if label == "仅未开奖":
        return False
    return None


def _render_maintenance_feedback() -> None:
    """渲染上一次维护操作反馈。"""

    feedback = st.session_state.pop("team_name_maintenance_feedback", None)
    if not feedback:
        return

    feedback_type = feedback.get("type")
    message = str(feedback.get("message") or "")
    if feedback_type == "success":
        st.success(message)
    elif feedback_type == "warning":
        st.warning(message)
    else:
        st.info(message)


def render_history_page() -> None:
    """渲染历史数据页。"""

    if APP_READ_ONLY:
        st.subheader("500.com 历史赔率与赛果")
        st.caption(f"只读演示库：{SFC500_DATABASE_PATH}")
        st.info("当前是只读演示环境：已隐藏同步和数据库维护入口。")
    else:
        st.subheader("500.com 历史赔率同步")
        st.caption(f"主库：{SFC500_DATABASE_PATH}")

        sync_control_col1, sync_control_col2 = st.columns([1, 2])
        selected_sync_label = sync_control_col1.selectbox(
            "同步范围",
            options=list(RECENT_SYNC_OPTIONS.keys()),
            index=0,
        )
        sync_button_clicked = sync_control_col2.button(
            "同步 500.com 历史赔率",
            key="sync_sfc500_recent_history",
            type="primary",
        )

        sync_status_placeholder = st.empty()
        sync_progress_placeholder = st.empty()

        if sync_button_clicked:
            progress_bar = sync_progress_placeholder.progress(0.0)

            def on_sync_progress(event: dict) -> None:
                ratio = _estimate_progress_ratio(event)
                progress_bar.progress(ratio)

                message = event.get("message", "正在同步...")
                stage = event.get("stage")

                if stage == "expect_error":
                    sync_status_placeholder.error(message)
                elif stage == "finish":
                    if event.get("status") == "success":
                        sync_status_placeholder.success(message)
                    else:
                        sync_status_placeholder.warning(message)
                else:
                    sync_status_placeholder.info(message)

            days = RECENT_SYNC_OPTIONS[selected_sync_label]
            with st.spinner(f"正在同步最近 {days} 天相关期次，请稍候..."):
                try:
                    summary = sync_recent_history(
                        days=days,
                        progress_callback=on_sync_progress,
                    )
                    st.session_state["sfc500_recent_sync_summary"] = summary
                    sync_progress_placeholder.progress(1.0)
                except Exception as exc:
                    st.session_state["sfc500_recent_sync_summary"] = None
                    sync_status_placeholder.error(f"同步失败：{exc}")
                    sync_progress_placeholder.empty()
                    st.error(f"同步失败：{exc}")

        if st.session_state["sfc500_recent_sync_summary"]:
            render_recent_sync_summary(st.session_state["sfc500_recent_sync_summary"])
        else:
            st.info("尚未执行页面内的 500.com 历史赔率同步。")

        st.divider()

        st.subheader("500.com 历史赔率与赛果")
    overview = get_sfc500_history_overview()
    st.caption("表格里的主队和客队展示标准名；原始名仍保留在数据库字段 home_team / away_team。")

    overview_col1, overview_col2, overview_col3, overview_col4 = st.columns(4)
    overview_col1.metric("records", overview.get("row_count") or 0)
    overview_col2.metric("expects", overview.get("expect_count") or 0)
    overview_col3.metric("settled", overview.get("settled_count") or 0)
    overview_col4.metric("competitions", overview.get("competition_count") or 0)

    min_match_time = overview.get("min_match_time")
    max_match_time = overview.get("max_match_time")
    if min_match_time and max_match_time:
        st.caption(f"覆盖时间：{min_match_time} -> {max_match_time}")
    else:
        st.caption("当前历史库还没有可展示的数据。")

    filter_options = get_sfc500_filter_options()
    default_start_date, default_end_date = _resolve_default_date_range(overview)

    filter_col1, filter_col2, filter_col3 = st.columns(3)
    start_date = filter_col1.date_input(
        "开始日期",
        value=default_start_date,
        key="history_start_date",
    )
    end_date = filter_col2.date_input(
        "结束日期",
        value=default_end_date,
        key="history_end_date",
    )
    result_limit = filter_col3.selectbox(
        "展示条数",
        options=[100, 200, 500, 1000],
        index=1,
    )

    filter_col4, filter_col5 = st.columns(2)
    selected_competitions = filter_col4.multiselect(
        "联赛",
        options=filter_options["competitions"],
        default=[],
    )
    selected_teams = filter_col5.multiselect(
        "球队",
        options=filter_options["teams"],
        default=[],
    )

    filter_col6, filter_col7, filter_col8 = st.columns(3)
    team_keyword = filter_col6.text_input("球队关键词", value="")
    expect_value = filter_col7.text_input("期次", value="")
    settled_filter_label = filter_col8.selectbox(
        "赛果状态",
        options=["全部", "仅已开奖", "仅未开奖"],
        index=0,
    )

    if start_date > end_date:
        st.error("开始日期不能晚于结束日期。")
    else:
        query_result = query_sfc500_matches(
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            competitions=selected_competitions,
            teams=selected_teams,
            team_keyword=team_keyword,
            expect=expect_value,
            settled_only=_resolve_settled_filter(settled_filter_label),
            limit=result_limit,
        )

        history_df = build_sfc500_history_dataframe(query_result["rows"])
        st.caption(
            f"匹配到 {query_result['total_count']} 条记录，当前展示前 {min(len(history_df), result_limit)} 条。"
        )

        if history_df.empty:
            st.info("当前筛选条件下没有记录。")
        else:
            st.dataframe(
                history_df,
                use_container_width=True,
                hide_index=True,
            )

    st.info(f"500.com 历史主库：{SFC500_DATABASE_PATH}")


def render_backtest_page() -> None:
    """渲染回测页。"""

    st.subheader("回测")
    st.caption("当前支持最低赔率单关、历史水位匹配价值投注和最低赔率串关；三种策略共用同一套历史数据和回测引擎。")

    overview = get_sfc500_history_overview()
    filter_options = get_sfc500_filter_options()
    default_start_date, default_end_date = _resolve_default_date_range(overview)
    min_match_time = overview.get("min_match_time")
    max_match_time = overview.get("max_match_time")

    overview_col1, overview_col2, overview_col3, overview_col4 = st.columns(4)
    overview_col1.metric("记录数", overview.get("row_count") or 0)
    overview_col2.metric("已开奖", overview.get("settled_count") or 0)
    overview_col3.metric("联赛数", overview.get("competition_count") or 0)
    overview_col4.metric("期次数", overview.get("expect_count") or 0)

    if min_match_time and max_match_time:
        st.caption(f"可回测覆盖时间：{min_match_time} -> {max_match_time}")
    else:
        st.warning("当前历史库还没有可用于回测的数据。")

    min_date, max_date = _resolve_date_bounds(overview)
    default_start_date, default_end_date = _resolve_default_date_range(overview)

    if "backtest_date_preset" not in st.session_state:
        st.session_state["backtest_date_preset"] = "最近 30 天"
    if "backtest_applied_preset" not in st.session_state:
        st.session_state["backtest_applied_preset"] = st.session_state["backtest_date_preset"]
    if "backtest_start_date" not in st.session_state:
        st.session_state["backtest_start_date"] = default_start_date
    if "backtest_end_date" not in st.session_state:
        st.session_state["backtest_end_date"] = default_end_date

    selected_preset = st.session_state["backtest_date_preset"]
    if st.session_state.get("backtest_applied_preset") != selected_preset:
        preset_start_date, preset_end_date = _resolve_preset_date_range(
            selected_preset,
            min_date=min_date,
            max_date=max_date,
        )
        st.session_state["backtest_start_date"] = preset_start_date
        st.session_state["backtest_end_date"] = preset_end_date
        st.session_state["backtest_applied_preset"] = selected_preset

    general_container = st.container(border=True)
    with general_container:
        st.markdown("#### 通用参数")
        control_col1, control_col2, control_col3 = st.columns(3)
        selected_strategy_label = control_col1.selectbox(
            "策略",
            options=list(BACKTEST_STRATEGY_OPTIONS.keys()),
            index=0,
            help="切换策略后，下方参数卡会立刻切换。",
        )
        selected_strategy_meta = BACKTEST_STRATEGY_OPTIONS[selected_strategy_label]
        selected_strategy_name = str(selected_strategy_meta["strategy_name"])
        selected_strategy_mode = str(selected_strategy_meta["mode"])
        control_col2.selectbox(
            "时间预设",
            options=BACKTEST_DATE_PRESET_OPTIONS,
            key="backtest_date_preset",
            help="选择常用回测区间；如需手工调整，直接改下面日期。",
        )
        control_col3.caption("固定投注金额会在下方对应策略参数里显示。")

        control_col4, control_col5, control_col6 = st.columns(3)
        start_date = control_col4.date_input(
            "开始日期",
            key="backtest_start_date",
            min_value=min_date,
            max_value=max_date,
        )
        end_date = control_col5.date_input(
            "结束日期",
            key="backtest_end_date",
            min_value=min_date,
            max_value=max_date,
        )
        detail_limit = int(
            control_col6.selectbox(
                "明细展示条数",
                options=[100, 300, 500, 1000],
                index=1,
            )
        )
        control_col6.caption(
            f"当前日期边界：{min_date.isoformat()} -> {max_date.isoformat()}"
        )

    selected_competitions: list[str] = []
    max_bets_per_day: int | None = None
    parlay_size: int | None = None
    history_match_count = 100
    min_history_matches = 20
    min_edge = 0.02
    lookback_days: int | None = DEFAULT_LOOKBACK_DAYS
    weighting_mode = "inverse_distance"
    value_mode = DEFAULT_VALUE_MODE
    min_edge_home_win = 0.03
    min_edge_draw = 0.05
    min_edge_away_win = 0.03
    staking_mode = "fixed"
    initial_bankroll = 1000.0
    kelly_fraction = 0.25
    max_stake_pct = 0.02
    same_competition_only = False
    fixed_stake = 10.0

    strategy_container = st.container(border=True)
    with strategy_container:
        st.markdown("#### 策略参数")
        if selected_strategy_mode == "single":
            st.caption("单关策略会按每场最低赔率下注；你可以限制每天只买最低赔率的前 k 场。")
            control_col7, control_col8, control_col9 = st.columns(3)
            selected_competitions = control_col7.multiselect(
                "赛事选择",
                options=filter_options["competitions"],
                default=[],
                help="留空表示全部联赛。",
            )
            max_bets_per_day_option = control_col8.selectbox(
                "每天只买 k 场",
                options=BACKTEST_DAILY_LIMIT_OPTIONS,
                index=0,
                format_func=_format_daily_limit_option,
                help="默认不限制；选择 0 表示当天不下注。",
            )
            max_bets_per_day = _resolve_daily_limit_value(max_bets_per_day_option)
            fixed_stake = float(
                control_col9.number_input(
                    "固定投注金额",
                    min_value=1.0,
                    step=1.0,
                    value=10.0,
                    help="每场单关下注固定金额。",
                )
            )
        elif selected_strategy_mode == "value_match":
            st.caption(
                "历史水位匹配策略会用当前胜平负庄家概率去历史里找最相近样本，"
                "拿样本赛果频率当模型概率；只有分数超过对应阈值才下注。"
            )
            control_col7, control_col8, control_col9 = st.columns(3)
            selected_competitions = control_col7.multiselect(
                "赛事选择",
                options=filter_options["competitions"],
                default=[],
                help="留空表示全部联赛。",
            )
            max_bets_per_day_option = control_col8.selectbox(
                "每天最多下注",
                options=BACKTEST_DAILY_LIMIT_OPTIONS,
                index=0,
                format_func=_format_daily_limit_option,
                help="如果有多场达到阈值，则按当前分数从高到低保留前 k 场。",
            )
            max_bets_per_day = _resolve_daily_limit_value(max_bets_per_day_option)
            history_match_count = int(
                control_col9.selectbox(
                    "匹配样本数",
                    options=BACKTEST_HISTORY_MATCH_COUNT_OPTIONS,
                    index=1,
                    help="从历史里取最相近的前 N 场来估算经验概率。",
                )
            )

            control_col10, control_col11, control_col12 = st.columns(3)
            lookback_days = _resolve_lookback_value(
                control_col10.selectbox(
                    "历史回看窗口",
                    options=BACKTEST_LOOKBACK_OPTIONS,
                    index=4,
                    format_func=_format_lookback_option,
                    help="限制历史匹配样本只来自最近一段时间。",
                )
            )
            weighting_mode = BACKTEST_WEIGHTING_MODE_OPTIONS[
                control_col11.selectbox(
                    "样本加权方式",
                    options=list(BACKTEST_WEIGHTING_MODE_OPTIONS.keys()),
                    index=1,
                    help="越相似的样本是否应该占更高权重。",
                )
            ]
            value_mode = BACKTEST_VALUE_MODE_OPTIONS[
                control_col12.selectbox(
                    "value 计算方式",
                    options=list(BACKTEST_VALUE_MODE_OPTIONS.keys()),
                    index=1,
                    help="概率差看的是模型概率和庄家概率的差；EV 看的是期望收益率。",
                )
            ]
            score_label = _resolve_value_mode_score_label(value_mode)
            threshold_defaults = _resolve_value_mode_threshold_defaults(value_mode)

            control_col13, control_col14, control_col15 = st.columns(3)
            staking_mode = BACKTEST_STAKING_MODE_OPTIONS[
                control_col13.selectbox(
                    "投注模式",
                    options=list(BACKTEST_STAKING_MODE_OPTIONS.keys()),
                    index=0,
                    help="固定投注沿用通用参数里的金额；Kelly 资金管理会按当前资金和模型优势动态决定下注额。",
                )
            ]
            min_history_matches = int(
                control_col14.number_input(
                    "最小样本数",
                    min_value=5,
                    step=5,
                    value=20,
                    help="低于这个样本数就直接跳过，不下注。",
                )
            )
            same_competition_only = bool(
                control_col15.checkbox(
                    "仅同联赛历史样本",
                    value=False,
                    help="打开后，只用相同联赛的历史比赛做匹配。",
                )
            )
            if staking_mode == "fractional_kelly":
                st.caption(
                    "Kelly 模式下会用当前资金、模型概率和赔率动态计算下注额；"
                    "不会再显示固定投注金额。"
                )
                control_col16, control_col17, control_col18 = st.columns(3)
                initial_bankroll = float(
                    control_col16.number_input(
                        "初始资金",
                        min_value=100.0,
                        step=100.0,
                        value=1000.0,
                        format="%.2f",
                        help="回测起始资金，Kelly 下注会随资金曲线动态变化。",
                    )
                )
                kelly_fraction = float(
                    control_col17.number_input(
                        "Kelly 折扣",
                        min_value=0.05,
                        max_value=1.0,
                        step=0.05,
                        value=0.25,
                        format="%.2f",
                        help="0.25 表示四分之一 Kelly，默认更保守。",
                    )
                )
                max_stake_pct = float(
                    control_col18.number_input(
                        "单场最大仓位",
                        min_value=0.005,
                        max_value=0.2,
                        step=0.005,
                        value=0.02,
                        format="%.3f",
                        help="限制单场投注额不超过当前资金的一定比例。",
                    )
                )
                st.caption(
                    "当前 Kelly 公式：`stake = 当前资金 x min(原始Kelly x 折扣, 单场最大仓位)`。"
                )
            else:
                fixed_stake = float(
                    st.number_input(
                        "固定投注金额",
                        min_value=1.0,
                        step=1.0,
                        value=10.0,
                        help="当前 value 策略每场会按固定金额下注。",
                    )
                )

            st.caption(
                f"下面三个阈值按下注结果分别生效，当前分数类型是 {score_label}。"
            )

            control_col19, control_col20, control_col21 = st.columns(3)
            min_edge_home_win = float(
                control_col19.number_input(
                    f"主胜最小{score_label}",
                    min_value=0.0,
                    step=0.005,
                    value=threshold_defaults["home_win"],
                    format="%.3f",
                    help=_format_threshold_meaning(value_mode, threshold_defaults["home_win"]),
                )
            )
            min_edge_draw = float(
                control_col20.number_input(
                    f"平局最小{score_label}",
                    min_value=0.0,
                    step=0.005,
                    value=threshold_defaults["draw"],
                    format="%.3f",
                    help=(
                        _format_threshold_meaning(value_mode, threshold_defaults["draw"])
                        + "；平局噪音通常更高，默认阈值也更高。"
                    ),
                )
            )
            min_edge_away_win = float(
                control_col21.number_input(
                    f"客胜最小{score_label}",
                    min_value=0.0,
                    step=0.005,
                    value=threshold_defaults["away_win"],
                    format="%.3f",
                    help=_format_threshold_meaning(value_mode, threshold_defaults["away_win"]),
                )
            )
            min_edge = min(min_edge_home_win, min_edge_draw, min_edge_away_win)
            st.caption(
                f"默认值现在是：回看窗口 {DEFAULT_LOOKBACK_DAYS} 天、距离反比加权；"
                f"{_format_value_mode_label(value_mode)} 模式下默认阈值为 "
                f"主胜 {threshold_defaults['home_win']:.3f} / "
                f"平局 {threshold_defaults['draw']:.3f} / "
                f"客胜 {threshold_defaults['away_win']:.3f}。"
            )
        else:
            st.caption("串关策略会按当天最低赔率依次选场，组成一张 n串1；当前不提供赛事过滤。")
            control_col7, control_col8, control_col9 = st.columns(3)
            parlay_size = int(
                control_col7.selectbox(
                    "串关类型",
                    options=BACKTEST_PARLAY_OPTIONS,
                    index=0,
                    format_func=lambda value: f"{value}串1",
                )
            )
            fixed_stake = float(
                control_col8.number_input(
                    "固定投注金额",
                    min_value=1.0,
                    step=1.0,
                    value=10.0,
                    help="每张串关票的固定投注金额。",
                )
            )
            control_col9.caption(
                f"每天会取最低赔率的前 {parlay_size} 场组成 1 张 {parlay_size}串1。"
            )

    run_backtest_submitted = st.button("运行回测", type="primary")

    if run_backtest_submitted:
        if start_date > end_date:
            st.session_state["backtest_last_error"] = "开始日期不能晚于结束日期。"
        else:
            try:
                progress_container = st.container(border=True)
                progress_container.markdown("#### 回测进度")
                progress_status_placeholder = progress_container.empty()
                progress_metric_columns = progress_container.columns(4)
                progress_metric_placeholders = [
                    column.empty() for column in progress_metric_columns
                ]
                progress_bar = progress_container.progress(0.0)

                def on_backtest_progress(event: dict) -> None:
                    progress = float(event.get("progress") or 0.0)
                    days_completed = int(event.get("days_completed") or 0)
                    total_days = int(event.get("total_days") or 0)
                    processed_matches = int(event.get("processed_matches") or 0)
                    total_matches = int(event.get("total_matches") or 0)
                    bets_placed = int(event.get("bets_placed") or 0)
                    skipped_matches_count = int(event.get("skipped_matches") or 0)
                    elapsed_seconds = float(event.get("elapsed_seconds") or 0.0)
                    eta_seconds = event.get("eta_seconds")
                    current_date_text = str(event.get("current_date") or "-")
                    stage = str(event.get("stage") or "running")

                    progress_bar.progress(min(max(progress, 0.0), 1.0))
                    if stage == "finish":
                        progress_status_placeholder.success(
                            f"回测完成：{days_completed}/{total_days} 天，累计用时 {_format_seconds_brief(elapsed_seconds)}。"
                        )
                    else:
                        progress_status_placeholder.info(
                            f"正在处理 {current_date_text}，已完成 {days_completed}/{total_days} 天，"
                            f"预计剩余 {_format_seconds_brief(None if eta_seconds is None else float(eta_seconds))}。"
                        )

                    progress_metric_placeholders[0].metric(
                        "天数",
                        f"{days_completed}/{total_days}",
                    )
                    progress_metric_placeholders[1].metric(
                        "比赛",
                        f"{processed_matches}/{total_matches}",
                    )
                    progress_metric_placeholders[2].metric("下注", bets_placed)
                    progress_metric_placeholders[3].metric(
                        "跳过",
                        skipped_matches_count,
                    )

                with st.spinner("正在执行回测，请稍候..."):
                    config = BacktestConfig(
                        start_date=start_date,
                        end_date=end_date,
                        fixed_stake=float(fixed_stake),
                        competitions=list(selected_competitions),
                        max_bets_per_day=max_bets_per_day,
                        parlay_size=parlay_size,
                        history_match_count=history_match_count,
                        min_history_matches=min_history_matches,
                        min_edge=min_edge,
                        lookback_days=lookback_days,
                        weighting_mode=weighting_mode,
                        value_mode=value_mode,
                        min_edge_home_win=min_edge_home_win,
                        min_edge_draw=min_edge_draw,
                        min_edge_away_win=min_edge_away_win,
                        staking_mode=staking_mode,
                        initial_bankroll=initial_bankroll,
                        kelly_fraction=kelly_fraction,
                        max_stake_pct=max_stake_pct,
                        same_competition_only=same_competition_only,
                        db_path=SFC500_DATABASE_PATH,
                    )
                    strategy = build_strategy(
                        selected_strategy_name,
                        fixed_stake=float(fixed_stake),
                        max_bets_per_day=max_bets_per_day,
                        parlay_size=parlay_size,
                        history_match_count=history_match_count,
                        min_history_matches=min_history_matches,
                        min_edge=min_edge,
                        lookback_days=lookback_days,
                        weighting_mode=weighting_mode,
                        value_mode=value_mode,
                        min_edge_home_win=min_edge_home_win,
                        min_edge_draw=min_edge_draw,
                        min_edge_away_win=min_edge_away_win,
                        staking_mode=staking_mode,
                        initial_bankroll=initial_bankroll,
                        kelly_fraction=kelly_fraction,
                        max_stake_pct=max_stake_pct,
                        same_competition_only=same_competition_only,
                    )
                    engine = BacktestEngine(
                        SQLiteBacktestDataSource(db_path=SFC500_DATABASE_PATH)
                    )
                    result = engine.run(
                        config=config,
                        strategy=strategy,
                        progress_callback=on_backtest_progress,
                    )
                st.session_state["backtest_last_result"] = result
                st.session_state["backtest_last_error"] = None
            except Exception as exc:
                st.session_state["backtest_last_error"] = f"回测失败：{exc}"

    backtest_error = st.session_state.get("backtest_last_error")
    if backtest_error:
        st.error(backtest_error)

    result = st.session_state.get("backtest_last_result")
    if result is None:
        st.info("尚未执行页面内的回测。")
        return

    st.divider()
    is_parlay_result = _is_parlay_strategy(result.strategy_name)
    is_value_result = _is_value_strategy(result.strategy_name)
    if is_parlay_result:
        parlay_size_label = result.diagnostics.get("parlay_size") or "-"
        st.caption(
            f"最近一次回测：{result.start_date} -> {result.end_date} | "
            f"策略：最低赔率串关 | 串关类型：{parlay_size_label}串1"
        )
    elif is_value_result:
        competitions = result.diagnostics.get("competitions") or []
        daily_limit_value = result.diagnostics.get("max_bets_per_day")
        daily_limit_label = (
            "不限制" if daily_limit_value is None else f"{daily_limit_value} 场"
        )
        lookback_label = _format_lookback_label(result.diagnostics.get("lookback_days"))
        weighting_label = _format_weighting_mode_label(
            str(result.diagnostics.get("weighting_mode") or "inverse_distance")
        )
        value_mode = str(result.diagnostics.get("value_mode") or DEFAULT_VALUE_MODE)
        value_mode_label = _format_value_mode_label(value_mode)
        score_label = _resolve_value_mode_score_label(value_mode)
        staking_mode_label = _format_staking_mode_label(
            str(result.diagnostics.get("staking_mode") or "fixed")
        )
        st.caption(
            f"最近一次回测：{result.start_date} -> {result.end_date} | "
            f"策略：历史水位匹配价值投注 | "
            f"联赛：{'全部' if not competitions else ', '.join(competitions)} | "
            f"每天最多下注：{daily_limit_label} | "
            f"匹配样本数：{result.diagnostics.get('history_match_count')} | "
            f"最小样本数：{result.diagnostics.get('min_history_matches')} | "
            f"回看窗口：{lookback_label} | "
            f"加权：{weighting_label} | "
            f"value：{value_mode_label} | "
            f"投注模式：{staking_mode_label}"
        )
        st.caption(
            f"{score_label} 阈值：主胜 "
            f"{float(result.diagnostics.get('min_edge_home_win') or 0.0):.3f} / 平局 "
            f"{float(result.diagnostics.get('min_edge_draw') or 0.0):.3f} / 客胜 "
            f"{float(result.diagnostics.get('min_edge_away_win') or 0.0):.3f}"
        )
        if str(result.diagnostics.get("staking_mode") or "fixed") == "fractional_kelly":
            st.caption(
                f"资金：{float(result.diagnostics.get('initial_bankroll') or 0.0):.2f} -> "
                f"{float(result.diagnostics.get('ending_bankroll') or 0.0):.2f} | "
                f"Kelly 折扣：{float(result.diagnostics.get('kelly_fraction') or 0.0):.2f} | "
                f"单场最大仓位：{float(result.diagnostics.get('max_stake_pct') or 0.0):.1%}"
            )
    else:
        competitions = result.diagnostics.get("competitions") or []
        daily_limit_value = result.diagnostics.get("max_bets_per_day")
        daily_limit_label = (
            "不限制" if daily_limit_value is None else f"{daily_limit_value} 场"
        )
        st.caption(
            f"最近一次回测：{result.start_date} -> {result.end_date} | "
            f"联赛：{'全部' if not competitions else ', '.join(competitions)} | "
            f"每天最多下注：{daily_limit_label}"
        )

    summary_col1, summary_col2, summary_col3, summary_col4 = st.columns(4)
    summary_col1.metric("纳入比赛", result.total_matches_considered)
    summary_col2.metric("票数" if is_parlay_result else "下注笔数", result.total_bets_placed)
    summary_col3.metric("总投注", f"{result.total_stake:.2f}")
    summary_col4.metric("总返还", f"{result.total_return:.2f}")

    summary_col5, summary_col6, summary_col7, summary_col8 = st.columns(4)
    summary_col5.metric("盈亏", f"{result.pnl:.2f}")
    summary_col6.metric("收益率", f"{result.roi:.2%}")
    summary_col7.metric("命中率", f"{result.win_rate:.2%}")
    summary_col8.metric("夏普比率", f"{result.sharpe_ratio:.2f}")

    summary_col9, summary_col10, summary_col11, summary_col12 = st.columns(4)
    summary_col9.metric("平均赔率", f"{result.average_odds:.3f}")
    summary_col10.metric("日均盈亏", f"{result.average_daily_pnl:.2f}")
    summary_col11.metric("最大回撤", f"{result.max_drawdown:.2f}")
    summary_col12.metric(
        "最长连黑",
        result.longest_losing_streak,
    )
    st.caption("夏普比率按有下注日的日收益率年化计算，假设无风险利率为 0。")

    if is_value_result:
        render_value_strategy_explanation_card(result, detail_limit=detail_limit)

    st.markdown("#### 每日资金曲线")
    render_backtest_pnl_chart(result)

    competition_df = build_backtest_competition_dataframe(result)
    if not is_parlay_result and not competition_df.empty:
        st.markdown("#### 联赛汇总")
        st.dataframe(competition_df, use_container_width=True, hide_index=True)

    if is_parlay_result:
        ticket_df = build_backtest_tickets_dataframe(result)
        st.markdown("#### 串关票明细")
        st.caption(f"当前展示前 {min(len(ticket_df), detail_limit)} 条。")
        if ticket_df.empty:
            st.info("当前参数下没有实际串关票。")
        else:
            st.dataframe(
                ticket_df.head(detail_limit),
                use_container_width=True,
                hide_index=True,
            )
    else:
        bets_df = build_backtest_bets_dataframe(result)
        st.markdown("#### 下注明细")
        st.caption(f"当前展示前 {min(len(bets_df), detail_limit)} 条。")
        if bets_df.empty:
            st.info("当前参数下没有实际下注记录。")
        else:
            st.dataframe(
                bets_df.head(detail_limit),
                use_container_width=True,
                hide_index=True,
            )

    skipped_df = build_backtest_skipped_dataframe(result)
    with st.expander("跳过比赛", expanded=False):
        if skipped_df.empty:
            st.info("当前参数下没有被跳过的比赛。")
        else:
            st.caption(f"当前展示前 {min(len(skipped_df), detail_limit)} 条。")
            st.dataframe(
                skipped_df.head(detail_limit),
                use_container_width=True,
                hide_index=True,
            )

    daily_df = build_backtest_daily_dataframe(result)
    with st.expander("每日结果明细", expanded=False):
        if daily_df.empty:
            st.info("当前没有每日结果。")
        else:
            st.dataframe(daily_df, use_container_width=True, hide_index=True)

    with st.expander("诊断信息", expanded=False):
        st.json(result.to_summary_dict(), expanded=False)
        st.json(
            {
                key: value
                for key, value in result.diagnostics.items()
                if key != "skip_reason_breakdown"
            },
            expanded=False,
        )
        st.json(
            {
                _format_backtest_skip_reason(key): value
                for key, value in (result.diagnostics.get("skip_reason_breakdown") or {}).items()
            },
            expanded=False,
        )


def render_database_maintenance_page() -> None:
    """渲染数据库维护页。"""

    st.subheader("数据库维护")
    maintenance_item = st.selectbox("维护项", options=["标准名统一"], index=0)
    if maintenance_item != "标准名统一":
        return

    _render_maintenance_feedback()
    st.caption(
        "候选统一后会写入 team_name_aliases，并立刻回填历史表的 canonical 字段；"
        "选择暂不统一会写入 review 决策表，之后不会反复出现。"
    )

    control_col1, control_col2, control_col3 = st.columns(3)
    candidate_limit = control_col1.selectbox("候选组数量", options=[10, 20, 50, 100], index=1)
    alias_limit = control_col2.selectbox("已确认映射预览", options=[20, 50, 100, 200], index=1)
    skipped_limit = control_col3.selectbox("已跳过候选预览", options=[10, 20, 50], index=1)

    with get_sfc500_connection(SFC500_DATABASE_PATH) as connection:
        candidate_rows = find_team_alias_candidates(
            connection,
            TEAM_NAME_TABLE_SPEC,
            limit=candidate_limit,
        )
        confirmed_alias_rows = list_team_name_aliases(
            connection,
            limit=alias_limit,
            sources=["manual", "seed"],
        )
        skipped_rows = list_team_name_review_decisions(
            connection,
            decision_type="skip",
            limit=skipped_limit,
        )
        manual_alias_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM team_name_aliases WHERE source = 'manual'"
            ).fetchone()[0]
        )
        skipped_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM team_name_review_decisions WHERE decision_type = 'skip'"
            ).fetchone()[0]
        )

    metric_col1, metric_col2, metric_col3 = st.columns(3)
    metric_col1.metric("待处理候选", len(candidate_rows))
    metric_col2.metric("手工映射", manual_alias_count)
    metric_col3.metric("已跳过候选", skipped_count)

    with st.expander("手工新增或修正一条映射", expanded=False):
        with st.form("manual_team_alias_form"):
            manual_alias_name = st.text_input("别名", value="")
            manual_canonical_name = st.text_input("标准名", value="")
            manual_submit = st.form_submit_button("保存映射", type="primary")

        if manual_submit:
            try:
                with get_sfc500_connection(SFC500_DATABASE_PATH) as connection:
                    with connection:
                        summary = apply_manual_team_name_alias(
                            connection,
                            TEAM_NAME_TABLE_SPEC,
                            alias_name=manual_alias_name,
                            canonical_name=manual_canonical_name,
                        )
                st.session_state["team_name_maintenance_feedback"] = {
                    "type": "success",
                    "message": (
                        f"已保存映射 {summary['alias_name']} -> {summary['canonical_name']}，"
                        f"并回填 {summary['rows_updated']} 条记录。"
                    ),
                }
            except Exception as exc:
                st.session_state["team_name_maintenance_feedback"] = {
                    "type": "warning",
                    "message": f"保存映射失败：{exc}",
                }
            st.rerun()

    st.markdown("#### 待确认候选")
    if not candidate_rows:
        st.success("当前没有待人工确认的候选组。")
    else:
        for index, candidate in enumerate(candidate_rows, start=1):
            title_prefix = candidate["variants"][0]["team_name"] if candidate.get("variants") else candidate["group_key"]
            with st.expander(
                f"{index}. {title_prefix} | {candidate['reason']} | {candidate['total_count']} 条",
                expanded=index <= 3,
            ):
                st.caption(f"group_key: {candidate['group_key']}")
                st.dataframe(
                    build_team_name_candidate_dataframe(candidate),
                    use_container_width=True,
                    hide_index=True,
                )

                canonical_options = _build_candidate_canonical_options(candidate) or [""]
                default_canonical = canonical_options[0] if canonical_options else ""

                with st.form(f"candidate_form_{index}_{candidate['group_key']}"):
                    choice_mode = st.radio(
                        "标准名选择",
                        options=["使用候选中的名称", "自定义标准名"],
                        horizontal=True,
                        key=f"candidate_choice_mode_{index}",
                    )
                    if choice_mode == "使用候选中的名称":
                        selected_canonical = st.selectbox(
                            "选择标准名",
                            options=canonical_options,
                            index=0,
                            key=f"candidate_canonical_select_{index}",
                        )
                    else:
                        selected_canonical = st.text_input(
                            "输入标准名",
                            value=default_canonical,
                            key=f"candidate_canonical_input_{index}",
                        )

                    action_col1, action_col2 = st.columns(2)
                    unify_submitted = action_col1.form_submit_button(
                        "统一到该标准名",
                        type="primary",
                    )
                    skip_submitted = action_col2.form_submit_button("暂不统一")

                if unify_submitted:
                    try:
                        with get_sfc500_connection(SFC500_DATABASE_PATH) as connection:
                            with connection:
                                summary = apply_team_name_candidate_unification(
                                    connection,
                                    TEAM_NAME_TABLE_SPEC,
                                    group_key=candidate["group_key"],
                                    canonical_name=selected_canonical,
                                    variants=candidate["variants"],
                                )
                        st.session_state["team_name_maintenance_feedback"] = {
                            "type": "success",
                            "message": (
                                f"候选组 {summary['group_key']} 已统一到 {summary['canonical_name']}，"
                                f"更新 {summary['aliases_updated']} 个别名，回填 {summary['rows_updated']} 条记录。"
                            ),
                        }
                    except Exception as exc:
                        st.session_state["team_name_maintenance_feedback"] = {
                            "type": "warning",
                            "message": f"统一失败：{exc}",
                        }
                    st.rerun()

                if skip_submitted:
                    with get_sfc500_connection(SFC500_DATABASE_PATH) as connection:
                        with connection:
                            skip_team_name_candidate(
                                connection,
                                group_key=candidate["group_key"],
                                variants=candidate["variants"],
                            )
                    st.session_state["team_name_maintenance_feedback"] = {
                        "type": "info",
                        "message": f"候选组 {candidate['group_key']} 已标记为暂不统一。",
                    }
                    st.rerun()

    with st.expander(f"已确认映射预览（前 {alias_limit} 条）", expanded=False):
        alias_df = build_team_name_alias_dataframe(confirmed_alias_rows)
        if alias_df.empty:
            st.info("当前还没有手工或种子映射。")
        else:
            st.dataframe(alias_df, use_container_width=True, hide_index=True)

    with st.expander(f"已跳过候选（前 {skipped_limit} 条）", expanded=False):
        if not skipped_rows:
            st.info("当前没有被标记为暂不统一的候选。")
        else:
            for index, decision in enumerate(skipped_rows, start=1):
                variant_names = " / ".join(
                    str(variant.get("team_name") or "")
                    for variant in decision.get("variants", [])[:5]
                    if variant.get("team_name")
                )
                restore_col1, restore_col2 = st.columns([5, 1])
                restore_col1.caption(
                    f"{index}. {decision['group_key']} | {variant_names or '-'} | {decision['updated_at']}"
                )
                if restore_col2.button("恢复", key=f"restore_skip_{index}_{decision['group_key']}"):
                    with get_sfc500_connection(SFC500_DATABASE_PATH) as connection:
                        with connection:
                            delete_team_name_review_decision(
                                connection,
                                group_key=decision["group_key"],
                            )
                    st.session_state["team_name_maintenance_feedback"] = {
                        "type": "success",
                        "message": f"候选组 {decision['group_key']} 已恢复到待处理列表。",
                    }
                    st.rerun()


def main() -> None:
    """渲染首页。"""

    st.set_page_config(page_title=APP_TITLE, layout="wide")
    ensure_sfc500_db_available()

    if "sfc500_recent_sync_summary" not in st.session_state:
        st.session_state["sfc500_recent_sync_summary"] = None
    if "backtest_last_result" not in st.session_state:
        st.session_state["backtest_last_result"] = None
    if "backtest_last_error" not in st.session_state:
        st.session_state["backtest_last_error"] = None

    st.title(APP_TITLE)
    st.caption(f"当前数据源：{SOURCE_SITE_URL}")
    if APP_READ_ONLY:
        st.warning("当前为只读演示模式：仅开放历史数据查看和回测，已禁用同步与数据库维护。")
    page_options = READ_ONLY_APP_PAGES if APP_READ_ONLY else APP_PAGES
    selected_page = st.sidebar.radio("页面", options=page_options, index=0)

    if selected_page == "历史数据":
        render_history_page()
    elif selected_page == "回测":
        render_backtest_page()
    else:
        render_database_maintenance_page()
