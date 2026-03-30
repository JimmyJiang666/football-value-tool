"""Streamlit 页面逻辑。"""

import json

import pandas as pd
import streamlit as st

from jczq_assistant.backtest import BacktestConfig
from jczq_assistant.backtest import BacktestEngine
from jczq_assistant.backtest import DEFAULT_LOOKBACK_DAYS
from jczq_assistant.backtest import DEFAULT_VALUE_MODE
from jczq_assistant.backtest import SQLiteBacktestDataSource
from jczq_assistant.backtest import TEAM_STRENGTH_DEFAULT_BAYES_PRIOR_STRENGTH
from jczq_assistant.backtest import TEAM_STRENGTH_DEFAULT_DECAY_HALF_LIFE_DAYS
from jczq_assistant.backtest import TEAM_STRENGTH_DEFAULT_FORM_WINDOW_MATCHES
from jczq_assistant.backtest import TEAM_STRENGTH_DEFAULT_GOAL_CAP
from jczq_assistant.backtest import TEAM_STRENGTH_DEFAULT_H2H_MAX_ADJUSTMENT
from jczq_assistant.backtest import TEAM_STRENGTH_DEFAULT_H2H_WINDOW_MATCHES
from jczq_assistant.backtest import TEAM_STRENGTH_DEFAULT_HOME_AWAY_SPLIT_WEIGHT
from jczq_assistant.backtest import build_strategy
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
from jczq_assistant.sfc500_team_history import (
    SFC500_TEAM_HISTORY_DATABASE_PATH,
    ensure_sfc500_team_history_db_available,
    get_sfc500_team_filter_options,
    get_sfc500_team_history_overview,
    is_sfc500_team_history_db_available,
    query_sfc500_team_matches,
    sync_recent_live_matches,
)
from jczq_assistant.team_names import (
    TeamTableSpec,
    apply_manual_team_name_alias,
    apply_team_name_candidate_unification,
    clean_team_name,
    disable_team_name_alias,
    delete_team_name_review_decision,
    find_team_alias_candidates,
    list_team_name_aliases,
    list_team_name_review_decisions,
    skip_team_name_candidate,
    update_team_name_alias,
)
from jczq_assistant.web_shared import (
    BACKTEST_DAILY_LIMIT_OPTIONS,
    BACKTEST_DATE_PRESET_OPTIONS,
    BACKTEST_HISTORY_MATCH_COUNT_OPTIONS,
    BACKTEST_LOOKBACK_OPTIONS,
    BACKTEST_PARLAY_OPTIONS,
    BACKTEST_STAKING_MODE_OPTIONS,
    BACKTEST_VALUE_MODE_OPTIONS,
    BACKTEST_WEIGHTING_MODE_OPTIONS,
    format_backtest_skip_reason as _shared_format_backtest_skip_reason,
    format_daily_limit_option as _format_daily_limit_option,
    format_lookback_label as _format_lookback_label,
    format_lookback_option as _format_lookback_option,
    format_seconds_brief as _format_seconds_brief,
    format_staking_mode_label as _format_staking_mode_label,
    format_threshold_meaning as _format_threshold_meaning,
    format_value_mode_label as _format_value_mode_label,
    format_weighting_mode_label as _format_weighting_mode_label,
    get_available_backtest_data_source_options as _get_available_backtest_data_source_options,
    is_parlay_strategy as _is_parlay_strategy,
    is_team_strength_strategy as _is_team_strength_strategy,
    is_value_strategy as _is_value_strategy,
    resolve_daily_limit_value as _resolve_daily_limit_value,
    resolve_date_bounds as _resolve_date_bounds,
    resolve_default_date_range as _resolve_default_date_range,
    resolve_lookback_value as _resolve_lookback_value,
    resolve_preset_date_range as _resolve_preset_date_range,
    resolve_value_mode_score_label as _resolve_value_mode_score_label,
    resolve_value_mode_score_column_label as _resolve_value_mode_score_column_label,
    resolve_value_mode_threshold_defaults as _resolve_value_mode_threshold_defaults,
)
from jczq_assistant.web_theme import (
    render_global_styles,
    render_page_banner,
)
from jczq_assistant.web_today import (
    build_recommendation_form_dataframe,
    build_recommendation_form_summary_dataframe,
    build_recommendation_h2h_dataframe,
    build_recommendation_history_matches_dataframe,
    build_recommendation_probability_dataframe,
    render_today_recommendations_page,
)


RECENT_SYNC_OPTIONS = {
    "最近 7 天": 7,
    "最近 14 天": 14,
    "最近 30 天": 30,
}


def _is_dixon_coles_strategy(strategy_name: str) -> bool:
    return str(strategy_name).startswith("dixon_coles")
TEAM_LIVE_SYNC_OPTIONS = {
    "最近 1 天": 1,
    "最近 3 天": 3,
    "最近 7 天": 7,
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
APP_PAGES = ["今日推荐", "历史数据", "回测实验室", "数据库维护"]
READ_ONLY_APP_PAGES = ["今日推荐", "历史数据", "回测实验室"]
BACKTEST_STRATEGY_OPTIONS = {
    "历史水位匹配价值投注": {
        "strategy_name": "historical_odds_value",
        "mode": "value_match",
    },
    "Dixon-Coles 价值投注": {
        "strategy_name": "dixon_coles_value",
        "mode": "dixon_coles",
    },
    "球队强度 Poisson 价值投注（v2）": {
        "strategy_name": "team_strength_poisson_value_v2_no_h2h",
        "mode": "team_strength",
    },
    "最低赔率单关": {
        "strategy_name": "lowest_odds_fixed",
        "mode": "single",
    },
    "最低赔率串关": {
        "strategy_name": "lowest_odds_parlay",
        "mode": "parlay",
    },
}
DIXON_COLES_WEB_DEFAULTS = {
    "dixon_coles_value": {
        "lookback_days": 365,
        "decay_half_life_days": 30,
        "bayes_prior_strength": 6.0,
        "goal_cap": 6,
        "thresholds_by_value_mode": {
            "expected_value": {"home_win": 0.02, "draw": 0.03, "away_win": 0.02},
        },
    },
}
TEAM_STRENGTH_WEB_DEFAULTS = {
    "team_strength_poisson_value_v2": {
        "lookback_days": 180,
        "form_window_matches": 8,
        "decay_half_life_days": 30,
        "bayes_prior_strength": 6.0,
        "home_away_split_weight": 0.55,
        "h2h_window_matches": 4,
        "h2h_max_adjustment": 0.04,
        "goal_cap": 6,
        "thresholds_by_value_mode": {
            "expected_value": {"home_win": 0.02, "draw": 0.03, "away_win": 0.02},
        },
    },
    "team_strength_poisson_value_v2_no_form": {
        "lookback_days": 180,
        "form_window_matches": 8,
        "decay_half_life_days": 30,
        "bayes_prior_strength": 6.0,
        "home_away_split_weight": 0.55,
        "h2h_window_matches": 4,
        "h2h_max_adjustment": 0.04,
        "goal_cap": 6,
        "thresholds_by_value_mode": {
            "expected_value": {"home_win": 0.02, "draw": 0.03, "away_win": 0.02},
        },
    },
    "team_strength_poisson_value_v2_no_h2h": {
        "lookback_days": 180,
        "form_window_matches": 8,
        "decay_half_life_days": 30,
        "bayes_prior_strength": 6.0,
        "home_away_split_weight": 0.55,
        "h2h_window_matches": 4,
        "h2h_max_adjustment": 0.04,
        "goal_cap": 6,
        "thresholds_by_value_mode": {
            "expected_value": {"home_win": 0.02, "draw": 0.03, "away_win": 0.02},
        },
    },
    "team_strength_poisson_value_v2_strength_only": {
        "lookback_days": 365,
        "form_window_matches": 8,
        "decay_half_life_days": 30,
        "bayes_prior_strength": 14.0,
        "home_away_split_weight": 0.85,
        "h2h_window_matches": 4,
        "h2h_max_adjustment": 0.04,
        "goal_cap": 6,
        "thresholds_by_value_mode": {
            "expected_value": {"home_win": 0.02, "draw": 0.03, "away_win": 0.02},
        },
    },
}
HISTORY_DISPLAY_SOURCE_OPTIONS = {
    "期次赔率库（小库）": {
        "db_path": SFC500_DATABASE_PATH,
        "source_kind": "expect",
        "source_label": "期次赔率库（小库）",
        "overview_fn": get_sfc500_history_overview,
        "filter_fn": get_sfc500_filter_options,
        "query_fn": query_sfc500_matches,
        "source_url": "https://trade.500.com/sfc/",
        "source_hint": "trade.500.com/sfc/ 期次页面",
    },
    "完整比赛库（大库）": {
        "db_path": SFC500_TEAM_HISTORY_DATABASE_PATH,
        "source_kind": "team",
        "source_label": "完整比赛库（大库）",
        "overview_fn": get_sfc500_team_history_overview,
        "filter_fn": get_sfc500_team_filter_options,
        "query_fn": query_sfc500_team_matches,
        "source_url": "https://live.500.com/",
        "source_hint": "live.500.com 最近完场列表与球队页历史赛程",
    },
}
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


def _format_compact_metric_value(value: int | float | None) -> str:
    """把较大的计数压缩成更适合卡片展示的格式。"""

    if value is None:
        return "0"
    numeric_value = float(value)
    abs_value = abs(numeric_value)
    if abs_value >= 100000000:
        return f"{numeric_value / 100000000:.2f}亿"
    if abs_value >= 10000:
        return f"{numeric_value / 10000:.2f}万"
    if numeric_value.is_integer():
        return str(int(numeric_value))
    return f"{numeric_value:.2f}"


def _render_history_source_hint(source_url: str, source_hint: str) -> None:
    """渲染历史页来源提示。"""

    st.markdown(
        f"""
        <div style="display:flex;align-items:center;gap:0.5rem;margin-top:-0.1rem;margin-bottom:0.25rem;">
          <span style="display:inline-flex;align-items:center;justify-content:center;width:1.1rem;height:1.1rem;border-radius:999px;background:#e2e8f0;color:#334155;font-size:0.75rem;font-weight:700;">?</span>
          <span style="color:#64748b;font-size:0.92rem;">
            更新来源：<a href="{source_url}" target="_blank">{source_hint}</a>
          </span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_history_overview_metrics(overview: dict, *, source_kind: str) -> None:
    """用中文渲染历史页基础指标。"""

    if source_kind == "team":
        stat_items = [
            ("比赛数", _format_compact_metric_value(overview.get("row_count") or 0)),
            ("球队数", _format_compact_metric_value(overview.get("team_count") or 0)),
            ("已完场", _format_compact_metric_value(overview.get("settled_count") or 0)),
            ("联赛数", _format_compact_metric_value(overview.get("competition_count") or 0)),
        ]
    else:
        stat_items = [
            ("记录数", _format_compact_metric_value(overview.get("row_count") or 0)),
            ("期次数", _format_compact_metric_value(overview.get("expect_count") or 0)),
            ("已开奖", _format_compact_metric_value(overview.get("settled_count") or 0)),
            ("联赛数", _format_compact_metric_value(overview.get("competition_count") or 0)),
        ]

    row_one_col1, row_one_col2 = st.columns(2)
    row_two_col1, row_two_col2 = st.columns(2)
    metric_slots = [row_one_col1, row_one_col2, row_two_col1, row_two_col2]

    for slot, (label, value) in zip(metric_slots, stat_items, strict=False):
        card = slot.container(border=True)
        with card:
            st.caption(label)
            st.markdown(
                f"<div style='font-size:1.5rem;font-weight:800;line-height:1.05;"
                f"letter-spacing:-0.03em;color:#0f172a;margin-top:-0.18rem;'>{value}</div>",
                unsafe_allow_html=True,
            )

    min_match_time = overview.get("min_match_time")
    max_match_time = overview.get("max_match_time")
    if min_match_time and max_match_time:
        st.caption(f"覆盖时间：{min_match_time} -> {max_match_time}")
    else:
        st.caption("当前数据源还没有可展示的数据。")


def _format_backtest_skip_reason(reason: str) -> str:
    """把回测跳过原因转成页面文案。"""

    return _shared_format_backtest_skip_reason(reason)


def _format_exception_for_ui(exc: Exception) -> str:
    """把异常格式化成更适合页面展示的文案。"""

    raw_message = str(exc).strip()
    if raw_message and raw_message not in {"''", '""'}:
        return f"{exc.__class__.__name__}: {raw_message}"
    return repr(exc)


def build_backtest_bets_dataframe(result) -> pd.DataFrame:
    """把回测下注明细转成表格。"""

    rows = [
        {
            "期次": bet.expect,
            "场次": bet.match_no,
            "联赛": bet.competition,
            "比赛时间": bet.match_time,
            "主队": bet.home_team,
            "比分": bet.final_score or "-",
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
            "比分",
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
    score_column_label: str | None = None,
) -> pd.DataFrame:
    """构造 value 策略的高 edge 下注样本表。"""

    if score_column_label is None:
        score_column_label = score_label
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
            score_column_label: bet.edge,
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
            score_column_label,
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
    score_column_label: str | None = None,
) -> pd.DataFrame:
    """构造 value 策略里盈亏极值下注样本表。"""

    if score_column_label is None:
        score_column_label = score_label
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
            score_column_label: bet.edge,
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
            score_column_label,
            "盈亏",
        ],
    )


def _load_backtest_bet_details(bet) -> dict:
    """解析单笔回测下注里保存的诊断明细。"""

    raw_value = str(getattr(bet, "details_json", "") or "").strip()
    if not raw_value:
        return {}
    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _render_backtest_bet_explanation(bet) -> None:
    """渲染单笔回测下注的可解释明细。"""

    details = _load_backtest_bet_details(bet)
    st.caption(
        f"{bet.match_time} | {bet.competition or '-'} | "
        f"{bet.home_team} vs {bet.away_team} | "
        f"下注 {bet.selection_label} | 赛果 {bet.result_label} | 比分 {bet.final_score or '-'}"
    )
    metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
    metric_col1.metric("赔率", f"{float(bet.odds or 0.0):.2f}")
    metric_col2.metric("模型概率", f"{float(bet.model_probability or 0.0):.2%}")
    metric_col3.metric("庄家概率", f"{float(bet.bookmaker_probability or 0.0):.2%}")
    metric_col4.metric("盈亏", f"{float(bet.pnl or 0.0):.2f}")

    if not details:
        st.caption(bet.reason or "当前没有可展示的诊断信息。")
        return

    probability_df = build_recommendation_probability_dataframe(details)
    if not probability_df.empty:
        st.dataframe(probability_df, use_container_width=True, hide_index=True)

    if _is_dixon_coles_strategy(bet.strategy_name):
        home_snapshot = details.get("home_snapshot") or {}
        away_snapshot = details.get("away_snapshot") or {}
        fit_summary = details.get("fit_summary") or {}
        lambda_components = details.get("lambda_components") or {}

        stat_col1, stat_col2, stat_col3, stat_col4 = st.columns(4)
        stat_col1.metric("主队 λ", f"{float(details.get('lambda_home') or 0.0):.2f}")
        stat_col2.metric("客队 λ", f"{float(details.get('lambda_away') or 0.0):.2f}")
        stat_col3.metric("样本数", int(details.get("sample_size") or bet.sample_size or 0))
        stat_col4.metric("value", f"{float(bet.edge or 0.0):.2%}")

        fit_col1, fit_col2, fit_col3, fit_col4 = st.columns(4)
        fit_col1.metric("rho", f"{float(fit_summary.get('rho') or 0.0):+.3f}")
        fit_col2.metric("主场优势", f"{float(fit_summary.get('home_advantage_multiplier') or 1.0):.3f}x")
        fit_col3.metric("拟合迭代", int(fit_summary.get("iterations_run") or 0))
        fit_col4.metric("加权样本", f"{float(fit_summary.get('weighted_match_count') or 0.0):.1f}")

        st.caption(
            f"历史样本模式：{details.get('history_selection_mode') or '-'} | "
            f"样本池：{details.get('history_pool_scope') or '-'} | "
            f"同联赛样本 {int(details.get('same_competition_history_count') or 0)} | "
            f"fallback 后样本 {int(details.get('fallback_history_count') or 0)}。"
        )
        st.caption(
            "主队：attack "
            f"{float(home_snapshot.get('attack_multiplier') or 1.0):.3f}x / defence "
            f"{float(home_snapshot.get('defence_multiplier') or 1.0):.3f}x"
        )
        st.caption(
            "客队：attack "
            f"{float(away_snapshot.get('attack_multiplier') or 1.0):.3f}x / defence "
            f"{float(away_snapshot.get('defence_multiplier') or 1.0):.3f}x"
        )
        st.caption(
            "lambda 拆解：主队基础 "
            f"{float(lambda_components.get('base_lambda_home') or 0.0):.3f} x "
            f"{float(lambda_components.get('home_attack_multiplier') or 1.0):.3f} x "
            f"{float(lambda_components.get('away_defence_multiplier') or 1.0):.3f}；"
            "客队基础 "
            f"{float(lambda_components.get('base_lambda_away') or 0.0):.3f} x "
            f"{float(lambda_components.get('away_attack_multiplier') or 1.0):.3f} x "
            f"{float(lambda_components.get('home_defence_multiplier') or 1.0):.3f}。"
        )

        tau_df = pd.DataFrame(details.get("dc_tau_rows") or [])
        if not tau_df.empty:
            st.markdown("**低比分修正（tau）**")
            st.dataframe(
                tau_df.rename(columns={"score": "比分", "tau": "修正系数"}),
                use_container_width=True,
                hide_index=True,
            )

        reference_col1, reference_col2 = st.columns(2)
        with reference_col1:
            st.markdown("**主队近期比赛（仅参考）**")
            home_form_df = build_recommendation_form_dataframe(details.get("home_recent_form") or [])
            if home_form_df.empty:
                st.info("当前没有主队近期比赛。")
            else:
                st.dataframe(home_form_df.head(6), use_container_width=True, hide_index=True)
        with reference_col2:
            st.markdown("**客队近期比赛（仅参考）**")
            away_form_df = build_recommendation_form_dataframe(details.get("away_recent_form") or [])
            if away_form_df.empty:
                st.info("当前没有客队近期比赛。")
            else:
                st.dataframe(away_form_df.head(6), use_container_width=True, hide_index=True)
    elif _is_team_strength_strategy(bet.strategy_name):
        home_snapshot = details.get("home_snapshot") or {}
        away_snapshot = details.get("away_snapshot") or {}
        h2h_summary = details.get("h2h_summary") or {}
        lambda_components = details.get("lambda_components") or {}

        stat_col1, stat_col2, stat_col3, stat_col4 = st.columns(4)
        stat_col1.metric("主队 λ", f"{float(details.get('lambda_home') or 0.0):.2f}")
        stat_col2.metric("客队 λ", f"{float(details.get('lambda_away') or 0.0):.2f}")
        stat_col3.metric("样本数", int(details.get("sample_size") or bet.sample_size or 0))
        stat_col4.metric("value", f"{float(bet.edge or 0.0):.2%}")

        adj_col1, adj_col2, adj_col3, adj_col4 = st.columns(4)
        adj_col1.metric("Form 修正", f"{float(lambda_components.get('form_delta') or 0.0):.3f}")
        adj_col2.metric("H2H 修正", f"{float(h2h_summary.get('adjustment') or 0.0):.2%}")
        adj_col3.metric("庄家水位", f"{float(details.get('bookmaker_overround') or 0.0):.2%}")
        adj_col4.metric(
            "Fallback",
            "是" if bool(details.get("fallback_applied")) else "否",
        )

        st.caption(
            f"历史样本模式：{details.get('history_selection_mode') or '-'} | "
            f"样本池：{details.get('history_pool_scope') or '-'} | "
            f"同联赛样本 {int(details.get('same_competition_history_count') or 0)} | "
            f"fallback 后样本 {int(details.get('fallback_history_count') or 0)}。"
        )
        st.caption(
            "主队：攻 "
            f"{float(home_snapshot.get('attack_rate') or 0.0):.2f} / 守 "
            f"{float(home_snapshot.get('defence_rate') or 0.0):.2f} / "
            f"近期得分率 {float(home_snapshot.get('recent_points_rate') or 0.0):.2%} / "
            f"近期净胜球 {float(home_snapshot.get('recent_goal_diff_rate') or 0.0):.2f}"
        )
        st.caption(
            "客队：攻 "
            f"{float(away_snapshot.get('attack_rate') or 0.0):.2f} / 守 "
            f"{float(away_snapshot.get('defence_rate') or 0.0):.2f} / "
            f"近期得分率 {float(away_snapshot.get('recent_points_rate') or 0.0):.2%} / "
            f"近期净胜球 {float(away_snapshot.get('recent_goal_diff_rate') or 0.0):.2f}"
        )
        form_summary_df = build_recommendation_form_summary_dataframe(details)
        if not form_summary_df.empty:
            st.markdown("**近期 Form 量化指标**")
            if not bool(details.get("use_recent_form", True)):
                st.caption("当前策略版本没有启用 recent form 修正，下面这些指标只作为参考。")
            st.dataframe(form_summary_df, use_container_width=True, hide_index=True)

        form_col1, form_col2 = st.columns(2)
        with form_col1:
            st.markdown("**主队近期 Form**")
            home_form_df = build_recommendation_form_dataframe(details.get("home_recent_form") or [])
            if home_form_df.empty:
                st.info("当前没有主队近期 form。")
            else:
                st.dataframe(home_form_df.head(6), use_container_width=True, hide_index=True)
        with form_col2:
            st.markdown("**客队近期 Form**")
            away_form_df = build_recommendation_form_dataframe(details.get("away_recent_form") or [])
            if away_form_df.empty:
                st.info("当前没有客队近期 form。")
            else:
                st.dataframe(away_form_df.head(6), use_container_width=True, hide_index=True)

        recent_h2h_df = build_recommendation_h2h_dataframe(details.get("recent_h2h") or [])
        if not recent_h2h_df.empty:
            st.markdown("**最近交手**")
            st.dataframe(recent_h2h_df.head(6), use_container_width=True, hide_index=True)
    else:
        st.caption(
            "解释：先把当前赔率转成归一化庄家概率，再去历史里找最接近的赔率结构样本。"
        )
        nearest_df = build_recommendation_history_matches_dataframe(
            details.get("nearest_matches") or []
        )
        if nearest_df.empty:
            st.info("当前没有可展示的历史参考样本。")
        else:
            st.dataframe(nearest_df.head(10), use_container_width=True, hide_index=True)

    if bet.reason:
        st.caption(f"决策摘要：{bet.reason}")


def _render_clickable_bet_group(
    *,
    title: str,
    bets: list,
    score_label: str,
    expand_count: int = 0,
) -> None:
    """把一组回测记录渲染成可点击展开的解释卡。"""

    if not bets:
        return
    st.markdown(f"**{title}**")
    for index, bet in enumerate(bets, start=1):
        edge_text = f"{float(bet.edge or 0.0):.2%}"
        pnl_text = f"{float(bet.pnl or 0.0):.2f}"
        expander_title = (
            f"{index}. {bet.home_team} vs {bet.away_team} | "
            f"{bet.selection_label} | {score_label} {edge_text} | 盈亏 {pnl_text}"
        )
        with st.expander(expander_title, expanded=index <= expand_count):
            _render_backtest_bet_explanation(bet)


def render_value_strategy_explanation_card(result, detail_limit: int) -> None:
    """渲染 value 类策略的解释卡。"""

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
    score_column_label = _resolve_value_mode_score_column_label(value_mode)
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
        if _is_dixon_coles_strategy(result.strategy_name):
            st.caption(
                "这套策略会先用历史比分拟合球队 attack / defence 参数和主场优势，"
                "再用 Dixon-Coles 的 rho 对低比分分布做修正。"
            )
            if value_mode == "expected_value":
                st.markdown(
                    "`lambda_home / lambda_away = fitted attack/defence x 主场优势`  |  "
                    "`模型概率 = Dixon-Coles 胜平负分布`  |  "
                    "`EV = 模型概率 x 当前赔率 - 1`"
                )
            elif value_mode == "model_probability":
                st.markdown(
                    "`lambda_home / lambda_away = fitted attack/defence x 主场优势`  |  "
                    "`模型概率 = Dixon-Coles 胜平负分布`  |  "
                    "`下注分数 = 模型概率本身`"
                )
            else:
                st.markdown(
                    "`lambda_home / lambda_away = fitted attack/defence x 主场优势`  |  "
                    "`模型概率 = Dixon-Coles 胜平负分布`  |  "
                    "`value = 模型概率 - 庄家概率`"
                )
        elif _is_team_strength_strategy(result.strategy_name):
            use_recent_form = bool(diagnostics.get("use_recent_form", True))
            use_h2h = bool(diagnostics.get("use_h2h", True))
            component_labels = ["球队攻防强度", "主客场拆分"]
            if use_recent_form:
                component_labels.append("近期状态")
            if use_h2h:
                component_labels.append("弱交手修正")
            st.caption(
                "这套策略会先用历史池里两队的 "
                + "、".join(component_labels)
                + "，估计主客队预期进球，再通过 Poisson 比分分布推导胜平负概率。"
            )
            if value_mode == "expected_value":
                st.markdown(
                    "`lambda_home / lambda_away = 收缩后的攻防强度 x 可选修正项`  |  "
                    "`模型概率 = Poisson 胜平负分布`  |  "
                    "`EV = 模型概率 x 当前赔率 - 1`"
                )
            elif value_mode == "model_probability":
                st.markdown(
                    "`lambda_home / lambda_away = 收缩后的攻防强度 x 可选修正项`  |  "
                    "`模型概率 = Poisson 胜平负分布`  |  "
                    "`下注分数 = 模型概率本身`"
                )
            else:
                st.markdown(
                    "`lambda_home / lambda_away = 收缩后的攻防强度 x 可选修正项`  |  "
                    "`模型概率 = Poisson 胜平负分布`  |  "
                    "`value = 模型概率 - 庄家概率`"
                )
        else:
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
            elif value_mode == "model_probability":
                st.markdown(
                    "`模型概率 = 历史相似样本的加权结果频率`  |  "
                    "`庄家概率 = 当前胜平负赔率归一化隐含概率`  |  "
                    "`下注分数 = 模型概率本身`"
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

        prediction_metrics = dict(diagnostics.get("prediction_metrics") or {})
        if prediction_metrics.get("prediction_count"):
            prob_metric_col1, prob_metric_col2, prob_metric_col3 = st.columns(3)
            prob_metric_col1.metric(
                "Brier Score",
                f"{float(prediction_metrics.get('brier_score') or 0.0):.4f}",
            )
            prob_metric_col2.metric(
                "Log Loss",
                f"{float(prediction_metrics.get('log_loss') or 0.0):.4f}",
            )
            prob_metric_col3.metric(
                "概率评估样本",
                int(prediction_metrics.get("prediction_count") or 0),
            )

        st.caption(
            f"当前使用：{_format_lookback_label(lookback_days)}、"
            f"{value_mode_label}、{staking_mode_label}"
            + (
                ""
                if _is_team_strength_strategy(result.strategy_name)
                else f"、{weighting_mode_label}"
            )
            + "。"
        )
        if _is_dixon_coles_strategy(result.strategy_name):
            fit_metrics = [
                _format_lookback_label(lookback_days),
                f"半衰期 {int(diagnostics.get('decay_half_life_days') or 0)} 天",
                f"收缩 {float(diagnostics.get('bayes_prior_strength') or 0.0):.1f}",
            ]
            st.caption("Dixon-Coles 参数：" + "，".join(fit_metrics) + "。")
        elif _is_team_strength_strategy(result.strategy_name):
            st.caption(
                "球队强度参数：近期窗口 "
                f"{int(diagnostics.get('form_window_matches') or 0)} 场，"
                f"半衰期 {int(diagnostics.get('decay_half_life_days') or 0)} 天，"
                f"贝叶斯收缩 {float(diagnostics.get('bayes_prior_strength') or 0.0):.1f}，"
                f"主客场权重 {float(diagnostics.get('home_away_split_weight') or 0.0):.2f}，"
                f"交手窗口 {int(diagnostics.get('h2h_window_matches') or 0)} 场，"
                f"交手上限 {float(diagnostics.get('h2h_max_adjustment') or 0.0):.1%}。"
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
            score_column_label=score_column_label,
        )
        top_edge_bets = sorted(
            result.bets,
            key=lambda bet: (
                -(float(bet.edge or 0.0)),
                -(float(bet.model_probability or 0.0)),
                bet.match_time,
            ),
        )[: min(detail_limit, 10)]
        if not top_edge_df.empty:
            st.caption("高分下注样本")
            st.dataframe(top_edge_df, use_container_width=True, hide_index=True)
            _render_clickable_bet_group(
                title="高分下注样本解释",
                bets=top_edge_bets,
                score_label=score_label,
                expand_count=0,
            )

        profit_col, loss_col = st.columns(2)
        top_profit_df = build_value_strategy_pnl_extremes_dataframe(
            result,
            limit=3,
            direction="profit",
            score_label=score_label,
            score_column_label=score_column_label,
        )
        top_profit_bets = sorted(
            result.bets,
            key=lambda bet: (
                float(bet.pnl),
                float(bet.stake),
                bet.match_time,
            ),
            reverse=True,
        )[:3]
        top_loss_df = build_value_strategy_pnl_extremes_dataframe(
            result,
            limit=3,
            direction="loss",
            score_label=score_label,
            score_column_label=score_column_label,
        )
        top_loss_bets = sorted(
            result.bets,
            key=lambda bet: (
                float(bet.pnl),
                float(bet.stake),
                bet.match_time,
            ),
        )[:3]
        with profit_col:
            st.caption("赚得最多 3 条")
            if top_profit_df.empty:
                st.info("当前没有可展示的盈利注单。")
            else:
                st.dataframe(top_profit_df, use_container_width=True, hide_index=True)
                _render_clickable_bet_group(
                    title="盈利注单解释",
                    bets=top_profit_bets,
                    score_label=score_label,
                )
        with loss_col:
            st.caption("亏得最多 3 条")
            if top_loss_df.empty:
                st.info("当前没有可展示的亏损注单。")
            else:
                st.dataframe(top_loss_df, use_container_width=True, hide_index=True)
                _render_clickable_bet_group(
                    title="亏损注单解释",
                    bets=top_loss_bets,
                    score_label=score_label,
                )

        calibration_rows = list(prediction_metrics.get("calibration") or [])
        if calibration_rows:
            calibration_df = pd.DataFrame(calibration_rows)
            with st.expander("Calibration Diagnostics", expanded=False):
                st.dataframe(calibration_df, use_container_width=True, hide_index=True)


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
    metric_columns[0].metric("同步天数", summary.get("days"))
    metric_columns[1].metric("同步状态", summary.get("status") or "-")
    metric_columns[2].metric("抓取记录", summary.get("rows_fetched") or 0)
    metric_columns[3].metric("新增记录", summary.get("rows_inserted") or 0)

    extra_columns = st.columns(2)
    extra_columns[0].metric("有效期次", summary.get("valid_expects") or 0)
    extra_columns[1].metric("扫描期次", summary.get("scanned_expects") or 0)

    if summary.get("errors"):
        st.error("部分期次同步失败：\n\n" + "\n".join(summary["errors"]))

    sample_df = build_sfc500_history_dataframe(summary.get("sample_matches", []))
    if not sample_df.empty:
        st.caption("样本预览")
        st.dataframe(sample_df, use_container_width=True, hide_index=True)


def render_team_live_sync_summary(summary: dict) -> None:
    """渲染最近一次球队大库 live 增量同步摘要。"""

    st.caption(f"最近一次同步写入：{summary.get('db_path')}")
    metric_columns = st.columns(4)
    metric_columns[0].metric("同步天数", summary.get("days") or 0)
    metric_columns[1].metric("日期数", summary.get("date_count") or 0)
    metric_columns[2].metric("抓取比赛", summary.get("rows_fetched") or 0)
    metric_columns[3].metric("新增比赛", summary.get("rows_inserted") or 0)

    if summary.get("start_date") and summary.get("end_date"):
        st.caption(f"同步日期范围：{summary['start_date']} -> {summary['end_date']}")

    if summary.get("errors"):
        st.error("部分日期同步失败：\n\n" + "\n".join(summary["errors"]))

    sample_rows = []
    for match in summary.get("sample_matches", []):
        sample_rows.append(
            {
                "比赛时间": match.get("match_time"),
                "联赛": match.get("competition"),
                "主队": match.get("home_team_canonical") or match.get("home_team"),
                "比分": match.get("final_score"),
                "客队": match.get("away_team_canonical") or match.get("away_team"),
                "半场": match.get("half_time_score"),
                "主胜均赔": match.get("avg_win_odds"),
                "平局均赔": match.get("avg_draw_odds"),
                "客胜均赔": match.get("avg_lose_odds"),
            }
        )
    sample_df = pd.DataFrame(
        sample_rows,
        columns=["比赛时间", "联赛", "主队", "比分", "客队", "半场", "主胜均赔", "平局均赔", "客胜均赔"],
    )
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

    team_history_db_available = bool(st.session_state.get("team_history_db_available", True))

    render_page_banner(
        title="历史数据",
        subtitle="这里同时管理期次小库和完整比赛大库：上面负责增量同步，下面统一做查询、筛选和比对，主客队默认优先展示标准名。",
        emoji="📚",
        chips=["双数据源", "增量同步", "统一筛选", "标准名展示"],
    )

    if APP_READ_ONLY:
        st.info("当前是只读演示环境：同步入口仅展示，不可执行；数据库维护页已隐藏。")
    else:
        st.caption("这里分成两个同步入口和一个统一的数据展示区域，页面展示默认优先使用标准化后的球队名。")

    sync_card_col1, sync_card_col2 = st.columns(2)
    with sync_card_col1:
        small_sync_card = st.container(border=True)
        with small_sync_card:
            st.markdown("#### 🧲 期次赔率同步（小库）")
            _render_history_source_hint(
                "https://trade.500.com/sfc/",
                "trade.500.com/sfc/ 期次页面",
            )
            st.caption(f"目标库：{SFC500_DATABASE_PATH}")
            _render_history_overview_metrics(
                get_sfc500_history_overview(),
                source_kind="expect",
            )

            sync_control_col1, sync_control_col2 = st.columns([1, 1.35])
            selected_sync_label = sync_control_col1.selectbox(
                "同步范围",
                options=list(RECENT_SYNC_OPTIONS.keys()),
                index=0,
            )
            sync_button_clicked = sync_control_col2.button(
                "同步小库",
                key="sync_sfc500_recent_history",
                type="primary",
                use_container_width=True,
                disabled=APP_READ_ONLY,
                help="只读演示环境已禁用写库同步。" if APP_READ_ONLY else None,
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

            if APP_READ_ONLY:
                st.caption("只读模式下按钮已禁用，云端用户无法通过页面更新小库。")

            if st.session_state["sfc500_recent_sync_summary"]:
                render_recent_sync_summary(st.session_state["sfc500_recent_sync_summary"])
            else:
                st.info("尚未执行页面内的期次赔率同步。")

    with sync_card_col2:
        large_sync_card = st.container(border=True)
        with large_sync_card:
            st.markdown("#### 🌐 完整比赛同步（大库）")
            _render_history_source_hint(
                "https://live.500.com/",
                "live.500.com 最近完场列表与球队页历史赛程",
            )
            st.caption(f"目标库：{SFC500_TEAM_HISTORY_DATABASE_PATH}")
            if team_history_db_available:
                team_overview = get_sfc500_team_history_overview()
                _render_history_overview_metrics(team_overview, source_kind="team")
            else:
                team_overview = {}
                st.warning("当前部署环境未包含完整比赛大库，已禁用大库同步与展示。")

            team_sync_control_col1, team_sync_control_col2 = st.columns([1, 1.35])
            selected_team_sync_label = team_sync_control_col1.selectbox(
                "增量同步范围",
                options=list(TEAM_LIVE_SYNC_OPTIONS.keys()),
                index=1,
                help="当前增量更新优先走 live.500.com 最近完场列表，不再重扫全部球队。",
            )
            team_sync_button_clicked = team_sync_control_col2.button(
                "同步大库",
                key="sync_sfc500_team_live_recent_history",
                type="primary",
                use_container_width=True,
                disabled=APP_READ_ONLY or not team_history_db_available,
                help=(
                    "当前部署未包含大库。"
                    if not team_history_db_available
                    else ("只读演示环境已禁用写库同步。" if APP_READ_ONLY else None)
                ),
            )

            team_sync_status_placeholder = st.empty()
            team_sync_progress_placeholder = st.empty()

            if team_sync_button_clicked:
                progress_bar = team_sync_progress_placeholder.progress(0.0)

                def on_team_live_sync_progress(event: dict) -> None:
                    ratio = _estimate_progress_ratio(event)
                    progress_bar.progress(ratio)

                    message = event.get("message", "正在同步大库...")
                    stage = event.get("stage")
                    if stage == "date_error":
                        team_sync_status_placeholder.error(message)
                    elif stage == "finish":
                        team_sync_status_placeholder.success(message)
                    else:
                        team_sync_status_placeholder.info(message)

                days = TEAM_LIVE_SYNC_OPTIONS[selected_team_sync_label]
                with st.spinner(f"正在用 live.500.com 同步最近 {days} 天完场比赛，请稍候..."):
                    try:
                        summary = sync_recent_live_matches(
                            days=days,
                            progress_callback=on_team_live_sync_progress,
                        )
                        st.session_state["sfc500_team_live_sync_summary"] = summary
                        team_sync_progress_placeholder.progress(1.0)
                    except Exception as exc:
                        st.session_state["sfc500_team_live_sync_summary"] = None
                        team_sync_status_placeholder.error(f"同步失败：{exc}")
                        team_sync_progress_placeholder.empty()
                        st.error(f"同步失败：{exc}")

            if not team_history_db_available:
                st.caption("当前环境没有部署大库文件；如果要启用，请在部署端提供大库 SQLite。")
            elif APP_READ_ONLY:
                st.caption("只读模式下按钮已禁用，云端用户无法通过页面更新大库。")

            if st.session_state["sfc500_team_live_sync_summary"]:
                render_team_live_sync_summary(st.session_state["sfc500_team_live_sync_summary"])
            else:
                st.info("尚未执行页面内的完整比赛同步。")

    display_card = st.container(border=True)
    with display_card:
        st.markdown("#### 🔎 数据展示")
        st.caption("表格中的主客队优先展示标准名；筛选仍同时支持标准名与原始名。")

        history_display_source_options = {
            label: meta
            for label, meta in HISTORY_DISPLAY_SOURCE_OPTIONS.items()
            if team_history_db_available or str(meta.get("source_kind")) != "team"
        }

        selected_display_source_label = st.radio(
            "展示数据源",
            options=list(history_display_source_options.keys()),
            horizontal=True,
        )
        display_source_meta = history_display_source_options[selected_display_source_label]
        overview = display_source_meta["overview_fn"](display_source_meta["db_path"])
        filter_options = display_source_meta["filter_fn"](display_source_meta["db_path"])
        query_fn = display_source_meta["query_fn"]
        source_kind = str(display_source_meta["source_kind"])

        _render_history_source_hint(
            str(display_source_meta["source_url"]),
            str(display_source_meta["source_hint"]),
        )
        st.caption(f"当前展示库：{display_source_meta['db_path']}")
        _render_history_overview_metrics(overview, source_kind=source_kind)

        default_start_date, default_end_date = _resolve_default_date_range(overview)

        filter_col1, filter_col2, filter_col3 = st.columns(3)
        start_date = filter_col1.date_input(
            "开始日期",
            value=default_start_date,
            key=f"history_start_date_{source_kind}",
        )
        end_date = filter_col2.date_input(
            "结束日期",
            value=default_end_date,
            key=f"history_end_date_{source_kind}",
        )
        result_limit = filter_col3.selectbox(
            "展示条数",
            options=[100, 200, 500, 1000],
            index=1,
            key=f"history_result_limit_{source_kind}",
        )

        filter_col4, filter_col5 = st.columns(2)
        selected_competitions = filter_col4.multiselect(
            "联赛",
            options=filter_options["competitions"],
            default=[],
            key=f"history_competitions_{source_kind}",
        )
        selected_teams = filter_col5.multiselect(
            "球队",
            options=filter_options["teams"],
            default=[],
            key=f"history_teams_{source_kind}",
        )

        filter_col6, filter_col7, filter_col8 = st.columns(3)
        team_keyword = filter_col6.text_input(
            "球队关键词",
            value="",
            key=f"history_team_keyword_{source_kind}",
        )
        expect_value = filter_col7.text_input(
            "期次",
            value="",
            key=f"history_expect_{source_kind}",
            help="大库下这里会匹配比赛编号 fixture_id；小库下仍然匹配原期次。",
        )
        settled_filter_label = filter_col8.selectbox(
            "赛果状态",
            options=["全部", "仅已开奖", "仅未开奖"],
            index=0,
            key=f"history_settled_filter_{source_kind}",
        )

        if start_date > end_date:
            st.error("开始日期不能晚于结束日期。")
        else:
            query_result = query_fn(
                db_path=display_source_meta["db_path"],
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

def render_backtest_page() -> None:
    """渲染回测页。"""

    team_history_db_available = bool(st.session_state.get("team_history_db_available", True))
    available_backtest_sources = _get_available_backtest_data_source_options(
        team_history_available=team_history_db_available
    )

    render_page_banner(
        title="回测实验室",
        subtitle="把同一套赔率和赛果历史，切成不同的模拟开盘池、训练集和资金管理模式，快速比较策略稳健性。",
        emoji="📈",
        chips=["四种策略", "训练集可切换", "资金曲线", "指标解释"],
    )
    selected_candidate_source_label = st.selectbox(
        "每日模拟开盘池",
        options=list(available_backtest_sources.keys()),
        index=0,
        help="这里决定每天有哪些比赛进入回测；默认仍然使用原来的 14 场期次主库。",
    )
    selected_candidate_source_meta = available_backtest_sources[selected_candidate_source_label]
    selected_db_path = selected_candidate_source_meta["db_path"]
    selected_source_kind = str(selected_candidate_source_meta["source_kind"])
    selected_source_label = str(selected_candidate_source_meta["source_label"])
    overview = selected_candidate_source_meta["overview_fn"](selected_db_path)
    filter_options = selected_candidate_source_meta["filter_fn"](selected_db_path)
    default_start_date, default_end_date = _resolve_default_date_range(overview)
    min_match_time = overview.get("min_match_time")
    max_match_time = overview.get("max_match_time")

    overview_col1, overview_col2, overview_col3, overview_col4 = st.columns(4)
    overview_col1.metric("记录数", overview.get("row_count") or 0)
    overview_col2.metric("已开奖", overview.get("settled_count") or 0)
    overview_col3.metric("联赛数", overview.get("competition_count") or 0)
    overview_col4.metric(
        "球队数" if selected_source_kind == "team" else "期次数",
        overview.get("team_count") or overview.get("expect_count") or 0,
    )

    if min_match_time and max_match_time:
        st.caption(
            f"{selected_source_label}可回测覆盖时间：{min_match_time} -> {max_match_time}"
        )
    else:
        st.warning(f"当前{selected_source_label}还没有可用于回测的数据。")

    min_date, max_date = _resolve_date_bounds(overview)
    default_start_date, default_end_date = _resolve_default_date_range(overview)

    if "backtest_date_preset" not in st.session_state:
        st.session_state["backtest_date_preset"] = "最近 30 天"
    if "backtest_applied_preset" not in st.session_state:
        st.session_state["backtest_applied_preset"] = st.session_state["backtest_date_preset"]
    if "backtest_last_candidate_source_label" not in st.session_state:
        st.session_state["backtest_last_candidate_source_label"] = selected_candidate_source_label
    if "backtest_start_date" not in st.session_state:
        st.session_state["backtest_start_date"] = default_start_date
    if "backtest_end_date" not in st.session_state:
        st.session_state["backtest_end_date"] = default_end_date

    if st.session_state["backtest_last_candidate_source_label"] != selected_candidate_source_label:
        st.session_state["backtest_start_date"] = default_start_date
        st.session_state["backtest_end_date"] = default_end_date
        st.session_state["backtest_last_candidate_source_label"] = selected_candidate_source_label

    if st.session_state["backtest_start_date"] < min_date:
        st.session_state["backtest_start_date"] = min_date
    if st.session_state["backtest_start_date"] > max_date:
        st.session_state["backtest_start_date"] = max_date
    if st.session_state["backtest_end_date"] < min_date:
        st.session_state["backtest_end_date"] = min_date
    if st.session_state["backtest_end_date"] > max_date:
        st.session_state["backtest_end_date"] = max_date

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
        if _is_value_strategy(selected_strategy_name):
            training_source_options = list(available_backtest_sources.keys())
            default_training_label = "球队大库" if "球队大库" in training_source_options else training_source_options[0]
            selected_training_source_label = control_col3.selectbox(
                "策略训练集",
                options=training_source_options,
                index=training_source_options.index(default_training_label),
                help=(
                    "只影响 value / Poisson 策略使用的历史训练样本；默认优先球队大库。"
                    if team_history_db_available
                    else "当前部署没有球队大库，训练集已自动回退到 14 场主库。"
                ),
            )
        else:
            selected_training_source_label = selected_candidate_source_label
            control_col3.caption(
                f"当前模拟开盘池：{selected_candidate_source_label}；单关和串关不会额外使用训练集。"
            )

        selected_training_source_meta = available_backtest_sources[selected_training_source_label]
        selected_training_db_path = selected_training_source_meta["db_path"]
        selected_training_source_kind = str(selected_training_source_meta["source_kind"])
        selected_training_source_label_value = str(
            selected_training_source_meta["source_label"]
        )

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
        if _is_value_strategy(selected_strategy_name):
            training_overview = selected_training_source_meta["overview_fn"](
                selected_training_db_path
            )
            control_col6.caption(
                f"训练集：{selected_training_source_label}，"
                f"records={training_overview.get('row_count') or 0}，"
                f"覆盖 {training_overview.get('min_match_time') or '-'} -> "
                f"{training_overview.get('max_match_time') or '-'}"
            )
            if not team_history_db_available:
                st.caption("当前部署环境未提供球队大库，value / Poisson 策略已自动使用 14 场主库训练。")

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
    form_window_matches = TEAM_STRENGTH_DEFAULT_FORM_WINDOW_MATCHES
    decay_half_life_days = TEAM_STRENGTH_DEFAULT_DECAY_HALF_LIFE_DAYS
    bayes_prior_strength = TEAM_STRENGTH_DEFAULT_BAYES_PRIOR_STRENGTH
    home_away_split_weight = TEAM_STRENGTH_DEFAULT_HOME_AWAY_SPLIT_WEIGHT
    h2h_window_matches = TEAM_STRENGTH_DEFAULT_H2H_WINDOW_MATCHES
    h2h_max_adjustment = TEAM_STRENGTH_DEFAULT_H2H_MAX_ADJUSTMENT
    goal_cap = TEAM_STRENGTH_DEFAULT_GOAL_CAP
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
            threshold_defaults = _resolve_value_mode_threshold_defaults(
                value_mode,
                strategy_name=selected_strategy_name,
            )

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
        elif selected_strategy_mode == "dixon_coles":
            dixon_defaults = DIXON_COLES_WEB_DEFAULTS.get(selected_strategy_name, {})
            tuned_threshold_defaults = (
                dixon_defaults.get("thresholds_by_value_mode", {}) or {}
            ).get(DEFAULT_VALUE_MODE, {})
            st.caption(
                "Dixon-Coles 会直接用历史比分拟合球队 attack / defence 参数和主场优势，"
                "再对低比分做相关性修正，最后推出 1X2 概率。"
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
            )
            max_bets_per_day = _resolve_daily_limit_value(max_bets_per_day_option)
            lookback_days = _resolve_lookback_value(
                control_col9.selectbox(
                    "历史回看窗口",
                    options=BACKTEST_LOOKBACK_OPTIONS,
                    index=BACKTEST_LOOKBACK_OPTIONS.index(
                        dixon_defaults.get("lookback_days", DEFAULT_LOOKBACK_DAYS)
                    ),
                    format_func=_format_lookback_option,
                )
            )

            control_col10, control_col11, control_col12 = st.columns(3)
            value_mode = BACKTEST_VALUE_MODE_OPTIONS[
                control_col10.selectbox(
                    "value 计算方式",
                    options=list(BACKTEST_VALUE_MODE_OPTIONS.keys()),
                    index=1,
                )
            ]
            staking_mode = BACKTEST_STAKING_MODE_OPTIONS[
                control_col11.selectbox(
                    "投注模式",
                    options=list(BACKTEST_STAKING_MODE_OPTIONS.keys()),
                    index=0,
                )
            ]
            min_history_matches = int(
                control_col12.number_input(
                    "每队最小历史样本数",
                    min_value=3,
                    step=1,
                    value=6,
                )
            )
            score_label = _resolve_value_mode_score_label(value_mode)
            threshold_defaults = _resolve_value_mode_threshold_defaults(
                value_mode,
                strategy_name=selected_strategy_name,
            )
            tuned_threshold_defaults = (
                dixon_defaults.get("thresholds_by_value_mode", {}) or {}
            ).get(value_mode, {})
            if tuned_threshold_defaults:
                threshold_defaults = {
                    "home_win": float(tuned_threshold_defaults.get("home_win", threshold_defaults["home_win"])),
                    "draw": float(tuned_threshold_defaults.get("draw", threshold_defaults["draw"])),
                    "away_win": float(tuned_threshold_defaults.get("away_win", threshold_defaults["away_win"])),
                }

            if staking_mode == "fractional_kelly":
                control_col13, control_col14, control_col15 = st.columns(3)
                initial_bankroll = float(control_col13.number_input("初始资金", min_value=100.0, step=100.0, value=1000.0, format="%.2f"))
                kelly_fraction = float(control_col14.number_input("Kelly 折扣", min_value=0.05, max_value=1.0, step=0.05, value=0.25, format="%.2f"))
                max_stake_pct = float(control_col15.number_input("单场最大仓位", min_value=0.005, max_value=0.2, step=0.005, value=0.02, format="%.3f"))
            else:
                fixed_stake = float(
                    st.number_input(
                        "固定投注金额",
                        min_value=1.0,
                        step=1.0,
                        value=10.0,
                    )
                )

            control_col16, control_col17, control_col18 = st.columns(3)
            same_competition_only = bool(control_col16.checkbox("仅同联赛历史样本", value=True))
            decay_half_life_days = int(
                control_col17.number_input(
                    "时间衰减半衰期（天）",
                    min_value=7,
                    max_value=365,
                    step=7,
                    value=int(dixon_defaults.get("decay_half_life_days", TEAM_STRENGTH_DEFAULT_DECAY_HALF_LIFE_DAYS)),
                )
            )
            bayes_prior_strength = float(
                control_col18.number_input(
                    "贝叶斯收缩强度",
                    min_value=1.0,
                    max_value=30.0,
                    step=1.0,
                    value=float(dixon_defaults.get("bayes_prior_strength", TEAM_STRENGTH_DEFAULT_BAYES_PRIOR_STRENGTH)),
                    format="%.1f",
                )
            )

            control_col19, control_col20, control_col21 = st.columns(3)
            goal_cap_options = [4, 5, 6, 7, 8]
            goal_cap = int(
                control_col19.selectbox(
                    "进球截断",
                    options=goal_cap_options,
                    index=goal_cap_options.index(int(dixon_defaults.get("goal_cap", TEAM_STRENGTH_DEFAULT_GOAL_CAP))),
                )
            )
            control_col20.caption("当前模型会拟合 attack / defence / home advantage / rho。")
            control_col21.caption("近期 form 和 h2h 不参与这个模型的拟合。")

            st.caption(f"下面三个阈值按下注结果分别生效，当前分数类型是 {score_label}。")
            control_col22, control_col23, control_col24 = st.columns(3)
            min_edge_home_win = float(control_col22.number_input(f"主胜最小{score_label}", min_value=0.0, step=0.005, value=threshold_defaults["home_win"], format="%.3f", help=_format_threshold_meaning(value_mode, threshold_defaults["home_win"])))
            min_edge_draw = float(control_col23.number_input(f"平局最小{score_label}", min_value=0.0, step=0.005, value=threshold_defaults["draw"], format="%.3f", help=_format_threshold_meaning(value_mode, threshold_defaults["draw"])))
            min_edge_away_win = float(control_col24.number_input(f"客胜最小{score_label}", min_value=0.0, step=0.005, value=threshold_defaults["away_win"], format="%.3f", help=_format_threshold_meaning(value_mode, threshold_defaults["away_win"])))
            min_edge = min(min_edge_home_win, min_edge_draw, min_edge_away_win)
        elif selected_strategy_mode == "team_strength":
            team_strength_defaults = TEAM_STRENGTH_WEB_DEFAULTS.get(
                selected_strategy_name,
                {},
            )
            tuned_threshold_defaults = (
                team_strength_defaults.get("thresholds_by_value_mode", {}) or {}
            ).get(DEFAULT_VALUE_MODE, {})
            st.caption(
                "球队强度 Poisson 策略会先估计两队当前攻防强度和近期状态，"
                "再用 Poisson 比分分布推出胜平负概率；只有超过阈值才下注。"
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
            lookback_days = _resolve_lookback_value(
                control_col9.selectbox(
                    "历史回看窗口",
                    options=BACKTEST_LOOKBACK_OPTIONS,
                    index=BACKTEST_LOOKBACK_OPTIONS.index(
                        team_strength_defaults.get("lookback_days", DEFAULT_LOOKBACK_DAYS)
                    ),
                    format_func=_format_lookback_option,
                    help="球队强度只会使用这段窗口内的历史比赛来估计状态。",
                )
            )

            control_col10, control_col11, control_col12 = st.columns(3)
            value_mode = BACKTEST_VALUE_MODE_OPTIONS[
                control_col10.selectbox(
                    "value 计算方式",
                    options=list(BACKTEST_VALUE_MODE_OPTIONS.keys()),
                    index=1,
                    help="概率差看模型概率相对庄家概率的优势；EV 看期望收益率。",
                )
            ]
            staking_mode = BACKTEST_STAKING_MODE_OPTIONS[
                control_col11.selectbox(
                    "投注模式",
                    options=list(BACKTEST_STAKING_MODE_OPTIONS.keys()),
                    index=0,
                )
            ]
            min_history_matches = int(
                control_col12.number_input(
                    "每队最小历史样本数",
                    min_value=3,
                    step=1,
                    value=6,
                    help="两队各自至少需要多少场历史比赛才允许下注。",
                )
            )
            score_label = _resolve_value_mode_score_label(value_mode)
            threshold_defaults = _resolve_value_mode_threshold_defaults(
                value_mode,
                strategy_name=selected_strategy_name,
            )
            tuned_threshold_defaults = (
                team_strength_defaults.get("thresholds_by_value_mode", {}) or {}
            ).get(value_mode, {})
            if tuned_threshold_defaults:
                threshold_defaults = {
                    "home_win": float(
                        tuned_threshold_defaults.get("home_win", threshold_defaults["home_win"])
                    ),
                    "draw": float(
                        tuned_threshold_defaults.get("draw", threshold_defaults["draw"])
                    ),
                    "away_win": float(
                        tuned_threshold_defaults.get("away_win", threshold_defaults["away_win"])
                    ),
                }

            if staking_mode == "fractional_kelly":
                st.caption(
                    "Kelly 模式会用模型概率和赔率动态决定下注额；"
                    "因此这里不再显示固定投注金额。"
                )
                control_col13, control_col14, control_col15 = st.columns(3)
                initial_bankroll = float(
                    control_col13.number_input(
                        "初始资金",
                        min_value=100.0,
                        step=100.0,
                        value=1000.0,
                        format="%.2f",
                    )
                )
                kelly_fraction = float(
                    control_col14.number_input(
                        "Kelly 折扣",
                        min_value=0.05,
                        max_value=1.0,
                        step=0.05,
                        value=0.25,
                        format="%.2f",
                    )
                )
                max_stake_pct = float(
                    control_col15.number_input(
                        "单场最大仓位",
                        min_value=0.005,
                        max_value=0.2,
                        step=0.005,
                        value=0.02,
                        format="%.3f",
                    )
                )
            else:
                fixed_stake = float(
                    st.number_input(
                        "固定投注金额",
                        min_value=1.0,
                        step=1.0,
                        value=10.0,
                        help="当前策略每场按固定金额下注。",
                    )
                )

            control_col16, control_col17, control_col18 = st.columns(3)
            same_competition_only = bool(
                control_col16.checkbox(
                    "仅同联赛历史样本",
                    value=True,
                    help="建议打开；只用相同联赛历史比赛估计球队强度。",
                )
            )
            form_window_matches = int(
                control_col17.number_input(
                    "近期状态窗口场数",
                    min_value=3,
                    max_value=20,
                    step=1,
                    value=int(
                        team_strength_defaults.get(
                            "form_window_matches",
                            TEAM_STRENGTH_DEFAULT_FORM_WINDOW_MATCHES,
                        )
                    ),
                    help="近期状态只看最近 N 场。",
                )
            )
            decay_half_life_days = int(
                control_col18.number_input(
                    "时间衰减半衰期（天）",
                    min_value=7,
                    max_value=365,
                    step=7,
                    value=int(
                        team_strength_defaults.get(
                            "decay_half_life_days",
                            TEAM_STRENGTH_DEFAULT_DECAY_HALF_LIFE_DAYS,
                        )
                    ),
                    help="越近的历史比赛权重越高。",
                )
            )

            control_col19, control_col20, control_col21 = st.columns(3)
            bayes_prior_strength = float(
                control_col19.number_input(
                    "贝叶斯收缩强度",
                    min_value=1.0,
                    max_value=30.0,
                    step=1.0,
                    value=float(
                        team_strength_defaults.get(
                            "bayes_prior_strength",
                            TEAM_STRENGTH_DEFAULT_BAYES_PRIOR_STRENGTH,
                        )
                    ),
                    format="%.1f",
                    help="值越大，球队数据越会向联赛平均回归。",
                )
            )
            home_away_split_weight = float(
                control_col20.slider(
                    "主客场拆分权重",
                    min_value=0.0,
                    max_value=1.0,
                    step=0.05,
                    value=float(
                        team_strength_defaults.get(
                            "home_away_split_weight",
                            TEAM_STRENGTH_DEFAULT_HOME_AWAY_SPLIT_WEIGHT,
                        )
                    ),
                    help="越高越强调主场/客场拆分表现。",
                )
            )
            h2h_window_matches = int(
                control_col21.number_input(
                    "交手参考场数",
                    min_value=1,
                    max_value=10,
                    step=1,
                    value=int(
                        team_strength_defaults.get(
                            "h2h_window_matches",
                            TEAM_STRENGTH_DEFAULT_H2H_WINDOW_MATCHES,
                        )
                    ),
                    help="交手只做弱修正，不建议设太大。",
                )
            )

            control_col22, control_col23, control_col24 = st.columns(3)
            h2h_max_adjustment = float(
                control_col22.slider(
                    "交手修正上限",
                    min_value=0.0,
                    max_value=0.15,
                    step=0.01,
                    value=float(
                        team_strength_defaults.get(
                            "h2h_max_adjustment",
                            TEAM_STRENGTH_DEFAULT_H2H_MAX_ADJUSTMENT,
                        )
                    ),
                    help="限制交手记录最多能把模型往某一边推多少。",
                )
            )
            goal_cap_options = [4, 5, 6, 7, 8]
            goal_cap = int(
                control_col23.selectbox(
                    "Poisson 进球截断",
                    options=goal_cap_options,
                    index=goal_cap_options.index(
                        int(team_strength_defaults.get("goal_cap", TEAM_STRENGTH_DEFAULT_GOAL_CAP))
                    ),
                    help="比分矩阵会算到这个档位，最后一档吸收尾部概率。",
                )
            )
            control_col24.caption(
                "当前模型会综合：攻防强度、主客场、近期状态、弱交手修正。"
            )

            st.caption(
                f"下面三个阈值按下注结果分别生效，当前分数类型是 {score_label}。"
            )
            control_col25, control_col26, control_col27 = st.columns(3)
            min_edge_home_win = float(
                control_col25.number_input(
                    f"主胜最小{score_label}",
                    min_value=0.0,
                    step=0.005,
                    value=threshold_defaults["home_win"],
                    format="%.3f",
                    help=_format_threshold_meaning(value_mode, threshold_defaults["home_win"]),
                )
            )
            min_edge_draw = float(
                control_col26.number_input(
                    f"平局最小{score_label}",
                    min_value=0.0,
                    step=0.005,
                    value=threshold_defaults["draw"],
                    format="%.3f",
                    help=(
                        _format_threshold_meaning(value_mode, threshold_defaults["draw"])
                        + "；平局通常更不稳定，默认阈值更高。"
                    ),
                )
            )
            min_edge_away_win = float(
                control_col27.number_input(
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
                f"默认值现在是：回看窗口 {DEFAULT_LOOKBACK_DAYS} 天、"
                f"{_format_value_mode_label(value_mode)}；"
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
                        form_window_matches=form_window_matches,
                        decay_half_life_days=decay_half_life_days,
                        bayes_prior_strength=bayes_prior_strength,
                        home_away_split_weight=home_away_split_weight,
                        h2h_window_matches=h2h_window_matches,
                        h2h_max_adjustment=h2h_max_adjustment,
                        goal_cap=goal_cap,
                        data_source_kind=selected_source_kind,
                        data_source_label=selected_source_label,
                        db_path=selected_db_path,
                        training_data_source_kind=selected_training_source_kind,
                        training_data_source_label=selected_training_source_label_value,
                        training_db_path=selected_training_db_path,
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
                        form_window_matches=form_window_matches,
                        decay_half_life_days=decay_half_life_days,
                        bayes_prior_strength=bayes_prior_strength,
                        home_away_split_weight=home_away_split_weight,
                        h2h_window_matches=h2h_window_matches,
                        h2h_max_adjustment=h2h_max_adjustment,
                        goal_cap=goal_cap,
                    )
                    engine = BacktestEngine(
                        SQLiteBacktestDataSource(
                            db_path=selected_db_path,
                            source_kind=selected_source_kind,
                        ),
                        SQLiteBacktestDataSource(
                            db_path=selected_training_db_path,
                            source_kind=selected_training_source_kind,
                        ),
                    )
                    result = engine.run(
                        config=config,
                        strategy=strategy,
                        progress_callback=on_backtest_progress,
                    )
                st.session_state["backtest_last_result"] = result
                st.session_state["backtest_last_error"] = None
            except Exception as exc:
                st.session_state["backtest_last_error"] = (
                    f"回测失败：{_format_exception_for_ui(exc)}"
                )

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
            f"候选池：{result.diagnostics.get('data_source_label') or '期次主库'} | "
            f"策略：最低赔率串关 | 串关类型：{parlay_size_label}串1"
        )
    elif _is_dixon_coles_strategy(result.strategy_name):
        competitions = result.diagnostics.get("competitions") or []
        daily_limit_value = result.diagnostics.get("max_bets_per_day")
        daily_limit_label = (
            "不限制" if daily_limit_value is None else f"{daily_limit_value} 场"
        )
        lookback_label = _format_lookback_label(result.diagnostics.get("lookback_days"))
        value_mode = str(result.diagnostics.get("value_mode") or DEFAULT_VALUE_MODE)
        value_mode_label = _format_value_mode_label(value_mode)
        score_label = _resolve_value_mode_score_label(value_mode)
        staking_mode_label = _format_staking_mode_label(
            str(result.diagnostics.get("staking_mode") or "fixed")
        )
        st.caption(
            f"最近一次回测：{result.start_date} -> {result.end_date} | "
            f"候选池：{result.diagnostics.get('data_source_label') or '期次主库'} | "
            f"训练集：{result.diagnostics.get('training_data_source_label') or '球队大库'} | "
            f"策略：Dixon-Coles 价值投注 | "
            f"联赛：{'全部' if not competitions else ', '.join(competitions)} | "
            f"每天最多下注：{daily_limit_label} | "
            f"回看窗口：{lookback_label} | "
            f"value：{value_mode_label} | "
            f"投注模式：{staking_mode_label}"
        )
        st.caption(
            f"{score_label} 阈值：主胜 "
            f"{float(result.diagnostics.get('min_edge_home_win') or 0.0):.3f} / 平局 "
            f"{float(result.diagnostics.get('min_edge_draw') or 0.0):.3f} / 客胜 "
            f"{float(result.diagnostics.get('min_edge_away_win') or 0.0):.3f}"
        )
        st.caption(
            "Dixon-Coles 参数：半衰期 "
            f"{int(result.diagnostics.get('decay_half_life_days') or 0)} 天，"
            f"贝叶斯收缩 {float(result.diagnostics.get('bayes_prior_strength') or 0.0):.1f}，"
            f"进球截断 {int(result.diagnostics.get('goal_cap') or 0)}。"
        )
    elif _is_team_strength_strategy(result.strategy_name):
        competitions = result.diagnostics.get("competitions") or []
        daily_limit_value = result.diagnostics.get("max_bets_per_day")
        daily_limit_label = (
            "不限制" if daily_limit_value is None else f"{daily_limit_value} 场"
        )
        lookback_label = _format_lookback_label(result.diagnostics.get("lookback_days"))
        value_mode = str(result.diagnostics.get("value_mode") or DEFAULT_VALUE_MODE)
        value_mode_label = _format_value_mode_label(value_mode)
        score_label = _resolve_value_mode_score_label(value_mode)
        staking_mode_label = _format_staking_mode_label(
            str(result.diagnostics.get("staking_mode") or "fixed")
        )
        st.caption(
            f"最近一次回测：{result.start_date} -> {result.end_date} | "
            f"候选池：{result.diagnostics.get('data_source_label') or '期次主库'} | "
            f"训练集：{result.diagnostics.get('training_data_source_label') or '球队大库'} | "
            f"策略：球队强度 Poisson 价值投注 | "
            f"联赛：{'全部' if not competitions else ', '.join(competitions)} | "
            f"每天最多下注：{daily_limit_label} | "
            f"回看窗口：{lookback_label} | "
            f"value：{value_mode_label} | "
            f"投注模式：{staking_mode_label}"
        )
        st.caption(
            f"{score_label} 阈值：主胜 "
            f"{float(result.diagnostics.get('min_edge_home_win') or 0.0):.3f} / 平局 "
            f"{float(result.diagnostics.get('min_edge_draw') or 0.0):.3f} / 客胜 "
            f"{float(result.diagnostics.get('min_edge_away_win') or 0.0):.3f}"
        )
        st.caption(
            "球队强度参数：近期窗口 "
            f"{int(result.diagnostics.get('form_window_matches') or 0)} 场，"
            f"半衰期 {int(result.diagnostics.get('decay_half_life_days') or 0)} 天，"
            f"贝叶斯收缩 {float(result.diagnostics.get('bayes_prior_strength') or 0.0):.1f}，"
            f"主客场权重 {float(result.diagnostics.get('home_away_split_weight') or 0.0):.2f}，"
            f"交手窗口 {int(result.diagnostics.get('h2h_window_matches') or 0)} 场。"
        )
        if str(result.diagnostics.get("staking_mode") or "fixed") == "fractional_kelly":
            st.caption(
                f"资金：{float(result.diagnostics.get('initial_bankroll') or 0.0):.2f} -> "
                f"{float(result.diagnostics.get('ending_bankroll') or 0.0):.2f} | "
                f"Kelly 折扣：{float(result.diagnostics.get('kelly_fraction') or 0.0):.2f} | "
                f"单场最大仓位：{float(result.diagnostics.get('max_stake_pct') or 0.0):.1%}"
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
            f"候选池：{result.diagnostics.get('data_source_label') or '期次主库'} | "
            f"训练集：{result.diagnostics.get('training_data_source_label') or '球队大库'} | "
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
            f"候选池：{result.diagnostics.get('data_source_label') or '期次主库'} | "
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

    render_page_banner(
        title="数据库维护",
        subtitle="在不覆盖原始数据的前提下，维护球队标准名映射、候选别名和人工确认记录，让后续筛选、统计和建模更稳定。",
        emoji="🛠️",
        chips=["可回滚", "可审计", "别名映射", "即时回填"],
    )
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
        editable_alias_rows = list_team_name_aliases(
            connection,
            limit=max(alias_limit, 500),
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

    with st.expander("修改或删除已确认映射", expanded=False):
        if "team_alias_editor_keyword" not in st.session_state:
            st.session_state["team_alias_editor_keyword"] = ""

        with st.form("team_alias_editor_search_form"):
            alias_search_keyword = st.text_input(
                "查找别名或标准名",
                value=str(st.session_state.get("team_alias_editor_keyword") or ""),
                help="输入关键词后点应用筛选；不会再因为回车即时重算整个列表。",
            )
            search_action_col1, search_action_col2 = st.columns(2)
            apply_alias_search = search_action_col1.form_submit_button("应用筛选")
            clear_alias_search = search_action_col2.form_submit_button("清空筛选")

        if apply_alias_search:
            st.session_state["team_alias_editor_keyword"] = alias_search_keyword
        if clear_alias_search:
            st.session_state["team_alias_editor_keyword"] = ""

        normalized_alias_search_keyword = clean_team_name(
            st.session_state.get("team_alias_editor_keyword")
        )
        filtered_editable_alias_rows = [
            row
            for row in editable_alias_rows
            if not normalized_alias_search_keyword
            or normalized_alias_search_keyword in clean_team_name(row.get("alias_name"))
            or normalized_alias_search_keyword in clean_team_name(row.get("canonical_name"))
        ]

        if not filtered_editable_alias_rows:
            st.info("当前筛选条件下没有可编辑的映射。")
        else:
            st.caption(
                f"当前可编辑映射 {len(filtered_editable_alias_rows)} 条；"
                f"关键词：{normalized_alias_search_keyword or '全部'}。"
            )
            alias_options: dict[str, dict] = {}
            for row in filtered_editable_alias_rows:
                option_label = (
                    f"{row['alias_name']} -> {row['canonical_name']} "
                    f"[{row['source']}]"
                )
                alias_options[option_label] = row

            preview_df = build_team_name_alias_dataframe(filtered_editable_alias_rows[:12]).rename(
                columns={
                    "alias_name": "别名",
                    "canonical_name": "标准名",
                    "source": "来源",
                    "confidence": "置信度",
                    "updated_at": "更新时间",
                }
            )
            if not preview_df.empty:
                st.dataframe(preview_df, use_container_width=True, hide_index=True)

            selected_alias_option = st.selectbox(
                "选择一条映射",
                options=list(alias_options.keys()),
            )
            selected_alias_row = alias_options[selected_alias_option]
            st.caption(
                f"当前来源：{selected_alias_row['source']}；最近更新时间：{selected_alias_row['updated_at']}"
            )
            if str(selected_alias_row.get("source") or "") == "seed":
                st.caption("内置种子映射删除时会转为停用状态，之后不会被种子规则自动写回。")

            with st.form(
                f"edit_team_alias_form_{selected_alias_row['alias_name']}_{selected_alias_row['source']}"
            ):
                edited_alias_name = st.text_input(
                    "别名",
                    value=str(selected_alias_row.get("alias_name") or ""),
                )
                edited_canonical_name = st.text_input(
                    "标准名",
                    value=str(selected_alias_row.get("canonical_name") or ""),
                )
                edit_action_col1, edit_action_col2 = st.columns(2)
                update_alias_submitted = edit_action_col1.form_submit_button(
                    "保存修改",
                    type="primary",
                )
                delete_alias_submitted = edit_action_col2.form_submit_button("删除映射")

            if update_alias_submitted:
                try:
                    with get_sfc500_connection(SFC500_DATABASE_PATH) as connection:
                        with connection:
                            summary = update_team_name_alias(
                                connection,
                                TEAM_NAME_TABLE_SPEC,
                                original_alias_name=str(selected_alias_row["alias_name"]),
                                alias_name=edited_alias_name,
                                canonical_name=edited_canonical_name,
                            )
                    st.session_state["team_name_maintenance_feedback"] = {
                        "type": "success",
                        "message": (
                            f"已更新映射 {summary['original_alias_name']} -> "
                            f"{summary['alias_name']} / {summary['canonical_name']}，"
                            f"回填 {summary['rows_updated']} 条记录。"
                        ),
                    }
                except Exception as exc:
                    st.session_state["team_name_maintenance_feedback"] = {
                        "type": "warning",
                        "message": f"修改映射失败：{exc}",
                    }
                st.rerun()

            if delete_alias_submitted:
                try:
                    with get_sfc500_connection(SFC500_DATABASE_PATH) as connection:
                        with connection:
                            summary = disable_team_name_alias(
                                connection,
                                TEAM_NAME_TABLE_SPEC,
                                alias_name=str(selected_alias_row["alias_name"]),
                            )
                    st.session_state["team_name_maintenance_feedback"] = {
                        "type": "success",
                        "message": (
                            f"已删除映射 {summary['alias_name']}，"
                            f"回填 {summary['rows_updated']} 条记录。"
                        ),
                    }
                except Exception as exc:
                    st.session_state["team_name_maintenance_feedback"] = {
                        "type": "warning",
                        "message": f"删除映射失败：{exc}",
                    }
                st.rerun()

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

    st.set_page_config(page_title=APP_TITLE, page_icon="🃏", layout="wide")
    render_global_styles()
    ensure_sfc500_db_available()
    team_history_db_available = is_sfc500_team_history_db_available()
    st.session_state["team_history_db_available"] = team_history_db_available

    if "sfc500_recent_sync_summary" not in st.session_state:
        st.session_state["sfc500_recent_sync_summary"] = None
    if "sfc500_team_live_sync_summary" not in st.session_state:
        st.session_state["sfc500_team_live_sync_summary"] = None
    if "backtest_last_result" not in st.session_state:
        st.session_state["backtest_last_result"] = None
    if "backtest_last_error" not in st.session_state:
        st.session_state["backtest_last_error"] = None

    st.markdown(
        f"""
        <section class="fv-app-shell">
          <div class="fv-app-brand">
            <h1 class="fv-app-title">{APP_TITLE}</h1>
            <p class="fv-app-subtitle">足球赔率、历史结果与策略实验的统一工作台。</p>
          </div>
        </section>
        """,
        unsafe_allow_html=True,
    )
    st.caption(f"当前主数据源：{SOURCE_SITE_URL}")
    if APP_READ_ONLY:
        st.warning("当前为只读演示模式：仅开放历史数据查看和回测，已禁用同步与数据库维护。")
    if not team_history_db_available:
        st.info("当前部署未包含完整比赛大库：历史数据、回测和今日推荐会自动回退到小库可用模式。")
    st.sidebar.markdown("### 页面导航")
    page_options = READ_ONLY_APP_PAGES if APP_READ_ONLY else APP_PAGES
    selected_page = st.sidebar.radio(
        "页面",
        options=page_options,
        index=0,
        label_visibility="collapsed",
    )

    if selected_page == "历史数据":
        render_history_page()
    elif selected_page == "今日推荐":
        render_today_recommendations_page()
    elif selected_page == "回测实验室":
        render_backtest_page()
    else:
        render_database_maintenance_page()
