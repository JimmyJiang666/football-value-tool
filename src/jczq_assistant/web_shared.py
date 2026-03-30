"""Web 页面共享常量和轻量工具。"""

from datetime import date
from datetime import datetime
from datetime import timedelta

import streamlit as st

from jczq_assistant.backtest import DEFAULT_LOOKBACK_DAYS
from jczq_assistant.backtest import DEFAULT_VALUE_MODE
from jczq_assistant.backtest import get_default_selection_thresholds
from jczq_assistant.sfc500_history import (
    SFC500_DATABASE_PATH,
    get_sfc500_filter_options,
    get_sfc500_history_overview,
)
from jczq_assistant.sfc500_team_history import (
    SFC500_TEAM_HISTORY_DATABASE_PATH,
    get_sfc500_team_filter_options,
    get_sfc500_team_history_overview,
)


BACKTEST_DATA_SOURCE_OPTIONS = {
    "14场主库": {
        "db_path": SFC500_DATABASE_PATH,
        "source_kind": "expect",
        "source_label": "期次主库",
        "overview_fn": get_sfc500_history_overview,
        "filter_fn": get_sfc500_filter_options,
    },
    "球队大库": {
        "db_path": SFC500_TEAM_HISTORY_DATABASE_PATH,
        "source_kind": "team",
        "source_label": "球队大库",
        "overview_fn": get_sfc500_team_history_overview,
        "filter_fn": get_sfc500_team_filter_options,
    },
}


def get_available_backtest_data_source_options(
    *,
    team_history_available: bool,
) -> dict[str, dict]:
    """返回当前环境下可用的回测数据源选项。"""

    if team_history_available:
        return dict(BACKTEST_DATA_SOURCE_OPTIONS)
    return {
        label: meta
        for label, meta in BACKTEST_DATA_SOURCE_OPTIONS.items()
        if str(meta.get("source_kind")) != "team"
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
    "模型概率优先": "model_probability",
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


def resolve_default_date_range(overview: dict) -> tuple[date, date]:
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


def resolve_date_bounds(overview: dict) -> tuple[date, date]:
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


def resolve_preset_date_range(
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


def format_daily_limit_option(option: str | int) -> str:
    """格式化每日下注场数选项。"""

    if option == "不限制":
        return "不限制"
    return f"{option} 场"


def resolve_daily_limit_value(option: str | int) -> int | None:
    """把页面选项转换成配置值。"""

    if option == "不限制":
        return None
    return int(option)


def format_lookback_option(option: str | int) -> str:
    """格式化历史回看窗口选项。"""

    if option == "全部历史":
        return "全部历史"
    return f"最近 {option} 天"


def resolve_lookback_value(option: str | int) -> int | None:
    """把历史回看窗口选项转换成配置值。"""

    if option == "全部历史":
        return None
    return int(option)


def format_lookback_label(lookback_days: int | None) -> str:
    """把历史回看窗口值格式化成页面文案。"""

    if lookback_days is None:
        return "全部历史"
    return f"最近 {lookback_days} 天"


def format_weighting_mode_label(weighting_mode: str) -> str:
    """把样本加权模式转换成页面文案。"""

    return BACKTEST_WEIGHTING_MODE_LABELS.get(weighting_mode, weighting_mode)


def format_value_mode_label(value_mode: str) -> str:
    """把 value 计算方式转换成页面文案。"""

    return BACKTEST_VALUE_MODE_LABELS.get(value_mode, value_mode)


def format_staking_mode_label(staking_mode: str) -> str:
    """把投注模式转换成页面文案。"""

    return BACKTEST_STAKING_MODE_LABELS.get(staking_mode, staking_mode)


def resolve_value_mode_score_label(value_mode: str) -> str:
    """返回当前 value 模式下的分数名称。"""

    if value_mode == "expected_value":
        return "EV"
    if value_mode == "model_probability":
        return "模型概率"
    return "概率差"


def resolve_value_mode_score_column_label(value_mode: str) -> str:
    """返回表格里使用的分数字段列名，避免和固定指标重名。"""

    if value_mode == "model_probability":
        return "下注分数"
    return resolve_value_mode_score_label(value_mode)


def resolve_value_mode_threshold_defaults(
    value_mode: str,
    *,
    strategy_name: str | None = None,
) -> dict[str, float]:
    """返回当前 value 模式下的默认阈值。"""

    defaults = get_default_selection_thresholds(value_mode, strategy_name=strategy_name)
    return {
        "home_win": float(defaults["home_win"]),
        "draw": float(defaults["draw"]),
        "away_win": float(defaults["away_win"]),
    }


def format_threshold_meaning(value_mode: str, threshold: float) -> str:
    """格式化阈值数值对应的含义。"""

    if value_mode == "expected_value":
        return f"{threshold:.3f} 表示期望收益率至少 {threshold:.1%}"
    if value_mode == "model_probability":
        return f"{threshold:.3f} 表示该结果模型概率至少达到 {threshold:.1%}"
    return f"{threshold:.3f} 表示模型概率至少高于庄家概率 {threshold:.1%}"


def is_parlay_strategy(strategy_name: str) -> bool:
    """判断是否为串关策略。"""

    return strategy_name == "lowest_odds_parlay"


def is_value_strategy(strategy_name: str) -> bool:
    """判断是否为 value 类策略。"""

    normalized_name = str(strategy_name)
    return (
        normalized_name == "historical_odds_value"
        or normalized_name.startswith("team_strength_poisson_value")
        or normalized_name.startswith("dixon_coles")
    )


def is_team_strength_strategy(strategy_name: str) -> bool:
    """判断是否为球队强度 Poisson 价值策略。"""

    normalized_name = str(strategy_name)
    return normalized_name.startswith("team_strength_poisson_value") or normalized_name.startswith(
        "dixon_coles"
    )


def format_seconds_brief(value: float | None) -> str:
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


def format_backtest_skip_reason(reason: str) -> str:
    """把回测跳过原因转成页面文案。"""

    return BACKTEST_SKIP_REASON_LABELS.get(reason, reason)


__all__ = [
    "BACKTEST_DATA_SOURCE_OPTIONS",
    "get_available_backtest_data_source_options",
    "BACKTEST_DAILY_LIMIT_OPTIONS",
    "BACKTEST_PARLAY_OPTIONS",
    "BACKTEST_HISTORY_MATCH_COUNT_OPTIONS",
    "BACKTEST_LOOKBACK_OPTIONS",
    "BACKTEST_DATE_PRESET_OPTIONS",
    "BACKTEST_WEIGHTING_MODE_OPTIONS",
    "BACKTEST_VALUE_MODE_OPTIONS",
    "BACKTEST_STAKING_MODE_OPTIONS",
    "DEFAULT_LOOKBACK_DAYS",
    "DEFAULT_VALUE_MODE",
    "resolve_default_date_range",
    "resolve_date_bounds",
    "resolve_preset_date_range",
    "format_daily_limit_option",
    "resolve_daily_limit_value",
    "format_lookback_option",
    "resolve_lookback_value",
    "format_lookback_label",
    "format_weighting_mode_label",
    "format_value_mode_label",
    "format_staking_mode_label",
    "resolve_value_mode_score_label",
    "resolve_value_mode_score_column_label",
    "resolve_value_mode_threshold_defaults",
    "format_threshold_meaning",
    "is_parlay_strategy",
    "is_value_strategy",
    "is_team_strength_strategy",
    "format_seconds_brief",
    "format_backtest_skip_reason",
]
