"""500.com 历史赔率通用回测框架。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import field
from datetime import date
from datetime import datetime
from datetime import timedelta
import csv
import json
import math
from pathlib import Path
import sqlite3
import time
from typing import Any
from typing import Callable
from typing import Literal
from urllib.parse import quote

from jczq_assistant.config import APP_READ_ONLY, DATA_DIR


Selection = Literal["home_win", "draw", "away_win"]
BacktestProgressCallback = Callable[[dict[str, Any]], None]
DEFAULT_BACKTEST_DATABASE_PATH = DATA_DIR / "sfc500_history.sqlite3"
WEIGHTING_MODES = {"equal", "inverse_distance"}
VALUE_MODES = {"probability_diff", "expected_value"}
STAKING_MODES = {"fixed", "fractional_kelly"}
DEFAULT_VALUE_MODE = "expected_value"
DEFAULT_LOOKBACK_DAYS = 1095
DEFAULT_SELECTION_THRESHOLDS_BY_VALUE_MODE: dict[str, dict[Selection, float]] = {
    "probability_diff": {
        "home_win": 0.02,
        "draw": 0.03,
        "away_win": 0.02,
    },
    "expected_value": {
        "home_win": 0.03,
        "draw": 0.05,
        "away_win": 0.03,
    },
}

SELECTION_LABELS: dict[Selection, str] = {
    "home_win": "主胜",
    "draw": "平",
    "away_win": "客胜",
}
SELECTION_TO_ODDS_FIELD: dict[Selection, str] = {
    "home_win": "avg_win_odds",
    "draw": "avg_draw_odds",
    "away_win": "avg_lose_odds",
}
RESULT_CODE_TO_SELECTION: dict[str, Selection] = {
    "3": "home_win",
    "1": "draw",
    "0": "away_win",
}


def get_default_selection_thresholds(value_mode: str) -> dict[Selection, float]:
    """返回给定 value 模式下的默认结果阈值。"""

    defaults = DEFAULT_SELECTION_THRESHOLDS_BY_VALUE_MODE.get(value_mode)
    if defaults is None:
        raise ValueError(f"未识别的 value_mode: {value_mode}")
    return dict(defaults)


@dataclass(frozen=True)
class BacktestConfig:
    """一次回测执行需要的核心参数。"""

    start_date: date
    end_date: date
    fixed_stake: float
    competitions: list[str] = field(default_factory=list)
    max_bets_per_day: int | None = None
    parlay_size: int | None = None
    history_match_count: int = 100
    min_history_matches: int = 20
    min_edge: float = 0.02
    lookback_days: int | None = DEFAULT_LOOKBACK_DAYS
    weighting_mode: str = "inverse_distance"
    value_mode: str = DEFAULT_VALUE_MODE
    min_edge_home_win: float | None = 0.03
    min_edge_draw: float | None = 0.05
    min_edge_away_win: float | None = 0.03
    staking_mode: str = "fixed"
    initial_bankroll: float = 1000.0
    kelly_fraction: float = 0.25
    max_stake_pct: float = 0.02
    same_competition_only: bool = False
    db_path: Path = DEFAULT_BACKTEST_DATABASE_PATH


@dataclass(frozen=True)
class BacktestMatch:
    """回测引擎使用的历史比赛快照。"""

    match_id: int
    expect: str
    match_no: int
    match_time: datetime
    competition: str
    home_team: str
    away_team: str
    avg_win_odds: float | None
    avg_draw_odds: float | None
    avg_lose_odds: float | None
    spf_result: str
    spf_result_code: str
    is_settled: bool

    @property
    def match_date(self) -> date:
        return self.match_time.date()

    @property
    def result_selection(self) -> Selection | None:
        return RESULT_CODE_TO_SELECTION.get(self.spf_result_code)

    def get_odds(self, selection: Selection) -> float | None:
        return getattr(self, SELECTION_TO_ODDS_FIELD[selection])

    def has_complete_odds(self) -> bool:
        return all(
            self.get_odds(selection) is not None
            for selection in ("home_win", "draw", "away_win")
        )

    def bookmaker_probability(self, selection: Selection) -> float | None:
        probabilities = self.bookmaker_probabilities()
        if probabilities is None:
            return None
        return probabilities[selection]

    def bookmaker_probabilities(self) -> dict[Selection, float] | None:
        if not self.has_complete_odds():
            return None

        implied_values: dict[Selection, float] = {}
        for selection in ("home_win", "draw", "away_win"):
            odds = self.get_odds(selection)
            if odds is None or odds <= 0:
                return None
            implied_values[selection] = 1.0 / float(odds)

        total = sum(implied_values.values())
        if total <= 0:
            return None

        return {
            selection: implied_values[selection] / total
            for selection in ("home_win", "draw", "away_win")
        }


@dataclass(frozen=True)
class BetDecision:
    """策略对单场比赛的下注决策。"""

    match_id: int
    selection: Selection
    stake: float
    reason: str = ""
    model_probability: float | None = None
    bookmaker_probability: float | None = None
    edge: float | None = None
    sample_size: int | None = None


@dataclass(frozen=True)
class TicketLegDecision:
    """串关票中的一条腿。"""

    match_id: int
    selection: Selection


@dataclass(frozen=True)
class TicketDecision:
    """策略返回的一张票。"""

    legs: list[TicketLegDecision]
    stake: float
    ticket_type: str
    reason: str = ""


@dataclass(frozen=True)
class SkipDecision:
    """策略显式跳过某场比赛。"""

    match_id: int
    reason: str


@dataclass
class StrategyBatchResult:
    """一次策略调用的输出。"""

    bets: list[BetDecision] = field(default_factory=list)
    tickets: list[TicketDecision] = field(default_factory=list)
    skips: list[SkipDecision] = field(default_factory=list)


@dataclass(frozen=True)
class StrategyContext:
    """提供给策略的只读上下文。"""

    strategy_name: str
    current_date: date
    start_date: date
    end_date: date
    fixed_stake: float
    max_bets_per_day: int | None
    parlay_size: int | None
    history_match_count: int
    min_history_matches: int
    min_edge: float
    lookback_days: int | None
    weighting_mode: str
    value_mode: str
    min_edge_home_win: float | None
    min_edge_draw: float | None
    min_edge_away_win: float | None
    staking_mode: str
    current_bankroll: float
    initial_bankroll: float
    kelly_fraction: float
    max_stake_pct: float
    same_competition_only: bool
    historical_matches: tuple[BacktestMatch, ...] = ()


class BacktestStrategy(ABC):
    """可插拔策略接口。"""

    name: str

    @abstractmethod
    def generate_bets(
        self,
        matches: list[BacktestMatch],
        context: StrategyContext,
    ) -> StrategyBatchResult:
        """基于当前批次可下注比赛返回下注决策。"""


@dataclass(frozen=True)
class SettledBetRecord:
    """一笔已执行并完成结算的下注记录。"""

    strategy_name: str
    match_id: int
    expect: str
    match_no: int
    match_time: str
    competition: str
    home_team: str
    away_team: str
    selection: Selection
    selection_label: str
    result_selection: Selection
    result_label: str
    odds: float
    stake: float
    payout: float
    pnl: float
    won: bool
    reason: str
    model_probability: float | None = None
    bookmaker_probability: float | None = None
    edge: float | None = None
    sample_size: int | None = None


@dataclass(frozen=True)
class SettledTicketRecord:
    """一张已执行并完成结算的票据。"""

    strategy_name: str
    ticket_no: int
    trade_date: str
    settled_time: str
    ticket_type: str
    legs_count: int
    competitions: str
    matches_summary: str
    selections_summary: str
    combined_odds: float
    stake: float
    payout: float
    pnl: float
    won: bool
    reason: str
    legs: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class SkippedMatchRecord:
    """一场被跳过的比赛以及原因。"""

    strategy_name: str
    match_id: int
    expect: str
    match_no: int
    match_time: str
    competition: str
    home_team: str
    away_team: str
    reason: str


@dataclass(frozen=True)
class DailyBacktestSummary:
    """按天聚合的回测结果。"""

    trade_date: str
    matches_considered: int
    bets_placed: int
    skipped_matches: int
    total_stake: float
    total_return: float
    pnl: float
    cumulative_pnl: float
    drawdown: float


@dataclass(frozen=True)
class CompetitionBacktestSummary:
    """按联赛聚合的回测结果。"""

    competition: str
    bets_placed: int
    total_stake: float
    total_return: float
    pnl: float
    roi: float
    win_rate: float
    average_odds: float


@dataclass
class BacktestResult:
    """回测主结果对象。"""

    strategy_name: str
    start_date: str
    end_date: str
    total_matches_considered: int
    total_bets_placed: int
    total_stake: float
    total_return: float
    pnl: float
    roi: float
    win_rate: float
    average_odds: float
    average_daily_pnl: float
    sharpe_ratio: float
    max_drawdown: float
    longest_losing_streak: int
    bets: list[SettledBetRecord]
    tickets: list[SettledTicketRecord]
    skipped_matches: list[SkippedMatchRecord]
    daily_results: list[DailyBacktestSummary]
    competition_summaries: list[CompetitionBacktestSummary]
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def to_summary_dict(self) -> dict[str, Any]:
        """返回适合 CLI 打印的摘要。"""

        return {
            "strategy_name": self.strategy_name,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "total_matches_considered": self.total_matches_considered,
            "total_bets_placed": self.total_bets_placed,
            "total_stake": round(self.total_stake, 4),
            "total_return": round(self.total_return, 4),
            "pnl": round(self.pnl, 4),
            "roi": round(self.roi, 6),
            "win_rate": round(self.win_rate, 6),
            "average_odds": round(self.average_odds, 6),
            "average_daily_pnl": round(self.average_daily_pnl, 6),
            "sharpe_ratio": round(self.sharpe_ratio, 6),
            "max_drawdown": round(self.max_drawdown, 4),
            "longest_losing_streak": self.longest_losing_streak,
            "diagnostics": self.diagnostics,
        }


class SQLiteBacktestDataSource:
    """集中负责从 SQLite 读取回测所需比赛数据。"""

    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = db_path or DEFAULT_BACKTEST_DATABASE_PATH

    def load_matches(
        self,
        *,
        start_date: date,
        end_date: date,
        competitions: list[str] | None = None,
    ) -> list[BacktestMatch]:
        """读取时间区间内的比赛，按开赛时间升序返回。"""

        next_day = end_date + timedelta(days=1)
        where_clauses = [
            "match_time >= ?",
            "match_time < ?",
        ]
        params: list[Any] = [
            f"{start_date.isoformat()} 00:00:00",
            f"{next_day.isoformat()} 00:00:00",
        ]

        if competitions:
            placeholders = ", ".join("?" for _ in competitions)
            where_clauses.append(f"competition IN ({placeholders})")
            params.extend(competitions)

        where_sql = " AND ".join(where_clauses)
        query_sql = f"""
        SELECT
            id AS match_id,
            expect,
            match_no,
            match_time,
            COALESCE(competition, '') AS competition,
            COALESCE(home_team_canonical, home_team, '') AS home_team,
            COALESCE(away_team_canonical, away_team, '') AS away_team,
            avg_win_odds,
            avg_draw_odds,
            avg_lose_odds,
            COALESCE(spf_result, '') AS spf_result,
            COALESCE(spf_result_code, '') AS spf_result_code,
            COALESCE(is_settled, 0) AS is_settled
        FROM sfc500_matches_raw
        WHERE {where_sql}
        ORDER BY match_time ASC, expect ASC, match_no ASC, id ASC
        """

        matches: list[BacktestMatch] = []
        with _get_backtest_connection(self.db_path) as connection:
            rows = connection.execute(query_sql, params).fetchall()

        for row in rows:
            raw_match_time = str(row["match_time"] or "").strip()
            if not raw_match_time:
                continue

            matches.append(
                BacktestMatch(
                    match_id=int(row["match_id"]),
                    expect=str(row["expect"]),
                    match_no=int(row["match_no"]),
                    match_time=datetime.fromisoformat(raw_match_time),
                    competition=str(row["competition"] or ""),
                    home_team=str(row["home_team"] or ""),
                    away_team=str(row["away_team"] or ""),
                    avg_win_odds=_to_optional_float(row["avg_win_odds"]),
                    avg_draw_odds=_to_optional_float(row["avg_draw_odds"]),
                    avg_lose_odds=_to_optional_float(row["avg_lose_odds"]),
                    spf_result=str(row["spf_result"] or ""),
                    spf_result_code=str(row["spf_result_code"] or ""),
                    is_settled=bool(int(row["is_settled"] or 0)),
                )
            )

        return matches

    def load_matches_before(
        self,
        *,
        before_date: date,
        competitions: list[str] | None = None,
    ) -> list[BacktestMatch]:
        """读取指定日期之前的全部比赛。"""

        last_date = before_date - timedelta(days=1)
        if last_date < date(2000, 1, 1):
            return []

        return self.load_matches(
            start_date=date(2000, 1, 1),
            end_date=last_date,
            competitions=competitions,
        )


class LowestOddsFixedStrategy(BacktestStrategy):
    """Baseline: 每场固定 stake，买三项里赔率最低的一项。"""

    name = "lowest_odds_fixed"

    def __init__(
        self,
        fixed_stake: float,
        *,
        max_bets_per_day: int | None = None,
    ) -> None:
        if fixed_stake <= 0:
            raise ValueError("fixed_stake 必须大于 0。")
        if max_bets_per_day is not None and max_bets_per_day < 0:
            raise ValueError("max_bets_per_day 不能小于 0。")
        self.fixed_stake = float(fixed_stake)
        self.max_bets_per_day = max_bets_per_day

    def generate_bets(
        self,
        matches: list[BacktestMatch],
        context: StrategyContext,
    ) -> StrategyBatchResult:
        result = StrategyBatchResult()
        candidate_bets: list[tuple[BacktestMatch, BetDecision, float]] = []

        for match in matches:
            if not match.is_settled:
                result.skips.append(
                    SkipDecision(match_id=match.match_id, reason="match_not_settled")
                )
                continue

            lowest_choice = _get_lowest_odds_selection(match)
            if lowest_choice is None:
                result.skips.append(
                    SkipDecision(match_id=match.match_id, reason="missing_odds")
                )
                continue

            selected_selection, selected_odds = lowest_choice
            candidate_bets.append(
                (
                    match,
                    BetDecision(
                        match_id=match.match_id,
                        selection=selected_selection,
                        stake=self.fixed_stake,
                        reason=f"lowest_odds={selected_odds}",
                    ),
                    float(selected_odds or 0.0),
                )
            )

        if self.max_bets_per_day is not None and len(candidate_bets) > self.max_bets_per_day:
            sorted_candidates = sorted(
                candidate_bets,
                key=lambda item: (item[2], item[0].match_time, item[0].match_id),
            )
            selected_match_ids = {
                item[0].match_id for item in sorted_candidates[: self.max_bets_per_day]
            }

            for match, decision, _ in candidate_bets:
                if match.match_id in selected_match_ids:
                    result.bets.append(decision)
                else:
                    result.skips.append(
                        SkipDecision(match_id=match.match_id, reason="outside_daily_limit")
                    )
            return result

        for _, decision, _ in candidate_bets:
            result.bets.append(
                BetDecision(
                    match_id=decision.match_id,
                    selection=decision.selection,
                    stake=decision.stake,
                    reason=decision.reason,
                    model_probability=decision.model_probability,
                    bookmaker_probability=decision.bookmaker_probability,
                    edge=decision.edge,
                    sample_size=decision.sample_size,
                )
            )

        return result


class HistoricalOddsMatchingValueStrategy(BacktestStrategy):
    """基于历史相似庄家概率做经验分布匹配，只在正 edge 时下注。"""

    name = "historical_odds_value"

    def __init__(
        self,
        fixed_stake: float,
        *,
        history_match_count: int = 100,
        min_history_matches: int = 20,
        min_edge: float = 0.02,
        lookback_days: int | None = DEFAULT_LOOKBACK_DAYS,
        weighting_mode: str = "inverse_distance",
        value_mode: str = DEFAULT_VALUE_MODE,
        min_edge_home_win: float | None = None,
        min_edge_draw: float | None = None,
        min_edge_away_win: float | None = None,
        staking_mode: str = "fixed",
        initial_bankroll: float = 1000.0,
        kelly_fraction: float = 0.25,
        max_stake_pct: float = 0.02,
        same_competition_only: bool = False,
    ) -> None:
        if fixed_stake <= 0:
            raise ValueError("fixed_stake 必须大于 0。")
        if history_match_count <= 0:
            raise ValueError("history_match_count 必须大于 0。")
        if min_history_matches <= 0:
            raise ValueError("min_history_matches 必须大于 0。")
        if min_history_matches > history_match_count:
            raise ValueError("min_history_matches 不能大于 history_match_count。")
        if min_edge < 0:
            raise ValueError("min_edge 不能小于 0。")
        if lookback_days is not None and lookback_days <= 0:
            raise ValueError("lookback_days 必须大于 0。")
        if weighting_mode not in WEIGHTING_MODES:
            raise ValueError(f"未识别的 weighting_mode: {weighting_mode}")
        if value_mode not in VALUE_MODES:
            raise ValueError(f"未识别的 value_mode: {value_mode}")
        if staking_mode not in STAKING_MODES:
            raise ValueError(f"未识别的 staking_mode: {staking_mode}")
        if initial_bankroll <= 0:
            raise ValueError("initial_bankroll 必须大于 0。")
        if kelly_fraction <= 0:
            raise ValueError("kelly_fraction 必须大于 0。")
        if max_stake_pct <= 0:
            raise ValueError("max_stake_pct 必须大于 0。")
        default_thresholds = get_default_selection_thresholds(value_mode)
        if min_edge_home_win is None:
            min_edge_home_win = default_thresholds["home_win"]
        if min_edge_draw is None:
            min_edge_draw = default_thresholds["draw"]
        if min_edge_away_win is None:
            min_edge_away_win = default_thresholds["away_win"]
        for label, threshold in (
            ("min_edge_home_win", min_edge_home_win),
            ("min_edge_draw", min_edge_draw),
            ("min_edge_away_win", min_edge_away_win),
        ):
            if threshold is not None and threshold < 0:
                raise ValueError(f"{label} 不能小于 0。")
        self.fixed_stake = float(fixed_stake)
        self.history_match_count = int(history_match_count)
        self.min_history_matches = int(min_history_matches)
        self.min_edge = float(min_edge)
        self.lookback_days = lookback_days
        self.weighting_mode = weighting_mode
        self.value_mode = value_mode
        self.min_edge_home_win = min_edge_home_win
        self.min_edge_draw = min_edge_draw
        self.min_edge_away_win = min_edge_away_win
        self.staking_mode = staking_mode
        self.initial_bankroll = float(initial_bankroll)
        self.kelly_fraction = float(kelly_fraction)
        self.max_stake_pct = float(max_stake_pct)
        self.same_competition_only = bool(same_competition_only)

    def generate_bets(
        self,
        matches: list[BacktestMatch],
        context: StrategyContext,
    ) -> StrategyBatchResult:
        result = StrategyBatchResult()
        history_pool = [
            match
            for match in context.historical_matches
            if match.is_settled and match.result_selection is not None and match.has_complete_odds()
        ]
        if self.lookback_days is not None:
            history_start_date = context.current_date - timedelta(days=self.lookback_days)
            history_pool = [
                match
                for match in history_pool
                if match.match_date >= history_start_date
            ]
        candidate_bets: list[tuple[BacktestMatch, BetDecision, float]] = []

        for match in matches:
            if not match.is_settled:
                result.skips.append(
                    SkipDecision(match_id=match.match_id, reason="match_not_settled")
                )
                continue

            bookmaker_probabilities = match.bookmaker_probabilities()
            if bookmaker_probabilities is None:
                result.skips.append(
                    SkipDecision(match_id=match.match_id, reason="missing_odds")
                )
                continue
            if (
                (self.staking_mode == "fractional_kelly" or context.staking_mode == "fractional_kelly")
                and context.current_bankroll <= 0
            ):
                result.skips.append(
                    SkipDecision(match_id=match.match_id, reason="bankroll_depleted")
                )
                continue

            eligible_history = history_pool
            if self.same_competition_only or context.same_competition_only:
                eligible_history = [
                    history_match
                    for history_match in history_pool
                    if history_match.competition == match.competition
                ]

            if len(eligible_history) < max(self.min_history_matches, context.min_history_matches):
                result.skips.append(
                    SkipDecision(
                        match_id=match.match_id,
                        reason="insufficient_history_matches",
                    )
                )
                continue

            nearest_history = _select_nearest_probability_matches(
                target_match=match,
                history_matches=eligible_history,
                limit=min(self.history_match_count, context.history_match_count),
            )
            if len(nearest_history) < max(self.min_history_matches, context.min_history_matches):
                result.skips.append(
                    SkipDecision(
                        match_id=match.match_id,
                        reason="insufficient_history_matches",
                    )
                )
                continue

            empirical_probabilities = _build_empirical_result_probabilities(
                nearest_history,
                weighting_mode=self.weighting_mode,
            )
            selection_values = {
                selection: _calculate_value_score(
                    value_mode=self.value_mode,
                    model_probability=empirical_probabilities[selection],
                    bookmaker_probability=bookmaker_probabilities[selection],
                    odds=float(match.get_odds(selection) or 0.0),
                )
                for selection in ("home_win", "draw", "away_win")
            }
            best_selection, best_edge = max(
                selection_values.items(),
                key=lambda item: (
                    item[1],
                    empirical_probabilities[item[0]],
                    -bookmaker_probabilities[item[0]],
                    -("home_win", "draw", "away_win").index(item[0]),
                ),
            )

            edge_threshold = _resolve_selection_threshold(
                selection=best_selection,
                base_threshold=max(self.min_edge, context.min_edge),
                home_win_threshold=self.min_edge_home_win,
                draw_threshold=self.min_edge_draw,
                away_win_threshold=self.min_edge_away_win,
                context_home_win_threshold=context.min_edge_home_win,
                context_draw_threshold=context.min_edge_draw,
                context_away_win_threshold=context.min_edge_away_win,
            )
            if best_edge <= edge_threshold:
                result.skips.append(
                    SkipDecision(match_id=match.match_id, reason="no_positive_edge")
                )
                continue

            model_probability = empirical_probabilities[best_selection]
            bookmaker_probability = bookmaker_probabilities[best_selection]
            selected_odds = float(match.get_odds(best_selection) or 0.0)
            stake = self.fixed_stake
            stake_reason = ""
            if self.staking_mode == "fractional_kelly" or context.staking_mode == "fractional_kelly":
                kelly_result = _calculate_fractional_kelly_stake(
                    bankroll=context.current_bankroll,
                    odds=selected_odds,
                    model_probability=model_probability,
                    kelly_fraction=self.kelly_fraction,
                    max_stake_pct=self.max_stake_pct,
                )
                if kelly_result is None:
                    result.skips.append(
                        SkipDecision(match_id=match.match_id, reason="non_positive_kelly")
                    )
                    continue
                stake = kelly_result["stake"]
                stake_reason = (
                    f" stake_mode=fractional_kelly bankroll={context.current_bankroll:.2f}"
                    f" raw_kelly={kelly_result['raw_fraction']:.4f}"
                    f" applied_kelly={kelly_result['applied_fraction']:.4f}"
                )
            candidate_bets.append(
                (
                    match,
                    BetDecision(
                        match_id=match.match_id,
                        selection=best_selection,
                        stake=stake,
                        reason=(
                            f"history_match_count={len(nearest_history)} "
                            f"model_prob={model_probability:.4f} "
                            f"book_prob={bookmaker_probability:.4f} "
                            f"value={best_edge:.4f} "
                            f"value_mode={self.value_mode} "
                            f"weighting_mode={self.weighting_mode}"
                            f"{stake_reason}"
                        ),
                        model_probability=model_probability,
                        bookmaker_probability=bookmaker_probability,
                        edge=best_edge,
                        sample_size=len(nearest_history),
                    ),
                    best_edge,
                )
            )

        daily_limit = context.max_bets_per_day
        if daily_limit is not None and len(candidate_bets) > daily_limit:
            sorted_candidates = sorted(
                candidate_bets,
                key=lambda item: (-float(item[2]), item[0].match_time, item[0].match_id),
            )
            selected_match_ids = {item[0].match_id for item in sorted_candidates[:daily_limit]}

            for match, decision, _ in candidate_bets:
                if match.match_id in selected_match_ids:
                    result.bets.append(decision)
                else:
                    result.skips.append(
                        SkipDecision(match_id=match.match_id, reason="outside_daily_limit")
                    )
            return result

        for _, decision, _ in candidate_bets:
            result.bets.append(decision)

        return result


class LowestOddsParlayStrategy(BacktestStrategy):
    """每天选最低赔率的前 N 场，组成一张 n串1。"""

    name = "lowest_odds_parlay"

    def __init__(self, fixed_stake: float, *, parlay_size: int) -> None:
        if fixed_stake <= 0:
            raise ValueError("fixed_stake 必须大于 0。")
        if parlay_size < 2:
            raise ValueError("parlay_size 必须大于等于 2。")
        self.fixed_stake = float(fixed_stake)
        self.parlay_size = int(parlay_size)

    def generate_bets(
        self,
        matches: list[BacktestMatch],
        context: StrategyContext,
    ) -> StrategyBatchResult:
        result = StrategyBatchResult()
        candidate_legs: list[tuple[BacktestMatch, TicketLegDecision, float]] = []

        for match in matches:
            if not match.is_settled:
                result.skips.append(
                    SkipDecision(match_id=match.match_id, reason="match_not_settled")
                )
                continue

            lowest_choice = _get_lowest_odds_selection(match)
            if lowest_choice is None:
                result.skips.append(
                    SkipDecision(match_id=match.match_id, reason="missing_odds")
                )
                continue

            selected_selection, selected_odds = lowest_choice
            candidate_legs.append(
                (
                    match,
                    TicketLegDecision(
                        match_id=match.match_id,
                        selection=selected_selection,
                    ),
                    selected_odds,
                )
            )

        if len(candidate_legs) < self.parlay_size:
            for match, _, _ in candidate_legs:
                result.skips.append(
                    SkipDecision(
                        match_id=match.match_id,
                        reason="insufficient_parlay_candidates",
                    )
                )
            return result

        sorted_candidates = sorted(
            candidate_legs,
            key=lambda item: (item[2], item[0].match_time, item[0].match_id),
        )
        selected_candidates = sorted_candidates[: self.parlay_size]
        selected_match_ids = {item[0].match_id for item in selected_candidates}
        legs = [item[1] for item in selected_candidates]
        result.tickets.append(
            TicketDecision(
                legs=legs,
                stake=self.fixed_stake,
                ticket_type=f"{self.parlay_size}串1",
                reason=f"lowest_odds_parlay_size={self.parlay_size}",
            )
        )

        for match, _, _ in candidate_legs:
            if match.match_id in selected_match_ids:
                continue
            result.skips.append(
                SkipDecision(match_id=match.match_id, reason="outside_parlay_selection")
            )

        return result


class BacktestEngine:
    """按时间顺序执行策略并完成结算。"""

    def __init__(self, data_source: SQLiteBacktestDataSource) -> None:
        self.data_source = data_source

    def run(
        self,
        *,
        config: BacktestConfig,
        strategy: BacktestStrategy,
        progress_callback: BacktestProgressCallback | None = None,
    ) -> BacktestResult:
        started_at = time.monotonic()
        matches = self.data_source.load_matches(
            start_date=config.start_date,
            end_date=config.end_date,
            competitions=config.competitions,
        )
        historical_matches = self.data_source.load_matches_before(
            before_date=config.start_date,
            competitions=None,
        )
        matches_by_date = _group_matches_by_date(matches)
        running_history = list(historical_matches)
        total_days = (config.end_date - config.start_date).days + 1
        total_matches = len(matches)

        all_bets: list[SettledBetRecord] = []
        all_tickets: list[SettledTicketRecord] = []
        skipped_matches: list[SkippedMatchRecord] = []
        current_bankroll = float(config.initial_bankroll)

        _emit_backtest_progress(
            progress_callback,
            stage="start",
            progress=0.0,
            current_date=config.start_date.isoformat(),
            days_completed=0,
            total_days=total_days,
            processed_matches=0,
            total_matches=total_matches,
            bets_placed=0,
            skipped_matches=0,
            elapsed_seconds=0.0,
            eta_seconds=None,
        )

        current_date = config.start_date
        processed_days = 0
        processed_matches = 0
        while current_date <= config.end_date:
            day_matches = matches_by_date.get(current_date, [])
            context = StrategyContext(
                strategy_name=strategy.name,
                current_date=current_date,
                start_date=config.start_date,
                end_date=config.end_date,
                fixed_stake=config.fixed_stake,
                max_bets_per_day=config.max_bets_per_day,
                parlay_size=config.parlay_size,
                history_match_count=config.history_match_count,
                min_history_matches=config.min_history_matches,
                min_edge=config.min_edge,
                lookback_days=config.lookback_days,
                weighting_mode=config.weighting_mode,
                value_mode=config.value_mode,
                min_edge_home_win=config.min_edge_home_win,
                min_edge_draw=config.min_edge_draw,
                min_edge_away_win=config.min_edge_away_win,
                staking_mode=config.staking_mode,
                current_bankroll=max(current_bankroll, 0.0),
                initial_bankroll=config.initial_bankroll,
                kelly_fraction=config.kelly_fraction,
                max_stake_pct=config.max_stake_pct,
                same_competition_only=config.same_competition_only,
                historical_matches=tuple(running_history),
            )
            batch_result = strategy.generate_bets(day_matches, context)
            day_bets, day_tickets, day_skips = self._settle_day(
                strategy=strategy,
                matches=day_matches,
                batch_result=batch_result,
            )
            all_bets.extend(day_bets)
            all_tickets.extend(day_tickets)
            skipped_matches.extend(day_skips)
            current_bankroll += sum(bet.pnl for bet in day_bets) + sum(ticket.pnl for ticket in day_tickets)
            running_history.extend(day_matches)
            processed_days += 1
            processed_matches += len(day_matches)
            elapsed_seconds = time.monotonic() - started_at
            average_seconds_per_day = (
                elapsed_seconds / processed_days if processed_days > 0 else 0.0
            )
            remaining_days = max(total_days - processed_days, 0)
            eta_seconds = average_seconds_per_day * remaining_days
            _emit_backtest_progress(
                progress_callback,
                stage="day_complete",
                progress=(processed_days / total_days) if total_days else 1.0,
                current_date=current_date.isoformat(),
                days_completed=processed_days,
                total_days=total_days,
                processed_matches=processed_matches,
                total_matches=total_matches,
                bets_placed=len(all_bets) + len(all_tickets),
                skipped_matches=len(skipped_matches),
                elapsed_seconds=elapsed_seconds,
                eta_seconds=eta_seconds,
            )
            current_date += timedelta(days=1)

        daily_results = _build_daily_results(
            start_date=config.start_date,
            end_date=config.end_date,
            matches=matches,
            bets=all_bets,
            tickets=all_tickets,
            skipped_matches=skipped_matches,
        )
        competition_summaries = _build_competition_summaries(all_bets)
        result = _build_backtest_result(
            config=config,
            strategy_name=strategy.name,
            matches=matches,
            bets=all_bets,
            tickets=all_tickets,
            skipped_matches=skipped_matches,
            daily_results=daily_results,
            competition_summaries=competition_summaries,
        )
        _emit_backtest_progress(
            progress_callback,
            stage="finish",
            progress=1.0,
            current_date=config.end_date.isoformat(),
            days_completed=total_days,
            total_days=total_days,
            processed_matches=total_matches,
            total_matches=total_matches,
            bets_placed=result.total_bets_placed,
            skipped_matches=len(skipped_matches),
            elapsed_seconds=time.monotonic() - started_at,
            eta_seconds=0.0,
        )
        return result

    def _settle_day(
        self,
        *,
        strategy: BacktestStrategy,
        matches: list[BacktestMatch],
        batch_result: StrategyBatchResult,
    ) -> tuple[list[SettledBetRecord], list[SettledTicketRecord], list[SkippedMatchRecord]]:
        """执行并结算某一天的策略输出。"""

        match_by_id = {match.match_id: match for match in matches}
        bet_by_match_id: dict[int, BetDecision] = {}
        skip_by_match_id: dict[int, SkipDecision] = {}
        covered_by_tickets_match_ids: set[int] = set()

        for decision in batch_result.bets:
            if decision.match_id not in match_by_id:
                raise ValueError(f"策略返回了未知 match_id: {decision.match_id}")
            if decision.match_id in bet_by_match_id:
                raise ValueError(f"策略对同一场比赛返回了重复下注: {decision.match_id}")
            bet_by_match_id[decision.match_id] = decision

        for decision in batch_result.skips:
            if decision.match_id not in match_by_id:
                raise ValueError(f"策略返回了未知 skip match_id: {decision.match_id}")
            if decision.match_id in skip_by_match_id:
                raise ValueError(f"策略对同一场比赛返回了重复 skip: {decision.match_id}")
            skip_by_match_id[decision.match_id] = decision

        settled_bets: list[SettledBetRecord] = []
        settled_tickets: list[SettledTicketRecord] = []
        skipped_matches: list[SkippedMatchRecord] = []

        for ticket in batch_result.tickets:
            seen_match_ids: set[int] = set()
            for leg in ticket.legs:
                if leg.match_id not in match_by_id:
                    raise ValueError(f"策略返回了未知 ticket leg match_id: {leg.match_id}")
                if leg.match_id in seen_match_ids:
                    raise ValueError(f"策略在同一张票内重复使用了比赛: {leg.match_id}")
                if leg.match_id in bet_by_match_id:
                    raise ValueError(f"同一场比赛不能既单关下注又进入串关: {leg.match_id}")
                if leg.match_id in skip_by_match_id:
                    raise ValueError(f"同一场比赛不能既跳过又进入串关: {leg.match_id}")
                seen_match_ids.add(leg.match_id)
                covered_by_tickets_match_ids.add(leg.match_id)

        for match in matches:
            if match.match_id in skip_by_match_id:
                skipped_matches.append(
                    _build_skipped_match_record(
                        strategy_name=strategy.name,
                        match=match,
                        reason=skip_by_match_id[match.match_id].reason,
                    )
                )
                continue

            if match.match_id in covered_by_tickets_match_ids:
                continue

            decision = bet_by_match_id.get(match.match_id)
            if decision is None:
                skipped_matches.append(
                    _build_skipped_match_record(
                        strategy_name=strategy.name,
                        match=match,
                        reason="strategy_no_bet",
                    )
                )
                continue

            validation_error = _validate_bet_decision(match, decision)
            if validation_error:
                skipped_matches.append(
                    _build_skipped_match_record(
                        strategy_name=strategy.name,
                        match=match,
                        reason=validation_error,
                    )
                )
                continue

            odds = float(match.get_odds(decision.selection) or 0.0)
            result_selection = match.result_selection
            if result_selection is None:
                skipped_matches.append(
                    _build_skipped_match_record(
                        strategy_name=strategy.name,
                        match=match,
                        reason="missing_result_code",
                    )
                )
                continue

            won = result_selection == decision.selection
            payout = decision.stake * odds if won else 0.0
            pnl = payout - decision.stake
            settled_bets.append(
                SettledBetRecord(
                    strategy_name=strategy.name,
                    match_id=match.match_id,
                    expect=match.expect,
                    match_no=match.match_no,
                    match_time=match.match_time.isoformat(sep=" "),
                    competition=match.competition,
                    home_team=match.home_team,
                    away_team=match.away_team,
                    selection=decision.selection,
                    selection_label=SELECTION_LABELS[decision.selection],
                    result_selection=result_selection,
                    result_label=SELECTION_LABELS[result_selection],
                    odds=odds,
                    stake=decision.stake,
                    payout=payout,
                    pnl=pnl,
                    won=won,
                    reason=decision.reason,
                    model_probability=decision.model_probability,
                    bookmaker_probability=decision.bookmaker_probability,
                    edge=decision.edge,
                    sample_size=decision.sample_size,
                )
            )

        for ticket_no, ticket in enumerate(batch_result.tickets, start=1):
            validation_error = _validate_ticket_decision(match_by_id, ticket)
            if validation_error:
                for leg in ticket.legs:
                    skipped_matches.append(
                        _build_skipped_match_record(
                            strategy_name=strategy.name,
                            match=match_by_id[leg.match_id],
                            reason=validation_error,
                        )
                    )
                continue

            leg_rows: list[dict[str, Any]] = []
            competitions: list[str] = []
            matches_summary_parts: list[str] = []
            selections_summary_parts: list[str] = []
            combined_odds = 1.0
            won = True
            settled_time = ""

            for leg in ticket.legs:
                match = match_by_id[leg.match_id]
                odds = float(match.get_odds(leg.selection) or 0.0)
                result_selection = match.result_selection
                leg_won = result_selection == leg.selection
                won = won and leg_won
                combined_odds *= odds
                settled_time = max(settled_time, match.match_time.isoformat(sep=" "))

                if match.competition and match.competition not in competitions:
                    competitions.append(match.competition)
                matches_summary_parts.append(f"{match.home_team} vs {match.away_team}")
                selections_summary_parts.append(
                    f"{SELECTION_LABELS[leg.selection]}@{odds:.2f}"
                )
                leg_rows.append(
                    {
                        "match_id": match.match_id,
                        "expect": match.expect,
                        "match_no": match.match_no,
                        "match_time": match.match_time.isoformat(sep=" "),
                        "competition": match.competition,
                        "home_team": match.home_team,
                        "away_team": match.away_team,
                        "selection": leg.selection,
                        "selection_label": SELECTION_LABELS[leg.selection],
                        "result_selection": result_selection,
                        "result_label": SELECTION_LABELS[result_selection] if result_selection else "",
                        "odds": odds,
                        "won": leg_won,
                    }
                )

            payout = ticket.stake * combined_odds if won else 0.0
            pnl = payout - ticket.stake
            settled_tickets.append(
                SettledTicketRecord(
                    strategy_name=strategy.name,
                    ticket_no=ticket_no,
                    trade_date=matches[0].match_date.isoformat() if matches else "",
                    settled_time=settled_time,
                    ticket_type=ticket.ticket_type,
                    legs_count=len(ticket.legs),
                    competitions=" / ".join(competitions),
                    matches_summary=" | ".join(matches_summary_parts),
                    selections_summary=" | ".join(selections_summary_parts),
                    combined_odds=combined_odds,
                    stake=ticket.stake,
                    payout=payout,
                    pnl=pnl,
                    won=won,
                    reason=ticket.reason,
                    legs=leg_rows,
                )
            )

        return settled_bets, settled_tickets, skipped_matches


def build_strategy(
    strategy_name: str,
    *,
    fixed_stake: float,
    max_bets_per_day: int | None = None,
    parlay_size: int | None = None,
    history_match_count: int = 100,
    min_history_matches: int = 20,
    min_edge: float = 0.02,
    lookback_days: int | None = DEFAULT_LOOKBACK_DAYS,
    weighting_mode: str = "inverse_distance",
    value_mode: str = DEFAULT_VALUE_MODE,
    min_edge_home_win: float | None = None,
    min_edge_draw: float | None = None,
    min_edge_away_win: float | None = None,
    staking_mode: str = "fixed",
    initial_bankroll: float = 1000.0,
    kelly_fraction: float = 0.25,
    max_stake_pct: float = 0.02,
    same_competition_only: bool = False,
) -> BacktestStrategy:
    """根据名称构造策略实例。"""

    if strategy_name == LowestOddsFixedStrategy.name:
        return LowestOddsFixedStrategy(
            fixed_stake=fixed_stake,
            max_bets_per_day=max_bets_per_day,
        )
    if strategy_name == HistoricalOddsMatchingValueStrategy.name:
        return HistoricalOddsMatchingValueStrategy(
            fixed_stake=fixed_stake,
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
    if strategy_name == LowestOddsParlayStrategy.name:
        if parlay_size is None:
            raise ValueError("lowest_odds_parlay 需要提供 parlay_size。")
        return LowestOddsParlayStrategy(
            fixed_stake=fixed_stake,
            parlay_size=parlay_size,
        )
    raise ValueError(f"未识别的 strategy: {strategy_name}")


def export_backtest_result(
    result: BacktestResult,
    *,
    output_dir: Path,
    save_csv: bool = True,
    save_json: bool = True,
) -> dict[str, str]:
    """把回测结果导出到指定目录。"""

    output_dir.mkdir(parents=True, exist_ok=True)
    exported_files: dict[str, str] = {}

    if save_json:
        summary_path = output_dir / "summary.json"
        summary_path.write_text(
            json.dumps(result.to_summary_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        exported_files["summary_json"] = str(summary_path)

    if save_csv:
        exported_files["bets_csv"] = str(
            _write_csv(output_dir / "bets.csv", [asdict(row) for row in result.bets])
        )
        exported_files["tickets_csv"] = str(
            _write_csv(output_dir / "tickets.csv", [asdict(row) for row in result.tickets])
        )
        exported_files["skipped_matches_csv"] = str(
            _write_csv(
                output_dir / "skipped_matches.csv",
                [asdict(row) for row in result.skipped_matches],
            )
        )
        exported_files["daily_results_csv"] = str(
            _write_csv(
                output_dir / "daily_results.csv",
                [asdict(row) for row in result.daily_results],
            )
        )
        exported_files["competition_summary_csv"] = str(
            _write_csv(
                output_dir / "competition_summary.csv",
                [asdict(row) for row in result.competition_summaries],
            )
        )

    return exported_files


def _to_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _emit_backtest_progress(
    callback: BacktestProgressCallback | None,
    **event: Any,
) -> None:
    if callback is None:
        return
    callback(event)


def _get_lowest_odds_selection(
    match: BacktestMatch,
) -> tuple[Selection, float] | None:
    odds_candidates = [
        (selection, match.get_odds(selection))
        for selection in ("home_win", "draw", "away_win")
    ]
    if any(odds is None for _, odds in odds_candidates):
        return None

    selected_selection, selected_odds = min(
        odds_candidates,
        key=lambda item: (
            float(item[1] or 0.0),
            ("home_win", "draw", "away_win").index(item[0]),
        ),
    )
    return selected_selection, float(selected_odds or 0.0)


def _select_nearest_probability_matches(
    *,
    target_match: BacktestMatch,
    history_matches: list[BacktestMatch],
    limit: int,
) -> list[tuple[BacktestMatch, float]]:
    target_probabilities = target_match.bookmaker_probabilities()
    if target_probabilities is None:
        return []

    scored_matches: list[tuple[float, BacktestMatch]] = []
    for history_match in history_matches:
        history_probabilities = history_match.bookmaker_probabilities()
        if history_probabilities is None:
            continue

        distance = sum(
            abs(target_probabilities[selection] - history_probabilities[selection])
            for selection in ("home_win", "draw", "away_win")
        )
        scored_matches.append((distance, history_match))

    scored_matches.sort(
        key=lambda item: (
            float(item[0]),
            item[1].match_time,
            item[1].match_id,
        )
    )
    return [(item[1], float(item[0])) for item in scored_matches[:limit]]


def _build_empirical_result_probabilities(
    matches: list[tuple[BacktestMatch, float]],
    *,
    weighting_mode: str,
) -> dict[Selection, float]:
    weights: dict[Selection, float] = {"home_win": 0.0, "draw": 0.0, "away_win": 0.0}

    for match, distance in matches:
        result_selection = match.result_selection
        if result_selection is None:
            continue
        weight = _resolve_match_weight(distance=distance, weighting_mode=weighting_mode)
        weights[result_selection] += weight

    total = sum(weights.values())
    if total <= 0:
        return {"home_win": 0.0, "draw": 0.0, "away_win": 0.0}

    return {
        selection: weights[selection] / total
        for selection in ("home_win", "draw", "away_win")
    }


def _resolve_match_weight(*, distance: float, weighting_mode: str) -> float:
    if weighting_mode == "equal":
        return 1.0
    if weighting_mode == "inverse_distance":
        return 1.0 / max(distance, 1e-6)
    raise ValueError(f"未识别的 weighting_mode: {weighting_mode}")


def _calculate_value_score(
    *,
    value_mode: str,
    model_probability: float,
    bookmaker_probability: float,
    odds: float,
) -> float:
    if value_mode == "probability_diff":
        return model_probability - bookmaker_probability
    if value_mode == "expected_value":
        return model_probability * odds - 1.0
    raise ValueError(f"未识别的 value_mode: {value_mode}")


def _calculate_fractional_kelly_stake(
    *,
    bankroll: float,
    odds: float,
    model_probability: float,
    kelly_fraction: float,
    max_stake_pct: float,
) -> dict[str, float] | None:
    if bankroll <= 0 or odds <= 1.0 or model_probability <= 0:
        return None

    odds_net = odds - 1.0
    raw_fraction = ((odds * model_probability) - 1.0) / odds_net
    if raw_fraction <= 0:
        return None

    applied_fraction = raw_fraction * kelly_fraction
    applied_fraction = min(applied_fraction, max_stake_pct, 1.0)
    if applied_fraction <= 0:
        return None

    stake = round(bankroll * applied_fraction, 2)
    if stake <= 0:
        return None

    return {
        "stake": stake,
        "raw_fraction": raw_fraction,
        "applied_fraction": applied_fraction,
    }


def _resolve_selection_threshold(
    *,
    selection: Selection,
    base_threshold: float,
    home_win_threshold: float | None,
    draw_threshold: float | None,
    away_win_threshold: float | None,
    context_home_win_threshold: float | None,
    context_draw_threshold: float | None,
    context_away_win_threshold: float | None,
) -> float:
    if selection == "home_win":
        return float(
            home_win_threshold
            if home_win_threshold is not None
            else (
                context_home_win_threshold
                if context_home_win_threshold is not None
                else base_threshold
            )
        )
    if selection == "draw":
        return float(
            draw_threshold
            if draw_threshold is not None
            else (
                context_draw_threshold
                if context_draw_threshold is not None
                else base_threshold
            )
        )
    return float(
        away_win_threshold
        if away_win_threshold is not None
        else (
            context_away_win_threshold
            if context_away_win_threshold is not None
            else base_threshold
        )
    )


def _get_backtest_connection(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        raise FileNotFoundError(f"回测数据库不存在: {db_path}")

    if APP_READ_ONLY:
        encoded_path = quote(str(db_path.resolve()), safe="/")
        connection = sqlite3.connect(f"file:{encoded_path}?mode=ro", uri=True)
    else:
        connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    return connection


def _group_matches_by_date(
    matches: list[BacktestMatch],
) -> dict[date, list[BacktestMatch]]:
    grouped: dict[date, list[BacktestMatch]] = defaultdict(list)
    for match in matches:
        grouped[match.match_date].append(match)
    return grouped


def _validate_bet_decision(
    match: BacktestMatch,
    decision: BetDecision,
) -> str | None:
    if decision.stake <= 0:
        return "invalid_stake"
    if decision.selection not in SELECTION_LABELS:
        return "invalid_selection"
    if not match.is_settled:
        return "match_not_settled"
    if match.get_odds(decision.selection) is None:
        return "missing_selected_odds"
    return None


def _validate_ticket_decision(
    match_by_id: dict[int, BacktestMatch],
    ticket: TicketDecision,
) -> str | None:
    if ticket.stake <= 0:
        return "invalid_stake"
    if not ticket.legs:
        return "empty_ticket"

    seen_match_ids: set[int] = set()
    for leg in ticket.legs:
        if leg.match_id in seen_match_ids:
            return "duplicate_ticket_leg"
        seen_match_ids.add(leg.match_id)

        match = match_by_id[leg.match_id]
        if leg.selection not in SELECTION_LABELS:
            return "invalid_selection"
        if not match.is_settled:
            return "match_not_settled"
        if match.get_odds(leg.selection) is None:
            return "missing_selected_odds"
        if match.result_selection is None:
            return "missing_result_code"

    return None


def _build_skipped_match_record(
    *,
    strategy_name: str,
    match: BacktestMatch,
    reason: str,
) -> SkippedMatchRecord:
    return SkippedMatchRecord(
        strategy_name=strategy_name,
        match_id=match.match_id,
        expect=match.expect,
        match_no=match.match_no,
        match_time=match.match_time.isoformat(sep=" "),
        competition=match.competition,
        home_team=match.home_team,
        away_team=match.away_team,
        reason=reason,
    )


def _build_daily_results(
    *,
    start_date: date,
    end_date: date,
    matches: list[BacktestMatch],
    bets: list[SettledBetRecord],
    tickets: list[SettledTicketRecord],
    skipped_matches: list[SkippedMatchRecord],
) -> list[DailyBacktestSummary]:
    matches_by_day: dict[str, int] = defaultdict(int)
    outcomes_by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    skipped_by_day: dict[str, int] = defaultdict(int)

    for match in matches:
        key = match.match_date.isoformat()
        matches_by_day[key] += 1

    for outcome in _collect_settled_outcomes(bets, tickets):
        key = str(outcome["settled_time"]).split(" ")[0]
        outcomes_by_day[key].append(outcome)

    for skipped_match in skipped_matches:
        key = str(skipped_match.match_time).split(" ")[0]
        skipped_by_day[key] += 1

    daily_results: list[DailyBacktestSummary] = []
    cumulative_pnl = 0.0
    running_peak = 0.0

    current_date = start_date
    while current_date <= end_date:
        key = current_date.isoformat()
        day_outcomes = outcomes_by_day.get(key, [])
        day_stake = sum(float(outcome["stake"]) for outcome in day_outcomes)
        day_return = sum(float(outcome["payout"]) for outcome in day_outcomes)
        day_pnl = day_return - day_stake
        cumulative_pnl += day_pnl
        running_peak = max(running_peak, cumulative_pnl)
        drawdown = running_peak - cumulative_pnl

        daily_results.append(
            DailyBacktestSummary(
                trade_date=key,
                matches_considered=matches_by_day.get(key, 0),
                bets_placed=len(day_outcomes),
                skipped_matches=skipped_by_day.get(key, 0),
                total_stake=day_stake,
                total_return=day_return,
                pnl=day_pnl,
                cumulative_pnl=cumulative_pnl,
                drawdown=drawdown,
            )
        )
        current_date += timedelta(days=1)

    return daily_results


def _build_competition_summaries(
    bets: list[SettledBetRecord],
) -> list[CompetitionBacktestSummary]:
    grouped: dict[str, list[SettledBetRecord]] = defaultdict(list)
    for bet in bets:
        grouped[bet.competition].append(bet)

    summaries: list[CompetitionBacktestSummary] = []
    for competition, competition_bets in sorted(grouped.items()):
        total_stake = sum(bet.stake for bet in competition_bets)
        total_return = sum(bet.payout for bet in competition_bets)
        pnl = total_return - total_stake
        win_count = sum(1 for bet in competition_bets if bet.won)
        average_odds = (
            sum(bet.odds for bet in competition_bets) / len(competition_bets)
            if competition_bets
            else 0.0
        )
        summaries.append(
            CompetitionBacktestSummary(
                competition=competition,
                bets_placed=len(competition_bets),
                total_stake=total_stake,
                total_return=total_return,
                pnl=pnl,
                roi=(pnl / total_stake) if total_stake else 0.0,
                win_rate=(win_count / len(competition_bets)) if competition_bets else 0.0,
                average_odds=average_odds,
            )
        )

    return summaries


def _build_backtest_result(
    *,
    config: BacktestConfig,
    strategy_name: str,
    matches: list[BacktestMatch],
    bets: list[SettledBetRecord],
    tickets: list[SettledTicketRecord],
    skipped_matches: list[SkippedMatchRecord],
    daily_results: list[DailyBacktestSummary],
    competition_summaries: list[CompetitionBacktestSummary],
) -> BacktestResult:
    settled_outcomes = _collect_settled_outcomes(bets, tickets)

    total_stake = sum(float(outcome["stake"]) for outcome in settled_outcomes)
    total_return = sum(float(outcome["payout"]) for outcome in settled_outcomes)
    pnl = total_return - total_stake
    win_count = sum(1 for outcome in settled_outcomes if bool(outcome["won"]))
    average_odds = (
        sum(float(outcome["odds"]) for outcome in settled_outcomes) / len(settled_outcomes)
        if settled_outcomes
        else 0.0
    )
    average_daily_pnl = pnl / len(daily_results) if daily_results else 0.0
    sharpe_ratio = _calculate_sharpe_ratio(daily_results)
    max_drawdown = _calculate_max_drawdown(settled_outcomes)

    losing_streak = 0
    longest_losing_streak = 0
    for outcome in settled_outcomes:
        if bool(outcome["won"]):
            losing_streak = 0
            continue
        losing_streak += 1
        longest_losing_streak = max(longest_losing_streak, losing_streak)

    skip_reason_breakdown: dict[str, int] = defaultdict(int)
    for skipped_match in skipped_matches:
        skip_reason_breakdown[skipped_match.reason] += 1

    diagnostics = {
        "calendar_days": (config.end_date - config.start_date).days + 1,
        "active_match_days": sum(1 for row in daily_results if row.matches_considered > 0),
        "active_bet_days": sum(1 for row in daily_results if row.total_stake > 0),
        "total_skipped_matches": len(skipped_matches),
        "competitions": config.competitions,
        "max_bets_per_day": config.max_bets_per_day,
        "parlay_size": config.parlay_size,
        "history_match_count": config.history_match_count,
        "min_history_matches": config.min_history_matches,
        "min_edge": config.min_edge,
        "lookback_days": config.lookback_days,
        "weighting_mode": config.weighting_mode,
        "value_mode": config.value_mode,
        "min_edge_home_win": config.min_edge_home_win,
        "min_edge_draw": config.min_edge_draw,
        "min_edge_away_win": config.min_edge_away_win,
        "staking_mode": config.staking_mode,
        "initial_bankroll": config.initial_bankroll,
        "ending_bankroll": config.initial_bankroll + pnl,
        "kelly_fraction": config.kelly_fraction,
        "max_stake_pct": config.max_stake_pct,
        "same_competition_only": config.same_competition_only,
        "skip_reason_breakdown": dict(sorted(skip_reason_breakdown.items())),
    }

    return BacktestResult(
        strategy_name=strategy_name,
        start_date=config.start_date.isoformat(),
        end_date=config.end_date.isoformat(),
        total_matches_considered=len(matches),
        total_bets_placed=len(settled_outcomes),
        total_stake=total_stake,
        total_return=total_return,
        pnl=pnl,
        roi=(pnl / total_stake) if total_stake else 0.0,
        win_rate=(win_count / len(settled_outcomes)) if settled_outcomes else 0.0,
        average_odds=average_odds,
        average_daily_pnl=average_daily_pnl,
        sharpe_ratio=sharpe_ratio,
        max_drawdown=max_drawdown,
        longest_losing_streak=longest_losing_streak,
        bets=bets,
        tickets=tickets,
        skipped_matches=skipped_matches,
        daily_results=daily_results,
        competition_summaries=competition_summaries,
        diagnostics=diagnostics,
    )


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> Path:
    if not rows:
        path.write_text("", encoding="utf-8")
        return path

    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _collect_settled_outcomes(
    bets: list[SettledBetRecord],
    tickets: list[SettledTicketRecord],
) -> list[dict[str, Any]]:
    outcomes: list[dict[str, Any]] = []

    for bet in bets:
        outcomes.append(
            {
                "settled_time": bet.match_time,
                "stake": bet.stake,
                "payout": bet.payout,
                "pnl": bet.pnl,
                "won": bet.won,
                "odds": bet.odds,
            }
        )

    for ticket in tickets:
        outcomes.append(
            {
                "settled_time": ticket.settled_time,
                "stake": ticket.stake,
                "payout": ticket.payout,
                "pnl": ticket.pnl,
                "won": ticket.won,
                "odds": ticket.combined_odds,
            }
        )

    outcomes.sort(key=lambda item: str(item["settled_time"]))
    return outcomes


def _calculate_max_drawdown(outcomes: list[dict[str, Any]]) -> float:
    cumulative_pnl = 0.0
    running_peak = 0.0
    max_drawdown = 0.0

    for outcome in outcomes:
        cumulative_pnl += float(outcome["pnl"])
        running_peak = max(running_peak, cumulative_pnl)
        max_drawdown = max(max_drawdown, running_peak - cumulative_pnl)

    return max_drawdown


def _calculate_sharpe_ratio(daily_results: list[DailyBacktestSummary]) -> float:
    """按有下注日的日收益率计算年化 Sharpe，假设无风险利率为 0。"""

    daily_returns = [
        (row.pnl / row.total_stake)
        for row in daily_results
        if row.total_stake > 0
    ]
    if len(daily_returns) < 2:
        return 0.0

    mean_return = sum(daily_returns) / len(daily_returns)
    variance = sum((daily_return - mean_return) ** 2 for daily_return in daily_returns) / (
        len(daily_returns) - 1
    )
    if variance <= 0:
        return 0.0

    standard_deviation = math.sqrt(variance)
    if standard_deviation <= 0:
        return 0.0

    return (mean_return / standard_deviation) * math.sqrt(365.0)
