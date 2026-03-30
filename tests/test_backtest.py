"""回测框架基础测试。"""

from __future__ import annotations

from datetime import date
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from jczq_assistant.backtest import BacktestConfig
from jczq_assistant.backtest import BacktestEngine
from jczq_assistant.backtest import SQLiteBacktestDataSource
from jczq_assistant.backtest import _build_dixon_coles_outcome_probabilities
from jczq_assistant.backtest import _build_poisson_outcome_probabilities
from jczq_assistant.backtest import build_strategy
from jczq_assistant.backtest import build_strategy_context_from_config


class BacktestFrameworkTestCase(unittest.TestCase):
    def setUp(self) -> None:
        fd, temp_path = tempfile.mkstemp(suffix=".sqlite3")
        os.close(fd)
        Path(temp_path).unlink(missing_ok=True)
        self.db_path = Path(temp_path)
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                """
                CREATE TABLE sfc500_matches_raw (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    expect TEXT NOT NULL,
                    match_no INTEGER NOT NULL,
                    competition TEXT,
                    match_time TEXT,
                    home_team TEXT,
                    away_team TEXT,
                    home_team_canonical TEXT,
                    away_team_canonical TEXT,
                    avg_win_odds REAL,
                    avg_draw_odds REAL,
                    avg_lose_odds REAL,
                    spf_result TEXT,
                    spf_result_code TEXT,
                    final_score TEXT,
                    is_settled INTEGER NOT NULL DEFAULT 0,
                    source_url TEXT NOT NULL,
                    fetched_at TEXT NOT NULL
                )
                """
            )
            connection.executemany(
                """
                INSERT INTO sfc500_matches_raw (
                    expect,
                    match_no,
                    competition,
                    match_time,
                    home_team,
                    away_team,
                    home_team_canonical,
                    away_team_canonical,
                    avg_win_odds,
                    avg_draw_odds,
                    avg_lose_odds,
                    spf_result,
                    spf_result_code,
                    final_score,
                    is_settled,
                    source_url,
                    fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        "98097",
                        1,
                        "测试联赛",
                        "2024-12-28 12:00:00",
                        "历史主队1",
                        "历史客队1",
                        "历史主队1",
                        "历史客队1",
                        2.10,
                        3.30,
                        3.80,
                        "胜",
                        "3",
                        "2:1",
                        1,
                        "x",
                        "2024-12-28T00:00:00",
                    ),
                    (
                        "98098",
                        1,
                        "测试联赛",
                        "2024-12-29 12:00:00",
                        "历史主队2",
                        "历史客队2",
                        "历史主队2",
                        "历史客队2",
                        2.05,
                        3.35,
                        3.90,
                        "胜",
                        "3",
                        "1:0",
                        1,
                        "x",
                        "2024-12-29T00:00:00",
                    ),
                    (
                        "98099",
                        1,
                        "测试联赛",
                        "2024-12-30 12:00:00",
                        "历史主队3",
                        "历史客队3",
                        "历史主队3",
                        "历史客队3",
                        2.12,
                        3.28,
                        3.85,
                        "平",
                        "1",
                        "1:1",
                        1,
                        "x",
                        "2024-12-30T00:00:00",
                    ),
                    (
                        "98100",
                        1,
                        "测试联赛",
                        "2024-12-31 12:00:00",
                        "历史主队4",
                        "历史客队4",
                        "历史主队4",
                        "历史客队4",
                        2.08,
                        3.32,
                        3.88,
                        "胜",
                        "3",
                        "3:1",
                        1,
                        "x",
                        "2024-12-31T00:00:00",
                    ),
                    (
                        "99001",
                        1,
                        "英超",
                        "2025-01-01 12:00:00",
                        "主队A",
                        "客队A",
                        "主队A",
                        "客队A",
                        1.50,
                        3.60,
                        5.00,
                        "胜",
                        "3",
                        "2:0",
                        1,
                        "x",
                        "2025-01-01T00:00:00",
                    ),
                    (
                        "99001",
                        2,
                        "英超",
                        "2025-01-01 18:00:00",
                        "主队B",
                        "客队B",
                        "主队B",
                        "客队B",
                        2.80,
                        3.20,
                        2.10,
                        "胜",
                        "3",
                        "2:1",
                        1,
                        "x",
                        "2025-01-01T00:00:00",
                    ),
                    (
                        "99003",
                        1,
                        "测试联赛",
                        "2025-01-03 12:00:00",
                        "目标主队",
                        "目标客队",
                        "目标主队",
                        "目标客队",
                        2.08,
                        3.30,
                        3.90,
                        "胜",
                        "3",
                        "2:0",
                        1,
                        "x",
                        "2025-01-03T00:00:00",
                    ),
                    (
                        "99002",
                        1,
                        "西甲",
                        "2025-01-02 12:00:00",
                        "主队C",
                        "客队C",
                        "主队C",
                        "客队C",
                        None,
                        3.10,
                        2.20,
                        "负",
                        "0",
                        "0:1",
                        1,
                        "x",
                        "2025-01-02T00:00:00",
                    ),
                    (
                        "99002",
                        2,
                        "西甲",
                        "2025-01-02 18:00:00",
                        "主队D",
                        "客队D",
                        "主队D",
                        "客队D",
                        1.80,
                        3.30,
                        4.40,
                        "",
                        "",
                        "",
                        0,
                        "x",
                        "2025-01-02T00:00:00",
                    ),
                    (
                        "99501",
                        1,
                        "策略联赛",
                        "2024-12-18 12:00:00",
                        "强主队",
                        "队甲",
                        "强主队",
                        "队甲",
                        1.85,
                        3.40,
                        4.40,
                        "胜",
                        "3",
                        "2:0",
                        1,
                        "x",
                        "2024-12-18T00:00:00",
                    ),
                    (
                        "99502",
                        1,
                        "策略联赛",
                        "2024-12-21 12:00:00",
                        "队乙",
                        "弱客队",
                        "队乙",
                        "弱客队",
                        1.95,
                        3.20,
                        3.90,
                        "胜",
                        "3",
                        "2:0",
                        1,
                        "x",
                        "2024-12-21T00:00:00",
                    ),
                    (
                        "99503",
                        1,
                        "策略联赛",
                        "2024-12-24 12:00:00",
                        "队丙",
                        "强主队",
                        "队丙",
                        "强主队",
                        2.90,
                        3.10,
                        2.20,
                        "负",
                        "0",
                        "0:1",
                        1,
                        "x",
                        "2024-12-24T00:00:00",
                    ),
                    (
                        "99504",
                        1,
                        "策略联赛",
                        "2024-12-27 12:00:00",
                        "弱客队",
                        "队丁",
                        "弱客队",
                        "队丁",
                        2.70,
                        3.10,
                        2.35,
                        "负",
                        "0",
                        "0:1",
                        1,
                        "x",
                        "2024-12-27T00:00:00",
                    ),
                    (
                        "99505",
                        1,
                        "策略联赛",
                        "2024-12-30 12:00:00",
                        "强主队",
                        "队戊",
                        "强主队",
                        "队戊",
                        1.78,
                        3.50,
                        4.60,
                        "胜",
                        "3",
                        "3:1",
                        1,
                        "x",
                        "2024-12-30T00:00:00",
                    ),
                    (
                        "99506",
                        1,
                        "策略联赛",
                        "2025-01-04 12:00:00",
                        "队己",
                        "弱客队",
                        "队己",
                        "弱客队",
                        1.92,
                        3.20,
                        4.10,
                        "胜",
                        "3",
                        "1:0",
                        1,
                        "x",
                        "2025-01-04T00:00:00",
                    ),
                    (
                        "99507",
                        1,
                        "策略联赛",
                        "2025-01-05 12:00:00",
                        "强主队",
                        "队庚",
                        "强主队",
                        "队庚",
                        1.88,
                        3.30,
                        4.20,
                        "胜",
                        "3",
                        "2:1",
                        1,
                        "x",
                        "2025-01-05T00:00:00",
                    ),
                    (
                        "99508",
                        1,
                        "策略联赛",
                        "2025-01-07 12:00:00",
                        "队辛",
                        "弱客队",
                        "队辛",
                        "弱客队",
                        2.00,
                        3.15,
                        3.80,
                        "胜",
                        "3",
                        "2:1",
                        1,
                        "x",
                        "2025-01-07T00:00:00",
                    ),
                    (
                        "99509",
                        1,
                        "策略联赛",
                        "2025-01-10 12:00:00",
                        "强主队",
                        "弱客队",
                        "强主队",
                        "弱客队",
                        2.20,
                        3.25,
                        3.35,
                        "胜",
                        "3",
                        "2:0",
                        1,
                        "x",
                        "2025-01-10T00:00:00",
                    ),
                ],
            )
            connection.commit()

    def tearDown(self) -> None:
        self.db_path.unlink(missing_ok=True)

    def test_lowest_odds_fixed_strategy_runs_end_to_end(self) -> None:
        config = BacktestConfig(
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 2),
            fixed_stake=10.0,
            db_path=self.db_path,
        )
        engine = BacktestEngine(SQLiteBacktestDataSource(self.db_path))
        strategy = build_strategy("lowest_odds_fixed", fixed_stake=10.0)

        result = engine.run(config=config, strategy=strategy)

        self.assertEqual(result.total_matches_considered, 4)
        self.assertEqual(result.total_bets_placed, 2)
        self.assertAlmostEqual(result.total_stake, 20.0)
        self.assertAlmostEqual(result.total_return, 15.0)
        self.assertAlmostEqual(result.pnl, -5.0)
        self.assertAlmostEqual(result.roi, -0.25)
        self.assertAlmostEqual(result.win_rate, 0.5)
        self.assertAlmostEqual(result.average_odds, 1.8)
        self.assertAlmostEqual(result.sharpe_ratio, 0.0)
        self.assertEqual(result.longest_losing_streak, 1)
        self.assertAlmostEqual(result.max_drawdown, 10.0)
        self.assertEqual(len(result.daily_results), 2)
        self.assertEqual(result.daily_results[0].bets_placed, 2)
        self.assertEqual(result.daily_results[1].bets_placed, 0)

    def test_missing_odds_and_unsettled_matches_are_skipped(self) -> None:
        config = BacktestConfig(
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 2),
            fixed_stake=10.0,
            db_path=self.db_path,
        )
        engine = BacktestEngine(SQLiteBacktestDataSource(self.db_path))
        strategy = build_strategy("lowest_odds_fixed", fixed_stake=10.0)

        result = engine.run(config=config, strategy=strategy)
        reasons = {row.reason for row in result.skipped_matches}

        self.assertIn("missing_odds", reasons)
        self.assertIn("match_not_settled", reasons)
        self.assertEqual(result.diagnostics["total_skipped_matches"], 2)

    def test_max_bets_per_day_limits_daily_selection_to_lowest_odds(self) -> None:
        config = BacktestConfig(
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 2),
            fixed_stake=10.0,
            max_bets_per_day=1,
            db_path=self.db_path,
        )
        engine = BacktestEngine(SQLiteBacktestDataSource(self.db_path))
        strategy = build_strategy(
            "lowest_odds_fixed",
            fixed_stake=10.0,
            max_bets_per_day=1,
        )

        result = engine.run(config=config, strategy=strategy)
        reasons = {row.reason for row in result.skipped_matches}

        self.assertEqual(result.total_bets_placed, 1)
        self.assertAlmostEqual(result.total_stake, 10.0)
        self.assertAlmostEqual(result.total_return, 15.0)
        self.assertAlmostEqual(result.pnl, 5.0)
        self.assertEqual(result.bets[0].match_no, 1)
        self.assertIn("outside_daily_limit", reasons)
        self.assertEqual(result.diagnostics["max_bets_per_day"], 1)

    def test_lowest_odds_parlay_runs_with_one_ticket_per_day(self) -> None:
        config = BacktestConfig(
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 2),
            fixed_stake=10.0,
            parlay_size=2,
            db_path=self.db_path,
        )
        engine = BacktestEngine(SQLiteBacktestDataSource(self.db_path))
        strategy = build_strategy(
            "lowest_odds_parlay",
            fixed_stake=10.0,
            parlay_size=2,
        )

        result = engine.run(config=config, strategy=strategy)
        reasons = {row.reason for row in result.skipped_matches}

        self.assertEqual(result.total_matches_considered, 4)
        self.assertEqual(result.total_bets_placed, 1)
        self.assertEqual(len(result.tickets), 1)
        self.assertAlmostEqual(result.total_stake, 10.0)
        self.assertAlmostEqual(result.total_return, 0.0)
        self.assertAlmostEqual(result.pnl, -10.0)
        self.assertAlmostEqual(result.average_odds, 3.15)
        self.assertAlmostEqual(result.win_rate, 0.0)
        self.assertEqual(result.tickets[0].ticket_type, "2串1")
        self.assertEqual(result.daily_results[0].bets_placed, 1)
        self.assertIn("missing_odds", reasons)
        self.assertIn("match_not_settled", reasons)
        self.assertEqual(result.diagnostics["parlay_size"], 2)

    def test_historical_odds_value_strategy_uses_prior_matches_and_positive_edge(self) -> None:
        config = BacktestConfig(
            start_date=date(2025, 1, 3),
            end_date=date(2025, 1, 3),
            fixed_stake=10.0,
            competitions=["测试联赛"],
            history_match_count=3,
            min_history_matches=3,
            min_edge=0.0,
            same_competition_only=True,
            db_path=self.db_path,
        )
        engine = BacktestEngine(SQLiteBacktestDataSource(self.db_path))
        strategy = build_strategy(
            "historical_odds_value",
            fixed_stake=10.0,
            history_match_count=3,
            min_history_matches=3,
            min_edge=0.0,
            same_competition_only=True,
        )

        result = engine.run(config=config, strategy=strategy)

        self.assertEqual(result.total_matches_considered, 1)
        self.assertEqual(result.total_bets_placed, 1)
        self.assertEqual(len(result.bets), 1)
        self.assertEqual(result.bets[0].selection, "home_win")
        self.assertEqual(result.bets[0].sample_size, 3)
        self.assertIsNotNone(result.bets[0].model_probability)
        self.assertIsNotNone(result.bets[0].bookmaker_probability)
        self.assertIsNotNone(result.bets[0].edge)
        self.assertGreater(result.bets[0].edge or 0.0, 0.0)
        self.assertAlmostEqual(result.total_return, 20.8)
        self.assertAlmostEqual(result.pnl, 10.8)

    def test_historical_odds_value_strategy_respects_lookback_window(self) -> None:
        config = BacktestConfig(
            start_date=date(2025, 1, 3),
            end_date=date(2025, 1, 3),
            fixed_stake=10.0,
            competitions=["测试联赛"],
            history_match_count=3,
            min_history_matches=2,
            min_edge=0.0,
            lookback_days=2,
            same_competition_only=True,
            db_path=self.db_path,
        )
        engine = BacktestEngine(SQLiteBacktestDataSource(self.db_path))
        strategy = build_strategy(
            "historical_odds_value",
            fixed_stake=10.0,
            history_match_count=3,
            min_history_matches=2,
            min_edge=0.0,
            lookback_days=2,
            same_competition_only=True,
        )

        result = engine.run(config=config, strategy=strategy)
        reasons = {row.reason for row in result.skipped_matches}

        self.assertEqual(result.total_matches_considered, 1)
        self.assertEqual(result.total_bets_placed, 0)
        self.assertIn("insufficient_history_matches", reasons)
        self.assertEqual(result.diagnostics["lookback_days"], 2)

    def test_historical_odds_value_strategy_supports_expected_value_mode(self) -> None:
        config = BacktestConfig(
            start_date=date(2025, 1, 3),
            end_date=date(2025, 1, 3),
            fixed_stake=10.0,
            competitions=["测试联赛"],
            history_match_count=3,
            min_history_matches=3,
            min_edge=0.0,
            lookback_days=None,
            value_mode="expected_value",
            min_edge_home_win=0.5,
            min_edge_draw=0.0,
            min_edge_away_win=0.0,
            same_competition_only=True,
            db_path=self.db_path,
        )
        engine = BacktestEngine(SQLiteBacktestDataSource(self.db_path))
        strategy = build_strategy(
            "historical_odds_value",
            fixed_stake=10.0,
            history_match_count=3,
            min_history_matches=3,
            min_edge=0.0,
            lookback_days=None,
            value_mode="expected_value",
            min_edge_home_win=0.5,
            min_edge_draw=0.0,
            min_edge_away_win=0.0,
            same_competition_only=True,
        )

        result = engine.run(config=config, strategy=strategy)

        self.assertEqual(result.total_matches_considered, 1)
        self.assertEqual(result.total_bets_placed, 1)
        self.assertEqual(result.bets[0].selection, "home_win")
        self.assertGreater(result.bets[0].edge or 0.0, 0.5)
        self.assertEqual(result.diagnostics["value_mode"], "expected_value")
        self.assertEqual(result.diagnostics["min_edge_home_win"], 0.5)

    def test_historical_odds_value_strategy_supports_model_probability_mode(self) -> None:
        config = BacktestConfig(
            start_date=date(2025, 1, 3),
            end_date=date(2025, 1, 3),
            fixed_stake=10.0,
            competitions=["测试联赛"],
            history_match_count=3,
            min_history_matches=3,
            min_edge=0.0,
            lookback_days=None,
            value_mode="model_probability",
            min_edge_home_win=0.6,
            min_edge_draw=0.0,
            min_edge_away_win=0.0,
            same_competition_only=True,
            db_path=self.db_path,
        )
        engine = BacktestEngine(SQLiteBacktestDataSource(self.db_path))
        strategy = build_strategy(
            "historical_odds_value",
            fixed_stake=10.0,
            history_match_count=3,
            min_history_matches=3,
            min_edge=0.0,
            lookback_days=None,
            value_mode="model_probability",
            min_edge_home_win=0.6,
            min_edge_draw=0.0,
            min_edge_away_win=0.0,
            same_competition_only=True,
        )

        result = engine.run(config=config, strategy=strategy)

        self.assertEqual(result.total_matches_considered, 1)
        self.assertEqual(result.total_bets_placed, 1)
        self.assertEqual(result.bets[0].selection, "home_win")
        self.assertAlmostEqual(
            float(result.bets[0].edge or 0.0),
            float(result.bets[0].model_probability or 0.0),
            places=10,
        )
        self.assertGreaterEqual(result.bets[0].edge or 0.0, 0.6)
        self.assertEqual(result.diagnostics["value_mode"], "model_probability")

    def test_historical_odds_value_strategy_supports_fractional_kelly_staking(self) -> None:
        config = BacktestConfig(
            start_date=date(2025, 1, 3),
            end_date=date(2025, 1, 3),
            fixed_stake=10.0,
            competitions=["测试联赛"],
            history_match_count=3,
            min_history_matches=3,
            min_edge=0.0,
            staking_mode="fractional_kelly",
            initial_bankroll=1000.0,
            kelly_fraction=0.25,
            max_stake_pct=0.02,
            same_competition_only=True,
            db_path=self.db_path,
        )
        engine = BacktestEngine(SQLiteBacktestDataSource(self.db_path))
        strategy = build_strategy(
            "historical_odds_value",
            fixed_stake=10.0,
            history_match_count=3,
            min_history_matches=3,
            min_edge=0.0,
            staking_mode="fractional_kelly",
            initial_bankroll=1000.0,
            kelly_fraction=0.25,
            max_stake_pct=0.02,
            same_competition_only=True,
        )

        result = engine.run(config=config, strategy=strategy)

        self.assertEqual(result.total_matches_considered, 1)
        self.assertEqual(result.total_bets_placed, 1)
        self.assertAlmostEqual(result.bets[0].stake, 20.0)
        self.assertAlmostEqual(result.total_stake, 20.0)
        self.assertAlmostEqual(result.total_return, 41.6)
        self.assertAlmostEqual(result.pnl, 21.6)
        self.assertEqual(result.diagnostics["staking_mode"], "fractional_kelly")
        self.assertAlmostEqual(result.diagnostics["ending_bankroll"], 1021.6)

    def test_team_strength_poisson_value_strategy_runs_end_to_end(self) -> None:
        config = BacktestConfig(
            start_date=date(2025, 1, 10),
            end_date=date(2025, 1, 10),
            fixed_stake=10.0,
            competitions=["策略联赛"],
            min_history_matches=3,
            min_edge=0.0,
            lookback_days=365,
            value_mode="expected_value",
            min_edge_home_win=0.0,
            min_edge_draw=0.0,
            min_edge_away_win=0.0,
            same_competition_only=True,
            form_window_matches=4,
            decay_half_life_days=60,
            bayes_prior_strength=6.0,
            home_away_split_weight=0.7,
            h2h_window_matches=3,
            h2h_max_adjustment=0.04,
            goal_cap=6,
            db_path=self.db_path,
        )
        engine = BacktestEngine(SQLiteBacktestDataSource(self.db_path))
        strategy = build_strategy(
            "team_strength_poisson_value",
            fixed_stake=10.0,
            min_history_matches=3,
            min_edge=0.0,
            lookback_days=365,
            value_mode="expected_value",
            min_edge_home_win=0.0,
            min_edge_draw=0.0,
            min_edge_away_win=0.0,
            same_competition_only=True,
            form_window_matches=4,
            decay_half_life_days=60,
            bayes_prior_strength=6.0,
            home_away_split_weight=0.7,
            h2h_window_matches=3,
            h2h_max_adjustment=0.04,
            goal_cap=6,
        )

        result = engine.run(config=config, strategy=strategy)

        self.assertEqual(result.total_matches_considered, 1)
        self.assertEqual(result.total_bets_placed, 1)
        self.assertEqual(result.bets[0].selection, "home_win")
        self.assertGreater(result.bets[0].edge or 0.0, 0.0)
        self.assertGreater(result.bets[0].sample_size or 0, 0)
        self.assertIn("lambda_home", result.bets[0].reason)
        self.assertEqual(result.diagnostics["form_window_matches"], 4)
        self.assertEqual(result.diagnostics["goal_cap"], 6)

    def test_team_strength_v2_event_time_includes_same_day_earlier_matches(self) -> None:
        with sqlite3.connect(self.db_path) as connection:
            connection.executemany(
                """
                INSERT INTO sfc500_matches_raw (
                    expect, match_no, competition, match_time,
                    home_team, away_team, home_team_canonical, away_team_canonical,
                    avg_win_odds, avg_draw_odds, avg_lose_odds,
                    spf_result, spf_result_code, final_score, is_settled,
                    source_url, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        "99600",
                        1,
                        "策略联赛",
                        "2025-01-12 10:00:00",
                        "队甲",
                        "队乙",
                        "队甲",
                        "队乙",
                        2.10,
                        3.20,
                        3.40,
                        "胜",
                        "3",
                        "1:0",
                        1,
                        "x",
                        "2025-01-12T00:00:00",
                    ),
                    (
                        "99600",
                        2,
                        "策略联赛",
                        "2025-01-12 20:00:00",
                        "强主队",
                        "弱客队",
                        "强主队",
                        "弱客队",
                        2.20,
                        3.25,
                        3.35,
                        "胜",
                        "3",
                        "2:0",
                        1,
                        "x",
                        "2025-01-12T00:00:00",
                    ),
                ],
            )
            connection.commit()

        base_kwargs = dict(
            fixed_stake=10.0,
            min_history_matches=3,
            min_edge=0.0,
            lookback_days=365,
            value_mode="expected_value",
            min_edge_home_win=0.0,
            min_edge_draw=0.0,
            min_edge_away_win=0.0,
            same_competition_only=True,
            form_window_matches=4,
            decay_half_life_days=60,
            bayes_prior_strength=6.0,
            home_away_split_weight=0.7,
            h2h_window_matches=3,
            h2h_max_adjustment=0.04,
            goal_cap=6,
        )
        data_source = SQLiteBacktestDataSource(self.db_path)
        historical_matches = data_source.load_matches_before(
            before_date=date(2025, 1, 12),
            competitions=None,
        )
        day_matches = data_source.load_matches(
            start_date=date(2025, 1, 12),
            end_date=date(2025, 1, 12),
            competitions=["策略联赛"],
        )
        target_match = max(day_matches, key=lambda row: row.match_time)
        same_day_earlier_matches = [
            row for row in day_matches if row.match_time < target_match.match_time
        ]

        original_strategy = build_strategy("team_strength_poisson_value", **base_kwargs)
        v2_strategy = build_strategy("team_strength_poisson_value_v2", **base_kwargs)
        context_config = BacktestConfig(
            start_date=date(2025, 1, 12),
            end_date=date(2025, 1, 12),
            db_path=self.db_path,
            **base_kwargs,
        )
        original_batch = original_strategy.generate_bets(
            [target_match],
            context=build_strategy_context_from_config(
                context_config,
                strategy_name=original_strategy.name,
                current_date=date(2025, 1, 12),
                historical_matches=tuple(historical_matches),
            ),
        )
        v2_batch = v2_strategy.generate_bets(
            [target_match],
            context=build_strategy_context_from_config(
                context_config,
                strategy_name=v2_strategy.name,
                current_date=date(2025, 1, 12),
                historical_matches=tuple(historical_matches + same_day_earlier_matches),
            ),
        )

        original_details = original_batch.bets[0].details
        v2_details = v2_batch.bets[0].details
        self.assertEqual(original_details["history_selection_mode"], "daily")
        self.assertEqual(v2_details["history_selection_mode"], "event_time")
        self.assertLess(original_details["history_matches_used"], v2_details["history_matches_used"])

    def test_team_strength_v2_competition_fallback_handles_sparse_league(self) -> None:
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                """
                INSERT INTO sfc500_matches_raw (
                    expect, match_no, competition, match_time,
                    home_team, away_team, home_team_canonical, away_team_canonical,
                    avg_win_odds, avg_draw_odds, avg_lose_odds,
                    spf_result, spf_result_code, final_score, is_settled,
                    source_url, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "99700",
                    1,
                    "新联赛",
                    "2025-01-11 12:00:00",
                    "强主队",
                    "弱客队",
                    "强主队",
                    "弱客队",
                    2.15,
                    3.25,
                    3.50,
                    "胜",
                    "3",
                    "2:0",
                    1,
                    "x",
                    "2025-01-11T00:00:00",
                ),
            )
            connection.commit()

        config = BacktestConfig(
            start_date=date(2025, 1, 11),
            end_date=date(2025, 1, 11),
            fixed_stake=10.0,
            competitions=["新联赛"],
            min_history_matches=3,
            min_edge=0.0,
            lookback_days=365,
            value_mode="expected_value",
            min_edge_home_win=0.0,
            min_edge_draw=0.0,
            min_edge_away_win=0.0,
            same_competition_only=True,
            db_path=self.db_path,
        )
        engine = BacktestEngine(SQLiteBacktestDataSource(self.db_path))
        result = engine.run(
            config=config,
            strategy=build_strategy(
                "team_strength_poisson_value_v2",
                fixed_stake=10.0,
                min_history_matches=3,
                min_edge=0.0,
                lookback_days=365,
                value_mode="expected_value",
                min_edge_home_win=0.0,
                min_edge_draw=0.0,
                min_edge_away_win=0.0,
                same_competition_only=True,
            ),
        )

        self.assertEqual(result.total_bets_placed, 1)
        details = json.loads(result.bets[0].details_json)
        self.assertTrue(details["fallback_applied"])
        self.assertEqual(details["history_pool_scope"], "fallback_global")

    def test_team_strength_recent_form_rows_use_pre_kickoff_global_recent_matches(self) -> None:
        with sqlite3.connect(self.db_path) as connection:
            connection.executemany(
                """
                INSERT INTO sfc500_matches_raw (
                    expect, match_no, competition, match_time,
                    home_team, away_team, home_team_canonical, away_team_canonical,
                    avg_win_odds, avg_draw_odds, avg_lose_odds,
                    spf_result, spf_result_code, final_score, is_settled,
                    source_url, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        "99801",
                        1,
                        "远期联赛",
                        "2024-02-01 12:00:00",
                        "表单主队",
                        "联赛对手甲",
                        "表单主队",
                        "联赛对手甲",
                        1.90,
                        3.20,
                        4.20,
                        "胜",
                        "3",
                        "2:0",
                        1,
                        "x",
                        "2024-02-01T00:00:00",
                    ),
                    (
                        "99802",
                        1,
                        "远期联赛",
                        "2024-03-01 12:00:00",
                        "联赛对手乙",
                        "表单客队",
                        "联赛对手乙",
                        "表单客队",
                        2.00,
                        3.10,
                        3.90,
                        "胜",
                        "3",
                        "1:0",
                        1,
                        "x",
                        "2024-03-01T00:00:00",
                    ),
                    (
                        "99803",
                        1,
                        "远期联赛",
                        "2024-04-01 12:00:00",
                        "表单主队",
                        "联赛对手丙",
                        "表单主队",
                        "联赛对手丙",
                        1.88,
                        3.30,
                        4.30,
                        "胜",
                        "3",
                        "3:1",
                        1,
                        "x",
                        "2024-04-01T00:00:00",
                    ),
                    (
                        "99804",
                        1,
                        "远期联赛",
                        "2024-05-01 12:00:00",
                        "联赛对手丁",
                        "表单客队",
                        "联赛对手丁",
                        "表单客队",
                        1.95,
                        3.20,
                        4.00,
                        "胜",
                        "3",
                        "2:0",
                        1,
                        "x",
                        "2024-05-01T00:00:00",
                    ),
                    (
                        "99805",
                        1,
                        "远期联赛",
                        "2024-06-01 12:00:00",
                        "表单主队",
                        "联赛对手戊",
                        "表单主队",
                        "联赛对手戊",
                        1.86,
                        3.35,
                        4.40,
                        "胜",
                        "3",
                        "2:1",
                        1,
                        "x",
                        "2024-06-01T00:00:00",
                    ),
                    (
                        "99806",
                        1,
                        "远期联赛",
                        "2024-07-01 12:00:00",
                        "联赛对手己",
                        "表单客队",
                        "联赛对手己",
                        "表单客队",
                        2.02,
                        3.15,
                        3.85,
                        "胜",
                        "3",
                        "1:0",
                        1,
                        "x",
                        "2024-07-01T00:00:00",
                    ),
                    (
                        "99807",
                        1,
                        "杯赛",
                        "2025-12-20 12:00:00",
                        "表单主队",
                        "杯赛对手甲",
                        "表单主队",
                        "杯赛对手甲",
                        1.92,
                        3.25,
                        4.10,
                        "胜",
                        "3",
                        "2:0",
                        1,
                        "x",
                        "2025-12-20T00:00:00",
                    ),
                    (
                        "99808",
                        1,
                        "杯赛",
                        "2025-12-22 12:00:00",
                        "杯赛对手乙",
                        "表单客队",
                        "杯赛对手乙",
                        "表单客队",
                        1.98,
                        3.20,
                        3.95,
                        "胜",
                        "3",
                        "1:0",
                        1,
                        "x",
                        "2025-12-22T00:00:00",
                    ),
                    (
                        "99809",
                        1,
                        "杯赛",
                        "2025-12-24 12:00:00",
                        "表单主队",
                        "杯赛对手丙",
                        "表单主队",
                        "杯赛对手丙",
                        1.89,
                        3.30,
                        4.25,
                        "平",
                        "1",
                        "1:1",
                        1,
                        "x",
                        "2025-12-24T00:00:00",
                    ),
                    (
                        "99810",
                        1,
                        "杯赛",
                        "2025-12-26 12:00:00",
                        "杯赛对手丁",
                        "表单客队",
                        "杯赛对手丁",
                        "表单客队",
                        2.05,
                        3.10,
                        3.80,
                        "平",
                        "1",
                        "1:1",
                        1,
                        "x",
                        "2025-12-26T00:00:00",
                    ),
                    (
                        "99811",
                        1,
                        "杯赛",
                        "2025-12-28 12:00:00",
                        "表单主队",
                        "杯赛对手戊",
                        "表单主队",
                        "杯赛对手戊",
                        1.87,
                        3.35,
                        4.30,
                        "胜",
                        "3",
                        "3:1",
                        1,
                        "x",
                        "2025-12-28T00:00:00",
                    ),
                    (
                        "99812",
                        1,
                        "杯赛",
                        "2025-12-30 12:00:00",
                        "杯赛对手己",
                        "表单客队",
                        "杯赛对手己",
                        "表单客队",
                        2.00,
                        3.12,
                        3.88,
                        "负",
                        "0",
                        "0:1",
                        1,
                        "x",
                        "2025-12-30T00:00:00",
                    ),
                    (
                        "99813",
                        1,
                        "远期联赛",
                        "2026-01-10 12:00:00",
                        "表单主队",
                        "表单客队",
                        "表单主队",
                        "表单客队",
                        2.20,
                        3.20,
                        3.40,
                        "胜",
                        "3",
                        "2:1",
                        1,
                        "x",
                        "2026-01-10T00:00:00",
                    ),
                ],
            )
            connection.commit()

        base_config = dict(
            start_date=date(2026, 1, 10),
            end_date=date(2026, 1, 10),
            fixed_stake=10.0,
            competitions=["远期联赛"],
            min_history_matches=3,
            min_edge=0.0,
            value_mode="expected_value",
            min_edge_home_win=0.0,
            min_edge_draw=0.0,
            min_edge_away_win=0.0,
            same_competition_only=True,
            form_window_matches=4,
            decay_half_life_days=60,
            bayes_prior_strength=6.0,
            home_away_split_weight=0.7,
            h2h_window_matches=3,
            h2h_max_adjustment=0.04,
            goal_cap=6,
            history_selection_mode="event_time",
            competition_fallback_enabled=True,
            db_path=self.db_path,
        )
        engine = BacktestEngine(SQLiteBacktestDataSource(self.db_path))

        result_365 = engine.run(
            config=BacktestConfig(lookback_days=365, **base_config),
            strategy=build_strategy(
                "team_strength_poisson_value_v2",
                fixed_stake=10.0,
                min_history_matches=3,
                min_edge=0.0,
                lookback_days=365,
                value_mode="expected_value",
                min_edge_home_win=0.0,
                min_edge_draw=0.0,
                min_edge_away_win=0.0,
                same_competition_only=True,
                form_window_matches=4,
                decay_half_life_days=60,
                bayes_prior_strength=6.0,
                home_away_split_weight=0.7,
                h2h_window_matches=3,
                h2h_max_adjustment=0.04,
                goal_cap=6,
            ),
        )
        result_1095 = engine.run(
            config=BacktestConfig(lookback_days=1095, **base_config),
            strategy=build_strategy(
                "team_strength_poisson_value_v2",
                fixed_stake=10.0,
                min_history_matches=3,
                min_edge=0.0,
                lookback_days=1095,
                value_mode="expected_value",
                min_edge_home_win=0.0,
                min_edge_draw=0.0,
                min_edge_away_win=0.0,
                same_competition_only=True,
                form_window_matches=4,
                decay_half_life_days=60,
                bayes_prior_strength=6.0,
                home_away_split_weight=0.7,
                h2h_window_matches=3,
                h2h_max_adjustment=0.04,
                goal_cap=6,
            ),
        )

        details_365 = json.loads(result_365.bets[0].details_json)
        details_1095 = json.loads(result_1095.bets[0].details_json)

        self.assertEqual(details_365["history_pool_scope"], "fallback_global")
        self.assertEqual(details_1095["history_pool_scope"], "same_competition")
        self.assertEqual(details_365["home_recent_form"], details_1095["home_recent_form"])
        self.assertEqual(details_365["away_recent_form"], details_1095["away_recent_form"])
        self.assertEqual(details_365["home_recent_form"][0]["competition"], "杯赛")
        self.assertEqual(details_1095["home_recent_form"][0]["competition"], "杯赛")

    def test_poisson_probabilities_are_strictly_normalized(self) -> None:
        probabilities = _build_poisson_outcome_probabilities(
            lambda_home=1.73,
            lambda_away=0.91,
            goal_cap=6,
        )
        self.assertAlmostEqual(sum(probabilities.values()), 1.0, places=10)

    def test_team_strength_v2_ablation_variants_run(self) -> None:
        config = BacktestConfig(
            start_date=date(2025, 1, 10),
            end_date=date(2025, 1, 10),
            fixed_stake=10.0,
            competitions=["策略联赛"],
            min_history_matches=3,
            min_edge=0.0,
            lookback_days=365,
            value_mode="expected_value",
            min_edge_home_win=0.0,
            min_edge_draw=0.0,
            min_edge_away_win=0.0,
            same_competition_only=True,
            db_path=self.db_path,
        )
        engine = BacktestEngine(SQLiteBacktestDataSource(self.db_path))
        for strategy_name in (
            "team_strength_poisson_value_v2_no_form",
            "team_strength_poisson_value_v2_no_h2h",
            "team_strength_poisson_value_v2_strength_only",
        ):
            result = engine.run(
                config=config,
                strategy=build_strategy(
                    strategy_name,
                    fixed_stake=10.0,
                    min_history_matches=3,
                    min_edge=0.0,
                    lookback_days=365,
                    value_mode="expected_value",
                    min_edge_home_win=0.0,
                    min_edge_draw=0.0,
                    min_edge_away_win=0.0,
                    same_competition_only=True,
                ),
            )
            self.assertGreaterEqual(result.total_matches_considered, 1)
            self.assertIsNotNone(result.diagnostics["prediction_metrics"])

    def test_dixon_coles_probabilities_are_strictly_normalized(self) -> None:
        probabilities = _build_dixon_coles_outcome_probabilities(
            lambda_home=1.41,
            lambda_away=0.97,
            rho=-0.06,
            goal_cap=6,
        )
        self.assertAlmostEqual(sum(probabilities.values()), 1.0, places=10)

    def test_datasource_normalizes_common_team_aliases(self) -> None:
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                """
                INSERT INTO sfc500_matches_raw (
                    expect, match_no, competition, match_time,
                    home_team, away_team, home_team_canonical, away_team_canonical,
                    avg_win_odds, avg_draw_odds, avg_lose_odds,
                    spf_result, spf_result_code, final_score, is_settled,
                    source_url, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "99980",
                    1,
                    "英超",
                    "2025-08-16 22:00:00",
                    "布赖顿",
                    "伯恩茅",
                    "布赖顿",
                    "伯恩茅",
                    2.10,
                    3.20,
                    3.50,
                    "胜",
                    "3",
                    "2:1",
                    1,
                    "x",
                    "2025-08-16T00:00:00",
                ),
            )
            connection.commit()

        rows = SQLiteBacktestDataSource(self.db_path).load_matches(
            start_date=date(2025, 8, 16),
            end_date=date(2025, 8, 16),
            competitions=["英超"],
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].home_team, "布莱顿")
        self.assertEqual(rows[0].away_team, "伯恩茅斯")

    def test_dixon_coles_strategy_runs_and_emits_debug_details(self) -> None:
        config = BacktestConfig(
            start_date=date(2025, 1, 10),
            end_date=date(2025, 1, 10),
            fixed_stake=10.0,
            competitions=["策略联赛"],
            min_history_matches=3,
            min_edge=0.0,
            lookback_days=365,
            value_mode="expected_value",
            min_edge_home_win=0.0,
            min_edge_draw=0.0,
            min_edge_away_win=0.0,
            same_competition_only=True,
            db_path=self.db_path,
        )
        engine = BacktestEngine(SQLiteBacktestDataSource(self.db_path))
        result = engine.run(
            config=config,
            strategy=build_strategy(
                "dixon_coles_value",
                fixed_stake=10.0,
                min_history_matches=3,
                min_edge=0.0,
                lookback_days=365,
                value_mode="expected_value",
                min_edge_home_win=0.0,
                min_edge_draw=0.0,
                min_edge_away_win=0.0,
                same_competition_only=True,
            ),
        )
        self.assertGreaterEqual(result.total_matches_considered, 1)
        self.assertIsNotNone(result.diagnostics["prediction_metrics"])
        if result.bets:
            details = json.loads(result.bets[0].details_json)
            self.assertEqual(details["model_family"], "dixon_coles")
            self.assertIn("fit_summary", details)
            self.assertIn("dc_tau_rows", details)

    def test_dixon_coles_ignores_invalid_team_name_rows_after_fallback(self) -> None:
        with sqlite3.connect(self.db_path) as connection:
            connection.executemany(
                """
                INSERT INTO sfc500_matches_raw (
                    expect, match_no, competition, match_time,
                    home_team, away_team, home_team_canonical, away_team_canonical,
                    avg_win_odds, avg_draw_odds, avg_lose_odds,
                    spf_result, spf_result_code, final_score, is_settled,
                    source_url, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        "99890",
                        1,
                        "脏数据联赛",
                        "2025-01-08 12:00:00",
                        "",
                        "脏队",
                        "",
                        "脏队",
                        2.10,
                        3.20,
                        3.50,
                        "平",
                        "1",
                        "1:1",
                        1,
                        "x",
                        "2025-01-08T00:00:00",
                    ),
                    (
                        "99891",
                        1,
                        "新联赛",
                        "2025-01-11 12:00:00",
                        "强主队",
                        "弱客队",
                        "强主队",
                        "弱客队",
                        2.15,
                        3.25,
                        3.50,
                        "胜",
                        "3",
                        "2:0",
                        1,
                        "x",
                        "2025-01-11T00:00:00",
                    ),
                ],
            )
            connection.commit()

        config = BacktestConfig(
            start_date=date(2025, 1, 11),
            end_date=date(2025, 1, 11),
            fixed_stake=10.0,
            competitions=["新联赛"],
            min_history_matches=3,
            min_edge=0.0,
            lookback_days=365,
            value_mode="expected_value",
            min_edge_home_win=0.0,
            min_edge_draw=0.0,
            min_edge_away_win=0.0,
            same_competition_only=True,
            db_path=self.db_path,
        )
        engine = BacktestEngine(SQLiteBacktestDataSource(self.db_path))
        result = engine.run(
            config=config,
            strategy=build_strategy(
                "dixon_coles_value",
                fixed_stake=10.0,
                min_history_matches=3,
                min_edge=0.0,
                lookback_days=365,
                value_mode="expected_value",
                min_edge_home_win=0.0,
                min_edge_draw=0.0,
                min_edge_away_win=0.0,
                same_competition_only=True,
            ),
        )

        self.assertEqual(result.total_matches_considered, 1)
        self.assertGreaterEqual(result.total_bets_placed, 1)
        details = json.loads(result.bets[0].details_json)
        self.assertTrue(details["fallback_applied"])

    def test_dixon_coles_uses_localized_fallback_pool_for_promoted_team_case(self) -> None:
        rows = [
            (
                "99001",
                1,
                "次级联赛",
                "2024-09-01 12:00:00",
                "升班马",
                "次级甲",
                "升班马",
                "次级甲",
                2.20,
                3.20,
                3.30,
                "胜",
                "3",
                "2:0",
                1,
                "x",
                "2024-09-01T00:00:00",
            ),
            (
                "99002",
                1,
                "次级联赛",
                "2024-09-08 12:00:00",
                "次级乙",
                "升班马",
                "次级乙",
                "升班马",
                2.20,
                3.20,
                3.30,
                "负",
                "0",
                "1:2",
                1,
                "x",
                "2024-09-08T00:00:00",
            ),
            (
                "99003",
                1,
                "次级联赛",
                "2024-09-15 12:00:00",
                "升班马",
                "次级丙",
                "升班马",
                "次级丙",
                2.20,
                3.20,
                3.30,
                "平",
                "1",
                "1:1",
                1,
                "x",
                "2024-09-15T00:00:00",
            ),
            (
                "99004",
                1,
                "顶级联赛",
                "2024-09-02 12:00:00",
                "老牌队",
                "顶级甲",
                "老牌队",
                "顶级甲",
                2.10,
                3.20,
                3.60,
                "胜",
                "3",
                "2:1",
                1,
                "x",
                "2024-09-02T00:00:00",
            ),
            (
                "99005",
                1,
                "顶级联赛",
                "2024-09-09 12:00:00",
                "顶级乙",
                "老牌队",
                "顶级乙",
                "老牌队",
                2.10,
                3.20,
                3.60,
                "负",
                "0",
                "0:1",
                1,
                "x",
                "2024-09-09T00:00:00",
            ),
            (
                "99006",
                1,
                "顶级联赛",
                "2024-09-16 12:00:00",
                "老牌队",
                "顶级丙",
                "老牌队",
                "顶级丙",
                2.10,
                3.20,
                3.60,
                "胜",
                "3",
                "3:1",
                1,
                "x",
                "2024-09-16T00:00:00",
            ),
            (
                "99007",
                1,
                "次级联赛",
                "2024-09-05 12:00:00",
                "次级甲",
                "次级乙",
                "次级甲",
                "次级乙",
                2.20,
                3.20,
                3.30,
                "平",
                "1",
                "1:1",
                1,
                "x",
                "2024-09-05T00:00:00",
            ),
            (
                "99008",
                1,
                "顶级联赛",
                "2024-09-06 12:00:00",
                "顶级甲",
                "顶级乙",
                "顶级甲",
                "顶级乙",
                2.10,
                3.20,
                3.60,
                "平",
                "1",
                "1:1",
                1,
                "x",
                "2024-09-06T00:00:00",
            ),
            (
                "99901",
                1,
                "顶级联赛",
                "2025-01-10 12:00:00",
                "升班马",
                "老牌队",
                "升班马",
                "老牌队",
                2.40,
                3.20,
                2.90,
                "负",
                "0",
                "0:1",
                1,
                "x",
                "2025-01-10T00:00:00",
            ),
        ]
        unrelated_rows = []
        for index in range(40):
            unrelated_rows.append(
                (
                    f"995{index:02d}",
                    1,
                    "无关联赛",
                    f"2024-10-{(index % 20) + 1:02d} 12:00:00",
                    f"无关队{index}",
                    f"无关队{index + 100}",
                    f"无关队{index}",
                    f"无关队{index + 100}",
                    2.10,
                    3.10,
                    3.50,
                    "胜",
                    "3",
                    "2:0",
                    1,
                    "x",
                    "2024-10-01T00:00:00",
                )
            )

        with sqlite3.connect(self.db_path) as connection:
            connection.executemany(
                """
                INSERT INTO sfc500_matches_raw (
                    expect, match_no, competition, match_time,
                    home_team, away_team, home_team_canonical, away_team_canonical,
                    avg_win_odds, avg_draw_odds, avg_lose_odds,
                    spf_result, spf_result_code, final_score, is_settled,
                    source_url, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows + unrelated_rows,
            )
            connection.commit()

        config = BacktestConfig(
            start_date=date(2025, 1, 10),
            end_date=date(2025, 1, 10),
            fixed_stake=10.0,
            competitions=["顶级联赛"],
            min_history_matches=2,
            min_edge=0.0,
            lookback_days=365,
            value_mode="model_probability",
            min_edge_home_win=0.0,
            min_edge_draw=0.0,
            min_edge_away_win=0.0,
            same_competition_only=True,
            db_path=self.db_path,
        )
        engine = BacktestEngine(SQLiteBacktestDataSource(self.db_path))
        result = engine.run(
            config=config,
            strategy=build_strategy(
                "dixon_coles_value",
                fixed_stake=10.0,
                min_history_matches=2,
                min_edge=0.0,
                lookback_days=365,
                value_mode="model_probability",
                min_edge_home_win=0.0,
                min_edge_draw=0.0,
                min_edge_away_win=0.0,
                same_competition_only=True,
            ),
        )

        self.assertEqual(result.total_matches_considered, 1)
        self.assertGreaterEqual(result.total_bets_placed, 1)
        details = json.loads(result.bets[0].details_json)
        self.assertTrue(details["fallback_applied"])
        self.assertEqual(details["history_pool_scope"], "fallback_localized_global")
        self.assertLess(details["history_matches_used"], 20)
        self.assertGreater(details["fallback_neighbor_team_count"], 0)


if __name__ == "__main__":
    unittest.main()
