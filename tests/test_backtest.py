"""回测框架基础测试。"""

from __future__ import annotations

from datetime import date
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
from jczq_assistant.backtest import build_strategy


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
                    is_settled,
                    source_url,
                    fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        0,
                        "x",
                        "2025-01-02T00:00:00",
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


if __name__ == "__main__":
    unittest.main()
