"""500.com 历史赔率回测 CLI。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from datetime import datetime


PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from jczq_assistant.backtest import BacktestConfig
from jczq_assistant.backtest import BacktestEngine
from jczq_assistant.backtest import DEFAULT_BACKTEST_DATABASE_PATH
from jczq_assistant.backtest import DEFAULT_LOOKBACK_DAYS
from jczq_assistant.backtest import DEFAULT_VALUE_MODE
from jczq_assistant.backtest import SQLiteBacktestDataSource
from jczq_assistant.backtest import build_strategy
from jczq_assistant.backtest import export_backtest_result
from jczq_assistant.backtest import get_default_selection_thresholds


def build_parser() -> argparse.ArgumentParser:
    """构建命令行参数。"""

    parser = argparse.ArgumentParser(description="运行 500.com 历史赔率回测")
    parser.add_argument(
        "--strategy",
        required=True,
        choices=["lowest_odds_fixed", "lowest_odds_parlay", "historical_odds_value"],
        help="策略名称",
    )
    parser.add_argument("--start-date", required=True, help="开始日期，格式 YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="结束日期，格式 YYYY-MM-DD")
    parser.add_argument(
        "--stake",
        type=float,
        required=True,
        help="固定每场下注金额，例如 10",
    )
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_BACKTEST_DATABASE_PATH),
        help="SQLite 文件路径，默认 data/sfc500_history.sqlite3",
    )
    parser.add_argument(
        "--competition",
        action="append",
        default=[],
        help="只回测指定联赛，可多次传入",
    )
    parser.add_argument(
        "--max-bets-per-day",
        type=int,
        help="每天最多下注多少场；如不传则不限制",
    )
    parser.add_argument(
        "--parlay-size",
        type=int,
        help="串关场数，仅 lowest_odds_parlay 使用，例如 3 表示 3串1",
    )
    parser.add_argument(
        "--history-match-count",
        type=int,
        default=100,
        help="历史匹配策略使用的近邻样本数",
    )
    parser.add_argument(
        "--min-history-matches",
        type=int,
        default=20,
        help="历史匹配策略的最小样本数",
    )
    parser.add_argument(
        "--min-edge",
        type=float,
        default=0.02,
        help="历史匹配策略的最小 value 阈值，例如 0.02",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help=f"历史匹配策略仅使用最近多少天的历史样本；默认 {DEFAULT_LOOKBACK_DAYS}",
    )
    parser.add_argument(
        "--weighting-mode",
        choices=["equal", "inverse_distance"],
        default="inverse_distance",
        help="历史样本加权方式",
    )
    parser.add_argument(
        "--value-mode",
        choices=["probability_diff", "expected_value"],
        default=DEFAULT_VALUE_MODE,
        help="value 计算方式",
    )
    parser.add_argument(
        "--min-edge-home-win",
        type=float,
        help="主胜下注阈值；probability_diff 模式下是概率差，expected_value 模式下是期望收益率",
    )
    parser.add_argument(
        "--min-edge-draw",
        type=float,
        help="平局下注阈值；probability_diff 模式下是概率差，expected_value 模式下是期望收益率",
    )
    parser.add_argument(
        "--min-edge-away-win",
        type=float,
        help="客胜下注阈值；probability_diff 模式下是概率差，expected_value 模式下是期望收益率",
    )
    parser.add_argument(
        "--same-competition-only",
        action="store_true",
        help="历史匹配策略仅使用同联赛历史样本",
    )
    parser.add_argument(
        "--staking-mode",
        choices=["fixed", "fractional_kelly"],
        default="fixed",
        help="投注模式；fixed 为固定投注，fractional_kelly 为分数 Kelly 资金管理",
    )
    parser.add_argument(
        "--initial-bankroll",
        type=float,
        default=1000.0,
        help="Kelly 模式的初始资金",
    )
    parser.add_argument(
        "--kelly-fraction",
        type=float,
        default=0.25,
        help="Kelly 折扣，例如 0.25 表示四分之一 Kelly",
    )
    parser.add_argument(
        "--max-stake-pct",
        type=float,
        default=0.02,
        help="Kelly 模式下单场最大资金占比，例如 0.02 表示单场最多 2%%",
    )
    parser.add_argument(
        "--output-dir",
        help="结果输出目录；如未提供但开启保存，会自动写入 results/backtests/...",
    )
    parser.add_argument(
        "--save-csv",
        action="store_true",
        help="保存下注明细、每日曲线、联赛汇总 CSV",
    )
    parser.add_argument(
        "--save-json",
        action="store_true",
        help="保存 summary.json",
    )
    return parser


def _parse_iso_date(value: str) -> datetime.date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _resolve_output_dir(args: argparse.Namespace) -> Path | None:
    if args.output_dir:
        return Path(args.output_dir)
    if not args.save_csv and not args.save_json:
        return None

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return (
        PROJECT_ROOT
        / "results"
        / "backtests"
        / f"{args.strategy}_{args.start_date}_{args.end_date}_{timestamp}"
    )


def main(argv: list[str] | None = None) -> int:
    """CLI 入口。"""

    parser = build_parser()
    args = parser.parse_args(argv)

    start_date = _parse_iso_date(args.start_date)
    end_date = _parse_iso_date(args.end_date)
    if end_date < start_date:
        parser.error("end-date 不能早于 start-date")
    if args.strategy == "lowest_odds_parlay" and args.parlay_size is None:
        parser.error("lowest_odds_parlay 需要传 --parlay-size")

    default_thresholds = get_default_selection_thresholds(args.value_mode)
    min_edge_home_win = (
        default_thresholds["home_win"]
        if args.min_edge_home_win is None
        else float(args.min_edge_home_win)
    )
    min_edge_draw = (
        default_thresholds["draw"]
        if args.min_edge_draw is None
        else float(args.min_edge_draw)
    )
    min_edge_away_win = (
        default_thresholds["away_win"]
        if args.min_edge_away_win is None
        else float(args.min_edge_away_win)
    )

    config = BacktestConfig(
        start_date=start_date,
        end_date=end_date,
        fixed_stake=args.stake,
        competitions=list(args.competition),
        max_bets_per_day=args.max_bets_per_day,
        parlay_size=args.parlay_size,
        history_match_count=args.history_match_count,
        min_history_matches=args.min_history_matches,
        min_edge=args.min_edge,
        lookback_days=args.lookback_days,
        weighting_mode=args.weighting_mode,
        value_mode=args.value_mode,
        min_edge_home_win=min_edge_home_win,
        min_edge_draw=min_edge_draw,
        min_edge_away_win=min_edge_away_win,
        staking_mode=args.staking_mode,
        initial_bankroll=args.initial_bankroll,
        kelly_fraction=args.kelly_fraction,
        max_stake_pct=args.max_stake_pct,
        same_competition_only=args.same_competition_only,
        db_path=Path(args.db_path),
    )
    data_source = SQLiteBacktestDataSource(db_path=config.db_path)
    engine = BacktestEngine(data_source)
    strategy = build_strategy(
        args.strategy,
        fixed_stake=args.stake,
        max_bets_per_day=args.max_bets_per_day,
        parlay_size=args.parlay_size,
        history_match_count=args.history_match_count,
        min_history_matches=args.min_history_matches,
        min_edge=args.min_edge,
        lookback_days=args.lookback_days,
        weighting_mode=args.weighting_mode,
        value_mode=args.value_mode,
        min_edge_home_win=min_edge_home_win,
        min_edge_draw=min_edge_draw,
        min_edge_away_win=min_edge_away_win,
        staking_mode=args.staking_mode,
        initial_bankroll=args.initial_bankroll,
        kelly_fraction=args.kelly_fraction,
        max_stake_pct=args.max_stake_pct,
        same_competition_only=args.same_competition_only,
    )

    result = engine.run(config=config, strategy=strategy)
    print(json.dumps(result.to_summary_dict(), ensure_ascii=False, indent=2))

    output_dir = _resolve_output_dir(args)
    if output_dir is not None:
        exported_files = export_backtest_result(
            result,
            output_dir=output_dir,
            save_csv=args.save_csv,
            save_json=args.save_json or args.save_csv,
        )
        print(json.dumps({"output_dir": str(output_dir), **exported_files}, ensure_ascii=False, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
