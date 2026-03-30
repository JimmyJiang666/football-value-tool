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
from jczq_assistant.backtest import DEFAULT_BACKTEST_SOURCE_LABEL
from jczq_assistant.backtest import DEFAULT_LOOKBACK_DAYS
from jczq_assistant.backtest import DEFAULT_TRAINING_DATABASE_PATH
from jczq_assistant.backtest import DEFAULT_TRAINING_SOURCE_KIND
from jczq_assistant.backtest import DEFAULT_TRAINING_SOURCE_LABEL
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
from jczq_assistant.backtest import export_backtest_result
from jczq_assistant.backtest import get_default_selection_thresholds


def build_parser() -> argparse.ArgumentParser:
    """构建命令行参数。"""

    parser = argparse.ArgumentParser(description="运行 500.com 历史赔率回测")
    parser.add_argument(
        "--strategy",
        required=True,
        nargs="+",
        choices=[
            "lowest_odds_fixed",
            "lowest_odds_parlay",
            "historical_odds_value",
            "team_strength_poisson_value",
            "team_strength_poisson_value_v2",
            "team_strength_poisson_value_v2_no_form",
            "team_strength_poisson_value_v2_no_h2h",
            "team_strength_poisson_value_v2_strength_only",
            "dixon_coles_value",
        ],
        help="策略名称；可一次传多个，做对照回测",
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
        help="候选池 SQLite 文件路径；如不传则按 source-kind 自动选择",
    )
    parser.add_argument(
        "--source-kind",
        choices=["expect", "team"],
        default="expect",
        help="每日模拟下注候选池；expect 为原来的 14 场期次主库，team 为球队页大库",
    )
    parser.add_argument(
        "--training-db-path",
        help="策略训练集 SQLite 文件路径；如不传则按 training-source-kind 自动选择",
    )
    parser.add_argument(
        "--training-source-kind",
        choices=["expect", "team"],
        default=DEFAULT_TRAINING_SOURCE_KIND,
        help="策略训练集数据源类型，默认球队大库",
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
        choices=["probability_diff", "expected_value", "model_probability"],
        default=DEFAULT_VALUE_MODE,
        help="value 计算方式",
    )
    parser.add_argument(
        "--min-edge-home-win",
        type=float,
        help="主胜下注阈值；probability_diff 模式下是概率差，expected_value 模式下是期望收益率，model_probability 模式下是最小模型概率",
    )
    parser.add_argument(
        "--min-edge-draw",
        type=float,
        help="平局下注阈值；probability_diff 模式下是概率差，expected_value 模式下是期望收益率，model_probability 模式下是最小模型概率",
    )
    parser.add_argument(
        "--min-edge-away-win",
        type=float,
        help="客胜下注阈值；probability_diff 模式下是概率差，expected_value 模式下是期望收益率，model_probability 模式下是最小模型概率",
    )
    parser.add_argument(
        "--same-competition-only",
        action="store_true",
        help="策略仅使用同联赛历史样本；对球队强度策略通常建议打开",
    )
    parser.add_argument(
        "--form-window-matches",
        type=int,
        default=TEAM_STRENGTH_DEFAULT_FORM_WINDOW_MATCHES,
        help="球队强度策略近期状态窗口场数",
    )
    parser.add_argument(
        "--decay-half-life-days",
        type=int,
        default=TEAM_STRENGTH_DEFAULT_DECAY_HALF_LIFE_DAYS,
        help="球队强度策略的时间衰减半衰期（天）",
    )
    parser.add_argument(
        "--bayes-prior-strength",
        type=float,
        default=TEAM_STRENGTH_DEFAULT_BAYES_PRIOR_STRENGTH,
        help="球队强度策略的贝叶斯收缩强度",
    )
    parser.add_argument(
        "--home-away-split-weight",
        type=float,
        default=TEAM_STRENGTH_DEFAULT_HOME_AWAY_SPLIT_WEIGHT,
        help="球队强度策略中主客场拆分权重，0 到 1",
    )
    parser.add_argument(
        "--h2h-window-matches",
        type=int,
        default=TEAM_STRENGTH_DEFAULT_H2H_WINDOW_MATCHES,
        help="球队强度策略最近交手参考场数",
    )
    parser.add_argument(
        "--h2h-max-adjustment",
        type=float,
        default=TEAM_STRENGTH_DEFAULT_H2H_MAX_ADJUSTMENT,
        help="球队强度策略交手修正的最大幅度",
    )
    parser.add_argument(
        "--goal-cap",
        type=int,
        default=TEAM_STRENGTH_DEFAULT_GOAL_CAP,
        help="Poisson 概率矩阵的进球截断档位，最后一档吸收尾部",
    )
    parser.add_argument(
        "--history-selection-mode",
        choices=["daily", "event_time"],
        default="daily",
        help="历史样本时间选择方式；event_time 会严格按 kickoff timestamp 屏蔽未来信息",
    )
    parser.add_argument(
        "--competition-fallback-enabled",
        action="store_true",
        help="same_competition_only 样本不足时回退到更宽的全局历史池",
    )
    parser.add_argument(
        "--disable-recent-form",
        action="store_true",
        help="关闭 recent form 修正，便于 ablation",
    )
    parser.add_argument(
        "--disable-h2h",
        action="store_true",
        help="关闭 head-to-head 修正，便于 ablation",
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
    strategy_label = args.strategy[0] if len(args.strategy) == 1 else "multi_strategy"
    return (
        PROJECT_ROOT
        / "results"
        / "backtests"
        / f"{strategy_label}_{args.start_date}_{args.end_date}_{timestamp}"
    )


def main(argv: list[str] | None = None) -> int:
    """CLI 入口。"""

    parser = build_parser()
    args = parser.parse_args(argv)

    start_date = _parse_iso_date(args.start_date)
    end_date = _parse_iso_date(args.end_date)
    if end_date < start_date:
        parser.error("end-date 不能早于 start-date")
    if "lowest_odds_parlay" in args.strategy and args.parlay_size is None:
        parser.error("lowest_odds_parlay 需要传 --parlay-size")

    default_thresholds = get_default_selection_thresholds(
        args.value_mode,
        strategy_name=args.strategy[0],
    )
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
    resolved_db_path = Path(args.db_path) if args.db_path else (
        DEFAULT_TRAINING_DATABASE_PATH if args.source_kind == "team" else DEFAULT_BACKTEST_DATABASE_PATH
    )
    resolved_training_db_path = Path(args.training_db_path) if args.training_db_path else (
        DEFAULT_TRAINING_DATABASE_PATH
        if args.training_source_kind == "team"
        else DEFAULT_BACKTEST_DATABASE_PATH
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
        form_window_matches=args.form_window_matches,
        decay_half_life_days=args.decay_half_life_days,
        bayes_prior_strength=args.bayes_prior_strength,
        home_away_split_weight=args.home_away_split_weight,
        h2h_window_matches=args.h2h_window_matches,
        h2h_max_adjustment=args.h2h_max_adjustment,
        goal_cap=args.goal_cap,
        history_selection_mode=args.history_selection_mode,
        competition_fallback_enabled=args.competition_fallback_enabled,
        use_recent_form=not args.disable_recent_form,
        use_h2h=not args.disable_h2h,
        data_source_kind=args.source_kind,
        data_source_label="球队大库" if args.source_kind == "team" else DEFAULT_BACKTEST_SOURCE_LABEL,
        db_path=resolved_db_path,
        training_data_source_kind=args.training_source_kind,
        training_data_source_label=(
            DEFAULT_TRAINING_SOURCE_LABEL
            if args.training_source_kind == "team"
            else DEFAULT_BACKTEST_SOURCE_LABEL
        ),
        training_db_path=resolved_training_db_path,
    )
    data_source = SQLiteBacktestDataSource(
        db_path=config.db_path,
        source_kind=args.source_kind,
    )
    training_data_source = SQLiteBacktestDataSource(
        db_path=config.training_db_path,
        source_kind=args.training_source_kind,
    )
    engine = BacktestEngine(data_source, training_data_source)
    summaries: list[dict[str, object]] = []
    output_dir = _resolve_output_dir(args)
    for strategy_name in args.strategy:
        strategy = build_strategy(
            strategy_name,
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
            form_window_matches=args.form_window_matches,
            decay_half_life_days=args.decay_half_life_days,
            bayes_prior_strength=args.bayes_prior_strength,
            home_away_split_weight=args.home_away_split_weight,
            h2h_window_matches=args.h2h_window_matches,
            h2h_max_adjustment=args.h2h_max_adjustment,
            goal_cap=args.goal_cap,
            history_selection_mode=args.history_selection_mode,
            competition_fallback_enabled=args.competition_fallback_enabled,
            use_recent_form=not args.disable_recent_form,
            use_h2h=not args.disable_h2h,
        )

        result = engine.run(config=config, strategy=strategy)
        summary = result.to_summary_dict()
        summaries.append(summary)

        if output_dir is not None:
            strategy_output_dir = output_dir / strategy_name if len(args.strategy) > 1 else output_dir
            exported_files = export_backtest_result(
                result,
                output_dir=strategy_output_dir,
                save_csv=args.save_csv,
                save_json=args.save_json or args.save_csv,
            )
            print(
                json.dumps(
                    {"strategy_name": strategy_name, "output_dir": str(strategy_output_dir), **exported_files},
                    ensure_ascii=False,
                    indent=2,
                )
            )

    if len(summaries) == 1:
        print(json.dumps(summaries[0], ensure_ascii=False, indent=2))
    else:
        print(json.dumps({"results": summaries}, ensure_ascii=False, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
