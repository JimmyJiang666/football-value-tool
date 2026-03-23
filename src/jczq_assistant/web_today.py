"""今日推荐页面。"""

from datetime import datetime

import pandas as pd
import streamlit as st

from jczq_assistant.backtest import BacktestConfig
from jczq_assistant.backtest import BacktestMatch
from jczq_assistant.backtest import SELECTION_LABELS
from jczq_assistant.backtest import SQLiteBacktestDataSource
from jczq_assistant.backtest import TEAM_STRENGTH_DEFAULT_BAYES_PRIOR_STRENGTH
from jczq_assistant.backtest import TEAM_STRENGTH_DEFAULT_DECAY_HALF_LIFE_DAYS
from jczq_assistant.backtest import TEAM_STRENGTH_DEFAULT_FORM_WINDOW_MATCHES
from jczq_assistant.backtest import TEAM_STRENGTH_DEFAULT_GOAL_CAP
from jczq_assistant.backtest import TEAM_STRENGTH_DEFAULT_H2H_MAX_ADJUSTMENT
from jczq_assistant.backtest import TEAM_STRENGTH_DEFAULT_H2H_WINDOW_MATCHES
from jczq_assistant.backtest import TEAM_STRENGTH_DEFAULT_HOME_AWAY_SPLIT_WEIGHT
from jczq_assistant.backtest import build_strategy
from jczq_assistant.backtest import build_strategy_context_from_config
from jczq_assistant.sfc500_team_history import (
    DEFAULT_LIVE_RECOMMENDATION_STATUS_CODES,
    fetch_live_matches_snapshot,
)
from jczq_assistant.web_shared import (
    BACKTEST_DATA_SOURCE_OPTIONS,
    BACKTEST_DAILY_LIMIT_OPTIONS,
    BACKTEST_HISTORY_MATCH_COUNT_OPTIONS,
    BACKTEST_LOOKBACK_OPTIONS,
    BACKTEST_STAKING_MODE_OPTIONS,
    BACKTEST_VALUE_MODE_OPTIONS,
    BACKTEST_WEIGHTING_MODE_OPTIONS,
    DEFAULT_LOOKBACK_DAYS,
    DEFAULT_VALUE_MODE,
    format_backtest_skip_reason,
    format_daily_limit_option,
    format_lookback_option,
    format_threshold_meaning,
    format_value_mode_label,
    resolve_daily_limit_value,
    resolve_lookback_value,
    resolve_value_mode_score_label,
    resolve_value_mode_threshold_defaults,
)
from jczq_assistant.web_theme import render_page_banner


TODAY_RECOMMENDATION_STRATEGY_OPTIONS = {
    "历史水位匹配价值": {
        "strategy_name": "historical_odds_value",
        "mode": "value_match",
    },
    "球队强度 Poisson 价值": {
        "strategy_name": "team_strength_poisson_value",
        "mode": "team_strength",
    },
}


def build_live_match_pool_dataframe(matches: list[dict]) -> pd.DataFrame:
    """把今日 live 候选比赛转成表格。"""

    rows = [
        {
            "状态": match.get("status_label"),
            "比赛时间": match.get("match_time"),
            "联赛": match.get("competition"),
            "主队": match.get("home_team_canonical") or match.get("home_team"),
            "比分": match.get("final_score") or "-",
            "客队": match.get("away_team_canonical") or match.get("away_team"),
            "主胜均赔": match.get("avg_win_odds"),
            "平局均赔": match.get("avg_draw_odds"),
            "客胜均赔": match.get("avg_lose_odds"),
            "盘口": match.get("asian_handicap_line"),
        }
        for match in matches
    ]
    return pd.DataFrame(
        rows,
        columns=["状态", "比赛时间", "联赛", "主队", "比分", "客队", "主胜均赔", "平局均赔", "客胜均赔", "盘口"],
    )


def build_recommendation_history_matches_dataframe(rows: list[dict]) -> pd.DataFrame:
    """构造历史匹配推荐的参考样本表。"""

    return pd.DataFrame(
        rows,
        columns=[
            "match_time",
            "competition",
            "home_team",
            "final_score",
            "away_team",
            "result_label",
            "avg_win_odds",
            "avg_draw_odds",
            "avg_lose_odds",
            "distance",
            "weight",
        ],
    ).rename(
        columns={
            "match_time": "比赛时间",
            "competition": "联赛",
            "home_team": "主队",
            "final_score": "比分",
            "away_team": "客队",
            "result_label": "赛果",
            "avg_win_odds": "主胜均赔",
            "avg_draw_odds": "平局均赔",
            "avg_lose_odds": "客胜均赔",
            "distance": "距离",
            "weight": "权重",
        }
    )


def build_recommendation_form_dataframe(rows: list[dict]) -> pd.DataFrame:
    """构造球队近期 form 表。"""

    return pd.DataFrame(
        rows,
        columns=[
            "match_time",
            "competition",
            "side_label",
            "opponent",
            "score",
            "result_label",
            "points",
        ],
    ).rename(
        columns={
            "match_time": "比赛时间",
            "competition": "联赛",
            "side_label": "主客",
            "opponent": "对手",
            "score": "比分",
            "result_label": "结果",
            "points": "积分",
        }
    )


def build_recommendation_h2h_dataframe(rows: list[dict]) -> pd.DataFrame:
    """构造近期交手表。"""

    return pd.DataFrame(
        rows,
        columns=["match_time", "competition", "home_team", "score", "away_team", "result_label"],
    ).rename(
        columns={
            "match_time": "比赛时间",
            "competition": "联赛",
            "home_team": "主队",
            "score": "比分",
            "away_team": "客队",
            "result_label": "赛果",
        }
    )


def build_recommendation_probability_dataframe(details: dict) -> pd.DataFrame:
    """构造三项概率与 value 对比表。"""

    model_probabilities = dict(details.get("model_probabilities") or {})
    bookmaker_probabilities = dict(details.get("bookmaker_probabilities") or {})
    selection_values = dict(details.get("selection_values") or {})
    value_mode = str(details.get("value_mode") or DEFAULT_VALUE_MODE)
    score_label = resolve_value_mode_score_label(value_mode)

    rows = []
    for selection in ("home_win", "draw", "away_win"):
        rows.append(
            {
                "结果": SELECTION_LABELS.get(selection, selection),
                "模型概率": model_probabilities.get(selection),
                "庄家概率": bookmaker_probabilities.get(selection),
                score_label: selection_values.get(selection),
            }
        )
    return pd.DataFrame(rows, columns=["结果", "模型概率", "庄家概率", score_label])


def build_today_recommendation_skipped_dataframe(rows: list[dict]) -> pd.DataFrame:
    """构造今日推荐里的跳过比赛表。"""

    return pd.DataFrame(
        [
            {
                "状态": row["match"].get("status_label"),
                "比赛时间": row["match"].get("match_time"),
                "联赛": row["match"].get("competition"),
                "主队": row["match"].get("home_team_canonical") or row["match"].get("home_team"),
                "客队": row["match"].get("away_team_canonical") or row["match"].get("away_team"),
                "原因": format_backtest_skip_reason(str(row.get("reason") or "")),
            }
            for row in rows
        ],
        columns=["状态", "比赛时间", "联赛", "主队", "客队", "原因"],
    )


def _build_live_candidate_match(match: dict) -> BacktestMatch | None:
    """把 live 比赛字典转成策略可复用的 BacktestMatch。"""

    raw_match_time = str(match.get("match_time") or "").strip()
    fixture_id = match.get("fixture_id")
    if not raw_match_time or fixture_id is None:
        return None

    return BacktestMatch(
        match_id=int(fixture_id),
        expect=str(fixture_id),
        match_no=1,
        match_time=datetime.fromisoformat(raw_match_time),
        competition=str(match.get("competition") or ""),
        home_team=str(match.get("home_team_canonical") or match.get("home_team") or ""),
        away_team=str(match.get("away_team_canonical") or match.get("away_team") or ""),
        home_goals=match.get("home_score"),
        away_goals=match.get("away_score"),
        avg_win_odds=float(match["avg_win_odds"]) if match.get("avg_win_odds") is not None else None,
        avg_draw_odds=float(match["avg_draw_odds"]) if match.get("avg_draw_odds") is not None else None,
        avg_lose_odds=float(match["avg_lose_odds"]) if match.get("avg_lose_odds") is not None else None,
        spf_result="",
        spf_result_code="",
        is_settled=True,
    )


def _run_today_strategy_recommendations(
    *,
    live_matches: list[dict],
    strategy_name: str,
    training_db_path,
    training_source_kind: str,
    config: BacktestConfig,
) -> dict:
    """对当前 live 候选池执行一套策略推荐。"""

    filtered_live_matches = list(live_matches)
    if config.competitions:
        selected_competitions = set(config.competitions)
        filtered_live_matches = [
            match
            for match in filtered_live_matches
            if str(match.get("competition") or "") in selected_competitions
        ]

    candidate_matches = [
        candidate
        for candidate in (_build_live_candidate_match(match) for match in filtered_live_matches)
        if candidate is not None
    ]
    candidate_matches.sort(key=lambda match: (match.match_time, match.match_id))

    current_date = config.start_date
    training_competitions = None
    if config.same_competition_only:
        training_competitions = sorted(
            {
                match.competition
                for match in candidate_matches
                if str(match.competition or "").strip()
            }
        ) or None

    training_source = SQLiteBacktestDataSource(
        db_path=training_db_path,
        source_kind=training_source_kind,
    )
    historical_matches = training_source.load_matches_before(
        before_date=current_date,
        competitions=training_competitions,
    )
    strategy = build_strategy(
        strategy_name,
        fixed_stake=float(config.fixed_stake),
        max_bets_per_day=config.max_bets_per_day,
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
        initial_bankroll=config.initial_bankroll,
        kelly_fraction=config.kelly_fraction,
        max_stake_pct=config.max_stake_pct,
        same_competition_only=config.same_competition_only,
        form_window_matches=config.form_window_matches,
        decay_half_life_days=config.decay_half_life_days,
        bayes_prior_strength=config.bayes_prior_strength,
        home_away_split_weight=config.home_away_split_weight,
        h2h_window_matches=config.h2h_window_matches,
        h2h_max_adjustment=config.h2h_max_adjustment,
        goal_cap=config.goal_cap,
    )
    context = build_strategy_context_from_config(
        config,
        strategy_name=strategy_name,
        current_date=current_date,
        historical_matches=tuple(historical_matches),
    )
    batch_result = strategy.generate_bets(candidate_matches, context)
    live_match_by_id = {
        int(match["fixture_id"]): match
        for match in filtered_live_matches
        if match.get("fixture_id") is not None
    }

    recommended_rows: list[dict] = []
    for decision in sorted(
        batch_result.bets,
        key=lambda item: (
            -float(item.edge or 0.0),
            -float(item.model_probability or 0.0),
            item.match_id,
        ),
    ):
        source_match = live_match_by_id.get(int(decision.match_id))
        if source_match is None:
            continue
        recommended_rows.append({"match": source_match, "decision": decision})

    skipped_rows = []
    for skip_decision in batch_result.skips:
        source_match = live_match_by_id.get(int(skip_decision.match_id))
        if source_match is None:
            continue
        skipped_rows.append({"match": source_match, "reason": skip_decision.reason})

    return {
        "candidate_count": len(candidate_matches),
        "training_match_count": len(historical_matches),
        "recommended_rows": recommended_rows,
        "skipped_rows": skipped_rows,
    }


def _render_today_recommendation_match_header(match: dict, decision) -> None:
    """渲染单场推荐卡顶部摘要。"""

    st.markdown(
        f"##### {(match.get('home_team_canonical') or match.get('home_team') or '-')}"
        f" vs {(match.get('away_team_canonical') or match.get('away_team') or '-')}"
    )
    st.caption(
        f"{match.get('status_label') or '-'} | "
        f"{match.get('match_time') or '-'} | "
        f"{match.get('competition') or '-'}"
    )
    metric_col1, metric_col2, metric_col3, metric_col4, metric_col5, metric_col6 = st.columns(6)
    metric_col1.metric("推荐", SELECTION_LABELS.get(decision.selection, decision.selection))
    metric_col2.metric("投注额", f"{float(decision.stake or 0.0):.2f}")
    selected_odds = (
        match.get("avg_win_odds")
        if decision.selection == "home_win"
        else match.get("avg_draw_odds")
        if decision.selection == "draw"
        else match.get("avg_lose_odds")
    )
    metric_col3.metric("赔率", f"{float(selected_odds or 0.0):.2f}")
    metric_col4.metric("模型概率", f"{float(decision.model_probability or 0.0):.2%}")
    metric_col5.metric("庄家概率", f"{float(decision.bookmaker_probability or 0.0):.2%}")
    metric_col6.metric("value", f"{float(decision.edge or 0.0):.2%}")


def _render_historical_recommendation_card(recommendation: dict) -> None:
    """渲染历史水位匹配推荐卡。"""

    match = recommendation["match"]
    decision = recommendation["decision"]
    details = dict(decision.details or {})

    with st.container(border=True):
        _render_today_recommendation_match_header(match, decision)
        probability_df = build_recommendation_probability_dataframe(details)
        if not probability_df.empty:
            st.dataframe(probability_df, use_container_width=True, hide_index=True)
        st.caption(
            "解释：先把当前胜平负赔率转成庄家概率，再去历史里找最相近的赔率结构样本；下面这些样本就是这次推荐里权重最高的一组参考。"
        )
        nearest_df = build_recommendation_history_matches_dataframe(
            details.get("nearest_matches") or []
        )
        if nearest_df.empty:
            st.info("当前没有可展示的历史参考样本。")
        else:
            st.dataframe(nearest_df.head(10), use_container_width=True, hide_index=True)


def _render_team_strength_recommendation_card(recommendation: dict) -> None:
    """渲染球队强度 Poisson 推荐卡。"""

    match = recommendation["match"]
    decision = recommendation["decision"]
    details = dict(decision.details or {})
    home_snapshot = details.get("home_snapshot") or {}
    away_snapshot = details.get("away_snapshot") or {}
    h2h_summary = details.get("h2h_summary") or {}

    with st.container(border=True):
        _render_today_recommendation_match_header(match, decision)
        probability_df = build_recommendation_probability_dataframe(details)
        if not probability_df.empty:
            st.dataframe(probability_df, use_container_width=True, hide_index=True)

        stat_col1, stat_col2, stat_col3, stat_col4 = st.columns(4)
        stat_col1.metric("主队 λ", f"{float(details.get('lambda_home') or 0.0):.2f}")
        stat_col2.metric("客队 λ", f"{float(details.get('lambda_away') or 0.0):.2f}")
        stat_col3.metric("近期交手数", int(h2h_summary.get("match_count") or 0))
        stat_col4.metric("交手修正", f"{float(h2h_summary.get('adjustment') or 0.0):.2%}")
        st.caption(
            "解释：这套策略综合近期 form、主客场攻防强度、时间衰减和弱交手修正，先估计双方预期进球，再用 Poisson 分布推出胜平负概率。"
        )

        factor_col1, factor_col2 = st.columns(2)
        factor_col1.caption(
            "主队：攻 "
            f"{float(home_snapshot.get('attack_rate') or 0.0):.2f} / 守 "
            f"{float(home_snapshot.get('defence_rate') or 0.0):.2f} / "
            f"近期得分率 {float(home_snapshot.get('recent_points_rate') or 0.0):.2%} / "
            f"近期净胜球 {float(home_snapshot.get('recent_goal_diff_rate') or 0.0):.2f}"
        )
        factor_col2.caption(
            "客队：攻 "
            f"{float(away_snapshot.get('attack_rate') or 0.0):.2f} / 守 "
            f"{float(away_snapshot.get('defence_rate') or 0.0):.2f} / "
            f"近期得分率 {float(away_snapshot.get('recent_points_rate') or 0.0):.2%} / "
            f"近期净胜球 {float(away_snapshot.get('recent_goal_diff_rate') or 0.0):.2f}"
        )

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


def _today_recommendation_result_key(strategy_name: str) -> str:
    return f"today_recommendation_result_{strategy_name}"


def _today_recommendation_error_key(strategy_name: str) -> str:
    return f"today_recommendation_error_{strategy_name}"


def _clear_today_recommendation_results() -> None:
    for strategy_meta in TODAY_RECOMMENDATION_STRATEGY_OPTIONS.values():
        strategy_name = str(strategy_meta["strategy_name"])
        st.session_state[_today_recommendation_result_key(strategy_name)] = None
        st.session_state[_today_recommendation_error_key(strategy_name)] = None


def _render_today_recommendation_results(
    *,
    strategy_name: str,
    recommendation_result: dict,
) -> None:
    recommended_rows = list(recommendation_result.get("recommended_rows") or [])
    skipped_rows = list(recommendation_result.get("skipped_rows") or [])
    candidate_count = int(recommendation_result.get("candidate_count") or 0)
    training_match_count = int(recommendation_result.get("training_match_count") or 0)
    recommendation_count = len(recommended_rows)

    average_edge = 0.0
    average_model_probability = 0.0
    if recommended_rows:
        average_edge = sum(float(row["decision"].edge or 0.0) for row in recommended_rows) / recommendation_count
        average_model_probability = sum(
            float(row["decision"].model_probability or 0.0) for row in recommended_rows
        ) / recommendation_count

    summary_col1, summary_col2, summary_col3, summary_col4 = st.columns(4)
    summary_col1.metric("候选比赛", candidate_count)
    summary_col2.metric("推荐场次", recommendation_count)
    summary_col3.metric("训练样本", training_match_count)
    summary_col4.metric("平均模型概率", f"{average_model_probability:.2%}")

    summary_col5, summary_col6, summary_col7 = st.columns(3)
    summary_col5.metric("平均 value", f"{average_edge:.2%}")
    summary_col6.metric(
        "命中候选率",
        f"{(recommendation_count / candidate_count):.2%}" if candidate_count else "0.00%",
    )
    summary_col7.metric("跳过场次", len(skipped_rows))

    if not recommended_rows:
        st.info("当前参数下没有满足条件的推荐。")
    else:
        st.markdown("#### 推荐清单")
        for index, recommendation in enumerate(recommended_rows, start=1):
            match = recommendation["match"]
            decision = recommendation["decision"]
            selection_label = SELECTION_LABELS.get(decision.selection, decision.selection)
            edge_text = f"{float(decision.edge or 0.0):.2%}"
            title = (
                f"{index}. "
                f"{match.get('home_team_canonical') or match.get('home_team') or '-'} vs "
                f"{match.get('away_team_canonical') or match.get('away_team') or '-'}"
                f" | 推荐 {selection_label} | value {edge_text}"
            )
            with st.expander(title, expanded=index <= 2):
                if strategy_name == "historical_odds_value":
                    _render_historical_recommendation_card(recommendation)
                else:
                    _render_team_strength_recommendation_card(recommendation)

    if skipped_rows:
        skip_reason_breakdown: dict[str, int] = {}
        for row in skipped_rows:
            reason = str(row.get("reason") or "")
            skip_reason_breakdown[reason] = skip_reason_breakdown.get(reason, 0) + 1

        with st.expander("未推荐比赛", expanded=False):
            breakdown_df = pd.DataFrame(
                [
                    {"原因": format_backtest_skip_reason(reason), "场次": count}
                    for reason, count in sorted(skip_reason_breakdown.items(), key=lambda item: (-item[1], item[0]))
                ]
            )
            if not breakdown_df.empty:
                st.dataframe(breakdown_df, use_container_width=True, hide_index=True)

            skipped_df = build_today_recommendation_skipped_dataframe(skipped_rows)
            if not skipped_df.empty:
                st.dataframe(skipped_df, use_container_width=True, hide_index=True)


def render_today_recommendations_page() -> None:
    """渲染今日推荐页。"""

    render_page_banner(
        title="今日推荐",
        subtitle="高进今天不发牌，只发推荐。把 live.500.com 当前还未完场的比赛池，实时转成两套可解释的下注建议，默认训练集使用球队大库。",
        emoji="🔥",
        chips=["实时候选池", "双策略解释", "参数可调", "实战推荐"],
    )

    if "today_live_snapshot" not in st.session_state:
        st.session_state["today_live_snapshot"] = None
    if "today_live_error" not in st.session_state:
        st.session_state["today_live_error"] = None
    if "today_live_fetched_at" not in st.session_state:
        st.session_state["today_live_fetched_at"] = None
    if "today_live_auto_loaded" not in st.session_state:
        st.session_state["today_live_auto_loaded"] = False

    pool_card = st.container(border=True)
    with pool_card:
        header_col1, header_col2 = st.columns([4, 1])
        header_col1.markdown("#### ⚽ 当前在售候选池")
        header_col1.caption("抓取来源：live.500.com 当日比分页，仅保留未开场和比赛进行中的场次。")
        refresh_live_snapshot = header_col2.button(
            "刷新候选池",
            key="refresh_today_live_snapshot",
            type="primary",
            use_container_width=True,
        )

        if refresh_live_snapshot or not st.session_state["today_live_auto_loaded"]:
            with st.spinner("正在抓取当前在售比赛，请稍候..."):
                try:
                    live_snapshot = fetch_live_matches_snapshot(
                        allowed_statuses=set(DEFAULT_LIVE_RECOMMENDATION_STATUS_CODES),
                    )
                    live_snapshot["matches"] = sorted(
                        live_snapshot.get("matches") or [],
                        key=lambda match: (
                            str(match.get("match_time") or ""),
                            int(match.get("fixture_id") or 0),
                        ),
                    )
                    st.session_state["today_live_snapshot"] = live_snapshot
                    st.session_state["today_live_error"] = None
                    st.session_state["today_live_fetched_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    _clear_today_recommendation_results()
                except Exception as exc:
                    st.session_state["today_live_error"] = f"抓取今日候选池失败：{exc}"
                st.session_state["today_live_auto_loaded"] = True

        live_error = st.session_state.get("today_live_error")
        if live_error:
            st.error(live_error)

        live_snapshot = st.session_state.get("today_live_snapshot")
        if not live_snapshot:
            st.info("当前还没有成功抓到今日在售比赛。")
            return

        live_matches = list(live_snapshot.get("matches") or [])
        not_started_count = sum(1 for match in live_matches if str(match.get("status_code") or "") == "0")
        in_progress_count = sum(
            1 for match in live_matches if str(match.get("status_code") or "") in {"1", "2", "3"}
        )
        competition_count = len(
            {str(match.get("competition") or "") for match in live_matches if match.get("competition")}
        )

        st.caption(
            f"比赛日：{live_snapshot.get('expect_date') or '-'} | "
            f"来源：{live_snapshot.get('source_url') or '-'} | "
            f"最近刷新：{st.session_state.get('today_live_fetched_at') or '-'}"
        )

        pool_metric_col1, pool_metric_col2, pool_metric_col3, pool_metric_col4 = st.columns(4)
        pool_metric_col1.metric("候选比赛", len(live_matches))
        pool_metric_col2.metric("未开场", not_started_count)
        pool_metric_col3.metric("进行中", in_progress_count)
        pool_metric_col4.metric("联赛数", competition_count)

        if not live_matches:
            st.warning("当前没有符合条件的未完场比赛。")
            return

        snapshot_col1, snapshot_col2 = st.columns([2.3, 1.2])
        with snapshot_col1:
            st.dataframe(
                build_live_match_pool_dataframe(live_matches),
                use_container_width=True,
                hide_index=True,
            )
        with snapshot_col2:
            league_count_df = (
                pd.DataFrame(live_matches)[["competition"]]
                .fillna("-")
                .value_counts()
                .reset_index(name="场次")
                .rename(columns={"competition": "联赛"})
                .head(8)
            )
            if not league_count_df.empty:
                st.caption("联赛分布")
                st.bar_chart(
                    league_count_df.set_index("联赛"),
                    x_label="联赛",
                    y_label="场次",
                    color="#0f766e",
                )

    live_competitions = sorted(
        {
            str(match.get("competition") or "")
            for match in live_matches
            if str(match.get("competition") or "").strip()
        }
    )
    strategy_tabs = st.tabs(list(TODAY_RECOMMENDATION_STRATEGY_OPTIONS.keys()))

    for strategy_label, tab in zip(TODAY_RECOMMENDATION_STRATEGY_OPTIONS.keys(), strategy_tabs):
        strategy_meta = TODAY_RECOMMENDATION_STRATEGY_OPTIONS[strategy_label]
        strategy_name = str(strategy_meta["strategy_name"])
        strategy_mode = str(strategy_meta["mode"])
        result_state_key = _today_recommendation_result_key(strategy_name)
        error_state_key = _today_recommendation_error_key(strategy_name)

        with tab:
            parameter_card = st.container(border=True)
            with parameter_card:
                st.markdown(f"#### 🎯 {strategy_label}")
                if strategy_mode == "value_match":
                    st.caption("用当前赔率结构去历史里找最相似的比赛，再把这些样本的赛果频率转成模型概率。")
                else:
                    st.caption("综合球队近期 form、攻防强度、主客场拆分和弱交手修正，估计预期进球后再推出胜平负概率。")

                with st.form(f"today_strategy_form_{strategy_name}"):
                    base_col1, base_col2, base_col3 = st.columns(3)
                    training_source_options = list(BACKTEST_DATA_SOURCE_OPTIONS.keys())
                    selected_training_source_label = base_col1.selectbox(
                        "策略训练集",
                        options=training_source_options,
                        index=training_source_options.index("球队大库"),
                        key=f"today_training_source_{strategy_name}",
                    )
                    selected_competitions = base_col2.multiselect(
                        "赛事选择",
                        options=live_competitions,
                        default=[],
                        key=f"today_competitions_{strategy_name}",
                    )
                    max_bets_per_day_option = base_col3.selectbox(
                        "最多推荐几场",
                        options=BACKTEST_DAILY_LIMIT_OPTIONS,
                        index=0,
                        format_func=format_daily_limit_option,
                        key=f"today_daily_limit_{strategy_name}",
                    )
                    max_bets_per_day = resolve_daily_limit_value(max_bets_per_day_option)

                    selected_training_source_meta = BACKTEST_DATA_SOURCE_OPTIONS[selected_training_source_label]
                    selected_training_db_path = selected_training_source_meta["db_path"]
                    selected_training_source_kind = str(selected_training_source_meta["source_kind"])
                    selected_training_source_label_value = str(selected_training_source_meta["source_label"])

                    fixed_stake = 10.0
                    history_match_count = 100
                    min_history_matches = 20
                    lookback_days: int | None = DEFAULT_LOOKBACK_DAYS
                    weighting_mode = "inverse_distance"
                    value_mode = DEFAULT_VALUE_MODE
                    staking_mode = "fixed"
                    same_competition_only = strategy_mode == "team_strength"
                    initial_bankroll = 1000.0
                    kelly_fraction = 0.25
                    max_stake_pct = 0.02
                    form_window_matches = TEAM_STRENGTH_DEFAULT_FORM_WINDOW_MATCHES
                    decay_half_life_days = TEAM_STRENGTH_DEFAULT_DECAY_HALF_LIFE_DAYS
                    bayes_prior_strength = TEAM_STRENGTH_DEFAULT_BAYES_PRIOR_STRENGTH
                    home_away_split_weight = TEAM_STRENGTH_DEFAULT_HOME_AWAY_SPLIT_WEIGHT
                    h2h_window_matches = TEAM_STRENGTH_DEFAULT_H2H_WINDOW_MATCHES
                    h2h_max_adjustment = TEAM_STRENGTH_DEFAULT_H2H_MAX_ADJUSTMENT
                    goal_cap = TEAM_STRENGTH_DEFAULT_GOAL_CAP

                    if strategy_mode == "value_match":
                        row2_col1, row2_col2, row2_col3 = st.columns(3)
                        history_match_count = int(row2_col1.selectbox("匹配样本数", options=BACKTEST_HISTORY_MATCH_COUNT_OPTIONS, index=1, key=f"today_history_match_count_{strategy_name}"))
                        min_history_matches = int(row2_col2.number_input("最小样本数", min_value=5, step=5, value=20, key=f"today_min_history_matches_{strategy_name}"))
                        lookback_days = resolve_lookback_value(
                            row2_col3.selectbox("历史回看窗口", options=BACKTEST_LOOKBACK_OPTIONS, index=4, format_func=format_lookback_option, key=f"today_lookback_{strategy_name}")
                        )

                        row3_col1, row3_col2, row3_col3 = st.columns(3)
                        weighting_mode = BACKTEST_WEIGHTING_MODE_OPTIONS[row3_col1.selectbox("样本加权方式", options=list(BACKTEST_WEIGHTING_MODE_OPTIONS.keys()), index=1, key=f"today_weighting_mode_{strategy_name}")]
                        value_mode = BACKTEST_VALUE_MODE_OPTIONS[row3_col2.selectbox("value 计算方式", options=list(BACKTEST_VALUE_MODE_OPTIONS.keys()), index=1, key=f"today_value_mode_{strategy_name}")]
                        same_competition_only = bool(row3_col3.checkbox("仅同联赛历史样本", value=False, key=f"today_same_competition_only_{strategy_name}"))
                    else:
                        row2_col1, row2_col2, row2_col3 = st.columns(3)
                        lookback_days = resolve_lookback_value(
                            row2_col1.selectbox("历史回看窗口", options=BACKTEST_LOOKBACK_OPTIONS, index=4, format_func=format_lookback_option, key=f"today_lookback_{strategy_name}")
                        )
                        min_history_matches = int(row2_col2.number_input("每队最小历史样本数", min_value=3, step=1, value=6, key=f"today_min_history_matches_{strategy_name}"))
                        value_mode = BACKTEST_VALUE_MODE_OPTIONS[row2_col3.selectbox("value 计算方式", options=list(BACKTEST_VALUE_MODE_OPTIONS.keys()), index=1, key=f"today_value_mode_{strategy_name}")]

                        row3_col1, row3_col2, row3_col3 = st.columns(3)
                        same_competition_only = bool(row3_col1.checkbox("仅同联赛历史样本", value=True, key=f"today_same_competition_only_{strategy_name}"))
                        form_window_matches = int(row3_col2.number_input("近期状态窗口场数", min_value=3, max_value=20, step=1, value=TEAM_STRENGTH_DEFAULT_FORM_WINDOW_MATCHES, key=f"today_form_window_matches_{strategy_name}"))
                        decay_half_life_days = int(row3_col3.number_input("时间衰减半衰期（天）", min_value=7, max_value=365, step=7, value=TEAM_STRENGTH_DEFAULT_DECAY_HALF_LIFE_DAYS, key=f"today_decay_half_life_days_{strategy_name}"))

                        row4_col1, row4_col2, row4_col3 = st.columns(3)
                        bayes_prior_strength = float(row4_col1.number_input("贝叶斯收缩强度", min_value=1.0, max_value=30.0, step=1.0, value=TEAM_STRENGTH_DEFAULT_BAYES_PRIOR_STRENGTH, format="%.1f", key=f"today_bayes_prior_strength_{strategy_name}"))
                        home_away_split_weight = float(row4_col2.slider("主客场拆分权重", min_value=0.0, max_value=1.0, step=0.05, value=float(TEAM_STRENGTH_DEFAULT_HOME_AWAY_SPLIT_WEIGHT), key=f"today_home_away_split_weight_{strategy_name}"))
                        h2h_window_matches = int(row4_col3.number_input("交手参考场数", min_value=1, max_value=10, step=1, value=TEAM_STRENGTH_DEFAULT_H2H_WINDOW_MATCHES, key=f"today_h2h_window_matches_{strategy_name}"))

                        row5_col1, row5_col2 = st.columns(2)
                        h2h_max_adjustment = float(row5_col1.slider("交手修正上限", min_value=0.0, max_value=0.15, step=0.01, value=float(TEAM_STRENGTH_DEFAULT_H2H_MAX_ADJUSTMENT), key=f"today_h2h_max_adjustment_{strategy_name}"))
                        goal_cap = int(row5_col2.selectbox("Poisson 进球截断", options=[4, 5, 6, 7, 8], index=2, key=f"today_goal_cap_{strategy_name}"))

                    value_mode_label = format_value_mode_label(value_mode)
                    score_label = resolve_value_mode_score_label(value_mode)
                    threshold_defaults = resolve_value_mode_threshold_defaults(value_mode, strategy_name=strategy_name)

                    row_threshold_col1, row_threshold_col2, row_threshold_col3 = st.columns(3)
                    min_edge_home_win = float(row_threshold_col1.number_input(f"主胜最小{score_label}", min_value=0.0, step=0.005, value=threshold_defaults["home_win"], format="%.3f", key=f"today_min_edge_home_win_{strategy_name}", help=format_threshold_meaning(value_mode, threshold_defaults["home_win"])))
                    min_edge_draw = float(row_threshold_col2.number_input(f"平局最小{score_label}", min_value=0.0, step=0.005, value=threshold_defaults["draw"], format="%.3f", key=f"today_min_edge_draw_{strategy_name}", help=format_threshold_meaning(value_mode, threshold_defaults["draw"])))
                    min_edge_away_win = float(row_threshold_col3.number_input(f"客胜最小{score_label}", min_value=0.0, step=0.005, value=threshold_defaults["away_win"], format="%.3f", key=f"today_min_edge_away_win_{strategy_name}", help=format_threshold_meaning(value_mode, threshold_defaults["away_win"])))
                    min_edge = min(min_edge_home_win, min_edge_draw, min_edge_away_win)

                    row_staking_col1, row_staking_col2, row_staking_col3 = st.columns(3)
                    staking_mode = BACKTEST_STAKING_MODE_OPTIONS[
                        row_staking_col1.selectbox("投注模式", options=list(BACKTEST_STAKING_MODE_OPTIONS.keys()), index=0, key=f"today_staking_mode_{strategy_name}")
                    ]
                    if staking_mode == "fractional_kelly":
                        initial_bankroll = float(row_staking_col2.number_input("当前资金", min_value=100.0, step=100.0, value=1000.0, format="%.2f", key=f"today_initial_bankroll_{strategy_name}"))
                        kelly_fraction = float(row_staking_col3.number_input("Kelly 折扣", min_value=0.05, max_value=1.0, step=0.05, value=0.25, format="%.2f", key=f"today_kelly_fraction_{strategy_name}"))
                        max_stake_pct = float(st.number_input("单场最大仓位", min_value=0.005, max_value=0.2, step=0.005, value=0.02, format="%.3f", key=f"today_max_stake_pct_{strategy_name}"))
                        st.caption("当前推荐页的 Kelly 只根据你设置的当前资金给出单场建议仓位，不会像回测那样滚动更新资金曲线。")
                    else:
                        fixed_stake = float(row_staking_col2.number_input("固定投注金额", min_value=1.0, step=1.0, value=10.0, key=f"today_fixed_stake_{strategy_name}"))
                        row_staking_col3.caption(f"当前分数类型：{value_mode_label}；阈值会按主胜 / 平局 / 客胜分别生效。")

                    run_strategy = st.form_submit_button("生成今日推荐", type="primary")

                if run_strategy:
                    try:
                        with st.spinner("正在计算今日推荐，请稍候..."):
                            recommendation_date = datetime.fromisoformat(str(live_snapshot["expect_date"])).date()
                            config = BacktestConfig(
                                start_date=recommendation_date,
                                end_date=recommendation_date,
                                fixed_stake=float(fixed_stake),
                                competitions=list(selected_competitions),
                                max_bets_per_day=max_bets_per_day,
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
                                data_source_kind="live",
                                data_source_label="今日 live 候选池",
                                db_path=selected_training_db_path,
                                training_data_source_kind=selected_training_source_kind,
                                training_data_source_label=selected_training_source_label_value,
                                training_db_path=selected_training_db_path,
                            )
                            recommendation_result = _run_today_strategy_recommendations(
                                live_matches=live_matches,
                                strategy_name=strategy_name,
                                training_db_path=selected_training_db_path,
                                training_source_kind=selected_training_source_kind,
                                config=config,
                            )
                        st.session_state[result_state_key] = recommendation_result
                        st.session_state[error_state_key] = None
                    except Exception as exc:
                        st.session_state[result_state_key] = None
                        st.session_state[error_state_key] = f"生成推荐失败：{exc}"

                strategy_error = st.session_state.get(error_state_key)
                if strategy_error:
                    st.error(strategy_error)

                recommendation_result = st.session_state.get(result_state_key)
                if recommendation_result is None:
                    st.info("当前还没有生成这套策略的今日推荐。")
                else:
                    _render_today_recommendation_results(
                        strategy_name=strategy_name,
                        recommendation_result=recommendation_result,
                    )
