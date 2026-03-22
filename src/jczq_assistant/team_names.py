"""球队名称标准化、别名映射和回填工具。"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from difflib import SequenceMatcher
import json
import re
import sqlite3
import unicodedata
from typing import Any


TEAM_NAME_ALIAS_TABLE = "team_name_aliases"
TEAM_NAME_REVIEW_TABLE = "team_name_review_decisions"
HOME_TEAM_CANONICAL_COLUMN = "home_team_canonical"
AWAY_TEAM_CANONICAL_COLUMN = "away_team_canonical"
_ALIAS_SOURCE_PRIORITY = {
    "manual": 0,
    "seed": 1,
    "auto_spacing": 2,
}

_WHITESPACE_RE = re.compile(r"[\s\u00A0\u1680\u180E\u2000-\u200B\u202F\u205F\u3000]+")
_ASCII_LETTER_RE = re.compile(r"[A-Za-z]")
_SEPARATOR_STRIP_RE = re.compile(r"[\s\-.·•'\"_/():]+")

_PUNCTUATION_TRANSLATION = str.maketrans(
    {
        "，": ",",
        "。": ".",
        "、": ",",
        "；": ";",
        "：": ":",
        "（": "(",
        "）": ")",
        "【": "[",
        "】": "]",
        "［": "[",
        "］": "]",
        "｛": "{",
        "｝": "}",
        "－": "-",
        "—": "-",
        "–": "-",
        "·": " ",
        "•": " ",
        "／": "/",
    }
)

_UPPERCASE_TOKENS = {"FC", "CF", "SC", "AC", "AFC", "BSC", "PSG", "SV", "IFK", "FK"}

DEFAULT_TEAM_NAME_ALIAS_ROWS = [
    {
        "alias_name": "巴萨",
        "canonical_name": "巴塞罗那",
        "source": "seed",
        "confidence": 1.0,
    },
    {
        "alias_name": "巴 萨",
        "canonical_name": "巴塞罗那",
        "source": "seed",
        "confidence": 1.0,
    },
    {
        "alias_name": "曼城",
        "canonical_name": "曼彻斯特城",
        "source": "seed",
        "confidence": 1.0,
    },
]


@dataclass(frozen=True)
class TeamTableSpec:
    """描述一个含有主客队字段的 SQLite 表。"""

    table_name: str
    id_column: str = "id"
    home_column: str = "home_team"
    away_column: str = "away_team"
    home_canonical_column: str = HOME_TEAM_CANONICAL_COLUMN
    away_canonical_column: str = AWAY_TEAM_CANONICAL_COLUMN


def clean_team_name(name: str | None) -> str:
    """做基础自动清洗，不引入人工别名映射。"""

    if name is None:
        return ""

    normalized = unicodedata.normalize("NFKC", str(name))
    normalized = normalized.translate(_PUNCTUATION_TRANSLATION)
    normalized = _WHITESPACE_RE.sub(" ", normalized).strip()
    normalized = normalized.strip("-_/")

    if not normalized:
        return ""

    if _ASCII_LETTER_RE.search(normalized):
        normalized = _normalize_ascii_team_name(normalized)

    return normalized


def _normalize_ascii_team_name(name: str) -> str:
    """统一英文队名的基础格式。"""

    tokens: list[str] = []

    for raw_token in name.split(" "):
        token = raw_token.strip()
        if not token:
            continue

        upper_token = token.upper()
        if upper_token in _UPPERCASE_TOKENS:
            tokens.append(upper_token)
            continue

        if token.isupper() and len(token) <= 4:
            tokens.append(token)
            continue

        if "-" in token:
            pieces = [piece.capitalize() if piece else "" for piece in token.split("-")]
            tokens.append("-".join(pieces))
            continue

        tokens.append(token.capitalize())

    return " ".join(tokens)


def compact_team_name(name: str | None) -> str:
    """构造一个更激进的对比 key，用于候选别名发现。"""

    cleaned = clean_team_name(name)
    if not cleaned:
        return ""

    compact = _SEPARATOR_STRIP_RE.sub("", cleaned)
    return compact.casefold()


def derive_spacing_canonical_name(name: str | None) -> str | None:
    """对纯中文/非英文名的中间空格做自动确认。"""

    cleaned = clean_team_name(name)
    if not cleaned or " " not in cleaned:
        return None

    if _ASCII_LETTER_RE.search(cleaned):
        return None

    collapsed = cleaned.replace(" ", "")
    if collapsed and collapsed != cleaned:
        return collapsed
    return None


def _build_default_alias_map() -> dict[str, str]:
    """构造种子别名映射。"""

    alias_map: dict[str, str] = {}
    for row in DEFAULT_TEAM_NAME_ALIAS_ROWS:
        canonical_name = clean_team_name(row["canonical_name"])
        alias_name = clean_team_name(row["alias_name"])
        if alias_name:
            alias_map[alias_name] = canonical_name
        compact_alias = compact_team_name(alias_name)
        if compact_alias:
            alias_map[compact_alias] = canonical_name
    return alias_map


def ensure_team_name_aliases_table(connection: sqlite3.Connection) -> None:
    """创建并填充球队别名表。"""

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {TEAM_NAME_ALIAS_TABLE} (
        alias_name TEXT PRIMARY KEY,
        canonical_name TEXT NOT NULL,
        source TEXT NOT NULL DEFAULT 'seed',
        confidence REAL NOT NULL DEFAULT 1.0,
        updated_at TEXT NOT NULL
    );
    """

    create_index_sql = f"""
    CREATE INDEX IF NOT EXISTS idx_{TEAM_NAME_ALIAS_TABLE}_canonical_name
    ON {TEAM_NAME_ALIAS_TABLE}(canonical_name);
    """

    connection.execute(create_table_sql)
    connection.execute(create_index_sql)

    now = datetime.now().isoformat(timespec="seconds")
    seed_sql = f"""
    INSERT INTO {TEAM_NAME_ALIAS_TABLE} (
        alias_name,
        canonical_name,
        source,
        confidence,
        updated_at
    ) VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(alias_name) DO UPDATE SET
        canonical_name = excluded.canonical_name,
        source = excluded.source,
        confidence = excluded.confidence,
        updated_at = excluded.updated_at
    """

    for row in DEFAULT_TEAM_NAME_ALIAS_ROWS:
        alias_name = clean_team_name(row["alias_name"])
        canonical_name = clean_team_name(row["canonical_name"])
        if not alias_name or not canonical_name:
            continue

        existing_row = connection.execute(
            f"""
            SELECT source
            FROM {TEAM_NAME_ALIAS_TABLE}
            WHERE alias_name = ?
            """,
            (alias_name,),
        ).fetchone()
        if existing_row is not None and str(existing_row["source"] or "") == "manual":
            continue

        connection.execute(
            seed_sql,
            (
                alias_name,
                canonical_name,
                row["source"],
                row["confidence"],
                now,
            ),
        )


def ensure_team_name_review_table(connection: sqlite3.Connection) -> None:
    """创建球队名称候选处理决策表。"""

    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TEAM_NAME_REVIEW_TABLE} (
            group_key TEXT PRIMARY KEY,
            decision_type TEXT NOT NULL,
            chosen_canonical_name TEXT,
            variants_json TEXT,
            updated_at TEXT NOT NULL
        );
        """
    )
    connection.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_{TEAM_NAME_REVIEW_TABLE}_decision_type
        ON {TEAM_NAME_REVIEW_TABLE}(decision_type);
        """
    )


def upsert_team_name_alias(
    connection: sqlite3.Connection,
    *,
    alias_name: str,
    canonical_name: str,
    source: str,
    confidence: float = 1.0,
) -> None:
    """幂等写入一条别名映射。"""

    ensure_team_name_aliases_table(connection)

    cleaned_alias = clean_team_name(alias_name)
    cleaned_canonical = clean_team_name(canonical_name)
    if not cleaned_alias or not cleaned_canonical:
        return

    now = datetime.now().isoformat(timespec="seconds")
    connection.execute(
        f"""
        INSERT INTO {TEAM_NAME_ALIAS_TABLE} (
            alias_name,
            canonical_name,
            source,
            confidence,
            updated_at
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(alias_name) DO UPDATE SET
            canonical_name = excluded.canonical_name,
            source = excluded.source,
            confidence = excluded.confidence,
            updated_at = excluded.updated_at
        """,
        (cleaned_alias, cleaned_canonical, source, confidence, now),
    )


def list_team_name_aliases(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
    sources: list[str] | None = None,
) -> list[dict[str, Any]]:
    """列出已确认的球队名称映射。"""

    ensure_team_name_aliases_table(connection)

    where_sql = ""
    params: list[Any] = []
    if sources:
        placeholders = ", ".join("?" for _ in sources)
        where_sql = f"WHERE source IN ({placeholders})"
        params.extend(sources)

    rows = connection.execute(
        f"""
        SELECT
            alias_name,
            canonical_name,
            source,
            confidence,
            updated_at
        FROM {TEAM_NAME_ALIAS_TABLE}
        {where_sql}
        ORDER BY
            CASE source
                WHEN 'manual' THEN 0
                WHEN 'seed' THEN 1
                WHEN 'auto_spacing' THEN 2
                ELSE 3
            END,
            canonical_name,
            alias_name
        LIMIT ?
        """,
        [*params, limit],
    ).fetchall()
    return [dict(row) for row in rows]


def upsert_team_name_review_decision(
    connection: sqlite3.Connection,
    *,
    group_key: str,
    decision_type: str,
    chosen_canonical_name: str | None = None,
    variants: list[dict[str, Any]] | None = None,
) -> None:
    """写入一条候选组处理决策。"""

    ensure_team_name_review_table(connection)

    cleaned_group_key = str(group_key).strip()
    if not cleaned_group_key:
        return

    cleaned_canonical = clean_team_name(chosen_canonical_name)
    variants_json = None
    if variants:
        variants_json = json.dumps(variants, ensure_ascii=False, sort_keys=True)

    connection.execute(
        f"""
        INSERT INTO {TEAM_NAME_REVIEW_TABLE} (
            group_key,
            decision_type,
            chosen_canonical_name,
            variants_json,
            updated_at
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(group_key) DO UPDATE SET
            decision_type = excluded.decision_type,
            chosen_canonical_name = excluded.chosen_canonical_name,
            variants_json = excluded.variants_json,
            updated_at = excluded.updated_at
        """,
        (
            cleaned_group_key,
            decision_type,
            cleaned_canonical or None,
            variants_json,
            datetime.now().isoformat(timespec="seconds"),
        ),
    )


def delete_team_name_review_decision(
    connection: sqlite3.Connection,
    *,
    group_key: str,
) -> None:
    """删除一条候选组处理决策。"""

    ensure_team_name_review_table(connection)
    connection.execute(
        f"DELETE FROM {TEAM_NAME_REVIEW_TABLE} WHERE group_key = ?",
        (str(group_key).strip(),),
    )


def list_team_name_review_decisions(
    connection: sqlite3.Connection,
    *,
    decision_type: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """列出球队名称候选处理决策。"""

    ensure_team_name_review_table(connection)

    where_sql = ""
    params: list[Any] = []
    if decision_type:
        where_sql = "WHERE decision_type = ?"
        params.append(decision_type)

    rows = connection.execute(
        f"""
        SELECT
            group_key,
            decision_type,
            chosen_canonical_name,
            variants_json,
            updated_at
        FROM {TEAM_NAME_REVIEW_TABLE}
        {where_sql}
        ORDER BY updated_at DESC, group_key
        LIMIT ?
        """,
        [*params, limit],
    ).fetchall()

    decisions: list[dict[str, Any]] = []
    for row in rows:
        decision = dict(row)
        if decision.get("variants_json"):
            decision["variants"] = json.loads(str(decision["variants_json"]))
        else:
            decision["variants"] = []
        decisions.append(decision)
    return decisions


def load_skipped_team_name_group_keys(connection: sqlite3.Connection) -> set[str]:
    """加载已标记为跳过的候选组 key。"""

    ensure_team_name_review_table(connection)
    rows = connection.execute(
        f"""
        SELECT group_key
        FROM {TEAM_NAME_REVIEW_TABLE}
        WHERE decision_type = 'skip'
        """
    ).fetchall()
    return {str(row["group_key"]) for row in rows if row["group_key"]}


def load_team_name_aliases(connection: sqlite3.Connection) -> dict[str, str]:
    """从 SQLite 加载别名映射。"""

    ensure_team_name_aliases_table(connection)
    alias_rows = connection.execute(
        f"""
        SELECT alias_name, canonical_name, source
        FROM {TEAM_NAME_ALIAS_TABLE}
        WHERE alias_name IS NOT NULL
          AND canonical_name IS NOT NULL
        ORDER BY
            CASE source
                WHEN 'manual' THEN 0
                WHEN 'seed' THEN 1
                WHEN 'auto_spacing' THEN 2
                ELSE 3
            END,
            alias_name
        """
    ).fetchall()

    alias_map = _build_default_alias_map()
    for row in alias_rows:
        alias_name = clean_team_name(row["alias_name"])
        canonical_name = clean_team_name(row["canonical_name"])
        if alias_name:
            alias_map.setdefault(alias_name, canonical_name)
        compact_alias = compact_team_name(alias_name)
        if compact_alias:
            alias_map.setdefault(compact_alias, canonical_name)
    return alias_map


def upsert_auto_spacing_aliases(
    connection: sqlite3.Connection,
    table_spec: TeamTableSpec,
) -> dict[str, int | str]:
    """把“仅空格差异”的别名自动写入映射表。"""

    ensure_team_name_aliases_table(connection)
    alias_map = load_team_name_aliases(connection)

    rows = connection.execute(
        f"""
        SELECT DISTINCT team_name
        FROM (
            SELECT {table_spec.home_column} AS team_name FROM {table_spec.table_name}
            UNION
            SELECT {table_spec.away_column} AS team_name FROM {table_spec.table_name}
        )
        WHERE team_name IS NOT NULL
          AND TRIM(team_name) <> ''
        """
    ).fetchall()

    inserted_count = 0
    for row in rows:
        team_name = clean_team_name(row["team_name"])
        collapsed_name = derive_spacing_canonical_name(team_name)
        if not collapsed_name:
            continue
        spacing_canonical = normalize_team_name(collapsed_name, alias_map=alias_map)

        existing_row = connection.execute(
            f"""
            SELECT canonical_name, source
            FROM {TEAM_NAME_ALIAS_TABLE}
            WHERE alias_name = ?
            """,
            (team_name,),
        ).fetchone()
        if existing_row is not None:
            existing_canonical = clean_team_name(existing_row["canonical_name"])
            existing_source = str(existing_row["source"] or "")
            if existing_source != "auto_spacing":
                continue
            if existing_canonical == spacing_canonical:
                continue

        upsert_team_name_alias(
            connection,
            alias_name=team_name,
            canonical_name=spacing_canonical,
            source="auto_spacing",
            confidence=0.99,
        )
        inserted_count += 1

    return {
        "table_name": table_spec.table_name,
        "auto_spacing_aliases_upserted": inserted_count,
    }


def normalize_team_name(
    name: str | None,
    *,
    alias_map: Mapping[str, str] | None = None,
) -> str:
    """返回球队标准名。"""

    cleaned = clean_team_name(name)
    if not cleaned:
        return ""

    effective_alias_map = alias_map or _build_default_alias_map()
    compact_name = compact_team_name(cleaned)
    spacing_canonical = derive_spacing_canonical_name(cleaned)
    resolved_spacing_canonical = ""
    if spacing_canonical:
        resolved_spacing_canonical = (
            effective_alias_map.get(spacing_canonical)
            or effective_alias_map.get(compact_team_name(spacing_canonical))
            or spacing_canonical
        )

    return (
        effective_alias_map.get(cleaned)
        or effective_alias_map.get(compact_name)
        or resolved_spacing_canonical
        or cleaned
    )


def normalize_match_teams(
    home_team: str | None,
    away_team: str | None,
    *,
    alias_map: Mapping[str, str] | None = None,
) -> tuple[str, str]:
    """同时规范化主客队名称。"""

    return (
        normalize_team_name(home_team, alias_map=alias_map),
        normalize_team_name(away_team, alias_map=alias_map),
    )


def attach_canonical_team_names(
    record: dict[str, Any],
    *,
    alias_map: Mapping[str, str] | None = None,
    home_column: str = "home_team",
    away_column: str = "away_team",
    home_canonical_column: str = HOME_TEAM_CANONICAL_COLUMN,
    away_canonical_column: str = AWAY_TEAM_CANONICAL_COLUMN,
) -> dict[str, Any]:
    """为一条比赛记录补上 canonical 字段。"""

    enriched_record = dict(record)
    home_canonical, away_canonical = normalize_match_teams(
        enriched_record.get(home_column),
        enriched_record.get(away_column),
        alias_map=alias_map,
    )
    enriched_record[home_canonical_column] = home_canonical
    enriched_record[away_canonical_column] = away_canonical
    return enriched_record


def ensure_team_canonical_columns(
    connection: sqlite3.Connection,
    table_name: str,
    *,
    home_canonical_column: str = HOME_TEAM_CANONICAL_COLUMN,
    away_canonical_column: str = AWAY_TEAM_CANONICAL_COLUMN,
) -> None:
    """为目标表补齐 canonical 字段。"""

    existing_columns = {
        row[1] for row in connection.execute(f"PRAGMA table_info({table_name})")
    }

    required_columns = {
        home_canonical_column: "TEXT",
        away_canonical_column: "TEXT",
    }

    for column_name, column_type in required_columns.items():
        if column_name not in existing_columns:
            connection.execute(
                f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
            )


def backfill_team_canonical_columns(
    connection: sqlite3.Connection,
    table_spec: TeamTableSpec,
) -> dict[str, int | str]:
    """幂等回填某张表的 canonical 字段。"""

    ensure_team_name_aliases_table(connection)
    ensure_team_canonical_columns(
        connection,
        table_spec.table_name,
        home_canonical_column=table_spec.home_canonical_column,
        away_canonical_column=table_spec.away_canonical_column,
    )
    upsert_auto_spacing_aliases(connection, table_spec)
    alias_map = load_team_name_aliases(connection)

    rows = connection.execute(
        f"""
        SELECT
            {table_spec.id_column} AS row_id,
            {table_spec.home_column} AS home_team,
            {table_spec.away_column} AS away_team,
            {table_spec.home_canonical_column} AS home_team_canonical,
            {table_spec.away_canonical_column} AS away_team_canonical
        FROM {table_spec.table_name}
        """
    ).fetchall()

    updates: list[tuple[str, str, Any]] = []
    for row in rows:
        home_canonical, away_canonical = normalize_match_teams(
            row["home_team"],
            row["away_team"],
            alias_map=alias_map,
        )
        if (
            clean_team_name(row["home_team_canonical"]) != home_canonical
            or clean_team_name(row["away_team_canonical"]) != away_canonical
        ):
            updates.append((home_canonical, away_canonical, row["row_id"]))

    if updates:
        connection.executemany(
            f"""
            UPDATE {table_spec.table_name}
            SET {table_spec.home_canonical_column} = ?,
                {table_spec.away_canonical_column} = ?
            WHERE {table_spec.id_column} = ?
            """,
            updates,
        )

    return {
        "table_name": table_spec.table_name,
        "rows_scanned": len(rows),
        "rows_updated": len(updates),
    }


def apply_team_name_candidate_unification(
    connection: sqlite3.Connection,
    table_spec: TeamTableSpec,
    *,
    group_key: str,
    canonical_name: str,
    variants: list[dict[str, Any]],
) -> dict[str, int | str]:
    """把一个候选组统一到指定标准名。"""

    chosen_canonical = clean_team_name(canonical_name)
    if not chosen_canonical:
        raise ValueError("canonical_name 不能为空。")

    unique_variant_names = sorted(
        {
            clean_team_name(variant.get("team_name"))
            for variant in variants
            if clean_team_name(variant.get("team_name"))
        }
    )
    if not unique_variant_names:
        raise ValueError("variants 里没有可用的 team_name。")

    for variant_name in unique_variant_names:
        upsert_team_name_alias(
            connection,
            alias_name=variant_name,
            canonical_name=chosen_canonical,
            source="manual",
            confidence=1.0,
        )

    upsert_team_name_review_decision(
        connection,
        group_key=group_key,
        decision_type="unify",
        chosen_canonical_name=chosen_canonical,
        variants=variants,
    )
    backfill_summary = backfill_team_canonical_columns(connection, table_spec)
    return {
        "group_key": str(group_key),
        "canonical_name": chosen_canonical,
        "aliases_updated": len(unique_variant_names),
        **backfill_summary,
    }


def apply_manual_team_name_alias(
    connection: sqlite3.Connection,
    table_spec: TeamTableSpec,
    *,
    alias_name: str,
    canonical_name: str,
) -> dict[str, int | str]:
    """手工确认一条球队别名并回填 canonical。"""

    cleaned_alias = clean_team_name(alias_name)
    cleaned_canonical = clean_team_name(canonical_name)
    if not cleaned_alias or not cleaned_canonical:
        raise ValueError("alias_name 和 canonical_name 都不能为空。")

    upsert_team_name_alias(
        connection,
        alias_name=cleaned_alias,
        canonical_name=cleaned_canonical,
        source="manual",
        confidence=1.0,
    )
    backfill_summary = backfill_team_canonical_columns(connection, table_spec)
    return {
        "alias_name": cleaned_alias,
        "canonical_name": cleaned_canonical,
        **backfill_summary,
    }


def skip_team_name_candidate(
    connection: sqlite3.Connection,
    *,
    group_key: str,
    variants: list[dict[str, Any]],
) -> None:
    """把一个候选组标记为暂不统一。"""

    upsert_team_name_review_decision(
        connection,
        group_key=group_key,
        decision_type="skip",
        variants=variants,
    )


def find_team_alias_candidates(
    connection: sqlite3.Connection,
    table_spec: TeamTableSpec,
    *,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """找出最可能需要人工合并的球队名候选。"""

    ensure_team_name_review_table(connection)
    skipped_group_keys = load_skipped_team_name_group_keys(connection)

    name_rows = connection.execute(
        f"""
        SELECT
            team_name,
            canonical_name,
            COUNT(*) AS row_count
        FROM (
            SELECT
                {table_spec.home_column} AS team_name,
                COALESCE({table_spec.home_canonical_column}, '') AS canonical_name
            FROM {table_spec.table_name}
            UNION ALL
            SELECT
                {table_spec.away_column} AS team_name,
                COALESCE({table_spec.away_canonical_column}, '') AS canonical_name
            FROM {table_spec.table_name}
        )
        WHERE team_name IS NOT NULL
          AND TRIM(team_name) <> ''
        GROUP BY team_name, canonical_name
        """
    ).fetchall()

    variants_by_name: dict[str, dict[str, Any]] = {}
    for row in name_rows:
        team_name = clean_team_name(row["team_name"])
        if not team_name:
            continue
        variants_by_name[team_name] = {
            "team_name": team_name,
            "canonical_name": clean_team_name(row["canonical_name"]),
            "row_count": int(row["row_count"] or 0),
            "compact_key": compact_team_name(team_name),
        }

    compact_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for variant in variants_by_name.values():
        if variant["compact_key"]:
            compact_groups[variant["compact_key"]].append(variant)

    candidates: list[dict[str, Any]] = []
    seen_group_keys: set[str] = set()

    for compact_key, variants in compact_groups.items():
        if len(variants) <= 1:
            continue
        if compact_key in skipped_group_keys:
            continue
        sorted_variants = sorted(
            variants,
            key=lambda item: (-int(item["row_count"]), str(item["team_name"])),
        )
        canonical_names = {
            item["canonical_name"] for item in sorted_variants if item["canonical_name"]
        }
        has_unresolved_variant = any(
            not clean_team_name(item["canonical_name"]) for item in sorted_variants
        )
        if len(canonical_names) == 1 and not has_unresolved_variant:
            continue
        candidates.append(
            {
                "reason": "same_compact_key",
                "group_key": compact_key,
                "variant_count": len(sorted_variants),
                "total_count": sum(int(item["row_count"]) for item in sorted_variants),
                "canonical_names": sorted(canonical_names),
                "variants": [
                    {
                        "team_name": item["team_name"],
                        "canonical_name": item["canonical_name"],
                        "row_count": int(item["row_count"]),
                    }
                    for item in sorted_variants
                ],
            }
        )
        seen_group_keys.add(compact_key)

    top_variants = sorted(
        variants_by_name.values(),
        key=lambda item: (-int(item["row_count"]), str(item["team_name"])),
    )[:300]

    for index, left_variant in enumerate(top_variants):
        left_key = str(left_variant["compact_key"])
        if not left_key:
            continue
        for right_variant in top_variants[index + 1 :]:
            right_key = str(right_variant["compact_key"])
            if not right_key or left_key == right_key:
                continue
            if left_key[0] != right_key[0]:
                continue

            similarity = SequenceMatcher(None, left_key, right_key).ratio()
            if similarity < 0.86:
                continue

            resolved_canonical_names = {
                left_variant["canonical_name"],
                right_variant["canonical_name"],
            } - {""}
            if len(resolved_canonical_names) == 1 and (
                left_variant["canonical_name"] and right_variant["canonical_name"]
            ):
                continue

            group_key = "::".join(sorted([left_key, right_key]))
            if group_key in seen_group_keys:
                continue
            if group_key in skipped_group_keys:
                continue

            candidates.append(
                {
                    "reason": "high_similarity",
                    "group_key": group_key,
                    "variant_count": 2,
                    "total_count": int(left_variant["row_count"])
                    + int(right_variant["row_count"]),
                    "similarity": round(similarity, 4),
                    "canonical_names": sorted(
                        {
                            left_variant["canonical_name"],
                            right_variant["canonical_name"],
                        }
                        - {""}
                    ),
                    "variants": [
                        {
                            "team_name": left_variant["team_name"],
                            "canonical_name": left_variant["canonical_name"],
                            "row_count": int(left_variant["row_count"]),
                        },
                        {
                            "team_name": right_variant["team_name"],
                            "canonical_name": right_variant["canonical_name"],
                            "row_count": int(right_variant["row_count"]),
                        },
                    ],
                }
            )
            seen_group_keys.add(group_key)

    candidates.sort(
        key=lambda item: (
            -int(item["total_count"]),
            -int(item["variant_count"]),
            str(item["group_key"]),
        )
    )
    return candidates[:limit]
