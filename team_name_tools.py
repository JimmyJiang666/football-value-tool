"""球队名称标准化工具 CLI。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3
import sys


PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from jczq_assistant.sfc500_history import init_sfc500_db
from jczq_assistant.sfc500_history import SFC500_DATABASE_PATH
from jczq_assistant.team_names import (
    TeamTableSpec,
    backfill_team_canonical_columns,
    ensure_team_canonical_columns,
    ensure_team_name_aliases_table,
    find_team_alias_candidates,
    upsert_auto_spacing_aliases,
    upsert_team_name_alias,
)


TARGET_SPEC = {
    "db_path": SFC500_DATABASE_PATH,
    "table_spec": TeamTableSpec(table_name="sfc500_matches_raw"),
    "init": init_sfc500_db,
}


def _connect(db_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    return connection


def build_parser() -> argparse.ArgumentParser:
    """构建命令行参数。"""

    parser = argparse.ArgumentParser(description="球队名称标准化工具")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser(
        "backfill",
        help="回填历史主表里的 canonical 字段",
    )

    candidates_parser = subparsers.add_parser(
        "list-candidates",
        help="列出最可能需要人工合并的球队别名候选",
    )
    candidates_parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="返回多少组候选，默认 20",
    )

    aliases_parser = subparsers.add_parser(
        "list-aliases",
        help="列出当前已确认的球队别名映射",
    )
    aliases_parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="返回多少条已确认映射，默认 100",
    )

    add_alias_parser = subparsers.add_parser(
        "add-alias",
        help="手工确认一条球队别名映射",
    )
    add_alias_parser.add_argument("--alias", required=True, help="别名")
    add_alias_parser.add_argument("--canonical", required=True, help="标准名")
    add_alias_parser.add_argument(
        "--source",
        default="manual",
        help="映射来源，默认 manual",
    )
    add_alias_parser.add_argument(
        "--confidence",
        type=float,
        default=1.0,
        help="置信度，默认 1.0",
    )

    return parser


def _ensure_history_context() -> tuple[Path, sqlite3.Connection]:
    TARGET_SPEC["init"]()
    db_path = Path(TARGET_SPEC["db_path"])
    connection = _connect(db_path)
    ensure_team_name_aliases_table(connection)
    ensure_team_canonical_columns(
        connection,
        TARGET_SPEC["table_spec"].table_name,
    )
    return db_path, connection


def main(argv: list[str] | None = None) -> int:
    """CLI 入口。"""

    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "backfill":
        db_path, connection = _ensure_history_context()
        with connection:
            auto_summary = upsert_auto_spacing_aliases(
                connection,
                TARGET_SPEC["table_spec"],
            )
            summary = backfill_team_canonical_columns(
                connection,
                TARGET_SPEC["table_spec"],
            )
        print(
            json.dumps(
                {
                    "db_path": str(db_path),
                    **auto_summary,
                    **summary,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    if args.command == "list-candidates":
        db_path, connection = _ensure_history_context()
        with connection:
            upsert_auto_spacing_aliases(
                connection,
                TARGET_SPEC["table_spec"],
            )
            backfill_team_canonical_columns(
                connection,
                TARGET_SPEC["table_spec"],
            )
            candidates = find_team_alias_candidates(
                connection,
                TARGET_SPEC["table_spec"],
                limit=args.limit,
            )

        print(
            json.dumps(
                {
                    "db_path": str(db_path),
                    "table_name": TARGET_SPEC["table_spec"].table_name,
                    "candidates": candidates,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    if args.command == "add-alias":
        db_path, connection = _ensure_history_context()
        with connection:
            upsert_team_name_alias(
                connection,
                alias_name=args.alias,
                canonical_name=args.canonical,
                source=args.source,
                confidence=args.confidence,
            )
            summary = backfill_team_canonical_columns(
                connection,
                TARGET_SPEC["table_spec"],
            )

        print(
            json.dumps(
                {
                    "db_path": str(db_path),
                    "alias": args.alias,
                    "canonical": args.canonical,
                    **summary,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    if args.command == "list-aliases":
        db_path, connection = _ensure_history_context()
        rows = connection.execute(
            """
            SELECT
                alias_name,
                canonical_name,
                source,
                confidence,
                updated_at
            FROM team_name_aliases
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
            (args.limit,),
        ).fetchall()

        print(
            json.dumps(
                {
                    "db_path": str(db_path),
                    "rows": [dict(row) for row in rows],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    parser.error("未识别的命令")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
