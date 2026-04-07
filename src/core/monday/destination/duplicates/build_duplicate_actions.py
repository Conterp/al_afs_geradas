from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from src.config.settings import LOG_PREFIX
from src.core.monday.destination.duplicates.find_duplicate_items import (
    build_df_duplicates,
    build_df_duplicates_summary,
)
from src.core.monday.destination.fetch_destination_audit_items import (
    build_df_destination_audit,
)


def log_info(message: str) -> None:
    print(f"{LOG_PREFIX} [INFO] {message}")


def log_warn(message: str) -> None:
    print(f"{LOG_PREFIX} [WARN] {message}")


def normalize_paid_value(value: Any) -> str:
    return str(value or "").strip().lower()


def is_paid_yes(value: Any) -> bool:
    paid_norm = normalize_paid_value(value)
    return paid_norm in {"sim", "yes", "true", "1"}


def count_filled_fields(row: pd.Series, fields: List[str]) -> int:
    count = 0
    for field in fields:
        value = row.get(field, "")
        if pd.notna(value) and str(value).strip() != "":
            count += 1
    return count


def safe_sort_date(value: Any) -> str:
    value_str = str(value or "").strip()
    return value_str if value_str else "9999-12-31"


def build_df_duplicate_actions(df_duplicates: pd.DataFrame) -> pd.DataFrame:
    if df_duplicates is None or df_duplicates.empty:
        df_duplicate_actions = pd.DataFrame(
            columns=[
                "afs",
                "board_id",
                "board_name",
                "id_item_monday",
                "item_name",
                "group_id",
                "group_title",
                "pago",
                "af_date",
                "duplicate_count",
                "filled_score",
                "rank_duplicate",
                "duplicate_action",
                "duplicate_reason",
            ]
        )
        log_warn("df_duplicates vazio; nenhum plano de acao para duplicados")
        return df_duplicate_actions

    df = df_duplicates.copy()

    score_fields = [
        "item_name",
        "group_id",
        "group_title",
        "pago",
        "af_date",
    ]

    df["paid_priority"] = df["pago"].apply(is_paid_yes).astype(int)
    df["filled_score"] = df.apply(
        lambda row: count_filled_fields(row, score_fields),
        axis=1,
    )
    df["safe_date"] = df["af_date"].apply(safe_sort_date)
    df["safe_item_id"] = df["id_item_monday"].astype(str).str.strip()

    grouped = df.groupby(["afs", "board_id", "board_name"], dropna=False)
    planned_rows: List[Dict[str, Any]] = []

    for _, group_df in grouped:
        sorted_group = group_df.sort_values(
            by=["paid_priority", "filled_score", "safe_date", "safe_item_id"],
            ascending=[False, False, True, True],
        ).reset_index(drop=True)

        for idx, (_, row) in enumerate(sorted_group.iterrows(), start=1):
            row_dict = row.to_dict()
            row_dict["rank_duplicate"] = idx

            if idx == 1:
                row_dict["duplicate_action"] = "KEEP"
                if row_dict["paid_priority"] == 1:
                    row_dict["duplicate_reason"] = "kept_by_paid_flag"
                elif row_dict["filled_score"] > 0:
                    row_dict["duplicate_reason"] = "kept_by_data_completeness"
                else:
                    row_dict["duplicate_reason"] = "kept_by_tiebreaker"
            else:
                row_dict["duplicate_action"] = "DELETE"
                row_dict["duplicate_reason"] = "duplicate_scheduled_for_deletion"

            planned_rows.append(row_dict)

    df_duplicate_actions = pd.DataFrame(planned_rows)

    keep_cols = [
        "afs",
        "board_id",
        "board_name",
        "id_item_monday",
        "item_name",
        "group_id",
        "group_title",
        "pago",
        "af_date",
        "duplicate_count",
        "filled_score",
        "rank_duplicate",
        "duplicate_action",
        "duplicate_reason",
    ]
    df_duplicate_actions = df_duplicate_actions[keep_cols].copy()

    df_duplicate_actions = df_duplicate_actions.sort_values(
        by=["board_name", "afs", "rank_duplicate"]
    ).reset_index(drop=True)

    log_info(f"df_duplicate_actions gerado com {len(df_duplicate_actions)} linhas")
    return df_duplicate_actions


def build_df_duplicates_keep(df_duplicate_actions: pd.DataFrame) -> pd.DataFrame:
    if df_duplicate_actions is None or df_duplicate_actions.empty:
        return pd.DataFrame(
            columns=df_duplicate_actions.columns if df_duplicate_actions is not None else []
        )
    return df_duplicate_actions[
        df_duplicate_actions["duplicate_action"] == "KEEP"
    ].copy()


def build_df_duplicates_delete(df_duplicate_actions: pd.DataFrame) -> pd.DataFrame:
    if df_duplicate_actions is None or df_duplicate_actions.empty:
        return pd.DataFrame(
            columns=df_duplicate_actions.columns if df_duplicate_actions is not None else []
        )
    return df_duplicate_actions[
        df_duplicate_actions["duplicate_action"] == "DELETE"
    ].copy()


if __name__ == "__main__":
    log_info("Executando teste local de build_duplicate_actions")
    df_destination_audit = build_df_destination_audit()
    df_duplicates_summary = build_df_duplicates_summary(
        df_destination_audit=df_destination_audit
    )
    df_duplicates = build_df_duplicates(
        df_destination_audit=df_destination_audit,
        df_duplicates_summary=df_duplicates_summary,
    )

    df_duplicate_actions = build_df_duplicate_actions(df_duplicates=df_duplicates)
    df_duplicates_keep = build_df_duplicates_keep(
        df_duplicate_actions=df_duplicate_actions
    )
    df_duplicates_delete = build_df_duplicates_delete(
        df_duplicate_actions=df_duplicate_actions
    )

    print(df_duplicate_actions)
    print(f"Total duplicate actions: {len(df_duplicate_actions)}")
    print(f"Total KEEP: {len(df_duplicates_keep)}")
    print(f"Total DELETE: {len(df_duplicates_delete)}")
