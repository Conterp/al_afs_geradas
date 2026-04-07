from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

from src.config.settings import LOG_PREFIX


def log_info(message: str) -> None:
    print(f"{LOG_PREFIX} [INFO] {message}")


def count_rows(df: Optional[pd.DataFrame]) -> int:
    if df is None or df.empty:
        return 0
    return int(len(df))


def count_status(df: Optional[pd.DataFrame], status_col: str, expected_value: str) -> int:
    if df is None or df.empty or status_col not in df.columns:
        return 0
    return int((df[status_col].fillna("").astype(str).str.strip() == expected_value).sum())


def build_df_execution_summary(
    df_create_results: Optional[pd.DataFrame] = None,
    df_duplicates_delete_results: Optional[pd.DataFrame] = None,
    df_paid_update_results: Optional[pd.DataFrame] = None,
    df_wrong_board_delete_results: Optional[pd.DataFrame] = None,
    df_wrong_group_move_results: Optional[pd.DataFrame] = None,
    df_no_origin_delete_results: Optional[pd.DataFrame] = None,
    df_wrong_paid_clear_results: Optional[pd.DataFrame] = None,
    execution_seconds: Optional[float] = None,
) -> pd.DataFrame:
    summary_rows: List[Dict[str, Any]] = [
        {
            "etapa": "CREATE DESTINATION ITEMS",
            "total_linhas": count_rows(df_create_results),
            "sucesso": count_status(df_create_results, "status_create", "created"),
            "erro": count_status(df_create_results, "status_create", "error"),
        },
        {
            "etapa": "DELETE DUPLICATES",
            "total_linhas": count_rows(df_duplicates_delete_results),
            "sucesso": count_status(df_duplicates_delete_results, "status_delete", "deleted"),
            "erro": count_status(df_duplicates_delete_results, "status_delete", "error"),
        },
        {
            "etapa": "UPDATE PAID",
            "total_linhas": count_rows(df_paid_update_results),
            "sucesso": count_status(df_paid_update_results, "status_update", "updated"),
            "erro": count_status(df_paid_update_results, "status_update", "error"),
        },
        {
            "etapa": "DELETE WRONG BOARD",
            "total_linhas": count_rows(df_wrong_board_delete_results),
            "sucesso": count_status(df_wrong_board_delete_results, "delete_status", "deleted"),
            "erro": count_status(df_wrong_board_delete_results, "delete_status", "error"),
        },
        {
            "etapa": "MOVE WRONG GROUP",
            "total_linhas": count_rows(df_wrong_group_move_results),
            "sucesso": count_status(df_wrong_group_move_results, "move_status", "moved"),
            "erro": count_status(df_wrong_group_move_results, "move_status", "error"),
        },
        {
            "etapa": "DELETE NO ORIGIN",
            "total_linhas": count_rows(df_no_origin_delete_results),
            "sucesso": count_status(df_no_origin_delete_results, "delete_status", "deleted"),
            "erro": count_status(df_no_origin_delete_results, "delete_status", "error"),
        },
        {
            "etapa": "CLEAR WRONG PAID",
            "total_linhas": count_rows(df_wrong_paid_clear_results),
            "sucesso": count_status(df_wrong_paid_clear_results, "clear_status", "cleared"),
            "erro": count_status(df_wrong_paid_clear_results, "clear_status", "error"),
        },
    ]

    df_execution_summary = pd.DataFrame(summary_rows)

    if execution_seconds is not None:
        duration_minutes = execution_seconds / 60.0
        duration_text = f"{duration_minutes:.2f} min"
        df_duration = pd.DataFrame(
            [
                {
                    "etapa": "PIPELINE DURATION",
                    "total_linhas": duration_text,
                    "sucesso": "",
                    "erro": "",
                }
            ]
        )
        df_execution_summary = pd.concat(
            [df_execution_summary, df_duration], ignore_index=True
        )

    log_info(
        f"df_execution_summary gerado com {len(df_execution_summary)} linhas"
    )
    return df_execution_summary
