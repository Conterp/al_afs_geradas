from __future__ import annotations

import pandas as pd

from src.config.settings import LOG_PREFIX
from src.core.monday.destination.fetch_destination_audit_items import (
    build_df_destination_audit,
)


def log_info(message: str) -> None:
    print(f"{LOG_PREFIX} [INFO] {message}")


def log_warn(message: str) -> None:
    print(f"{LOG_PREFIX} [WARN] {message}")


def build_df_duplicates_summary(df_destination_audit: pd.DataFrame) -> pd.DataFrame:
    if df_destination_audit is None or df_destination_audit.empty:
        df_duplicates_summary = pd.DataFrame(
            columns=["afs", "board_id", "board_name", "duplicate_count"]
        )
        log_warn("df_destination_audit vazio; nenhuma duplicidade para validar")
        return df_duplicates_summary

    df = df_destination_audit.copy()

    df["afs"] = df["afs"].fillna("").astype(str).str.strip()
    df["board_id"] = df["board_id"].fillna("").astype(str).str.strip()
    df["board_name"] = df["board_name"].fillna("").astype(str).str.strip()

    df = df[df["afs"] != ""].copy()

    df_duplicates_summary = (
        df.groupby(["afs", "board_id", "board_name"], as_index=False)
        .size()
        .rename(columns={"size": "duplicate_count"})
    )

    df_duplicates_summary = df_duplicates_summary[
        df_duplicates_summary["duplicate_count"] > 1
    ].copy()

    df_duplicates_summary = df_duplicates_summary.sort_values(
        by=["board_name", "afs"]
    ).reset_index(drop=True)

    log_info(
        f"df_duplicates_summary gerado com {len(df_duplicates_summary)} chaves duplicadas"
    )
    return df_duplicates_summary


def build_df_duplicates(
    df_destination_audit: pd.DataFrame,
    df_duplicates_summary: pd.DataFrame,
) -> pd.DataFrame:
    if (
        df_destination_audit is None
        or df_destination_audit.empty
        or df_duplicates_summary is None
        or df_duplicates_summary.empty
    ):
        df_duplicates = pd.DataFrame(
            columns=[
                "afs",
                "id_item_monday",
                "item_name",
                "board_id",
                "board_name",
                "group_id",
                "group_title",
                "pago",
                "af_date",
                "duplicate_count",
            ]
        )
        log_warn("Nenhuma duplicidade detalhada encontrada")
        return df_duplicates

    df = df_destination_audit.copy()
    df["afs"] = df["afs"].fillna("").astype(str).str.strip()
    df["board_id"] = df["board_id"].fillna("").astype(str).str.strip()

    df_duplicates = df.merge(
        df_duplicates_summary,
        on=["afs", "board_id", "board_name"],
        how="inner",
    )

    df_duplicates = df_duplicates[
        [
            "afs",
            "id_item_monday",
            "item_name",
            "board_id",
            "board_name",
            "group_id",
            "group_title",
            "pago",
            "af_date",
            "duplicate_count",
        ]
    ].copy()

    df_duplicates = df_duplicates.sort_values(
        by=["board_name", "afs", "af_date", "id_item_monday"]
    ).reset_index(drop=True)

    log_info(f"df_duplicates gerado com {len(df_duplicates)} linhas detalhadas")
    return df_duplicates


if __name__ == "__main__":
    log_info("Executando teste local de find_duplicate_items")
    df_destination_audit = build_df_destination_audit()

    df_duplicates_summary = build_df_duplicates_summary(
        df_destination_audit=df_destination_audit
    )
    print(df_duplicates_summary)
    print(f"Total duplicate keys: {len(df_duplicates_summary)}")

    df_duplicates = build_df_duplicates(
        df_destination_audit=df_destination_audit,
        df_duplicates_summary=df_duplicates_summary,
    )
    print(df_duplicates)
    print(f"Total duplicate rows: {len(df_duplicates)}")
