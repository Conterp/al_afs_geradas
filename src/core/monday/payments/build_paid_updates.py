from __future__ import annotations

from typing import Any, Set

import pandas as pd

from src.config.settings import LOG_PREFIX
from src.core.monday.destination.fetch_destination_audit_items import (
    build_df_destination_audit,
)
from src.core.monday.payments.fetch_payment_items import build_df_payments_realized


def log_info(message: str) -> None:
    print(f"{LOG_PREFIX} [INFO] {message}")


def log_warn(message: str) -> None:
    print(f"{LOG_PREFIX} [WARN] {message}")


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_paid_value(value: Any) -> str:
    return normalize_text(value)


def is_paid_yes(value: Any) -> bool:
    # Regra de negocio: considerar pago somente quando vier exatamente "Sim".
    return normalize_paid_value(value) == "Sim"


def normalize_afs(value: Any) -> str:
    afs = normalize_text(value)
    if afs.endswith(".0"):
        afs = afs[:-2]
    return afs


def build_paid_afs_set(df_payments_realized: pd.DataFrame) -> Set[str]:
    if df_payments_realized is None or df_payments_realized.empty:
        return set()

    return set(
        df_payments_realized["afs"]
        .fillna("")
        .apply(normalize_afs)
        .replace("", pd.NA)
        .dropna()
        .tolist()
    )


def build_df_paid_to_update(
    df_destination_audit: pd.DataFrame,
    df_payments_realized: pd.DataFrame,
) -> pd.DataFrame:
    if df_destination_audit is None or df_destination_audit.empty:
        log_warn("df_destination_audit vazio; nada para atualizar")
        return pd.DataFrame()

    paid_afs = build_paid_afs_set(df_payments_realized=df_payments_realized)
    if not paid_afs:
        log_warn("Nenhum AF encontrado em pagamentos realizados")
        return pd.DataFrame()

    df = df_destination_audit.copy()

    for col in [
        "afs",
        "id_item_monday",
        "board_id",
        "board_name",
        "group_id",
        "group_title",
        "pago",
    ]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()

    df["afs"] = df["afs"].apply(normalize_afs)
    df["should_be_paid"] = df["afs"].isin(paid_afs)
    df["already_paid"] = df["pago"].apply(is_paid_yes)

    df_paid_to_update = df[(df["should_be_paid"]) & (~df["already_paid"])].copy()

    if df_paid_to_update.empty:
        total_should_be_paid = int(df["should_be_paid"].sum())
        total_already_paid = int(df["already_paid"].sum())
        log_info(
            "Nenhum item precisa de update de PAGO | "
            f"should_be_paid={total_should_be_paid} | already_paid={total_already_paid}"
        )
        return pd.DataFrame(
            columns=[
                "afs",
                "id_item_monday",
                "board_id",
                "board_name",
                "group_id",
                "group_title",
                "pago_current",
                "pago_target",
                "update_action",
            ]
        )

    df_paid_to_update["pago_current"] = df_paid_to_update["pago"]
    df_paid_to_update["pago_target"] = "Sim"
    df_paid_to_update["update_action"] = "SET_PAGO_TRUE"

    df_paid_to_update = df_paid_to_update[
        [
            "afs",
            "id_item_monday",
            "board_id",
            "board_name",
            "group_id",
            "group_title",
            "pago_current",
            "pago_target",
            "update_action",
        ]
    ].drop_duplicates(subset=["id_item_monday"]).reset_index(drop=True)

    log_info(f"df_paid_to_update gerado com {len(df_paid_to_update)} linhas")
    return df_paid_to_update


def build_df_pago_to_update(
    df_destination_audit: pd.DataFrame,
    df_payments_realized: pd.DataFrame,
) -> pd.DataFrame:
    return build_df_paid_to_update(
        df_destination_audit=df_destination_audit,
        df_payments_realized=df_payments_realized,
    )


if __name__ == "__main__":
    log_info("Executando teste local de build_paid_updates")
    df_destination_audit = build_df_destination_audit()
    df_payments_realized = build_df_payments_realized()

    df_paid_to_update = build_df_paid_to_update(
        df_destination_audit=df_destination_audit,
        df_payments_realized=df_payments_realized,
    )
    print(df_paid_to_update)
    print(f"Total itens para atualizar PAGO: {len(df_paid_to_update)}")
