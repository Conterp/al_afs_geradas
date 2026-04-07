from __future__ import annotations

from typing import Any, Dict, Set

import pandas as pd

from src.config.settings import BOARDS_DESTINATION, LOG_PREFIX
from src.core.monday.destination.build_destination_board_map import identify_destination_board
from src.core.monday.destination.fetch_destination_audit_items import (
    build_df_destination_audit,
)
from src.core.monday.origin.build_enriched_afs import determine_destination_group
from src.core.monday.origin.fetch_origin_items import build_df_afs_origin
from src.core.monday.payments.build_paid_updates import is_paid_yes
from src.core.monday.payments.fetch_payment_items import build_df_payments_realized


def log_info(message: str) -> None:
    print(f"{LOG_PREFIX} [INFO] {message}")


def normalize_text(value: Any) -> str:
    return str(value or "").strip()


def normalize_afs(value: Any) -> str:
    afs = normalize_text(value)
    if afs.endswith(".0"):
        afs = afs[:-2]
    return afs


def build_board_name_to_key_map() -> Dict[str, str]:
    return {
        normalize_text(config.get("board_name")): board_key
        for board_key, config in BOARDS_DESTINATION.items()
    }


def build_board_key_to_name_map() -> Dict[str, str]:
    return {
        board_key: normalize_text(config.get("board_name"))
        for board_key, config in BOARDS_DESTINATION.items()
    }


def resolve_expected_group_name(destination_board_key: str, group_id: str) -> str:
    board_key = normalize_text(destination_board_key)
    group_norm = normalize_text(group_id)
    board_config = BOARDS_DESTINATION.get(board_key, {})
    if not board_config or not group_norm:
        return ""

    if group_norm == normalize_text(board_config.get("grupo_afs")):
        return "AFs Geradas"
    if group_norm == normalize_text(board_config.get("grupo_canceladas")):
        return "Canceladas"
    if group_norm == normalize_text(board_config.get("grupo_2024")):
        return "2024"
    return ""


def build_df_origem_expected_destino(df_afs_origem: pd.DataFrame) -> pd.DataFrame:
    if df_afs_origem is None or df_afs_origem.empty:
        return pd.DataFrame(
            columns=[
                "afs",
                "id_item_monday_origem",
                "board_id_origem",
                "board_name_origem",
                "group_id_origem",
                "group_title_origem",
                "cost_center",
                "board_destino_esperado",
                "grupo_destino_esperado",
            ]
        )

    df = df_afs_origem.copy()
    df["afs"] = df["afs"].apply(normalize_afs)

    if "destination_board" in df.columns:
        df["board_destino_esperado"] = (
            df["destination_board"].fillna("").astype(str).str.strip()
        )
    else:
        df["board_destino_esperado"] = df["cost_center"].apply(identify_destination_board)
        df["board_destino_esperado"] = df["board_destino_esperado"].fillna("")

    df["grupo_destino_esperado"] = df.apply(
        lambda row: determine_destination_group(
            destination_board=normalize_text(row.get("board_destino_esperado")),
            origin_group_id=normalize_text(row.get("group_id")),
        )
        or "",
        axis=1,
    )
    df["grupo_destino_esperado_name"] = df.apply(
        lambda row: resolve_expected_group_name(
            destination_board_key=row.get("board_destino_esperado"),
            group_id=row.get("grupo_destino_esperado"),
        ),
        axis=1,
    )
    board_key_to_name = build_board_key_to_name_map()
    df["board_destino_esperado_name"] = df["board_destino_esperado"].map(
        lambda x: board_key_to_name.get(normalize_text(x), normalize_text(x))
    )

    df = df.rename(
        columns={
            "id_item_monday": "id_item_monday_origem",
            "board_id": "board_id_origem",
            "board_name": "board_name_origem",
            "group_id": "group_id_origem",
            "group_title": "group_title_origem",
        }
    )

    keep_cols = [
        "afs",
        "id_item_monday_origem",
        "board_id_origem",
        "board_name_origem",
        "group_id_origem",
        "group_title_origem",
        "cost_center",
        "board_destino_esperado",
        "board_destino_esperado_name",
        "grupo_destino_esperado",
        "grupo_destino_esperado_name",
    ]
    return df[keep_cols].copy()


def build_df_wrong_board(
    df_destino_auditoria: pd.DataFrame,
    df_origem_expected: pd.DataFrame,
) -> pd.DataFrame:
    if df_destino_auditoria is None or df_destino_auditoria.empty:
        return pd.DataFrame()
    if df_origem_expected is None or df_origem_expected.empty:
        return pd.DataFrame()

    df_dest = df_destino_auditoria.copy()
    df_dest["afs"] = df_dest["afs"].apply(normalize_afs)

    board_name_to_key = build_board_name_to_key_map()
    board_key_to_name = build_board_key_to_name_map()
    df_dest["board_destino_atual"] = df_dest["board_name"].map(
        lambda x: board_name_to_key.get(normalize_text(x), "")
    )
    df_dest["board_destino_atual_name"] = df_dest["board_destino_atual"].map(
        lambda x: board_key_to_name.get(normalize_text(x), normalize_text(x))
    )

    df_merged = df_dest.merge(df_origem_expected, on="afs", how="inner")

    df_wrong_board = df_merged[
        (df_merged["board_destino_esperado"].fillna("") != "")
        & (df_merged["board_destino_atual"].fillna("") != "")
        & (df_merged["board_destino_esperado"] != df_merged["board_destino_atual"])
    ].copy()
    df_wrong_board["board_destino_esperado_name"] = df_wrong_board[
        "board_destino_esperado"
    ].map(lambda x: board_key_to_name.get(normalize_text(x), normalize_text(x)))

    keep_cols = [
        "afs",
        "id_item_monday",
        "item_name",
        "board_id",
        "board_name",
        "group_id",
        "group_title",
        "pago",
        "af_date",
        "cost_center",
        "board_destino_atual",
        "board_destino_atual_name",
        "board_destino_esperado",
        "board_destino_esperado_name",
        "grupo_destino_esperado",
        "id_item_monday_origem",
        "board_id_origem",
        "board_name_origem",
        "group_id_origem",
        "group_title_origem",
    ]
    keep_cols = [col for col in keep_cols if col in df_wrong_board.columns]
    return df_wrong_board[keep_cols].sort_values(by=["board_name", "afs"]).reset_index(
        drop=True
    )


def build_df_wrong_group(
    df_destino_auditoria: pd.DataFrame,
    df_origem_expected: pd.DataFrame,
) -> pd.DataFrame:
    if df_destino_auditoria is None or df_destino_auditoria.empty:
        return pd.DataFrame()
    if df_origem_expected is None or df_origem_expected.empty:
        return pd.DataFrame()

    df_dest = df_destino_auditoria.copy()
    df_dest["afs"] = df_dest["afs"].apply(normalize_afs)

    df_merged = df_dest.merge(df_origem_expected, on="afs", how="inner")

    df_wrong_group = df_merged[
        (df_merged["grupo_destino_esperado"].fillna("") != "")
        & (df_merged["group_id"].fillna("") != "")
        & (df_merged["group_id"] != df_merged["grupo_destino_esperado"])
    ].copy()
    if "grupo_destino_esperado_name" not in df_wrong_group.columns:
        df_wrong_group["grupo_destino_esperado_name"] = df_wrong_group.apply(
            lambda row: resolve_expected_group_name(
                destination_board_key=row.get("board_destino_esperado"),
                group_id=row.get("grupo_destino_esperado"),
            ),
            axis=1,
        )

    keep_cols = [
        "afs",
        "id_item_monday",
        "item_name",
        "board_id",
        "board_name",
        "group_id",
        "group_title",
        "cost_center",
        "grupo_destino_esperado",
        "grupo_destino_esperado_name",
        "id_item_monday_origem",
        "board_id_origem",
        "board_name_origem",
        "group_id_origem",
        "group_title_origem",
    ]
    keep_cols = [col for col in keep_cols if col in df_wrong_group.columns]
    return df_wrong_group[keep_cols].sort_values(by=["board_name", "afs"]).reset_index(
        drop=True
    )


def build_df_no_origin(
    df_destino_auditoria: pd.DataFrame,
    df_afs_origem: pd.DataFrame,
) -> pd.DataFrame:
    if df_destino_auditoria is None or df_destino_auditoria.empty:
        return pd.DataFrame()

    df_dest = df_destino_auditoria.copy()
    df_dest["afs"] = df_dest["afs"].apply(normalize_afs)
    df_dest["item_name"] = df_dest["item_name"].fillna("").astype(str).str.strip()

    origem_afs: Set[str] = set()
    if df_afs_origem is not None and not df_afs_origem.empty:
        origem_afs = set(df_afs_origem["afs"].apply(normalize_afs).tolist())

    df_no_origin = df_dest[
        (df_dest["afs"] == "")
        | (df_dest["item_name"] == "")
        | (df_dest["item_name"].str.lower() == "new item")
        | (~df_dest["afs"].isin(origem_afs))
    ].copy()

    return df_no_origin.sort_values(by=["board_name", "afs", "item_name"]).reset_index(
        drop=True
    )


def build_df_wrong_pago(
    df_destino_auditoria: pd.DataFrame,
    df_pagamentos_realizados: pd.DataFrame,
) -> pd.DataFrame:
    if df_destino_auditoria is None or df_destino_auditoria.empty:
        return pd.DataFrame()

    df_dest = df_destino_auditoria.copy()
    df_dest["afs"] = df_dest["afs"].apply(normalize_afs)
    df_dest["pago"] = df_dest["pago"].fillna("").astype(str).str.strip()

    pagamentos_afs: Set[str] = set()
    if df_pagamentos_realizados is not None and not df_pagamentos_realizados.empty:
        pagamentos_afs = set(df_pagamentos_realizados["afs"].apply(normalize_afs).tolist())

    df_wrong_pago = df_dest[
        (df_dest["afs"] != "")
        & (df_dest["pago"].apply(is_paid_yes))
        & (~df_dest["afs"].isin(pagamentos_afs))
    ].copy()

    df_wrong_pago["novo_pago"] = ""

    keep_cols = [
        "afs",
        "id_item_monday",
        "item_name",
        "board_id",
        "board_name",
        "group_id",
        "group_title",
        "pago",
        "novo_pago",
        "af_date",
    ]
    keep_cols = [col for col in keep_cols if col in df_wrong_pago.columns]
    return df_wrong_pago[keep_cols].sort_values(by=["board_name", "afs"]).reset_index(
        drop=True
    )


if __name__ == "__main__":
    log_info("Executando teste local de find_orphan_items")
    df_afs_origem = build_df_afs_origin()
    df_destino_auditoria = build_df_destination_audit()
    df_pagamentos_realizados = build_df_payments_realized()

    df_origem_expected = build_df_origem_expected_destino(df_afs_origem=df_afs_origem)
    df_wrong_board = build_df_wrong_board(
        df_destino_auditoria=df_destino_auditoria,
        df_origem_expected=df_origem_expected,
    )
    df_wrong_group = build_df_wrong_group(
        df_destino_auditoria=df_destino_auditoria,
        df_origem_expected=df_origem_expected,
    )
    df_no_origin = build_df_no_origin(
        df_destino_auditoria=df_destino_auditoria,
        df_afs_origem=df_afs_origem,
    )
    df_wrong_pago = build_df_wrong_pago(
        df_destino_auditoria=df_destino_auditoria,
        df_pagamentos_realizados=df_pagamentos_realizados,
    )

    print(df_wrong_board)
    print(df_wrong_group)
    print(df_no_origin)
    print(df_wrong_pago)
    print(f"WRONG BOARD: {len(df_wrong_board)}")
    print(f"WRONG GROUP: {len(df_wrong_group)}")
    print(f"NO ORIGIN: {len(df_no_origin)}")
    print(f"WRONG PAGO: {len(df_wrong_pago)}")
