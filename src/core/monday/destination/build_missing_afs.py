from __future__ import annotations

from typing import Set, Tuple

import pandas as pd

from src.config.settings import (
    BOARDS_DESTINATION,
    COLS_DF_AFS_DIFF,
    COLS_DF_AFS_TO_CREATE,
    IGNORAR_CC_NAO_MAPEADO,
    LOG_PREFIX,
)
from src.core.monday.destination.build_destination_board_map import add_destination_board
from src.core.monday.destination.fetch_destination_items import build_df_afs_destination
from src.core.monday.origin.fetch_origin_items import build_df_afs_origin


def log_info(message: str) -> None:
    print(f"{LOG_PREFIX} [INFO] {message}")


def log_warn(message: str) -> None:
    print(f"{LOG_PREFIX} [WARN] {message}")


def build_destination_afs_lookup(
    df_afs_destination: pd.DataFrame,
) -> Set[Tuple[str, str]]:
    if df_afs_destination is None or df_afs_destination.empty:
        return set()

    df_destination = df_afs_destination.copy()

    board_name_to_key = {
        board_config["board_name"]: board_key
        for board_key, board_config in BOARDS_DESTINATION.items()
    }

    df_destination["destination_board"] = df_destination["board_name"].map(
        board_name_to_key
    )

    return {
        (str(row["afs"]).strip(), str(row["destination_board"]).strip())
        for _, row in df_destination.iterrows()
        if str(row["afs"]).strip() and str(row["destination_board"]).strip()
    }


def build_df_afs_diff(
    df_afs_origin: pd.DataFrame,
    df_afs_destination: pd.DataFrame,
) -> pd.DataFrame:
    df_afs_origin_routed = add_destination_board(df_afs_origin=df_afs_origin)
    existing_lookup = build_destination_afs_lookup(
        df_afs_destination=df_afs_destination
    )

    if IGNORAR_CC_NAO_MAPEADO:
        df_afs_origin_routed = df_afs_origin_routed[
            df_afs_origin_routed["destination_board"].notna()
        ].copy()

    if df_afs_origin_routed.empty:
        df_afs_diff = pd.DataFrame(columns=COLS_DF_AFS_DIFF)
        log_warn("Nenhuma AF elegivel para comparacao apos route")
        return df_afs_diff

    mask_not_exists = ~df_afs_origin_routed.apply(
        lambda row: (
            str(row["afs"]).strip(),
            str(row["destination_board"]).strip(),
        ) in existing_lookup,
        axis=1,
    )

    df_afs_diff = df_afs_origin_routed.loc[mask_not_exists, COLS_DF_AFS_DIFF].copy()

    log_info(f"df_afs_diff gerado com {len(df_afs_diff)} linhas")
    return df_afs_diff


def build_df_afs_to_create(df_afs_diff: pd.DataFrame) -> pd.DataFrame:
    if df_afs_diff is None or df_afs_diff.empty:
        df_afs_to_create = pd.DataFrame(columns=COLS_DF_AFS_TO_CREATE)
        log_warn("df_afs_diff esta vazio; nenhuma AF selecionada para criacao")
        return df_afs_to_create

    df = df_afs_diff.copy()

    df["afs"] = df["afs"].fillna("").astype(str).str.strip()
    df["cost_center"] = df["cost_center"].fillna("").astype(str).str.strip()
    df["destination_board"] = (
        df["destination_board"].fillna("").astype(str).str.strip()
    )

    df = df[
        (df["afs"] != "")
        & (df["cost_center"] != "")
        & (df["destination_board"] != "")
    ].copy()

    df = df.drop_duplicates(subset=["afs", "destination_board"], keep="first")

    df_afs_to_create = df.loc[:, COLS_DF_AFS_TO_CREATE].copy()

    log_info(f"df_afs_to_create gerado com {len(df_afs_to_create)} linhas")
    return df_afs_to_create


if __name__ == "__main__":
    log_info("Executando teste local de build_missing_afs")

    df_afs_origin = build_df_afs_origin()
    df_afs_destination = build_df_afs_destination()

    df_afs_diff = build_df_afs_diff(
        df_afs_origin=df_afs_origin,
        df_afs_destination=df_afs_destination,
    )
    print(df_afs_diff)
    print(f"Total AFs diff: {len(df_afs_diff)}")

    df_afs_to_create = build_df_afs_to_create(df_afs_diff=df_afs_diff)
    print(df_afs_to_create)
    print(f"Total AFs to create: {len(df_afs_to_create)}")
