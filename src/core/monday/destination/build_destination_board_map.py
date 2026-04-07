from __future__ import annotations

from typing import Optional

import pandas as pd

from src.config.settings import BOARDS_DESTINATION, IGNORAR_CC_NAO_MAPEADO, LOG_PREFIX
from src.core.monday.origin.fetch_origin_items import build_df_afs_origin


def log_info(message: str) -> None:
    print(f"{LOG_PREFIX} [INFO] {message}")


def normalize_text(value: str) -> str:
    return (value or "").strip().upper()


def identify_destination_board(cost_center: str) -> Optional[str]:
    cost_center_norm = normalize_text(cost_center)

    if not cost_center_norm:
        return None

    for board_key, board_config in BOARDS_DESTINATION.items():
        cc_keywords = board_config.get("cc_keywords", [])
        for keyword in cc_keywords:
            if normalize_text(keyword) in cost_center_norm:
                return board_key

    return None


def add_destination_board(df_afs_origin: pd.DataFrame) -> pd.DataFrame:
    if df_afs_origin is None:
        log_info("df_afs_origin ausente; nenhum destination_board para mapear")
        return pd.DataFrame()

    if df_afs_origin.empty:
        log_info("df_afs_origin vazio; nenhum destination_board para mapear")
        df_result = df_afs_origin.copy()
        df_result["destination_board"] = pd.Series(dtype="object")
        return df_result

    df_result = df_afs_origin.copy()
    df_result["destination_board"] = df_result["cost_center"].apply(
        identify_destination_board
    )

    total_unmapped = int(df_result["destination_board"].isna().sum())
    log_info(
        f"Mapeamento de destination_board concluido. "
        f"total_linhas={len(df_result)} | sem_mapeamento={total_unmapped}"
    )

    if IGNORAR_CC_NAO_MAPEADO:
        df_result = df_result[df_result["destination_board"].notna()].copy()
        df_result = df_result.reset_index(drop=True)
        log_info(
            f"IGNORAR_CC_NAO_MAPEADO=True: dataframe filtrado para {len(df_result)} linhas"
        )
    else:
        log_info(
            "IGNORAR_CC_NAO_MAPEADO=False: linhas sem mapeamento foram mantidas"
        )

    return df_result


if __name__ == "__main__":
    log_info("Executando teste local de build_destination_board_map")
    df_afs_origin = build_df_afs_origin()
    df_afs_origin = add_destination_board(df_afs_origin)
    print(df_afs_origin)
    print(f"Total AFs com destination_board: {len(df_afs_origin)}")
