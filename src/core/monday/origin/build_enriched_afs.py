from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd
from tqdm.auto import tqdm

from src.config.settings import (
    BOARDS_DESTINATION,
    COLUNAS_ENRIQUECIMENTO,
    COLUNAS_ENRIQUECIMENTO_POR_DESTINO,
    GRUPO_2024_ORIGEM_ID,
    GRUPO_CANCELADAS_ORIGEM_ID,
    LOG_PREFIX,
    MOSTRAR_PROGRESSO,
)
from src.core.monday.destination.build_missing_afs import (
    build_df_afs_diff,
    build_df_afs_to_create,
)
from src.core.monday.destination.fetch_destination_items import build_df_afs_destination
from src.core.monday.origin.fetch_origin_item_details import (
    build_enrichment_columns_for_group,
    fetch_origin_item_details_by_board,
)
from src.core.monday.origin.fetch_origin_items import build_df_afs_origin


def log_info(message: str) -> None:
    print(f"{LOG_PREFIX} [INFO] {message}")


def log_warn(message: str) -> None:
    print(f"{LOG_PREFIX} [WARN] {message}")


def determine_destination_group(
    destination_board: str,
    origin_group_id: str,
) -> Optional[str]:
    destination_config = BOARDS_DESTINATION.get(destination_board, {})

    if not destination_config:
        return None

    if origin_group_id == GRUPO_CANCELADAS_ORIGEM_ID:
        return destination_config.get("grupo_canceladas")

    if destination_board == "ENEVA" and origin_group_id == GRUPO_2024_ORIGEM_ID:
        return destination_config.get("grupo_2024")

    return destination_config.get("grupo_afs")


def build_df_afs_enriched(df_afs_to_create: pd.DataFrame) -> pd.DataFrame:
    if df_afs_to_create is None or df_afs_to_create.empty:
        log_warn("df_afs_to_create vazio; nada para enriquecer")
        return pd.DataFrame()

    df_light = df_afs_to_create.copy()
    df_light["id_item_monday"] = df_light["id_item_monday"].astype(str).str.strip()

    enriched_records: List[Dict[str, Any]] = []

    grouped = df_light.groupby(["board_id", "board_name"], dropna=False)
    iterator = grouped
    if MOSTRAR_PROGRESSO:
        iterator = tqdm(
            grouped,
            total=df_light[["board_id", "board_name"]].drop_duplicates().shape[0],
            desc="ENRICH",
        )

    for (_, board_name), df_group in iterator:
        item_ids = df_group["id_item_monday"].dropna().astype(str).unique().tolist()
        column_ids = build_enrichment_columns_for_group(df_group)

        detail_records = fetch_origin_item_details_by_board(
            board_name=board_name,
            item_ids=item_ids,
            column_ids=column_ids,
        )
        df_detail = pd.DataFrame(detail_records)

        if df_detail.empty:
            log_warn(f"{board_name}: nenhum detalhe retornado para enriquecimento")
            continue

        df_detail["id_item_monday"] = df_detail["id_item_monday"].astype(str).str.strip()
        detail_map = df_detail.set_index("id_item_monday").to_dict(orient="index")

        missing_item_names: List[str] = []

        for _, row in df_group.iterrows():
            row_dict = row.to_dict()
            item_id = str(row_dict.get("id_item_monday", "")).strip()
            detail = detail_map.get(item_id, {})

            row_dict["item_name"] = detail.get("item_name")
            row_dict["group_id_detail"] = detail.get("group_id_detail")
            row_dict["group_title_detail"] = detail.get("group_title_detail")

            for column_id in column_ids:
                row_dict[column_id] = detail.get(column_id)

            if not detail.get("item_name"):
                missing_item_names.append(item_id)

            row_dict["destination_group"] = determine_destination_group(
                destination_board=str(row_dict.get("destination_board", "")).strip(),
                origin_group_id=str(row_dict.get("group_id", "")).strip(),
            )

            enriched_records.append(row_dict)

        if missing_item_names:
            log_warn(
                f"{board_name}: {len(missing_item_names)} itens sem item_name apos enrich. "
                f"Exemplos: {missing_item_names[:10]}"
            )

        log_info(f"{board_name}: {len(df_group)} linhas enriquecidas")

    df_afs_enriched = pd.DataFrame(enriched_records)

    if not df_afs_enriched.empty:
        preferred_cols = [
            "afs",
            "id_item_monday",
            "item_name",
            "board_id",
            "board_name",
            "group_id",
            "group_title",
            "cost_center",
            "destination_board",
            "destination_group",
            "group_id_detail",
            "group_title_detail",
        ]
        enrichment_cols = [
            col
            for col in (
                COLUNAS_ENRIQUECIMENTO
                + sorted(
                    {
                        c
                        for cols in COLUNAS_ENRIQUECIMENTO_POR_DESTINO.values()
                        for c in cols
                    }
                )
            )
            if col in df_afs_enriched.columns
        ]

        ordered_cols = [
            col for col in preferred_cols if col in df_afs_enriched.columns
        ] + enrichment_cols
        remaining_cols = [col for col in df_afs_enriched.columns if col not in ordered_cols]
        df_afs_enriched = df_afs_enriched[ordered_cols + remaining_cols]

    log_info(f"df_afs_enriched gerado com {len(df_afs_enriched)} linhas")
    return df_afs_enriched


if __name__ == "__main__":
    log_info("Executando teste local de build_enriched_afs")
    df_afs_origin = build_df_afs_origin()
    df_afs_destination = build_df_afs_destination()
    df_afs_diff = build_df_afs_diff(
        df_afs_origin=df_afs_origin,
        df_afs_destination=df_afs_destination,
    )
    df_afs_to_create = build_df_afs_to_create(df_afs_diff=df_afs_diff)
    df_afs_enriched = build_df_afs_enriched(df_afs_to_create=df_afs_to_create)
    print(df_afs_enriched)
    print(f"Total AFs enriched: {len(df_afs_enriched)}")
