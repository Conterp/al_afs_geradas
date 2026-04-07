from __future__ import annotations

import json
import time
from typing import Any, Dict, List

import pandas as pd

from src.config.settings import (
    COLUNAS_ENRIQUECIMENTO,
    COLUNAS_ENRIQUECIMENTO_POR_DESTINO,
    ENRICH_BATCH_SIZE,
    LOG_PREFIX,
    SLEEP_BETWEEN_REQUESTS,
)
from src.core.monday.execute_monday_query import execute_monday_query
from src.core.monday.origin.fetch_origin_items import extract_column_text


def log_info(message: str) -> None:
    print(f"{LOG_PREFIX} [INFO] {message}")


def chunk_list(values: List[str], chunk_size: int) -> List[List[str]]:
    return [values[i : i + chunk_size] for i in range(0, len(values), chunk_size)]


def build_origin_item_detail_query(item_ids: List[str], column_ids: List[str]) -> str:
    item_ids_str = ", ".join(str(item_id) for item_id in item_ids)

    return f"""
    query {{
      items(ids: [{item_ids_str}]) {{
        id
        name
        group {{
          id
          title
        }}
        column_values(ids: {json.dumps(column_ids)}) {{
          id
          text
        }}
      }}
    }}
    """


def build_enrichment_columns_for_group(df_group: pd.DataFrame) -> List[str]:
    base_columns = list(COLUNAS_ENRIQUECIMENTO)
    extra_columns = set()

    for destination_board in df_group["destination_board"].dropna().unique().tolist():
        for column_id in COLUNAS_ENRIQUECIMENTO_POR_DESTINO.get(destination_board, []):
            extra_columns.add(column_id)

    return base_columns + sorted(extra_columns)


def fetch_origin_item_details_by_board(
    board_name: str,
    item_ids: List[str],
    column_ids: List[str],
) -> List[Dict[str, Any]]:
    all_records: List[Dict[str, Any]] = []
    effective_batch_size = min(ENRICH_BATCH_SIZE, 25)

    for chunk in chunk_list(item_ids, effective_batch_size):
        query = build_origin_item_detail_query(item_ids=chunk, column_ids=column_ids)
        data = execute_monday_query(
            query=query,
            operation_name=f"fetch_origin_item_details:{board_name}",
        )

        items = data.get("items", [])
        for item in items:
            record: Dict[str, Any] = {
                "id_item_monday": str(item.get("id", "")).strip(),
                "item_name": item.get("name", ""),
                "group_id_detail": (item.get("group") or {}).get("id", ""),
                "group_title_detail": (item.get("group") or {}).get("title", ""),
            }

            for column_id in column_ids:
                record[column_id] = extract_column_text(
                    item.get("column_values", []),
                    column_id,
                )

            all_records.append(record)

        time.sleep(SLEEP_BETWEEN_REQUESTS)

    return all_records


if __name__ == "__main__":
    log_info("Modulo fetch_origin_item_details carregado com sucesso.")
