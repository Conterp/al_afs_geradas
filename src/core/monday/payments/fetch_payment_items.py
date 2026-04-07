from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

import pandas as pd
from tqdm.auto import tqdm

from src.config.settings import (
    BOARDS_PAYMENTS,
    COLUNA_NUMERO_AF,
    LOG_PREFIX,
    MOSTRAR_PROGRESSO,
    PAGE_LIMIT,
    SLEEP_BETWEEN_REQUESTS,
)
from src.core.monday.execute_monday_query import execute_monday_query
from src.core.monday.origin.fetch_origin_items import extract_column_text


def log_info(message: str) -> None:
    print(f"{LOG_PREFIX} [INFO] {message}")


def log_warn(message: str) -> None:
    print(f"{LOG_PREFIX} [WARN] {message}")


def build_payments_items_query(board_id: str, cursor: Optional[str] = None) -> str:
    if cursor:
        return f"""
        query {{
          next_items_page(limit: {PAGE_LIMIT}, cursor: "{cursor}") {{
            cursor
            items {{
              id
              name
              group {{
                id
                title
              }}
              column_values(ids: ["{COLUNA_NUMERO_AF}"]) {{
                id
                text
              }}
            }}
          }}
        }}
        """

    return f"""
    query {{
      boards(ids: [{board_id}]) {{
        id
        name
        items_page(limit: {PAGE_LIMIT}) {{
          cursor
          items {{
            id
            name
            group {{
              id
              title
            }}
            column_values(ids: ["{COLUNA_NUMERO_AF}"]) {{
              id
              text
            }}
          }}
        }}
      }}
    }}
    """


def fetch_payment_board_items(
    board_key: str,
    board_config: Dict[str, Any],
) -> List[Dict[str, Any]]:
    board_id = str(board_config["board_id"]).strip()
    board_name = str(board_config["board_name"]).strip()

    records: List[Dict[str, Any]] = []
    cursor: Optional[str] = None

    while True:
        query = build_payments_items_query(board_id=board_id, cursor=cursor)
        data = execute_monday_query(
            query=query,
            operation_name=f"fetch_payment_board_items:{board_name}",
        )

        if cursor:
            page = data.get("next_items_page", {})
        else:
            boards = data.get("boards", [])
            if not boards:
                break
            page = boards[0].get("items_page", {})

        items = page.get("items", [])
        cursor = page.get("cursor")

        for item in items:
            group = item.get("group", {}) or {}
            afs = extract_column_text(item.get("column_values", []), COLUNA_NUMERO_AF)

            if not afs:
                continue

            records.append(
                {
                    "afs": str(afs).strip(),
                    "id_item_monday": str(item.get("id", "")).strip(),
                    "item_name": str(item.get("name", "")).strip(),
                    "payment_board_id": board_id,
                    "payment_board_name": board_name,
                    "payment_group_id": str(group.get("id", "")).strip(),
                    "payment_group_title": str(group.get("title", "")).strip(),
                }
            )

        if not cursor:
            break

        time.sleep(SLEEP_BETWEEN_REQUESTS)

    log_info(f"{board_key}: {len(records)} registros de pagamentos coletados")
    return records


def build_df_payments_realized(show_progress: bool = MOSTRAR_PROGRESSO) -> pd.DataFrame:
    all_records: List[Dict[str, Any]] = []
    iterator = BOARDS_PAYMENTS.items()

    if show_progress:
        iterator = tqdm(iterator, total=len(BOARDS_PAYMENTS), desc="REQ payments")

    for board_key, board_config in iterator:
        records = fetch_payment_board_items(board_key=board_key, board_config=board_config)
        all_records.extend(records)

    df_payments_realized = pd.DataFrame(
        all_records,
        columns=[
            "afs",
            "id_item_monday",
            "item_name",
            "payment_board_id",
            "payment_board_name",
            "payment_group_id",
            "payment_group_title",
        ],
    )

    if df_payments_realized.empty:
        log_warn("df_payments_realized vazio")
        return df_payments_realized

    for col in df_payments_realized.columns:
        df_payments_realized[col] = (
            df_payments_realized[col].fillna("").astype(str).str.strip()
        )

    df_payments_realized = df_payments_realized.drop_duplicates(subset=["afs"]).reset_index(drop=True)
    log_info(f"df_payments_realized gerado com {len(df_payments_realized)} linhas")
    return df_payments_realized


def build_df_pagamentos_realizados(show_progress: bool = MOSTRAR_PROGRESSO) -> pd.DataFrame:
    return build_df_payments_realized(show_progress=show_progress)


if __name__ == "__main__":
    log_info("Executando teste local de fetch_payment_items")
    df_payments_realized = build_df_payments_realized()
    print(df_payments_realized)
    print(f"Total pagamentos realizados: {len(df_payments_realized)}")
