from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

import pandas as pd
from tqdm.auto import tqdm

from src.config.settings import (
    BOARDS_DESTINATION,
    COLS_DF_AFS_DESTINATION,
    COLUNA_NUMERO_AF,
    COLUNA_PAGO,
    IGNORAR_SEM_NUMERO_AF,
    LOG_PREFIX,
    MOSTRAR_PROGRESSO,
    PAGE_LIMIT,
    SLEEP_BETWEEN_REQUESTS,
)
from src.core.monday.execute_monday_query import execute_monday_query
from src.core.monday.origin.fetch_origin_items import extract_column_text


def log_info(message: str) -> None:
    print(f"{LOG_PREFIX} [INFO] {message}")


def build_destination_items_query(board_id: str, cursor: Optional[str] = None) -> str:
    if cursor:
        return f"""
        query {{
          next_items_page(limit: {PAGE_LIMIT}, cursor: "{cursor}") {{
            cursor
            items {{
              id
              name
              column_values(ids: ["{COLUNA_NUMERO_AF}", "{COLUNA_PAGO}"]) {{
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
            column_values(ids: ["{COLUNA_NUMERO_AF}", "{COLUNA_PAGO}"]) {{
              id
              text
            }}
          }}
        }}
      }}
    }}
    """


def fetch_destination_board_items(
    board_key: str,
    board_config: Dict[str, Any],
) -> List[Dict[str, Any]]:
    board_id = board_config["board_id"]
    board_name = board_config["board_name"]

    records: List[Dict[str, Any]] = []
    cursor: Optional[str] = None

    while True:
        query = build_destination_items_query(board_id=board_id, cursor=cursor)
        data = execute_monday_query(
            query=query,
            operation_name=f"fetch_destination_board_items:{board_name}",
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
            numero_af = extract_column_text(
                item.get("column_values", []),
                COLUNA_NUMERO_AF,
            )
            pago = extract_column_text(
                item.get("column_values", []),
                COLUNA_PAGO,
            )

            if IGNORAR_SEM_NUMERO_AF and not numero_af:
                continue

            records.append(
                {
                    "afs": numero_af,
                    "id_item_monday": item.get("id", ""),
                    "board_id": board_id,
                    "board_name": board_name,
                    "pago": pago,
                }
            )

        if not cursor:
            break

        time.sleep(SLEEP_BETWEEN_REQUESTS)

    log_info(f"{board_key}: {len(records)} registros de destino coletados")
    return records


def build_df_afs_destination(show_progress: bool = MOSTRAR_PROGRESSO) -> pd.DataFrame:
    all_records: List[Dict[str, Any]] = []
    iterator = BOARDS_DESTINATION.items()

    if show_progress:
        iterator = tqdm(iterator, total=len(BOARDS_DESTINATION), desc="REQ destination")

    for board_key, board_config in iterator:
        records = fetch_destination_board_items(
            board_key=board_key,
            board_config=board_config,
        )
        all_records.extend(records)

    df_afs_destination = pd.DataFrame(all_records, columns=COLS_DF_AFS_DESTINATION)

    if not df_afs_destination.empty:
        df_afs_destination["afs"] = (
            df_afs_destination["afs"].fillna("").astype(str).str.strip()
        )
        df_afs_destination["pago"] = (
            df_afs_destination["pago"].fillna("").astype(str).str.strip()
        )

    log_info(f"df_afs_destination gerado com {len(df_afs_destination)} linhas")
    return df_afs_destination


if __name__ == "__main__":
    log_info("Executando teste local de fetch_destination_items")
    df_afs_destination = build_df_afs_destination()
    print(df_afs_destination)
    print(f"Total AFs de destination: {len(df_afs_destination)}")
