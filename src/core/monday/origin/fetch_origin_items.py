from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

import pandas as pd
from tqdm.auto import tqdm

from src.config.settings import (
    BOARDS_ORIGIN,
    COLS_DF_AFS_ORIGIN,
    COLUNA_CENTRO_CUSTO,
    COLUNA_NUMERO_AF,
    IGNORAR_SEM_NUMERO_AF,
    LOG_PREFIX,
    MOSTRAR_PROGRESSO,
    PAGE_LIMIT,
    SLEEP_BETWEEN_REQUESTS,
)
from src.core.monday.execute_monday_query import execute_monday_query


def log_info(message: str) -> None:
    print(f"{LOG_PREFIX} [INFO] {message}")


def build_origin_items_query(board_id: str, cursor: Optional[str] = None) -> str:
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
              column_values(ids: ["{COLUNA_NUMERO_AF}", "{COLUNA_CENTRO_CUSTO}"]) {{
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
            column_values(ids: ["{COLUNA_NUMERO_AF}", "{COLUNA_CENTRO_CUSTO}"]) {{
              id
              text
            }}
          }}
        }}
      }}
    }}
    """


def extract_column_text(column_values: List[Dict[str, Any]], column_id: str) -> str:
    for column in column_values:
        if column.get("id") == column_id:
            return (column.get("text") or "").strip()
    return ""


def fetch_origin_board_items(
    board_key: str,
    board_config: Dict[str, Any],
) -> List[Dict[str, Any]]:
    board_id = board_config["board_id"]
    board_name = board_config["board_name"]
    allowed_group_ids = set(board_config.get("group_ids", []))

    records: List[Dict[str, Any]] = []
    cursor: Optional[str] = None

    while True:
        query = build_origin_items_query(board_id=board_id, cursor=cursor)
        data = execute_monday_query(
            query=query,
            operation_name=f"fetch_origin_board_items:{board_name}",
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
            group_id = group.get("id", "")
            group_title = group.get("title", "")

            if allowed_group_ids and group_id not in allowed_group_ids:
                continue

            numero_af = extract_column_text(
                item.get("column_values", []),
                COLUNA_NUMERO_AF,
            )
            cost_center = extract_column_text(
                item.get("column_values", []),
                COLUNA_CENTRO_CUSTO,
            )

            if IGNORAR_SEM_NUMERO_AF and not numero_af:
                continue

            records.append(
                {
                    "afs": numero_af,
                    "id_item_monday": item.get("id", ""),
                    "board_id": board_id,
                    "board_name": board_name,
                    "group_id": group_id,
                    "group_title": group_title,
                    "cost_center": cost_center,
                }
            )

        if not cursor:
            break

        time.sleep(SLEEP_BETWEEN_REQUESTS)

    log_info(f"{board_key}: {len(records)} registros de origem coletados")
    return records


def build_df_afs_origin(show_progress: bool = MOSTRAR_PROGRESSO) -> pd.DataFrame:
    all_records: List[Dict[str, Any]] = []
    iterator = BOARDS_ORIGIN.items()

    if show_progress:
        iterator = tqdm(iterator, total=len(BOARDS_ORIGIN), desc="REQ origin")

    for board_key, board_config in iterator:
        records = fetch_origin_board_items(
            board_key=board_key,
            board_config=board_config,
        )
        all_records.extend(records)

    df_afs_origin = pd.DataFrame(all_records, columns=COLS_DF_AFS_ORIGIN)

    if not df_afs_origin.empty:
        df_afs_origin["afs"] = df_afs_origin["afs"].astype(str).str.strip()
        df_afs_origin["cost_center"] = (
            df_afs_origin["cost_center"].fillna("").astype(str).str.strip()
        )

    log_info(f"df_afs_origin gerado com {len(df_afs_origin)} linhas")
    return df_afs_origin


if __name__ == "__main__":
    log_info("Executando teste local de fetch_origin_items")
    df_afs_origin = build_df_afs_origin()
    print(df_afs_origin)
    print(f"Total AFs de origin: {len(df_afs_origin)}")
