from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional

import pandas as pd
from tqdm.auto import tqdm

from src.config.settings import (
    BOARDS_DESTINATION,
    COLUNA_NUMERO_AF,
    COLUNA_PAGO,
    IGNORAR_SEM_NUMERO_AF,
    LOG_PREFIX,
    MOSTRAR_PROGRESSO,
    PAGE_LIMIT,
    SLEEP_BETWEEN_REQUESTS,
)
from src.core.monday.execute_monday_query import execute_monday_query


def log_info(message: str) -> None:
    print(f"{LOG_PREFIX} [INFO] {message}")


def extract_column_text_or_value(
    column_values: List[Dict[str, Any]],
    column_id: str,
) -> str:
    for column in column_values:
        if column.get("id") != column_id:
            continue

        text_value = str(column.get("text") or "").strip()
        if text_value:
            return text_value

        raw_value = column.get("value")
        if not raw_value:
            return ""

        try:
            parsed_value = json.loads(raw_value)
        except Exception:
            return str(raw_value).strip()

        for key in ["label", "text", "value"]:
            val = parsed_value.get(key)
            if val is not None and str(val).strip() != "":
                return str(val).strip()
        return ""

    return ""


def build_destination_audit_query(board_id: str, cursor: Optional[str] = None) -> str:
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
              column_values(ids: ["{COLUNA_NUMERO_AF}", "{COLUNA_PAGO}", "date_mkkvsdmb"]) {{
                id
                text
                value
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
            column_values(ids: ["{COLUNA_NUMERO_AF}", "{COLUNA_PAGO}", "date_mkkvsdmb"]) {{
              id
              text
              value
            }}
          }}
        }}
      }}
    }}
    """


def fetch_destination_audit_items(
    board_key: str,
    board_config: Dict[str, Any],
) -> List[Dict[str, Any]]:
    board_id = board_config["board_id"]
    board_name = board_config["board_name"]

    records: List[Dict[str, Any]] = []
    cursor: Optional[str] = None

    while True:
        query = build_destination_audit_query(board_id=board_id, cursor=cursor)
        data = execute_monday_query(
            query=query,
            operation_name=f"fetch_destination_audit_items:{board_name}",
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
            afs = extract_column_text_or_value(item.get("column_values", []), COLUNA_NUMERO_AF)
            paid_flag = extract_column_text_or_value(item.get("column_values", []), COLUNA_PAGO)
            af_date = extract_column_text_or_value(item.get("column_values", []), "date_mkkvsdmb")

            if IGNORAR_SEM_NUMERO_AF and not afs:
                continue

            group = item.get("group", {}) or {}

            records.append(
                {
                    "afs": afs,
                    "id_item_monday": str(item.get("id", "")).strip(),
                    "item_name": str(item.get("name", "")).strip(),
                    "board_id": board_id,
                    "board_name": board_name,
                    "group_id": str(group.get("id", "")).strip(),
                    "group_title": str(group.get("title", "")).strip(),
                    "pago": paid_flag,
                    "af_date": af_date,
                }
            )

        if not cursor:
            break

        time.sleep(SLEEP_BETWEEN_REQUESTS)

    log_info(f"{board_key}: {len(records)} registros de auditoria coletados")
    return records


def build_df_destination_audit(show_progress: bool = MOSTRAR_PROGRESSO) -> pd.DataFrame:
    all_records: List[Dict[str, Any]] = []
    iterator = BOARDS_DESTINATION.items()

    if show_progress:
        iterator = tqdm(iterator, total=len(BOARDS_DESTINATION), desc="AUDIT Destination")

    for board_key, board_config in iterator:
        records = fetch_destination_audit_items(
            board_key=board_key,
            board_config=board_config,
        )
        all_records.extend(records)

    df_destination_audit = pd.DataFrame(
        all_records,
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
        ],
    )

    if not df_destination_audit.empty:
        for col in [
            "afs",
            "id_item_monday",
            "item_name",
            "board_id",
            "board_name",
            "group_id",
            "group_title",
            "pago",
            "af_date",
        ]:
            df_destination_audit[col] = (
                df_destination_audit[col].fillna("").astype(str).str.strip()
            )

    log_info(f"df_destination_audit gerado com {len(df_destination_audit)} linhas")
    return df_destination_audit


if __name__ == "__main__":
    log_info("Executando teste local de fetch_destination_audit_items")
    df_destination_audit = build_df_destination_audit()
    print(df_destination_audit)
    print(f"Total destination audit: {len(df_destination_audit)}")
