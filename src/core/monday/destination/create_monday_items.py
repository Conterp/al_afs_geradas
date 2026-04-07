from __future__ import annotations

import json
import time
from typing import Any, Dict, List

import pandas as pd
from tqdm.auto import tqdm

from src.config.settings import (
    BOARDS_DESTINATION,
    COLUNAS_ENRIQUECIMENTO,
    COLUNAS_ENRIQUECIMENTO_POR_DESTINO,
    LOG_PREFIX,
    MOSTRAR_PROGRESSO,
    SLEEP_BETWEEN_REQUESTS,
)
from src.core.monday.execute_monday_query import execute_monday_query
from src.core.monday.origin.build_enriched_afs import build_df_afs_enriched
from src.core.monday.destination.build_missing_afs import (
    build_df_afs_diff,
    build_df_afs_to_create,
)
from src.core.monday.destination.fetch_destination_items import build_df_afs_destination
from src.core.monday.origin.fetch_origin_items import build_df_afs_origin


def log_info(message: str) -> None:
    print(f"{LOG_PREFIX} [INFO] {message}")


def log_warn(message: str) -> None:
    print(f"{LOG_PREFIX} [WARN] {message}")


def log_error(message: str) -> None:
    print(f"{LOG_PREFIX} [ERROR] {message}")


def is_blank_value(value: Any) -> bool:
    if value is None:
        return True
    if pd.isna(value):
        return True
    value_str = str(value).strip()
    return value_str == "" or value_str.lower() == "nan"


def clean_string(value: Any) -> str:
    if is_blank_value(value):
        return ""
    return str(value).strip()


def escape_graphql_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)[1:-1]


def resolve_item_name(row: pd.Series) -> str:
    return clean_string(row.get("item_name"))


def build_create_payload_from_row(row: pd.Series) -> Dict[str, Any]:
    column_values: Dict[str, Any] = {}

    all_column_ids = list(COLUNAS_ENRIQUECIMENTO)
    all_column_ids += COLUNAS_ENRIQUECIMENTO_POR_DESTINO.get(
        clean_string(row.get("destination_board")),
        [],
    )

    for column_id in all_column_ids:
        if column_id in row.index:
            value = clean_string(row.get(column_id))
            if value:
                column_values[column_id] = value

    return column_values


def build_create_item_mutation(
    board_id: str,
    group_id: str,
    item_name: str,
    column_values: Dict[str, Any],
) -> str:
    column_values_json = json.dumps(column_values, ensure_ascii=False)
    safe_group_id = escape_graphql_string(group_id)
    safe_item_name = escape_graphql_string(item_name)
    safe_column_values = escape_graphql_string(column_values_json)

    return f"""
    mutation {{
      create_item(
        board_id: {board_id},
        group_id: "{safe_group_id}",
        item_name: "{safe_item_name}",
        column_values: "{safe_column_values}"
      ) {{
        id
        name
      }}
    }}
    """


def create_item_from_row(row: pd.Series) -> Dict[str, Any]:
    destination_board = clean_string(row.get("destination_board"))
    destination_config = BOARDS_DESTINATION.get(destination_board, {})

    board_id = clean_string(destination_config.get("board_id"))
    destination_group = clean_string(row.get("destination_group"))
    item_name = resolve_item_name(row)

    result_record = {
        "afs": clean_string(row.get("afs")),
        "id_item_monday": clean_string(row.get("id_item_monday")),
        "item_name": item_name,
        "destination_board": destination_board,
        "destination_group": destination_group,
        "created_item_id": None,
        "status_create": "pending",
        "error_message": "",
    }

    if not board_id:
        result_record["status_create"] = "error"
        result_record["error_message"] = "destination board_id not found"
        return result_record

    if not destination_group:
        result_record["status_create"] = "error"
        result_record["error_message"] = "destination_group not defined"
        return result_record

    if not item_name:
        result_record["status_create"] = "error"
        result_record["error_message"] = "invalid item_name for creation"
        return result_record

    column_values = build_create_payload_from_row(row)

    if not column_values:
        result_record["status_create"] = "error"
        result_record["error_message"] = "empty column_values for creation"
        return result_record

    try:
        mutation = build_create_item_mutation(
            board_id=board_id,
            group_id=destination_group,
            item_name=item_name,
            column_values=column_values,
        )

        data = execute_monday_query(
            query=mutation,
            operation_name=f"create_item:{destination_board}",
        )

        created_item = data.get("create_item", {})
        result_record["created_item_id"] = created_item.get("id")
        result_record["status_create"] = "created"

    except Exception as exc:
        result_record["status_create"] = "error"
        result_record["error_message"] = str(exc)
        log_error(
            f"Falha ao criar item AF {result_record['afs']} "
            f"(item_name={item_name}) em {destination_board}: {exc}"
        )

    return result_record


def build_df_create_results(df_afs_enriched: pd.DataFrame) -> pd.DataFrame:
    if df_afs_enriched is None or df_afs_enriched.empty:
        log_warn("df_afs_enriched vazio; nada para criar")
        return pd.DataFrame()

    creation_results: List[Dict[str, Any]] = []

    grouped = df_afs_enriched.groupby("destination_board", dropna=False)
    grouped_items = list(grouped)

    board_iterator = grouped_items
    if MOSTRAR_PROGRESSO:
        board_iterator = tqdm(grouped_items, total=len(grouped_items), desc="CREATE boards")

    for destination_board, df_group in board_iterator:
        log_info(
            f"Iniciando criacao para destino {destination_board} com {len(df_group)} linhas"
        )

        row_iterator = df_group.iterrows()
        if MOSTRAR_PROGRESSO:
            row_iterator = tqdm(
                df_group.iterrows(),
                total=len(df_group),
                desc=f"CREATE {destination_board}",
                leave=False,
            )

        created_count = 0
        error_count = 0

        for _, row in row_iterator:
            result_record = create_item_from_row(row)
            creation_results.append(result_record)

            if result_record["status_create"] == "created":
                created_count += 1
            else:
                error_count += 1

            time.sleep(SLEEP_BETWEEN_REQUESTS)

        log_info(
            f"Finalizado destino {destination_board}: "
            f"{created_count} criados, {error_count} erros, {len(df_group)} processados"
        )

    df_create_results = pd.DataFrame(creation_results)
    log_info(f"df_create_results gerado com {len(df_create_results)} linhas")
    return df_create_results


if __name__ == "__main__":
    log_info("Executando teste local de create_monday_items")

    df_afs_origin = build_df_afs_origin()
    df_afs_destination = build_df_afs_destination()
    df_afs_diff = build_df_afs_diff(
        df_afs_origin=df_afs_origin,
        df_afs_destination=df_afs_destination,
    )
    df_afs_to_create = build_df_afs_to_create(df_afs_diff=df_afs_diff)
    df_afs_enriched = build_df_afs_enriched(df_afs_to_create=df_afs_to_create)

    df_create_results = build_df_create_results(df_afs_enriched=df_afs_enriched)
    print(df_create_results)
    print(f"Total create results: {len(df_create_results)}")
