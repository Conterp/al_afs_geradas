from __future__ import annotations

import time
from typing import Any, Dict, List

import pandas as pd
from tqdm.auto import tqdm

from src.config.settings import LOG_PREFIX, MOSTRAR_PROGRESSO, SLEEP_BETWEEN_REQUESTS
from src.core.monday.destination.fetch_destination_audit_items import (
    build_df_destination_audit,
)
from src.core.monday.destination.orphans.find_orphan_items import build_df_no_origin
from src.core.monday.execute_monday_query import execute_monday_query
from src.core.monday.origin.fetch_origin_items import build_df_afs_origin


def log_info(message: str) -> None:
    print(f"{LOG_PREFIX} [INFO] {message}")


def log_warn(message: str) -> None:
    print(f"{LOG_PREFIX} [WARN] {message}")


def log_error(message: str) -> None:
    print(f"{LOG_PREFIX} [ERROR] {message}")


def clean_string(value: Any) -> str:
    if value is None:
        return ""
    value_str = str(value).strip()
    return "" if value_str.lower() == "nan" else value_str


def build_delete_item_mutation(item_id: str) -> str:
    return f"""
    mutation {{
      delete_item(item_id: {item_id}) {{
        id
      }}
    }}
    """


def delete_no_origin_item(row: pd.Series, dry_run: bool = False) -> Dict[str, Any]:
    afs = clean_string(row.get("afs"))
    item_id = clean_string(row.get("id_item_monday"))
    item_name = clean_string(row.get("item_name"))
    board_name = clean_string(row.get("board_name"))
    group_title = clean_string(row.get("group_title"))

    result = {
        "afs": afs,
        "id_item_monday": item_id,
        "item_name": item_name,
        "board_name": board_name,
        "group_title": group_title,
        "delete_status": "pending",
        "error_message": "",
    }

    if not item_id:
        result["delete_status"] = "error"
        result["error_message"] = "id_item_monday invalido para deletar"
        return result

    if dry_run:
        result["delete_status"] = "dry_run"
        return result

    try:
        mutation = build_delete_item_mutation(item_id=item_id)
        execute_monday_query(
            query=mutation,
            operation_name=f"delete_no_origin:{board_name}",
        )
        result["delete_status"] = "deleted"
    except Exception as exc:
        result["delete_status"] = "error"
        result["error_message"] = str(exc)
        log_error(
            f"Falha ao deletar no_origin AF {afs} | item {item_id} | board {board_name}: {exc}"
        )

    return result


def build_df_no_origin_delete_results(
    df_no_origin: pd.DataFrame,
    dry_run: bool = False,
) -> pd.DataFrame:
    if df_no_origin is None or df_no_origin.empty:
        log_warn("df_no_origin vazio; nada para deletar")
        return pd.DataFrame()

    delete_results: List[Dict[str, Any]] = []

    grouped = df_no_origin.groupby("board_name", dropna=False)
    grouped_items = list(grouped)

    board_iterator = grouped_items
    if MOSTRAR_PROGRESSO:
        board_iterator = tqdm(
            grouped_items,
            total=len(grouped_items),
            desc="DELETE NO ORIGIN boards",
        )

    for board_name, df_group in board_iterator:
        log_info(f"Iniciando delecao de no_origin em {board_name} com {len(df_group)} linhas")

        row_iterator = df_group.iterrows()
        if MOSTRAR_PROGRESSO:
            row_iterator = tqdm(
                df_group.iterrows(),
                total=len(df_group),
                desc=f"DELETE NO ORIGIN {board_name}",
                leave=False,
            )

        deleted_count = 0
        error_count = 0

        for _, row in row_iterator:
            result_record = delete_no_origin_item(row=row, dry_run=dry_run)
            delete_results.append(result_record)

            if result_record["delete_status"] in {"deleted", "dry_run"}:
                deleted_count += 1
            else:
                error_count += 1

            time.sleep(SLEEP_BETWEEN_REQUESTS)

        log_info(
            f"Finalizado {board_name}: "
            f"{deleted_count} sucesso, {error_count} erros, {len(df_group)} processados"
        )

    df_no_origin_delete_results = pd.DataFrame(delete_results)
    preferred_cols = [
        "afs",
        "id_item_monday",
        "item_name",
        "board_name",
        "group_title",
        "delete_status",
        "error_message",
    ]
    existing_cols = [col for col in preferred_cols if col in df_no_origin_delete_results.columns]
    if existing_cols:
        df_no_origin_delete_results = df_no_origin_delete_results[existing_cols].copy()

    log_info(f"df_no_origin_delete_results gerado com {len(df_no_origin_delete_results)} linhas")
    return df_no_origin_delete_results


if __name__ == "__main__":
    log_info("Executando teste local de delete_no_origin_items")
    df_afs_origin = build_df_afs_origin()
    df_destination_audit = build_df_destination_audit()
    df_no_origin = build_df_no_origin(
        df_destino_auditoria=df_destination_audit,
        df_afs_origem=df_afs_origin,
    )

    df_no_origin_delete_results = build_df_no_origin_delete_results(
        df_no_origin=df_no_origin,
        dry_run=True,
    )
    print(df_no_origin_delete_results)
    print(f"Total no_origin deletes (dry_run): {len(df_no_origin_delete_results)}")
