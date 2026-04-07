from __future__ import annotations

import time
from typing import Any, Dict, List

import pandas as pd
from tqdm.auto import tqdm

from src.config.settings import LOG_PREFIX, MOSTRAR_PROGRESSO, SLEEP_BETWEEN_REQUESTS
from src.core.monday.destination.fetch_destination_audit_items import (
    build_df_destination_audit,
)
from src.core.monday.destination.orphans.find_orphan_items import (
    build_df_origem_expected_destino,
    build_df_wrong_board,
)
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


def delete_wrong_board_item(row: pd.Series, dry_run: bool = False) -> Dict[str, Any]:
    item_id = clean_string(row.get("id_item_monday"))
    afs = clean_string(row.get("afs"))
    board_id = clean_string(row.get("board_id"))
    board_name = clean_string(row.get("board_name"))
    item_name = clean_string(row.get("item_name"))
    board_destino_atual_name = clean_string(row.get("board_destino_atual_name"))
    board_destino_esperado_name = clean_string(row.get("board_destino_esperado_name"))

    result_record = {
        "afs": afs,
        "board_id": board_id,
        "board_name": board_name,
        "board_atual_name": board_destino_atual_name or board_name,
        "board_destino_esperado_name": board_destino_esperado_name,
        "id_item_monday": item_id,
        "item_name": item_name,
        "delete_status": "pending",
        "error_message": "",
    }

    if not item_id:
        result_record["delete_status"] = "error"
        result_record["error_message"] = "id_item_monday invalido para deletar"
        return result_record

    if dry_run:
        result_record["delete_status"] = "dry_run"
        return result_record

    try:
        mutation = build_delete_item_mutation(item_id=item_id)
        execute_monday_query(
            query=mutation,
            operation_name=f"delete_wrong_board:{board_name}",
        )
        result_record["delete_status"] = "deleted"
    except Exception as exc:
        result_record["delete_status"] = "error"
        result_record["error_message"] = str(exc)
        log_error(
            f"Falha ao deletar wrong_board AF {afs} | item {item_id} | board {board_name}: {exc}"
        )

    return result_record


def build_df_wrong_board_delete_results(
    df_wrong_board: pd.DataFrame,
    dry_run: bool = False,
) -> pd.DataFrame:
    if df_wrong_board is None or df_wrong_board.empty:
        log_warn("df_wrong_board vazio; nada para deletar")
        return pd.DataFrame()

    delete_results: List[Dict[str, Any]] = []

    grouped = df_wrong_board.groupby("board_name", dropna=False)
    grouped_items = list(grouped)

    board_iterator = grouped_items
    if MOSTRAR_PROGRESSO:
        board_iterator = tqdm(
            grouped_items,
            total=len(grouped_items),
            desc="DELETE WRONG BOARD boards",
        )

    for board_name, df_group in board_iterator:
        log_info(f"Iniciando delecao de wrong_board em {board_name} com {len(df_group)} linhas")

        row_iterator = df_group.iterrows()
        if MOSTRAR_PROGRESSO:
            row_iterator = tqdm(
                df_group.iterrows(),
                total=len(df_group),
                desc=f"DELETE WRONG BOARD {board_name}",
                leave=False,
            )

        deleted_count = 0
        error_count = 0

        for _, row in row_iterator:
            result_record = delete_wrong_board_item(row=row, dry_run=dry_run)
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

    df_wrong_board_delete_results = pd.DataFrame(delete_results)
    preferred_cols = [
        "afs",
        "id_item_monday",
        "item_name",
        "board_atual_name",
        "board_destino_esperado_name",
        "delete_status",
        "error_message",
    ]
    existing_cols = [col for col in preferred_cols if col in df_wrong_board_delete_results.columns]
    if existing_cols:
        df_wrong_board_delete_results = df_wrong_board_delete_results[existing_cols].copy()
    log_info(f"df_wrong_board_delete_results gerado com {len(df_wrong_board_delete_results)} linhas")
    return df_wrong_board_delete_results


if __name__ == "__main__":
    log_info("Executando teste local de delete_wrong_board_items")
    df_afs_origin = build_df_afs_origin()
    df_destination_audit = build_df_destination_audit()
    df_origin_expected = build_df_origem_expected_destino(df_afs_origem=df_afs_origin)
    df_wrong_board = build_df_wrong_board(
        df_destino_auditoria=df_destination_audit,
        df_origem_expected=df_origin_expected,
    )

    df_wrong_board_delete_results = build_df_wrong_board_delete_results(
        df_wrong_board=df_wrong_board,
        dry_run=True,
    )
    print(df_wrong_board_delete_results)
    print(f"Total wrong board delete results: {len(df_wrong_board_delete_results)}")
