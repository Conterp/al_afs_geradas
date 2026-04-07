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
    build_df_wrong_group,
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


def build_move_item_to_group_mutation(item_id: str, group_id: str) -> str:
    return f"""
    mutation {{
      move_item_to_group(item_id: {item_id}, group_id: "{group_id}") {{
        id
      }}
    }}
    """


def move_wrong_group_item(row: pd.Series, dry_run: bool = False) -> Dict[str, Any]:
    afs = clean_string(row.get("afs"))
    item_id = clean_string(row.get("id_item_monday"))
    item_name = clean_string(row.get("item_name"))
    board_name = clean_string(row.get("board_name"))
    current_group_id = clean_string(row.get("group_id"))
    current_group_title = clean_string(row.get("group_title"))
    expected_group_id = clean_string(row.get("grupo_destino_esperado"))
    expected_group_title = clean_string(row.get("grupo_destino_esperado_name"))

    result = {
        "afs": afs,
        "id_item_monday": item_id,
        "item_name": item_name,
        "board_name": board_name,
        "group_id_atual": current_group_id,
        "group_title_atual": current_group_title,
        "group_id_esperado": expected_group_id,
        "group_title_esperado": expected_group_title,
        "move_status": "pending",
        "error_message": "",
    }

    if not item_id:
        result["move_status"] = "error"
        result["error_message"] = "id_item_monday invalido para mover"
        return result

    if not expected_group_id:
        result["move_status"] = "error"
        result["error_message"] = "grupo_destino_esperado vazio"
        return result

    if dry_run:
        result["move_status"] = "dry_run"
        return result

    try:
        mutation = build_move_item_to_group_mutation(
            item_id=item_id,
            group_id=expected_group_id,
        )
        execute_monday_query(
            query=mutation,
            operation_name=f"move_wrong_group:{board_name}",
        )
        result["move_status"] = "moved"
    except Exception as exc:
        result["move_status"] = "error"
        result["error_message"] = str(exc)
        log_error(
            f"Falha ao mover wrong_group AF {afs} | item {item_id} | board {board_name}: {exc}"
        )

    return result


def build_df_wrong_group_move_results(
    df_wrong_group: pd.DataFrame,
    dry_run: bool = False,
) -> pd.DataFrame:
    if df_wrong_group is None or df_wrong_group.empty:
        log_warn("df_wrong_group vazio; nada para mover")
        return pd.DataFrame()

    move_results: List[Dict[str, Any]] = []

    grouped = df_wrong_group.groupby("board_name", dropna=False)
    grouped_items = list(grouped)

    board_iterator = grouped_items
    if MOSTRAR_PROGRESSO:
        board_iterator = tqdm(
            grouped_items,
            total=len(grouped_items),
            desc="MOVE WRONG GROUP boards",
        )

    for board_name, df_group in board_iterator:
        log_info(f"Iniciando movimentacao de wrong_group em {board_name} com {len(df_group)} linhas")

        row_iterator = df_group.iterrows()
        if MOSTRAR_PROGRESSO:
            row_iterator = tqdm(
                df_group.iterrows(),
                total=len(df_group),
                desc=f"MOVE WRONG GROUP {board_name}",
                leave=False,
            )

        moved_count = 0
        error_count = 0

        for _, row in row_iterator:
            result_record = move_wrong_group_item(row=row, dry_run=dry_run)
            move_results.append(result_record)

            if result_record["move_status"] in {"moved", "dry_run"}:
                moved_count += 1
            else:
                error_count += 1

            time.sleep(SLEEP_BETWEEN_REQUESTS)

        log_info(
            f"Finalizado {board_name}: "
            f"{moved_count} sucesso, {error_count} erros, {len(df_group)} processados"
        )

    df_wrong_group_move_results = pd.DataFrame(move_results)
    preferred_cols = [
        "afs",
        "id_item_monday",
        "item_name",
        "board_name",
        "group_title_atual",
        "group_title_esperado",
        "move_status",
        "error_message",
    ]
    existing_cols = [col for col in preferred_cols if col in df_wrong_group_move_results.columns]
    if existing_cols:
        df_wrong_group_move_results = df_wrong_group_move_results[existing_cols].copy()

    log_info(f"df_wrong_group_move_results gerado com {len(df_wrong_group_move_results)} linhas")
    return df_wrong_group_move_results


if __name__ == "__main__":
    log_info("Executando teste local de move_wrong_group_items")
    df_afs_origin = build_df_afs_origin()
    df_destination_audit = build_df_destination_audit()
    df_origin_expected = build_df_origem_expected_destino(df_afs_origem=df_afs_origin)
    df_wrong_group = build_df_wrong_group(
        df_destino_auditoria=df_destination_audit,
        df_origem_expected=df_origin_expected,
    )

    df_wrong_group_move_results = build_df_wrong_group_move_results(
        df_wrong_group=df_wrong_group,
        dry_run=True,
    )
    print(df_wrong_group_move_results)
    print(f"Total wrong group moves (dry_run): {len(df_wrong_group_move_results)}")
