from __future__ import annotations

import json
import time
from typing import Any, Dict, List

import pandas as pd
from tqdm.auto import tqdm

from src.config.settings import COLUNA_PAGO, LOG_PREFIX, MOSTRAR_PROGRESSO, SLEEP_BETWEEN_REQUESTS
from src.core.monday.destination.fetch_destination_audit_items import (
    build_df_destination_audit,
)
from src.core.monday.destination.orphans.find_orphan_items import build_df_wrong_pago
from src.core.monday.execute_monday_query import execute_monday_query
from src.core.monday.payments.fetch_payment_items import build_df_payments_realized


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


def escape_graphql_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)[1:-1]


def build_clear_paid_mutation(board_id: str, item_id: str) -> str:
    column_values_json = json.dumps({COLUNA_PAGO: None}, ensure_ascii=False)
    safe_column_values = escape_graphql_string(column_values_json)

    return f"""
    mutation {{
      change_multiple_column_values(
        board_id: {board_id},
        item_id: {item_id},
        column_values: "{safe_column_values}"
      ) {{
        id
      }}
    }}
    """


def clear_wrong_paid_item(row: pd.Series, dry_run: bool = False) -> Dict[str, Any]:
    afs = clean_string(row.get("afs"))
    item_id = clean_string(row.get("id_item_monday"))
    board_id = clean_string(row.get("board_id"))
    board_name = clean_string(row.get("board_name"))
    item_name = clean_string(row.get("item_name"))
    paid_current = clean_string(row.get("pago"))

    result = {
        "afs": afs,
        "id_item_monday": item_id,
        "item_name": item_name,
        "board_name": board_name,
        "pago_atual": paid_current,
        "novo_pago": "",
        "clear_status": "pending",
        "error_message": "",
    }

    if not item_id or not board_id:
        result["clear_status"] = "error"
        result["error_message"] = "missing item_id or board_id"
        return result

    if dry_run:
        result["clear_status"] = "dry_run"
        return result

    try:
        mutation = build_clear_paid_mutation(board_id=board_id, item_id=item_id)
        execute_monday_query(
            query=mutation,
            operation_name=f"clear_wrong_paid:{board_name}",
        )
        result["clear_status"] = "cleared"
    except Exception as exc:
        result["clear_status"] = "error"
        result["error_message"] = str(exc)
        log_error(
            f"Falha ao limpar wrong_paid AF {afs} | item {item_id} | board {board_name}: {exc}"
        )

    return result


def build_df_wrong_paid_clear_results(
    df_wrong_paid: pd.DataFrame,
    dry_run: bool = False,
) -> pd.DataFrame:
    if df_wrong_paid is None or df_wrong_paid.empty:
        log_warn("df_wrong_paid vazio; nada para limpar")
        return pd.DataFrame()

    clear_results: List[Dict[str, Any]] = []

    grouped = df_wrong_paid.groupby("board_name", dropna=False)
    grouped_items = list(grouped)

    board_iterator = grouped_items
    if MOSTRAR_PROGRESSO:
        board_iterator = tqdm(
            grouped_items,
            total=len(grouped_items),
            desc="CLEAR WRONG PAID boards",
        )

    for board_name, df_group in board_iterator:
        log_info(f"Iniciando limpeza de wrong_paid em {board_name} com {len(df_group)} linhas")

        row_iterator = df_group.iterrows()
        if MOSTRAR_PROGRESSO:
            row_iterator = tqdm(
                df_group.iterrows(),
                total=len(df_group),
                desc=f"CLEAR WRONG PAID {board_name}",
                leave=False,
            )

        clear_count = 0
        error_count = 0

        for _, row in row_iterator:
            result_record = clear_wrong_paid_item(row=row, dry_run=dry_run)
            clear_results.append(result_record)

            if result_record["clear_status"] in {"cleared", "dry_run"}:
                clear_count += 1
            else:
                error_count += 1

            time.sleep(SLEEP_BETWEEN_REQUESTS)

        log_info(
            f"Finalizado {board_name}: "
            f"{clear_count} sucesso, {error_count} erros, {len(df_group)} processados"
        )

    df_wrong_paid_clear_results = pd.DataFrame(clear_results)
    preferred_cols = [
        "afs",
        "id_item_monday",
        "item_name",
        "board_name",
        "pago_atual",
        "novo_pago",
        "clear_status",
        "error_message",
    ]
    existing_cols = [col for col in preferred_cols if col in df_wrong_paid_clear_results.columns]
    if existing_cols:
        df_wrong_paid_clear_results = df_wrong_paid_clear_results[existing_cols].copy()

    log_info(f"df_wrong_paid_clear_results gerado com {len(df_wrong_paid_clear_results)} linhas")
    return df_wrong_paid_clear_results


def build_df_wrong_pago_clear_results(
    df_wrong_pago: pd.DataFrame,
    dry_run: bool = False,
) -> pd.DataFrame:
    return build_df_wrong_paid_clear_results(
        df_wrong_paid=df_wrong_pago,
        dry_run=dry_run,
    )


if __name__ == "__main__":
    log_info("Executando teste local de clear_wrong_paid_items")
    df_destination_audit = build_df_destination_audit()
    df_payments_realized = build_df_payments_realized()
    df_wrong_paid = build_df_wrong_pago(
        df_destino_auditoria=df_destination_audit,
        df_pagamentos_realizados=df_payments_realized,
    )

    df_wrong_paid_clear_results = build_df_wrong_paid_clear_results(
        df_wrong_paid=df_wrong_paid,
        dry_run=True,
    )
    print(df_wrong_paid_clear_results)
    print(f"Total wrong_paid clear (dry_run): {len(df_wrong_paid_clear_results)}")
