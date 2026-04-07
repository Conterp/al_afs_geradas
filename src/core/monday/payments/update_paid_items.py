from __future__ import annotations

import json
import time
from typing import Any, Dict, List

import pandas as pd
from tqdm.auto import tqdm

from src.config.settings import (
    COLUNA_PAGO,
    LOG_PREFIX,
    MOSTRAR_PROGRESSO,
    SLEEP_BETWEEN_REQUESTS,
)
from src.core.monday.destination.fetch_destination_audit_items import (
    build_df_destination_audit,
)
from src.core.monday.execute_monday_query import execute_monday_query
from src.core.monday.payments.build_paid_updates import build_df_paid_to_update
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


def build_update_paid_mutation(
    board_id: str,
    item_id: str,
    paid_target: str = "Sim",
) -> str:
    column_values_json = json.dumps({COLUNA_PAGO: paid_target}, ensure_ascii=False)
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


def update_paid_item(row: pd.Series, dry_run: bool = False) -> Dict[str, Any]:
    afs = clean_string(row.get("afs"))
    item_id = clean_string(row.get("id_item_monday"))
    board_id = clean_string(row.get("board_id"))
    board_name = clean_string(row.get("board_name"))
    paid_target = clean_string(row.get("pago_target")) or "Sim"

    result = {
        "afs": afs,
        "id_item_monday": item_id,
        "board_name": board_name,
        "board_id": board_id,
        "pago_target": paid_target,
        "status_update": "pending",
        "error_message": "",
    }

    if not item_id or not board_id:
        result["status_update"] = "error"
        result["error_message"] = "missing item_id or board_id"
        return result

    if dry_run:
        result["status_update"] = "dry_run"
        return result

    try:
        mutation = build_update_paid_mutation(
            board_id=board_id,
            item_id=item_id,
            paid_target=paid_target,
        )
        execute_monday_query(
            query=mutation,
            operation_name=f"update_paid_item:{item_id}",
        )
        result["status_update"] = "updated"
    except Exception as exc:
        result["status_update"] = "error"
        result["error_message"] = str(exc)
        log_error(f"Falha ao atualizar PAGO no item {item_id} (AF {afs}): {exc}")

    return result


def build_df_paid_update_results(
    df_paid_to_update: pd.DataFrame,
    dry_run: bool = False,
) -> pd.DataFrame:
    if df_paid_to_update is None or df_paid_to_update.empty:
        log_warn("df_paid_to_update vazio; nada para atualizar")
        return pd.DataFrame()

    results: List[Dict[str, Any]] = []

    iterator = df_paid_to_update.iterrows()
    if MOSTRAR_PROGRESSO:
        iterator = tqdm(
            df_paid_to_update.iterrows(),
            total=len(df_paid_to_update),
            desc="UPDATE paid",
        )

    for _, row in iterator:
        results.append(update_paid_item(row=row, dry_run=dry_run))
        time.sleep(SLEEP_BETWEEN_REQUESTS)

    df_paid_update_results = pd.DataFrame(results)
    preferred_cols = [
        "afs",
        "id_item_monday",
        "board_name",
        "board_id",
        "pago_target",
        "status_update",
        "error_message",
    ]
    existing_cols = [col for col in preferred_cols if col in df_paid_update_results.columns]
    if existing_cols:
        df_paid_update_results = df_paid_update_results[existing_cols].copy()
    log_info(f"df_paid_update_results gerado com {len(df_paid_update_results)} linhas")
    return df_paid_update_results


def build_df_pago_update_results(
    df_pago_to_update: pd.DataFrame,
    dry_run: bool = False,
) -> pd.DataFrame:
    return build_df_paid_update_results(
        df_paid_to_update=df_pago_to_update,
        dry_run=dry_run,
    )


if __name__ == "__main__":
    log_info("Executando teste local de update_paid_items")
    df_destination_audit = build_df_destination_audit()
    df_payments_realized = build_df_payments_realized()
    df_paid_to_update = build_df_paid_to_update(
        df_destination_audit=df_destination_audit,
        df_payments_realized=df_payments_realized,
    )

    df_paid_update_results = build_df_paid_update_results(
        df_paid_to_update=df_paid_to_update,
        dry_run=True,
    )
    print(df_paid_update_results)
    print(f"Total itens atualizados (dry_run): {len(df_paid_update_results)}")
