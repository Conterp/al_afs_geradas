from __future__ import annotations

import time
from typing import Any, Dict, List

import pandas as pd
from tqdm.auto import tqdm

from src.config.settings import LOG_PREFIX, MOSTRAR_PROGRESSO, SLEEP_BETWEEN_REQUESTS
from src.core.monday.destination.duplicates.build_duplicate_actions import (
    build_df_duplicate_actions,
    build_df_duplicates_delete,
)
from src.core.monday.destination.duplicates.find_duplicate_items import (
    build_df_duplicates,
    build_df_duplicates_summary,
)
from src.core.monday.destination.fetch_destination_audit_items import (
    build_df_destination_audit,
)
from src.core.monday.execute_monday_query import execute_monday_query


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


def delete_duplicate_item(item_id: str, dry_run: bool = False) -> Dict[str, Any]:
    # Acao possivel: dry_run=True para simular sem deletar.
    if dry_run:
        return {
            "id_item_monday": item_id,
            "status_delete": "dry_run",
            "deleted_item_id": item_id,
            "error_message": "",
        }

    try:
        mutation = build_delete_item_mutation(item_id=item_id)
        data = execute_monday_query(
            query=mutation,
            operation_name=f"delete_duplicate_item:{item_id}",
        )
        deleted_id = clean_string(data.get("delete_item", {}).get("id"))
        return {
            "id_item_monday": item_id,
            "status_delete": "deleted",
            "deleted_item_id": deleted_id or item_id,
            "error_message": "",
        }
    except Exception as exc:
        log_error(f"Falha ao deletar item duplicado {item_id}: {exc}")
        return {
            "id_item_monday": item_id,
            "status_delete": "error",
            "deleted_item_id": "",
            "error_message": str(exc),
        }


def build_df_duplicates_delete_results(
    df_duplicates_delete: pd.DataFrame,
    dry_run: bool = False,
) -> pd.DataFrame:
    if df_duplicates_delete is None or df_duplicates_delete.empty:
        log_warn("df_duplicates_delete vazio; nada para deletar")
        return pd.DataFrame()

    df = df_duplicates_delete.copy()
    if "id_item_monday" not in df.columns:
        raise ValueError("df_duplicates_delete sem coluna obrigatoria: id_item_monday")

    ids_to_delete = (
        df["id_item_monday"]
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .unique()
        .tolist()
    )

    if not ids_to_delete:
        log_warn("Nenhum id_item_monday valido para deletar")
        return pd.DataFrame()

    delete_results: List[Dict[str, Any]] = []

    iterator = ids_to_delete
    if MOSTRAR_PROGRESSO:
        iterator = tqdm(
            ids_to_delete,
            total=len(ids_to_delete),
            desc="DELETE duplicates",
        )

    for item_id in iterator:
        delete_results.append(delete_duplicate_item(item_id=item_id, dry_run=dry_run))
        time.sleep(SLEEP_BETWEEN_REQUESTS)

    df_delete_results = pd.DataFrame(delete_results)
    log_info(f"df_duplicates_delete_results gerado com {len(df_delete_results)} linhas")
    return df_delete_results


if __name__ == "__main__":
    log_info("Executando teste local de delete_duplicate_items")
    df_destination_audit = build_df_destination_audit()
    df_duplicates_summary = build_df_duplicates_summary(
        df_destination_audit=df_destination_audit
    )
    df_duplicates = build_df_duplicates(
        df_destination_audit=df_destination_audit,
        df_duplicates_summary=df_duplicates_summary,
    )
    df_duplicate_actions = build_df_duplicate_actions(df_duplicates=df_duplicates)
    df_duplicates_delete = build_df_duplicates_delete(
        df_duplicate_actions=df_duplicate_actions
    )

    # Em teste local, mantemos dry_run=True para seguranca.
    df_duplicates_delete_results = build_df_duplicates_delete_results(
        df_duplicates_delete=df_duplicates_delete,
        dry_run=True,
    )
    print(df_duplicates_delete_results)
    print(f"Total duplicate deletions: {len(df_duplicates_delete_results)}")
