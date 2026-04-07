from __future__ import annotations

import sys
import time

from src.config.settings import LOG_PREFIX, check_required_envs
from src.core.monday.destination.build_destination_board_map import add_destination_board
from src.core.monday.destination.build_missing_afs import (
    build_df_afs_diff,
    build_df_afs_to_create,
)
from src.core.monday.destination.create_monday_items import build_df_create_results
from src.core.monday.destination.duplicates.build_duplicate_actions import (
    build_df_duplicate_actions,
    build_df_duplicates_delete,
    build_df_duplicates_keep,
)
from src.core.monday.destination.duplicates.delete_duplicate_items import (
    build_df_duplicates_delete_results,
)
from src.core.monday.destination.duplicates.find_duplicate_items import (
    build_df_duplicates,
    build_df_duplicates_summary,
)
from src.core.monday.destination.fetch_destination_audit_items import (
    build_df_destination_audit,
)
from src.core.monday.destination.fetch_destination_items import build_df_afs_destination
from src.core.monday.destination.orphans.delete_wrong_board_items import (
    build_df_wrong_board_delete_results,
)
from src.core.monday.destination.orphans.delete_no_origin_items import (
    build_df_no_origin_delete_results,
)
from src.core.monday.destination.orphans.clear_wrong_paid_items import (
    build_df_wrong_paid_clear_results,
)
from src.core.monday.destination.orphans.find_orphan_items import (
    build_df_no_origin,
    build_df_origem_expected_destino,
    build_df_wrong_board,
    build_df_wrong_group,
    build_df_wrong_pago,
)
from src.core.monday.destination.orphans.move_wrong_group_items import (
    build_df_wrong_group_move_results,
)
from src.core.monday.destination.summary.build_execution_summary import (
    build_df_execution_summary,
)
from src.core.monday.origin.build_enriched_afs import build_df_afs_enriched
from src.core.monday.origin.fetch_origin_items import build_df_afs_origin
from src.core.monday.payments.build_paid_updates import build_df_paid_to_update
from src.core.monday.payments.fetch_payment_items import build_df_payments_realized
from src.core.monday.payments.update_paid_items import build_df_paid_update_results


def log_info(message: str) -> None:
    print(f"{LOG_PREFIX} [INFO] {message}")


def log_error(message: str) -> None:
    print(f"{LOG_PREFIX} [ERROR] {message}")


def print_panel(
    title: str,
    df,
    explanation: str,
    board_col: str = "board_name",
    preview_cols=None,
    preview_rows: int = 8,
) -> None:
    print("\n" + "=" * 92)
    print(f"{title}")
    print("=" * 92)

    if df is None or df.empty:
        print("Total: 0")
        print(f"O que significa: {explanation}")
        print("Sem registros.")
        return

    total_rows = len(df)
    print(f"Total: {total_rows}")
    print(f"O que significa: {explanation}")

    if board_col in df.columns:
        board_counts = df[board_col].fillna("").astype(str).str.strip().value_counts()
        if not board_counts.empty:
            print("\nDistribuicao por board:")
            print(board_counts.to_string())

    if preview_cols is None:
        preview_df = df.head(preview_rows)
    else:
        existing_cols = [col for col in preview_cols if col in df.columns]
        preview_df = df[existing_cols].head(preview_rows)

    print("\nPreview:")
    print(preview_df)


def main() -> int:
    pipeline_start_ts = time.perf_counter()
    print("---------------------------------")
    print("\nIniciando pipeline AL_AFS_GERADAS...\n")
    print("---------------------------------")

    try:
        print("\n1) Validando variaveis de ambiente...")
        check_required_envs()

        print("\n")
        print("\n2) Lendo itens dos boards de origin...")
        df_afs_origin = build_df_afs_origin()
        print(df_afs_origin)
        print(f"Total AFs de origin: {len(df_afs_origin)}")

        print("\n")
        print("\n3) Lendo itens dos boards de destination...")
        df_afs_destination = build_df_afs_destination()
        print(df_afs_destination)
        print(f"Total AFs de destination: {len(df_afs_destination)}")

        print("\n")
        print("\n4) Mapeando cost_center para destination_board...")
        df_afs_origin = add_destination_board(df_afs_origin)
        print(df_afs_origin)
        print(f"Total AFs com destination_board: {len(df_afs_origin)}")

        print("\n")
        print("\n5) Identificando AFs faltantes para criacao...")
        df_afs_diff = build_df_afs_diff(
            df_afs_origin=df_afs_origin,
            df_afs_destination=df_afs_destination,
        )
        print(f"Total AFs diff: {len(df_afs_diff)}")

        df_afs_to_create = build_df_afs_to_create(df_afs_diff=df_afs_diff)
        print(df_afs_to_create)
        print(f"Total AFs para criar: {len(df_afs_to_create)}")

        print("\n")
        print("\n6) Buscando detalhes dos itens de origem para enriquecimento...")
        df_afs_enriched = build_df_afs_enriched(df_afs_to_create=df_afs_to_create)
        print(df_afs_enriched)
        print(f"Total AFs enriched: {len(df_afs_enriched)}")

        print("\n")
        print("\n7) Criando itens faltantes nos boards de destino...")
        df_create_results = build_df_create_results(df_afs_enriched=df_afs_enriched)
        print(df_create_results)
        print(f"Total create results: {len(df_create_results)}")

        print("\n")
        print("\n8) Recarregando destino para auditoria...")
        df_destination_audit = build_df_destination_audit()
        print(df_destination_audit)
        print(f"Total destination audit: {len(df_destination_audit)}")

        print("\n")
        print("\n9) Encontrando itens duplicados no destino...")
        df_duplicates_summary = build_df_duplicates_summary(
            df_destination_audit=df_destination_audit
        )
        print(df_duplicates_summary)
        print(f"Total duplicate keys: {len(df_duplicates_summary)}")

        df_duplicates = build_df_duplicates(
            df_destination_audit=df_destination_audit,
            df_duplicates_summary=df_duplicates_summary,
        )
        print(df_duplicates)
        print(f"Total duplicate rows: {len(df_duplicates)}")

        print("\n")
        print("\n10) Definindo resolucao dos duplicados...")
        # Acoes possiveis para cada item duplicado:
        # - KEEP: manter o item com melhor prioridade/completude
        # - DELETE: excluir item duplicado restante
        df_duplicate_actions = build_df_duplicate_actions(df_duplicates=df_duplicates)
        print(df_duplicate_actions)
        print(f"Total duplicate actions: {len(df_duplicate_actions)}")

        df_duplicates_keep = build_df_duplicates_keep(
            df_duplicate_actions=df_duplicate_actions
        )
        print(f"Total KEEP: {len(df_duplicates_keep)}")

        df_duplicates_delete = build_df_duplicates_delete(
            df_duplicate_actions=df_duplicate_actions
        )
        print(f"Total DELETE: {len(df_duplicates_delete)}")

        print("\n")
        print("\n11) Deletando itens duplicados...")
        # Acoes possiveis:
        # - dry_run=True: simula sem deletar no Monday
        # - dry_run=False: executa delecao real
        df_duplicates_delete_results = build_df_duplicates_delete_results(
            df_duplicates_delete=df_duplicates_delete,
            dry_run=False,
        )
        print(df_duplicates_delete_results)
        print(f"Total duplicate deletions: {len(df_duplicates_delete_results)}")

        print("\n")
        print("\n12) Lendo boards de pagamentos realizados...")
        df_payments_realized = build_df_payments_realized()
        print(df_payments_realized)
        print(f"Total pagamentos realizados: {len(df_payments_realized)}")

        print("\n")
        print("\n13) Identificando itens com PAGO para atualizar...")
        df_paid_to_update = build_df_paid_to_update(
            df_destination_audit=df_destination_audit,
            df_payments_realized=df_payments_realized,
        )
        print(df_paid_to_update)
        print(f"Total itens para atualizar PAGO: {len(df_paid_to_update)}")

        print("\n")
        print("\n14) Atualizando coluna PAGO...")
        # Acoes possiveis:
        # - dry_run=True: simula sem alterar no Monday
        # - dry_run=False: atualiza no Monday
        df_paid_update_results = build_df_paid_update_results(
            df_paid_to_update=df_paid_to_update,
            dry_run=False,
        )
        display_cols = [
            "afs",
            "id_item_monday",
            "board_name",
            "pago_target",
            "status_update",
            "error_message",
        ]
        existing_display_cols = [
            col for col in display_cols if col in df_paid_update_results.columns
        ]
        print(df_paid_update_results[existing_display_cols])
        print(f"Total updates de PAGO: {len(df_paid_update_results)}")

        print("\n")
        print("\n15) Encontrando wrong board, wrong group, no origin e wrong pago...")
        # Base de referência: para cada AF de origem, define board/grupo esperados no destino.
        df_origin_expected = build_df_origem_expected_destino(df_afs_origem=df_afs_origin)
        print_panel(
            title="Origin expected",
            df=df_origin_expected,
            explanation=(
                "Quantidade de AFs da origem que possuem destino esperado (board/grupo) "
                "e serao usadas para validar o destino."
            ),
            board_col="board_name_origem",
            preview_cols=[
                "afs",
                "board_name_origem",
                "group_title_origem",
                "cost_center",
                "board_destino_esperado_name",
                "grupo_destino_esperado_name",
            ],
        )

        # Item existe no destino, mas caiu em board diferente do esperado para a AF.
        df_wrong_board = build_df_wrong_board(
            df_destino_auditoria=df_destination_audit,
            df_origem_expected=df_origin_expected,
        )
        print_panel(
            title="Wrong board",
            df=df_wrong_board,
            explanation=(
                "Itens que existem no destino, mas estao no board errado em relacao ao "
                "board esperado da origem."
            ),
            preview_cols=[
                "afs",
                "item_name",
                "board_name",
                "board_destino_atual_name",
                "board_destino_esperado_name",
                "group_title",
            ],
        )

        # Item está no board correto, porém no grupo errado.
        df_wrong_group = build_df_wrong_group(
            df_destino_auditoria=df_destination_audit,
            df_origem_expected=df_origin_expected,
        )
        print_panel(
            title="Wrong group",
            df=df_wrong_group,
            explanation=(
                "Itens no board correto, mas em grupo diferente do grupo esperado "
                "(ex.: AFs Geradas, Canceladas, 2024)."
            ),
            preview_cols=[
                "afs",
                "item_name",
                "board_name",
                "group_title",
                "grupo_destino_esperado_name",
                "group_title_origem",
            ],
        )

        # Item no destino sem correspondência válida na origem.
        df_no_origin = build_df_no_origin(
            df_destino_auditoria=df_destination_audit,
            df_afs_origem=df_afs_origin,
        )
        print_panel(
            title="No origin",
            df=df_no_origin,
            explanation=(
                "Itens que existem no destino, mas nao possuem AF valida correspondente "
                "na base de origem."
            ),
            preview_cols=[
                "afs",
                "item_name",
                "board_name",
                "group_title",
                "pago",
                "af_date",
            ],
        )

        # Item marcado como pago no destino, mas AF não aparece em pagamentos realizados.
        df_wrong_pago = build_df_wrong_pago(
            df_destino_auditoria=df_destination_audit,
            df_pagamentos_realizados=df_payments_realized,
        )
        print_panel(
            title="Wrong pago",
            df=df_wrong_pago,
            explanation=(
                "Itens marcados como 'Sim' em PAGO no destino, mas que nao foram "
                "encontrados nos boards de pagamentos realizados."
            ),
            preview_cols=[
                "afs",
                "item_name",
                "board_name",
                "group_title",
                "pago",
                "novo_pago",
                "af_date",
            ],
        )

        print("\n")
        print("\n16) Deletando itens no board errado...")
        # Acoes possiveis:
        # - dry_run=True: simula sem deletar no Monday
        # - dry_run=False: executa delecao real
        df_wrong_board_delete_results = build_df_wrong_board_delete_results(
            df_wrong_board=df_wrong_board,
            dry_run=False,
        )
        display_cols = [
            "afs",
            "id_item_monday",
            "item_name",
            "board_atual_name",
            "board_destino_esperado_name",
            "delete_status",
            "error_message",
        ]
        existing_display_cols = [
            col for col in display_cols if col in df_wrong_board_delete_results.columns
        ]
        print(df_wrong_board_delete_results[existing_display_cols])
        print(f"Total wrong board deletes: {len(df_wrong_board_delete_results)}")

        print("\n")
        print("\n17) Movendo itens para o grupo correto...")
        # Acoes possiveis:
        # - dry_run=True: simula sem mover no Monday
        # - dry_run=False: executa movimentacao real
        df_wrong_group_move_results = build_df_wrong_group_move_results(
            df_wrong_group=df_wrong_group,
            dry_run=False,
        )
        display_cols = [
            "afs",
            "id_item_monday",
            "item_name",
            "board_name",
            "group_title_atual",
            "group_title_esperado",
            "move_status",
            "error_message",
        ]
        existing_display_cols = [
            col for col in display_cols if col in df_wrong_group_move_results.columns
        ]
        print(df_wrong_group_move_results[existing_display_cols])
        print(f"Total wrong group moves: {len(df_wrong_group_move_results)}")

        print("\n")
        print("\n18) Deletando itens sem origem...")
        # Acoes possiveis:
        # - dry_run=True: simula sem deletar no Monday
        # - dry_run=False: executa delecao real
        df_no_origin_delete_results = build_df_no_origin_delete_results(
            df_no_origin=df_no_origin,
            dry_run=False,
        )
        display_cols = [
            "afs",
            "id_item_monday",
            "item_name",
            "board_name",
            "group_title",
            "delete_status",
            "error_message",
        ]
        existing_display_cols = [
            col for col in display_cols if col in df_no_origin_delete_results.columns
        ]
        print(df_no_origin_delete_results[existing_display_cols])
        print(f"Total no origin deletes: {len(df_no_origin_delete_results)}")

        print("\n")
        print("\n19) Limpando itens com PAGO incorreto...")
        # Acoes possiveis:
        # - dry_run=True: simula sem limpar no Monday
        # - dry_run=False: executa limpeza real
        df_wrong_paid_clear_results = build_df_wrong_paid_clear_results(
            df_wrong_paid=df_wrong_pago,
            dry_run=False,
        )
        display_cols = [
            "afs",
            "id_item_monday",
            "item_name",
            "board_name",
            "pago_atual",
            "novo_pago",
            "clear_status",
            "error_message",
        ]
        existing_display_cols = [
            col for col in display_cols if col in df_wrong_paid_clear_results.columns
        ]
        print(df_wrong_paid_clear_results[existing_display_cols])
        print(f"Total wrong paid clear: {len(df_wrong_paid_clear_results)}")

        print("\n")
        print("\n20) Gerando resumo final da execucao...")
        execution_seconds = time.perf_counter() - pipeline_start_ts
        df_execution_summary = build_df_execution_summary(
            df_create_results=df_create_results,
            df_duplicates_delete_results=df_duplicates_delete_results,
            df_paid_update_results=df_paid_update_results,
            df_wrong_board_delete_results=df_wrong_board_delete_results,
            df_wrong_group_move_results=df_wrong_group_move_results,
            df_no_origin_delete_results=df_no_origin_delete_results,
            df_wrong_paid_clear_results=df_wrong_paid_clear_results,
            execution_seconds=execution_seconds,
        )
        print(df_execution_summary)

        print("\nPipeline AL_AFS_GERADAS concluido.\n")
        return 0

    except Exception as exc:
        log_error(f"Falha na execucao do pipeline: {exc}")
        raise


if __name__ == "__main__":
    sys.exit(main())
