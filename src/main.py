from __future__ import annotations

import sys

from src.config.settings import LOG_PREFIX, check_required_envs
from src.core.monday.destination.build_destination_board_map import add_destination_board
from src.core.monday.destination.build_missing_afs import (
    build_df_afs_diff,
    build_df_afs_to_create,
)
from src.core.monday.destination.fetch_destination_items import build_df_afs_destination
from src.core.monday.origin.fetch_origin_items import build_df_afs_origin


def log_info(message: str) -> None:
    print(f"{LOG_PREFIX} [INFO] {message}")


def log_error(message: str) -> None:
    print(f"{LOG_PREFIX} [ERROR] {message}")


def main() -> int:
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

        print("\n6) Buscando detalhes dos itens de origem para enriquecimento...")
        # TODO:
        # from src.core.monday.origin.fetch_origin_item_details import ...
        # from src.core.monday.origin.build_enriched_afs import build_df_afs_enriched
        # df_afs_enriched = build_df_afs_enriched(df_afs_to_create)

        print("\n7) Criando itens faltantes nos boards de destino...")
        # TODO:
        # from src.core.monday.destination.create_monday_items import build_df_create_results
        # df_create_results = build_df_create_results(df_afs_enriched)

        print("\n8) Recarregando destino para auditoria...")
        # TODO:
        # from src.core.monday.destination.fetch_destination_audit_items import (
        #     build_df_destino_auditoria,
        # )
        # df_destino_auditoria = build_df_destino_auditoria()

        print("\n9) Encontrando itens duplicados no destino...")
        # TODO:
        # from src.core.monday.destination.duplicates.find_duplicate_items import (
        #     build_df_duplicates_summary,
        #     build_df_duplicates,
        # )
        # df_duplicates_summary = build_df_duplicates_summary(df_destino_auditoria)
        # df_duplicates = build_df_duplicates(df_destino_auditoria, df_duplicates_summary)

        print("\n10) Definindo resolucao dos duplicados...")
        # TODO:
        # from src.core.monday.destination.duplicates.build_duplicate_actions import (
        #     build_df_duplicate_actions,
        #     build_df_duplicates_keep,
        #     build_df_duplicates_delete,
        # )
        # df_duplicate_actions = build_df_duplicate_actions(df_duplicates)
        # df_duplicates_keep = build_df_duplicates_keep(df_duplicate_actions)
        # df_duplicates_delete = build_df_duplicates_delete(df_duplicate_actions)

        print("\n11) Deletando itens duplicados...")
        # TODO:
        # from src.core.monday.destination.duplicates.delete_duplicate_items import (
        #     build_df_duplicates_delete_results,
        # )
        # df_duplicates_delete_results = build_df_duplicates_delete_results(df_duplicates_delete)

        print("\n12) Lendo boards de pagamentos realizados...")
        # TODO:
        # from src.core.monday.payments.fetch_payment_items import build_df_pagamentos_realizados
        # df_pagamentos_realizados = build_df_pagamentos_realizados()

        print("\n13) Identificando itens com PAGO para atualizar...")
        # TODO:
        # from src.core.monday.payments.build_pago_updates import build_df_pago_to_update
        # df_pago_to_update = build_df_pago_to_update(
        #     df_destino_auditoria=df_destino_auditoria,
        #     df_pagamentos_realizados=df_pagamentos_realizados,
        # )

        print("\n14) Atualizando coluna PAGO...")
        # TODO:
        # from src.core.monday.payments.update_pago_items import build_df_pago_update_results
        # df_pago_update_results = build_df_pago_update_results(df_pago_to_update)

        print("\n15) Encontrando wrong board, wrong group, no origin e wrong pago...")
        # TODO:
        # from src.core.monday.destination.orphans.find_orphan_items import (
        #     build_df_origem_expected_destino,
        #     build_df_wrong_board,
        #     build_df_wrong_group,
        #     build_df_no_origin,
        #     build_df_wrong_pago,
        # )
        # df_origin_expected = build_df_origem_expected_destino(df_afs_origin)
        # df_wrong_board = build_df_wrong_board(df_destino_auditoria, df_origin_expected)
        # df_wrong_group = build_df_wrong_group(df_destino_auditoria, df_origin_expected)
        # df_no_origin = build_df_no_origin(df_destino_auditoria, df_origin_expected)
        # df_wrong_pago = build_df_wrong_pago(df_destino_auditoria, df_pagamentos_realizados)

        print("\n16) Deletando itens no board errado...")
        # TODO:
        # from src.core.monday.destination.orphans.delete_wrong_board_items import (
        #     build_df_wrong_board_delete_results,
        # )
        # df_wrong_board_delete_results = build_df_wrong_board_delete_results(df_wrong_board)

        print("\n17) Movendo itens para o grupo correto...")
        # TODO:
        # from src.core.monday.destination.orphans.move_wrong_group_items import (
        #     build_df_wrong_group_move_results,
        # )
        # df_wrong_group_move_results = build_df_wrong_group_move_results(df_wrong_group)

        print("\n18) Deletando itens sem origem...")
        # TODO:
        # from src.core.monday.destination.orphans.delete_no_origin_items import (
        #     build_df_no_origin_delete_results,
        # )
        # df_no_origin_delete_results = build_df_no_origin_delete_results(df_no_origin)

        print("\n19) Limpando itens com PAGO incorreto...")
        # TODO:
        # from src.core.monday.destination.orphans.clear_wrong_pago_items import (
        #     build_df_wrong_pago_clear_results,
        # )
        # df_wrong_pago_clear_results = build_df_wrong_pago_clear_results(df_wrong_pago)

        print("\n20) Gerando resumo final da execucao...")
        # TODO:
        # from src.core.monday.destination.summary.build_execution_summary import (
        #     build_df_resumo_final,
        # )
        # df_resumo_final = build_df_resumo_final(
        #     df_create_results=df_create_results,
        #     df_duplicates_delete_results=df_duplicates_delete_results,
        #     df_pago_update_results=df_pago_update_results,
        #     df_wrong_board_delete_results=df_wrong_board_delete_results,
        #     df_wrong_group_move_results=df_wrong_group_move_results,
        #     df_no_origin_delete_results=df_no_origin_delete_results,
        #     df_wrong_pago_clear_results=df_wrong_pago_clear_results,
        # )

        print("\nPipeline AL_AFS_GERADAS concluido.\n")
        return 0

    except Exception as exc:
        log_error(f"Falha na execucao do pipeline: {exc}")
        raise


if __name__ == "__main__":
    sys.exit(main())
