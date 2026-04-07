import json
import os
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

load_dotenv()


def _get_str(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.getenv(name)
    if value is None:
        return default
    value = value.strip()
    return value if value != "" else default


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return default
    return int(value)


def _get_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return default
    return float(value)


def _get_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return default

    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "sim", "s"}:
        return True
    if normalized in {"0", "false", "no", "n", "nao"}:
        return False

    raise ValueError(f"Variavel {name} invalida para bool: {value}")


def _get_json(name: str, default: Any) -> Any:
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return default

    try:
        return json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Variavel {name} contem JSON invalido: {exc}") from exc


def _mask_token(token: Optional[str], head: int = 4, tail: int = 3) -> str:
    if not token:
        return "MISSING"
    if len(token) <= head + tail:
        return "*" * len(token)
    return f"{token[:head]}...{token[-tail:]} (len={len(token)})"


def _preview(value: Optional[str], size: int = 12) -> str:
    if not value:
        return "MISSING"
    value = str(value)
    return value[:size] + ("..." if len(value) > size else "")


API_URL: str = _get_str("MONDAY_BASE_URL", "https://api.monday.com/v2")
TOKEN_MONDAY: Optional[str] = _get_str("MONDAY_API_TOKEN")

HEADERS: Dict[str, str] = {
    "Authorization": TOKEN_MONDAY or "",
    "Content-Type": "application/json",
}

MAX_RETRIES: int = _get_int("MONDAY_MAX_RETRIES", 5)
BASE_DELAY: float = _get_float("MONDAY_BACKOFF_BASE", 1.0)
MAX_DELAY: float = _get_float("MONDAY_BACKOFF_CAP", 30.0)
BACKOFF_FACTOR: float = _get_float("MONDAY_BACKOFF_FACTOR", 2.0)
JITTER_MIN: float = _get_float("MONDAY_JITTER_MIN", 0.0)
JITTER_MAX: float = _get_float("MONDAY_JITTER_MAX", 0.5)
REQUEST_TIMEOUT: int = _get_int("MONDAY_REQUEST_TIMEOUT", 60)

PAGE_LIMIT: int = _get_int("MONDAY_PAGE_LIMIT", 500)
ENRICH_BATCH_SIZE: int = _get_int("MONDAY_ENRICH_BATCH_SIZE", 25)
CREATE_BATCH_SIZE: int = _get_int("MONDAY_CREATE_BATCH_SIZE", 50)
SLEEP_BETWEEN_REQUESTS: float = _get_float("MONDAY_SLEEP_BETWEEN_REQUESTS", 0.2)

BOARDS_ORIGIN: Dict[str, Dict[str, Any]] = {
    "2026": {
        "board_id": "18392819713",
        "board_name": "AFs Geradas 2026",
        "group_ids": [
            "group_mkz3h2g5",
            "group_mkz3cjnc",
            "group_mkz3f1bw",
            "group_mkz3w0af",
            "group_mkz3yq4b",
            "group_mkz3peyv",
            "group_mkz34ypv",
            "group_mkz3vawv",
            "1737659708_conterp_planilha_af_mkmfzxfp",
            "new_group_mkn5pny3",
        ],
    },
    "2025": {
        "board_id": "8205046069",
        "board_name": "AFs Geradas 2025",
        "group_ids": [
            "group_mkqmy99g",
            "group_mkqmdt2c",
            "group_mkqmg5f8",
            "group_mkqmv7bm",
            "group_mkqmeb7m",
            "group_mkqmeg6y",
            "group_mkqm737g",
            "group_mkqhvd5y",
            "group_mkpkzr3b",
            "group_mkny3wjb",
            "1737659708_conterp_planilha_af_mkmfzxfp",
            "new_group_mkn5pny3",
            "group_mky4ga9v",
        ],
    },
    "2025_2": {
        "board_id": "18390748622",
        "board_name": "AFs Geradas 2025 #2",
        "group_ids": [
            "group_mkywg5xr",
            "1737659708_conterp_planilha_af_mkmfzxfp",
            "group_mky9tbhd",
            "new_group_mkn5pny3",
        ],
    },
}

BOARDS_DESTINATION: Dict[str, Dict[str, Any]] = {
    "ENEVA": {
        "board_id": "18405107581",
        "board_name": "ENEVA - AFs Geradas",
        "grupo_afs": "group_mkyhw7g6",
        "grupo_canceladas": "group_mkyjs8vw",
        "grupo_2024": "group_mkymvq31",
        "cc_keywords": ["ENEVA"],
    },
    "FS_BIO_CPT01": {
        "board_id": "18405431049",
        "board_name": "FS Bio & CPT01 - AFs Geradas",
        "grupo_afs": "group_mkyhw7g6",
        "grupo_canceladas": "group_mkyjs8vw",
        "cc_keywords": ["FS BIOENERGIA", "PERFURACAO PTB BA"],
    },
    "SPTs": {
        "board_id": "18406537549",
        "board_name": "SPTs - AFs Geradas",
        "grupo_afs": "group_mkyhw7g6",
        "grupo_canceladas": "group_mkyjs8vw",
        "cc_keywords": ["SPT"],
    },
    "ATP": {
        "board_id": "18406537943",
        "board_name": "ATP - AFs Geradas",
        "grupo_afs": "group_mkyhw7g6",
        "grupo_canceladas": "group_mkyjs8vw",
        "cc_keywords": ["ATP", "DESPARAFINACAO"],
    },
    "FLUIDOS_MAR": {
        "board_id": "18406538056",
        "board_name": "Fluidos & Mar - AFs Geradas",
        "grupo_afs": "group_mkyhw7g6",
        "grupo_canceladas": "group_mkyjs8vw",
        "cc_keywords": ["SERGIPE MAR", "FLUIDO"],
    },
}

BOARDS_PAYMENTS: Dict[str, Dict[str, Any]] = {
    "PAGAMENTOS_2025_JAN_JUN": {
        "board_id": "8572162665",
        "board_name": "Pagamentos Realizados Jan-Jun 2025",
    },
    "PAGAMENTOS_2025_JUL_DEZ": {
        "board_id": "9678080018",
        "board_name": "Pagamentos Realizados Jul-Dez 2025",
    },
    "PY_PAGAMENTOS_2025_JAN_JUN": {
        "board_id": "9927439992",
        "board_name": "[Py] Pagamentos Realizados Jan - Jun 2025",
    },
    "PY_PAGAMENTOS_2025_JUL_DEZ": {
        "board_id": "18126969654",
        "board_name": "[Py] Pagamentos Realizados Jul - Dez 2025",
    },
    "PY_PAGAMENTOS_2026_JAN_JUN": {
        "board_id": "18393715465",
        "board_name": "[Py] Pagamentos Realizados Jan - Jun 2026",
    },
}

COLUNA_NUMERO_AF: str = "numero_af_mkm1ex9p"
COLUNA_CENTRO_CUSTO: str = "dropdown_mkp5727h"
COLUNA_PAGO: str = "color_mkrznew8"

COLUNAS_ENRIQUECIMENTO: List[str] = [
    "date_mkkvsdmb",
    "numero_af_mkm1ex9p",
    "dropdown_mkp56871",
    "numbers_mkkvk2ck",
    "dropdown_mkp5a95m",
    "dropdown_mkp5m3d4",
    "dropdown_mkp5727h",
    "text_mkqczz6e",
    "text_mkkvp6w7",
    "dropdown_mkw54z0m",
    "dropdown_mkp5n5tj",
    "text_mkntr5tx",
    "dropdown_mkp5d391",
    "dropdown_mkp5att3",
    "date_mkqqkpx9",
    "date_mkqrpxz6",
    "date_mkqrbzrt",
    "color_mkrznew8",
]

COLUNAS_ENRIQUECIMENTO_POR_DESTINO: Dict[str, List[str]] = {
    "ENEVA": [
        "color_mkrzpd3f",
        "color_mksa7x5v",
        "text_mkrzjyjj",
    ],
}

GRUPO_CANCELADAS_ORIGEM_ID: str = "new_group_mkn5pny3"
GRUPO_2024_ORIGEM_ID: str = "group_mky9tbhd"

IGNORAR_SEM_NUMERO_AF: bool = _get_bool("PIPELINE_IGNORE_WITHOUT_AF", True)
IGNORAR_CC_NAO_MAPEADO: bool = _get_bool("PIPELINE_IGNORE_UNMAPPED_CC", True)

COLS_DF_AFS_ORIGIN: List[str] = [
    "afs",
    "id_item_monday",
    "board_id",
    "board_name",
    "group_id",
    "group_title",
    "cost_center",
]

COLS_DF_AFS_DESTINATION: List[str] = [
    "afs",
    "id_item_monday",
    "board_id",
    "board_name",
    "pago",
]

COLS_DF_AFS_DIFF: List[str] = [
    "afs",
    "id_item_monday",
    "board_id",
    "board_name",
    "group_id",
    "group_title",
    "cost_center",
    "destination_board",
]

COLS_DF_AFS_TO_CREATE: List[str] = [
    "afs",
    "id_item_monday",
    "board_id",
    "board_name",
    "group_id",
    "group_title",
    "cost_center",
    "destination_board",
]

LOG_PREFIX: str = _get_str("PIPELINE_LOG_PREFIX", "[AFS]") or "[AFS]"
MOSTRAR_PROGRESSO: bool = _get_bool("PIPELINE_SHOW_PROGRESS", True)
MOSTRAR_ERROS_DE_PAYLOAD: bool = _get_bool("PIPELINE_SHOW_PAYLOAD_ERRORS", True)


def check_required_envs() -> None:
    missing = [
        name
        for name, value in [
            ("MONDAY_API_TOKEN", TOKEN_MONDAY),
        ]
        if not value
    ]

    if missing:
        missing_lines = "\n".join(f"- {name}" for name in missing)
        print(f"[SETTINGS] Variaveis ausentes/invalidas:\n{missing_lines}")
        return

    print("[SETTINGS] Configuracoes essenciais carregadas.")
    print(f"[SETTINGS] Monday URL: {_preview(API_URL)} | Token: {_mask_token(TOKEN_MONDAY)}")
    print(
        f"[SETTINGS] Retry: max={MAX_RETRIES}, base={BASE_DELAY}, cap={MAX_DELAY}, "
        f"factor={BACKOFF_FACTOR}, jitter=({JITTER_MIN}, {JITTER_MAX})"
    )
    print(
        f"[SETTINGS] Paginacao: page_limit={PAGE_LIMIT}, enrich_batch={ENRICH_BATCH_SIZE}, "
        f"create_batch={CREATE_BATCH_SIZE}, sleep={SLEEP_BETWEEN_REQUESTS}"
    )
    print(
        f"[SETTINGS] Boards: origin={len(BOARDS_ORIGIN)}, "
        f"destination={len(BOARDS_DESTINATION)}, payments={len(BOARDS_PAYMENTS)}"
    )
    print(
        f"[SETTINGS] Flags: ignorar_sem_numero_af={IGNORAR_SEM_NUMERO_AF}, "
        f"ignorar_cc_nao_mapeado={IGNORAR_CC_NAO_MAPEADO}, "
        f"mostrar_progresso={MOSTRAR_PROGRESSO}"
    )


if __name__ == "__main__":
    check_required_envs()
