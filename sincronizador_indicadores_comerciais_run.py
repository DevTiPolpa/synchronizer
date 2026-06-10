#!/usr/bin/env python3
"""
Sincronizador Sankhya -> SQL Server com agendamento interno.

Este programa fica rodando continuamente e executa a sincronização todos os dias
às 06:00, 12:00 e 17:00.

Recomendação: mantenha credenciais em variáveis de ambiente, não fixadas no código.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import date, datetime, time as dtime, timedelta
from threading import Lock
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pyodbc
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# -----------------------------------------------------------------------------
# Carregamento automático de .env
# -----------------------------------------------------------------------------

ENV_FILE_LOADED: Optional[str] = None
ENV_FILE_ATTEMPTS: List[str] = []


def load_env_file() -> None:
    """
    Carrega variáveis de ambiente de um arquivo de configuração local.

    O script procura, nesta ordem:
        1. Caminho informado em ENV_FILE, se existir;
        2. .env no mesmo diretório do script;
        3. .env no diretório atual do terminal;
        4. .env.sincronizador no mesmo diretório do script;
        5. .env.sincronizador no diretório atual do terminal.

    Variáveis já definidas no sistema operacional têm prioridade e não são sobrescritas.
    """
    global ENV_FILE_LOADED, ENV_FILE_ATTEMPTS

    script_dir = os.path.dirname(os.path.abspath(__file__))
    current_dir = os.getcwd()

    candidates: List[str] = []
    explicit_env_file = os.getenv("ENV_FILE")
    if explicit_env_file:
        candidates.append(explicit_env_file)

    candidates.extend(
        [
            os.path.join(script_dir, ".env"),
            os.path.join(current_dir, ".env"),
            os.path.join(script_dir, ".env.sincronizador"),
            os.path.join(current_dir, ".env.sincronizador"),
        ]
    )

    # Remove duplicados preservando a ordem.
    candidates = list(dict.fromkeys(os.path.abspath(path) for path in candidates))
    ENV_FILE_ATTEMPTS = candidates

    for env_path in candidates:
        if not os.path.exists(env_path):
            continue

        with open(env_path, "r", encoding="utf-8-sig") as env_file:
            for raw_line in env_file:
                line = raw_line.strip()

                if not line or line.startswith("#") or "=" not in line:
                    continue

                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")

                if key and key not in os.environ:
                    os.environ[key] = value

        ENV_FILE_LOADED = env_path
        return


load_env_file()


# -----------------------------------------------------------------------------
# Configuração
# -----------------------------------------------------------------------------

SANKHYA_AUTH_URL = os.getenv("SANKHYA_AUTH_URL", "https://api.sankhya.com.br/authenticate")
SANKHYA_VIEW_URL = os.getenv(
    "SANKHYA_VIEW_URL",
    "https://api.sankhya.com.br/gateway/v1/mge/service.sbr?serviceName=CRUDServiceProvider.loadView&outputType=json",
)
SANKHYA_VIEW_NAME = os.getenv("SANKHYA_VIEW_NAME", "AD_VW_EXPORTA_BI_INDICADORESCOMERCIAIS")

CLIENT_ID = os.getenv("SANKHYA_CLIENT_ID")
CLIENT_SECRET = os.getenv("SANKHYA_CLIENT_SECRET")
SANKHYA_APP_TOKEN = os.getenv("SANKHYA_APP_TOKEN")

DB_DRIVER = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")
DB_SERVER = os.getenv("DB_SERVER")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_TABLE = os.getenv("DB_TABLE", "INDICADORESCOMERCIAIS")
DB_SCHEMA = os.getenv("DB_SCHEMA", "dbo")

NUNOTA_MIN_DEFAULT = int(os.getenv("NUNOTA_MIN", "0"))
NUNOTA_MAX_DEFAULT = int(os.getenv("NUNOTA_MAX", "3000000"))
NUNOTA_STEP_DEFAULT = int(os.getenv("NUNOTA_STEP", "50000"))
BATCH_SIZE_DEFAULT = int(os.getenv("BATCH_SIZE", "1000"))
REQUEST_SLEEP_SECONDS = float(os.getenv("REQUEST_SLEEP_SECONDS", "0.2"))

# Horários fixos solicitados: 06:00, 12:00 e 17:00.
# Se quiser alterar depois, use variável de ambiente: SCHEDULE_TIMES="06:00,12:00,17:00"
SCHEDULE_TIMES_TEXT = os.getenv("SCHEDULE_TIMES", "08:03,11:59,17:00")

# Use America/Sao_Paulo por padrão. Se o servidor estiver em outro fuso e você
# quiser usar o horário local do servidor, configure SCHEDULER_TIMEZONE="local".
SCHEDULER_TIMEZONE = os.getenv("SCHEDULER_TIMEZONE", "America/Sao_Paulo")
SCHEDULER_CHECK_SECONDS = int(os.getenv("SCHEDULER_CHECK_SECONDS", "30"))

DATE_FIELDS = {"DTNEG", "AD_DT_EMBARQUE", "DTPREVENT", "AD_DT_ORIGINAL", "DTMOV", "AD_DTENTREGAREAL"}
DATETIME_FIELDS = {"DTATUAL", "DTALTER"}
DECIMAL_FIELDS = {
    "QTDENTREGUE",
    "QTDNEG",
    "PERCDESCBONIF",
    "VLRICMS",
    "QTDNEG_CALC",
    "QTDNEG_AJUSTADO",
    "QTDPENDENTE",
    "PESOLIQ",
    "VALORPENDENTE",
    "VLR_PIS",
    "VLR_COFINS",
    "VLR_ST",
    "AD_VLFRETE",
    "QTD_VOA",
}
INTEGER_FIELDS = {"CODPARC", "NUNOTA", "CODTIPOPER", "REFERENCIA", "AD_REEMISSAO", "CODPROD", "SEQUENCIA"}

SQL_TABLE_COLS = [
    "TIPMOV",
    "QTDENTREGUE",
    "QTDNEG",
    "CODPARC",
    "RAZAOSOCIAL",
    "NUNOTA",
    "NUMNOTA",
    "TIPPESSOA",
    "DTNEG",
    "AD_DT_EMBARQUE",
    "DTPREVENT",
    "AD_DT_ORIGINAL",
    "CODTIPOPER",
    "DESCROPER",
    "REFERENCIA",
    "DESCRPROD",
    "NCM",
    "PERCDESCBONIF",
    "VLRICMS",
    "QTDNEG_CALC",
    "CONTROLE",
    "QTDPENDENTE",
    "PESOLIQ",
    "ValorPendente",
    "VLR_PIS",
    "VLR_COFINS",
    "VLR_ST",
    "PROJETO",
    "PERFILPARC",
    "AD_MERCADO_VENDAS",
    "VENDEDOR",
    "DESCRGRUPOPROD",
    "CONFIRMADA",
    "NOMECID",
    "UF",
    "DESCRICAO",
    "NECESSIDADE_AGENDAMENTO_ENTREGA",
    "REPRESENTANTE",
    "DESCRICAO_SABOR",
    "FORMATO_PRODUTO",
    "QTD_VOA",
    "DTATUAL",
    "DTALTER",
    "DTMOV",
    "AD_DTENTREGAREAL",
    "AD_VLFRETE",
    "AD_REEMISSAO",
    "AD_MOTIVO_REPROG_EMB",
    "OBSERVACAO",
]

COLUMN_MAPPING = {
    "QTDNEG_AJUSTADO": "QTDNEG_CALC",
    "PAIS": "DESCRICAO",
    "VALORPENDENTE": "ValorPendente",
}
DB_TO_API_COLUMN = {db_col: api_col for api_col, db_col in COLUMN_MAPPING.items()}

_SYNC_LOCK = Lock()


# -----------------------------------------------------------------------------
# Utilitários
# -----------------------------------------------------------------------------

def setup_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if ENV_FILE_LOADED:
        logging.info("Arquivo de configuração carregado: %s", ENV_FILE_LOADED)
    else:
        logging.warning(
            "Nenhum arquivo .env foi encontrado. Caminhos verificados: %s",
            " | ".join(ENV_FILE_ATTEMPTS),
        )


def require_env_vars() -> None:
    required = {
        "SANKHYA_CLIENT_ID": CLIENT_ID,
        "SANKHYA_CLIENT_SECRET": CLIENT_SECRET,
        "SANKHYA_APP_TOKEN": SANKHYA_APP_TOKEN,
        "DB_SERVER": DB_SERVER,
        "DB_NAME": DB_NAME,
        "DB_USER": DB_USER,
        "DB_PASSWORD": DB_PASSWORD,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        env_help = (
            "Variáveis de ambiente obrigatórias não configuradas: "
            + ", ".join(missing)
            + ". Crie um arquivo chamado .env na mesma pasta do script "
            + f"({os.path.dirname(os.path.abspath(__file__))}) "
            + "ou informe o caminho completo em ENV_FILE. "
            + "Caminhos verificados: "
            + " | ".join(ENV_FILE_ATTEMPTS)
        )
        raise RuntimeError(env_help)


def bracket_identifier(identifier: str) -> str:
    return "[" + identifier.replace("]", "]]") + "]"


def qualified_table_name(schema: str, table: str) -> str:
    return f"{bracket_identifier(schema)}.{bracket_identifier(table)}"


def safe_int(value: Any) -> Optional[int]:
    try:
        if value in (None, ""):
            return None
        return int(float(str(value).replace(",", ".")))
    except (TypeError, ValueError):
        return None


def safe_float(value: Any) -> Optional[float]:
    try:
        if value in (None, ""):
            return None
        return float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return None


def parse_date(value: Any) -> Optional[date]:
    if value in (None, ""):
        return None
    text = str(value).strip()
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    logging.warning("Data inválida ignorada: %s", text)
    return None


def parse_datetime(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    text = str(value).strip()
    for fmt in ("%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    logging.warning("Data/hora inválida ignorada: %s", text)
    return None


def create_http_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"]),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# -----------------------------------------------------------------------------
# Sankhya API
# -----------------------------------------------------------------------------

def get_access_token(session: requests.Session) -> str:
    logging.info("Solicitando token de acesso ao Sankhya.")
    headers = {
        "X-Token": SANKHYA_APP_TOKEN or "",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials",
    }

    response = session.post(SANKHYA_AUTH_URL, headers=headers, data=payload, timeout=30)
    response.raise_for_status()
    token = response.json().get("access_token")

    if not token:
        raise RuntimeError("A API Sankhya não retornou access_token.")

    logging.info("Token de acesso obtido com sucesso.")
    return token


def fetch_view_range(session: requests.Session, access_token: str, nunota_min: int, nunota_max: int) -> List[Dict[str, Any]]:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "X-Token": SANKHYA_APP_TOKEN or "",
        "Content-Type": "application/json",
    }
    where_clause = f"NUNOTA >= {nunota_min} AND NUNOTA < {nunota_max}"
    payload = {
        "serviceName": "CRUDServiceProvider.loadView",
        "requestBody": {
            "query": {
                "viewName": SANKHYA_VIEW_NAME,
                "where": {"$": where_clause},
                "fields": {"field": {"$": "*"}},
            }
        },
    }

    response = session.post(SANKHYA_VIEW_URL, headers=headers, json=payload, timeout=90)
    response.raise_for_status()
    view_data = response.json()
    records_obj = view_data.get("responseBody", {}).get("records", {})
    records = records_obj.get("record", [])

    if isinstance(records, dict):
        return [records]
    if isinstance(records, list):
        return records
    return []


def get_all_view_data(
    session: requests.Session,
    access_token: str,
    nunota_min: int,
    nunota_max: int,
    step: int,
) -> List[Dict[str, Any]]:
    all_entities: List[Dict[str, Any]] = []
    current_min = nunota_min

    logging.info("Iniciando busca por faixas de NUNOTA: mínimo=%s, máximo=%s, passo=%s.", nunota_min, nunota_max, step)

    while current_min < nunota_max:
        current_max = min(current_min + step, nunota_max)
        logging.info("Buscando faixa NUNOTA >= %s e < %s.", current_min, current_max)

        try:
            records = fetch_view_range(session, access_token, current_min, current_max)
            all_entities.extend(records)
            logging.info("Faixa concluída: %s registros; total acumulado: %s.", len(records), len(all_entities))
        except requests.RequestException as exc:
            logging.exception("Erro de comunicação com a API na faixa %s-%s: %s", current_min, current_max, exc)
        except ValueError as exc:
            logging.exception("Resposta JSON inválida na faixa %s-%s: %s", current_min, current_max, exc)

        current_min = current_max
        time.sleep(REQUEST_SLEEP_SECONDS)

    logging.info("Total coletado da API: %s registros.", len(all_entities))
    return all_entities


# -----------------------------------------------------------------------------
# Transformação
# -----------------------------------------------------------------------------

def extract_api_value(value_obj: Any) -> Any:
    if isinstance(value_obj, dict):
        return value_obj.get("$")
    return value_obj


def transform_record(entity: Dict[str, Any]) -> Dict[str, Any]:
    record: Dict[str, Any] = {}

    for key, value_obj in entity.items():
        value = extract_api_value(value_obj)

        if key in DATE_FIELDS:
            record[key] = parse_date(value)
        elif key in DATETIME_FIELDS:
            record[key] = parse_datetime(value)
        elif key in DECIMAL_FIELDS:
            record[key] = safe_float(value)
        elif key in INTEGER_FIELDS:
            record[key] = safe_int(value)
        else:
            record[key] = value

    return record


def transform_data(entities: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    logging.info("Transformando registros.")
    transformed = [transform_record(entity) for entity in entities]
    logging.info("Transformação concluída: %s registros.", len(transformed))
    return transformed


# -----------------------------------------------------------------------------
# SQL Server
# -----------------------------------------------------------------------------

def get_sql_connection() -> pyodbc.Connection:
    conn_str = (
        f"DRIVER={{{DB_DRIVER}}};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_NAME};"
        f"UID={DB_USER};"
        f"PWD={DB_PASSWORD};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


def build_insert_sql(table_name: str, columns: Sequence[str]) -> str:
    cols = ", ".join(bracket_identifier(col) for col in columns)
    placeholders = ", ".join(["?"] * len(columns))
    return f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"


def row_from_record(record: Dict[str, Any], columns: Sequence[str]) -> Tuple[Any, ...]:
    row = []
    for db_col in columns:
        api_key = DB_TO_API_COLUMN.get(db_col, db_col)
        row.append(record.get(api_key))
    return tuple(row)


def clear_table(cursor: pyodbc.Cursor, table_name: str, truncate: bool) -> None:
    if truncate:
        logging.info("Limpando tabela com TRUNCATE TABLE %s.", table_name)
        cursor.execute(f"TRUNCATE TABLE {table_name}")
    else:
        logging.info("Limpando tabela com DELETE FROM %s.", table_name)
        cursor.execute(f"DELETE FROM {table_name}")


def sync_to_sql_server(data: Sequence[Dict[str, Any]], batch_size: int, truncate: bool) -> None:
    if not data:
        logging.warning("Sem dados para processar. A tabela não será limpa nem carregada.")
        return

    table_name = qualified_table_name(DB_SCHEMA, DB_TABLE)
    insert_sql = build_insert_sql(table_name, SQL_TABLE_COLS)
    total = len(data)

    logging.info("Conectando ao SQL Server em %s/%s.", DB_SERVER, DB_NAME)
    conn = get_sql_connection()

    try:
        cursor = conn.cursor()
        cursor.fast_executemany = True

        clear_table(cursor, table_name, truncate=truncate)
        logging.info("Iniciando inserção de %s registros em lotes de %s.", total, batch_size)

        for start in range(0, total, batch_size):
            batch = data[start : start + batch_size]
            batch_values = [row_from_record(record, SQL_TABLE_COLS) for record in batch]
            cursor.executemany(insert_sql, batch_values)
            logging.info("Progresso: %s/%s registros.", min(start + batch_size, total), total)

        conn.commit()
        logging.info("Sincronização concluída com sucesso.")
    except Exception:
        conn.rollback()
        logging.exception("Erro durante sincronização. Transação revertida.")
        raise
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# Execução da sincronização
# -----------------------------------------------------------------------------

def run_sync_once(args: argparse.Namespace) -> bool:
    """Executa uma sincronização completa e retorna True se terminou sem erro."""
    if not _SYNC_LOCK.acquire(blocking=False):
        logging.warning("Já existe uma sincronização em andamento. Esta execução será ignorada.")
        return False

    start_time = time.time()
    try:
        logging.info("Iniciando ciclo de sincronização.")
        require_env_vars()

        session = create_http_session()
        token = get_access_token(session)
        raw = get_all_view_data(session, token, args.nunota_min, args.nunota_max, args.step)
        clean = transform_data(raw)
        sync_to_sql_server(clean, args.batch_size, truncate=args.truncate)

        elapsed = round(time.time() - start_time, 2)
        logging.info("Ciclo de sincronização finalizado com sucesso em %s segundos.", elapsed)
        return True
    except Exception as exc:
        elapsed = round(time.time() - start_time, 2)
        logging.exception("Falha no ciclo de sincronização após %s segundos: %s", elapsed, exc)
        return False
    finally:
        _SYNC_LOCK.release()


# -----------------------------------------------------------------------------
# Scheduler interno
# -----------------------------------------------------------------------------

def parse_schedule_times(text: str) -> List[dtime]:
    times: List[dtime] = []
    for item in text.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            hour_text, minute_text = item.split(":", 1)
            times.append(dtime(hour=int(hour_text), minute=int(minute_text)))
        except Exception as exc:
            raise ValueError(f"Horário inválido em SCHEDULE_TIMES: {item!r}. Use formato HH:MM.") from exc

    if not times:
        raise ValueError("Nenhum horário válido foi configurado em SCHEDULE_TIMES.")

    return sorted(times)


def get_scheduler_timezone() -> Optional[ZoneInfo]:
    """
    Retorna o fuso configurado para o scheduler.

    Em alguns ambientes Windows, o Python não encontra a base IANA de fusos
    horários, gerando erro para chaves como America/Sao_Paulo quando o pacote
    tzdata não está instalado. Nesse caso, o script usa o horário local da
    máquina como fallback para não interromper o programa.
    """
    if SCHEDULER_TIMEZONE.lower() == "local":
        return None

    try:
        return ZoneInfo(SCHEDULER_TIMEZONE)
    except ZoneInfoNotFoundError:
        logging.warning(
            "Fuso horário %s não encontrado neste ambiente. "
            "Usando o horário local da máquina como fallback. "
            "Se quiser manter America/Sao_Paulo explicitamente, instale: pip install tzdata",
            SCHEDULER_TIMEZONE,
        )
        return None


def now_in_scheduler_tz(tz: Optional[ZoneInfo]) -> datetime:
    if tz is None:
        return datetime.now()
    return datetime.now(tz)


def combine_date_time(current_date: date, current_time: dtime, tz: Optional[ZoneInfo]) -> datetime:
    if tz is None:
        return datetime.combine(current_date, current_time)
    return datetime.combine(current_date, current_time, tzinfo=tz)


def next_run_at(now: datetime, schedule_times: Sequence[dtime], tz: Optional[ZoneInfo]) -> datetime:
    today = now.date()
    for scheduled_time in schedule_times:
        candidate = combine_date_time(today, scheduled_time, tz)
        if candidate > now:
            return candidate
    return combine_date_time(today + timedelta(days=1), schedule_times[0], tz)


def sleep_until(target: datetime, tz: Optional[ZoneInfo]) -> None:
    while True:
        now = now_in_scheduler_tz(tz)
        seconds = (target - now).total_seconds()
        if seconds <= 0:
            return
        time.sleep(min(seconds, SCHEDULER_CHECK_SECONDS))


def scheduler_loop(args: argparse.Namespace) -> int:
    schedule_times = parse_schedule_times(args.schedule_times)
    tz = get_scheduler_timezone()

    logging.info(
        "Scheduler iniciado. O programa ficará rodando continuamente. Horários configurados: %s. Fuso: %s.",
        ", ".join(t.strftime("%H:%M") for t in schedule_times),
        SCHEDULER_TIMEZONE,
    )

    if args.run_now:
        logging.info("Opção --run-now informada. Executando sincronização inicial imediatamente.")
        run_sync_once(args)

    while True:
        now = now_in_scheduler_tz(tz)
        target = next_run_at(now, schedule_times, tz)
        logging.info("Próxima sincronização agendada para: %s.", target.strftime("%Y-%m-%d %H:%M:%S %Z"))

        sleep_until(target, tz)

        logging.info("Horário agendado atingido: %s. Iniciando sincronização.", target.strftime("%Y-%m-%d %H:%M:%S %Z"))
        run_sync_once(args)


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sincroniza dados de uma view Sankhya para uma tabela SQL Server com agendamento interno.")
    parser.add_argument("--once", action="store_true", help="Executa uma única sincronização e encerra. Útil para teste manual.")
    parser.add_argument("--run-now", action="store_true", help="No modo contínuo, executa uma sincronização imediatamente ao iniciar e depois segue a agenda.")
    parser.add_argument("--schedule-times", default=SCHEDULE_TIMES_TEXT, help="Horários diários no formato HH:MM separados por vírgula. Padrão: 06:00,12:00,17:00.")
    parser.add_argument("--nunota-min", type=int, default=NUNOTA_MIN_DEFAULT, help="Valor inicial de NUNOTA, inclusivo.")
    parser.add_argument("--nunota-max", type=int, default=NUNOTA_MAX_DEFAULT, help="Valor final de NUNOTA, exclusivo.")
    parser.add_argument("--step", type=int, default=NUNOTA_STEP_DEFAULT, help="Tamanho da faixa de NUNOTA por requisição.")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE_DEFAULT, help="Quantidade de registros por lote de insert.")
    parser.add_argument("--truncate", action="store_true", help="Usa TRUNCATE TABLE em vez de DELETE FROM para limpar a tabela.")
    return parser.parse_args(argv)


def validate_args(args: argparse.Namespace) -> None:
    if args.step <= 0:
        raise ValueError("--step deve ser maior que zero.")
    if args.batch_size <= 0:
        raise ValueError("--batch-size deve ser maior que zero.")
    if args.nunota_max <= args.nunota_min:
        raise ValueError("--nunota-max deve ser maior que --nunota-min.")
    parse_schedule_times(args.schedule_times)


def main(argv: Optional[Sequence[str]] = None) -> int:
    setup_logging()
    args = parse_args(argv)

    try:
        validate_args(args)

        if args.once:
            return 0 if run_sync_once(args) else 1

        return scheduler_loop(args)
    except KeyboardInterrupt:
        logging.info("Programa interrompido manualmente.")
        return 0
    except Exception as exc:
        logging.exception("Falha crítica ao iniciar o sincronizador: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())