
import csv
import io
import logging
import os
from datetime import datetime, timezone

import azure.functions as func
import pyodbc
from azure.storage.filedatalake import DataLakeServiceClient


app = func.FunctionApp()


# Tabelas que serão extraídas do banco de origem
ITSM_TABLES = (
    "analista",
    "categoria",
    "chamado",
    "chamado_sla",
    "chamado_status_historico",
    "cliente_organizacao",
    "csat_avaliacao",
    "fila",
    "sla",
    "solicitante",
)

# Marcador para preservar valores NULL nos arquivos CSV
CSV_NULL = "__AZURE_SQL_NULL_9F31A2__"


def _required_setting(name: str) -> str:
    """Obtém uma variável de ambiente obrigatória."""
    value = os.getenv(name)

    if not value:
        raise RuntimeError(
            f"Variável de ambiente não configurada: {name}"
        )

    return value


def _odbc_value(value: str) -> str:
    """Protege valores utilizados na string ODBC."""
    return "{" + value.replace("}", "}}") + "}"


def _sql_connection_string() -> str:
    """Monta a conexão SQL sem expor as credenciais."""

    sql_server = _required_setting("HOST")
    sql_database = _required_setting("DATABASE")
    sql_user = _required_setting("USER")
    sql_password = _required_setting("PASSWORD")

    return (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={_odbc_value(sql_server)};"
        f"DATABASE={_odbc_value(sql_database)};"
        f"UID={_odbc_value(sql_user)};"
        f"PWD={_odbc_value(sql_password)};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )


def _csv_value(value):
    """Converte valores SQL para gravação no CSV."""

    if value is None:
        return CSV_NULL

    if isinstance(value, str) and value == CSV_NULL:
        return CSV_NULL + CSV_NULL

    return value


def _extract_table(cursor, table_name, bronze, now):
    """Extrai uma tabela e grava seu CSV no Data Lake."""

    logging.info(
        "Iniciando extração da tabela itsm.%s",
        table_name
    )

    query = f"SELECT * FROM [itsm].[{table_name}]"

    cursor.execute(query)

    columns = [
        column[0]
        for column in cursor.description
    ]

    buffer = io.StringIO()
    writer = csv.writer(buffer)

    # Cabeçalho do CSV
    writer.writerow(columns)

    # Registros da tabela
    total_rows = 0

    while True:
        rows = cursor.fetchmany(1000)

        if not rows:
            break

        for row in rows:
            writer.writerow([
                _csv_value(value)
                for value in row
            ])

        total_rows += len(rows)

    # Caminho do arquivo no Data Lake
    file_path = (
        f"{table_name}/{now:%Y/%m/%d}/"
        f"{table_name}_{now:%Y%m%d_%H%M%S}.csv"
    )

    file_client = bronze.get_file_client(file_path)

    file_client.upload_data(
        buffer.getvalue().encode("utf-8"),
        overwrite=True
    )

    buffer.close()

    logging.info(
        "Tabela itsm.%s: %s registros exportados para bronze/%s",
        table_name,
        total_rows,
        file_path
    )

    return total_rows


# Executa diariamente às 04:00 UTC
@app.timer_trigger(
    schedule="0 0 4 * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=True,
)
def extract_itsm_tables(myTimer: func.TimerRequest) -> None:

    logging.info("Iniciando extração das tabelas ITSM")

    if myTimer.past_due:
        logging.warning(
            "A execução do Timer Trigger está atrasada."
        )

    # Configurações
    conn_str = _sql_connection_string()

    datalake_connection_string = _required_setting(
        "DATALAKE_CONNECTION_STRING"
    )

    # Conexão com o Data Lake
    datalake = DataLakeServiceClient.from_connection_string(
        datalake_connection_string
    )

    bronze = datalake.get_file_system_client("bronze")

    # Mesmo timestamp para todas as tabelas
    now = datetime.now(timezone.utc)

    failures = []
    total_records = 0

    # Conexão com o banco SQL de origem
    with pyodbc.connect(conn_str) as conn:

        for table_name in ITSM_TABLES:

            try:
                cursor = conn.cursor()

                try:
                    count = _extract_table(
                        cursor,
                        table_name,
                        bronze,
                        now
                    )

                    total_records += count

                finally:
                    cursor.close()

            except Exception:
                logging.exception(
                    "Falha na extração de itsm.%s",
                    table_name
                )

                failures.append(table_name)

    # Verifica falhas na extração
    if failures:
        raise RuntimeError(
            "Falha na extração das tabelas: "
            + ", ".join(failures)
        )

    logging.info(
        "Extração finalizada: %s tabelas e %s registros.",
        len(ITSM_TABLES),
        total_records
    )
