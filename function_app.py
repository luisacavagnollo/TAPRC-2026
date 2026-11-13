import os
from datetime import datetime, timezone

import pyodbc
import azure.functions as func
import pyodbc
from azure.storage.filedatalake import DataLakeServiceClient

app = func.FunctionApp()

@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def extract_chamado(myTimer: func.TimerRequest) -> None:
    logging.info('tabela chamado')
    
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    logging.info(f'servidor={sql_server}, banco de dados={sql_database}, usuario={sql_user}, senha={sql_pass} ')
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


def _required_setting(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Configuração obrigatória ausente: {name}")
    return value


@app.timer_trigger(
    schedule="0 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_itsm_tables(myTimer: func.TimerRequest) -> None:
    logging.info("Iniciando extração das tabelas ITSM")

    sql_server = _required_setting("HOST")
    sql_database = _required_setting("DATABASE")
    sql_user = _required_setting("USER")
    sql_pass = _required_setting("PASSWORD")
    datalake_connection_string = _required_setting("DATALAKE_CONNECTION_STRING")

    # Configura a string de conexão para o banco de dados SQL Server
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
@@ -34,118 +57,52 @@ def extract_chamado(myTimer: func.TimerRequest) -> None:
        "Connection Timeout=30;"
    )

    datalake = DataLakeServiceClient.from_connection_string(
        datalake_connection_string
    )
    bronze = datalake.get_file_system_client("bronze")
    now = datetime.now(timezone.utc)
    failures = []

    try:
        # Estabelece a conexão com o banco de dados usando pyodbc
        with pyodbc.connect(conn_str) as conn:
            # Cria um cursor para executar a consulta
            cursor = conn.cursor()
            
            query = "select * from itsm.chamado"

            # Executa a consulta SQL
            cursor.execute(query)

            # Busca todos os resultados da consulta
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f'{len(rows)} linhas lidas de itsm.chamado')

        # Cria um buffer de memória para armazenar os dados CSV
        buffer = io.StringIO()
        # Cria um objeto CSV writer e escreve os dados no buffer
        writer = csv.writer(buffer)
        # Escreve o cabeçalho e as linhas no buffer CSV
        writer.writerow(columns)
        # Escreve todas as linhas no buffer CSV
        writer.writerows(rows)

        # Grava o arquivo no contêiner bronze do Data Lake, particionado por data
        now = datetime.now(timezone.utc)
        file_path = f"chamado/{now:%Y/%m/%d}/chamado_{now:%Y%m%d_%H%M%S}.csv"

        #itsm/chamado/year=2024/month=06/day=05/chamado_20240605_123456.csv
        #file_path = f"itsm/chamado/year={now:%Y}/month={now:%m}/day={now:%d}/chamado_{now:%Y%m%d_%H%M%S}.csv"

        # Cria o cliente do Data Lake e grava o arquivo no contêiner bronze
        datalake = DataLakeServiceClient.from_connection_string(os.getenv("DATALAKE_CONNECTION_STRING"))
        # Cria o cliente do sistema de arquivos e grava o arquivo no contêiner bronze
        file_client = datalake.get_file_system_client("bronze").get_file_client(file_path)
        # Grava os dados do buffer no arquivo do Data Lake, sobrescrevendo se já existir
        file_client.upload_data(buffer.getvalue().encode("utf-8"), overwrite=True)

        logging.info(f'Arquivo gravado em bronze/{file_path}')

    except Exception as e:
        logging.error(f"Erro ao ler itsm.chamado: {str(e)}")
            for table_name in ITSM_TABLES:
                try:
                    cursor = conn.cursor()
                    cursor.execute(f"SELECT * FROM [itsm].[{table_name}]")
                    rows = cursor.fetchall()
                    columns = [column[0] for column in cursor.description]

                    buffer = io.StringIO()
                    writer = csv.writer(buffer)
                    writer.writerow(columns)
                    writer.writerows(rows)

                    file_path = (
                        f"{table_name}/{now:%Y/%m/%d}/"
                        f"{table_name}_{now:%Y%m%d_%H%M%S}.csv"
                    )
                    bronze.get_file_client(file_path).upload_data(
                        buffer.getvalue().encode("utf-8"),
                        overwrite=True,
                    )

                    logging.info(
                        "%s: %s linhas lidas; arquivo gravado em bronze/%s",
                        table_name,
                        len(rows),
                        file_path,
                    )
                except Exception:
                    logging.exception("Falha ao extrair a tabela itsm.%s", table_name)
                    failures.append(table_name)
    except Exception:
        logging.exception("Falha na conexão ou na execução das extrações ITSM")
        raise
    
def extract_analista(myTimer: func.TimerRequest) -> None:
    logging.info('tabela analista')
    
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    logging.info(f'servidor={sql_server}, banco de dados={sql_database}, usuario={sql_user}, senha={sql_pass} ')

    # Configura a string de conexão para o banco de dados SQL Server
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    if failures:
        raise RuntimeError(
            "Falha na extração das tabelas: " + ", ".join(failures)
        )

    try:
        # Estabelece a conexão com o banco de dados usando pyodbc
        with pyodbc.connect(conn_str) as conn:
            # Cria um cursor para executar a consulta
            cursor = conn.cursor()
            
            query = "select * from itsm.analista"

            # Executa a consulta SQL
            cursor.execute(query)

            # Busca todos os resultados da consulta
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f'{len(rows)} linhas lidas de itsm.analista')

        # Cria um buffer de memória para armazenar os dados CSV
        buffer = io.StringIO()
        # Cria um objeto CSV writer e escreve os dados no buffer
        writer = csv.writer(buffer)
        # Escreve o cabeçalho e as linhas no buffer CSV
        writer.writerow(columns)
        # Escreve todas as linhas no buffer CSV
        writer.writerows(rows)

        # Grava o arquivo no contêiner bronze do Data Lake, particionado por data
        now = datetime.now(timezone.utc)
        file_path = f"analista/{now:%Y/%m/%d}/analista_{now:%Y%m%d_%H%M%S}.csv"

        #itsm/analista/year=2024/month=06/day=05/analista_20240605_123456.csv
        #file_path = f"itsm/analista/year={now:%Y}/month={now:%m}/day={now:%d}/analista_{now:%Y%m%d_%H%M%S}.csv"

        # Cria o cliente do Data Lake e grava o arquivo no contêiner bronze
        datalake = DataLakeServiceClient.from_connection_string(os.getenv("DATALAKE_CONNECTION_STRING"))
        # Cria o cliente do sistema de arquivos e grava o arquivo no contêiner bronze
        file_client = datalake.get_file_system_client("bronze").get_file_client(file_path)
        # Grava os dados do buffer no arquivo do Data Lake, sobrescrevendo se já existir
        file_client.upload_data(buffer.getvalue().encode("utf-8"), overwrite=True)

        logging.info(f'Arquivo gravado em bronze/{file_path}')

    except Exception as e:
        logging.error(f"Erro ao ler itsm.analista: {str(e)}")
        raise
    logging.info("Extração concluída para %s tabelas ITSM", len(ITSM_TABLES))