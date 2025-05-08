from prefect import flow, task
from prefect.logging import get_run_logger
import sys
import os
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Connection

# Add the utils directory to the path so we can import custom_log_handler
sys.path.append("/opt/prefect/flows")
from utils.logger import setup_file_logging
from db.database_conn import get_connection_postgres
from utils.config import settings 
from db.sql_resource import SQL_RESOURCES


def MappingFlow(list_of_query: list[str]) -> dict[str, str]:
    """
    Returns a dictionary containing the selected queries from SQLRESOURCE.
    """
    return {key: SQL_RESOURCES[key] for key in list_of_query if key in SQL_RESOURCES}

@task
def UpdateResource():
    logger = get_run_logger()
    logger.info(f"starting task: update resource")
    logger.info(f"get database connection..")
    conn: Connection = get_connection_postgres()
    SQLFlows = [
        "initSaldoAccrual",
        # 'initSaldoBaghas',
        # 'initSaldoPajak',
        # 'initSaldoZakat',
        # 'initSaldoBiaya',
        # 'initNisbahDasarRekening',
        "initNisbahSpesial",
        "initSaldoDitahan",
        # 'initTieringNisbah',
        "initCadangan",
        "FixConfidential",
        "initBiayaAdmBulanan",
        "initECR",
        "initJumlahHariPerTahun",
        "initNisbahBonusDasar",
        # 'copyNisbahDasarProduk',
        "initRekeningCustomerBalanceSign",
        "initRekeningKasBalanceSign",
        "initJumlahAro",
        "initJumlahBagHas",
        "initTanggalJTDepo_Null_B",
        "initTanggalJTDepo_Null_H",
        "initTanggalBGHDepo_Null_B",
        "initTanggalBGHDepo_Null_H",
        "initTanggalJatuhTempoRencana",
        "syncGLAccountName",
    ]
    DB_SCHEMA = settings.DB_SCHEMA
    try:
        # Begin a transaction
        logger.info(f"begin transaction..")
        with conn.begin():
            logger.info(f"getting list of flows..")
            flows = MappingFlow(SQLFlows)
            conn.execute(f"SET search_path TO {DB_SCHEMA}")
            for flow,sql in flows.items():
                logger.info(f"start executing flow: {flow}")
                conn.execute(
                    text(sql)
                )
        logger.info(f"transaction committed successfully.")

    except SQLAlchemyError as e:
        logger.error(f"Database error occurred: {e}. Rolled back", exc_info=True)
        lo

    except Exception as e:
        logger.error(f"Unexpected error: {e}. Rolled back", exc_info=True)

    finally:
        conn.close()

@flow(name="data_init")
def DataInit():
    flow_name = "data_init"
    logger = get_run_logger()
    logger.info(f"starting flow: {flow_name}")
    UpdateResource()
    logger.info(f"flow {flow_name} has finished")

    # dictParam.update(
    #     dbutil.mapDBTableNames(
    #         config,
    #         [
    #             "RekeningLiabilitas", # ini buat mapping tabel, ntar dipake di bawah
    #             "Produk",
    #             "Deposito",
    #             "RekeningTransaksi",
    #             "RekeningRencana",
    #             "Transaksi",
    #             "DetilTransaksi",
    #             "Account",
    #         ],h
    #     )
    # )