from prefect import flow
from prefect.logging import get_run_logger
import sys
import os

# Add the utils directory to the path so we can import custom_log_handler
sys.path.append("/opt/prefect/script")
from ..task.data_init import UpdateResource



@flow(name="flow_eod")
def DataInit():
    flow_name = "flow_eod"
    logger = get_run_logger()
    logger.info(f"starting flow: {flow_name}")
    logger.info(f"starting task: data_init")
    UpdateResource()
    logger.info(f"task data_init has finished")
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