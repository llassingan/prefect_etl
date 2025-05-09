# Belum Digunakan

from prefect_sqlalchemy import SqlAlchemyConnector, ConnectionComponents, SyncDriver

connector = SqlAlchemyConnector(
    connection_info=ConnectionComponents(
        driver=SyncDriver.POSTGRESQL_PSYCOPG2,
        username="USERNAME-PLACEHOLDER",
        password="PASSWORD-PLACEHOLDER",
        host="localhost",
        port=5432,
        database="DATABASE-PLACEHOLDER",
    )
)

connector.save("BLOCK_NAME-PLACEHOLDER")