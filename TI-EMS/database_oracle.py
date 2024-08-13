import cx_Oracle
import asyncio

def connect_to_oracle_gl_posting():
    hostname = "10.0.28.172"
    port = 1548
    service_name = "PROD"
    username = "TI_IIOT"
    password = "tiiot2022"

    # Construct the DSN string
    dsn = cx_Oracle.makedsn(hostname, port, service_name=service_name)

    # Connect to the Oracle database
    connection = cx_Oracle.connect(username, password, dsn)

    return connection


def connect_to_oracle():
    hostname = "10.0.28.172"
    port = 1548
    service_name = "PROD"
    username = "TI_IIOT"
    password = "tiiot2022"

    # Construct the DSN string
    dsn = cx_Oracle.makedsn(hostname, port, service_name=service_name)

    # Connect to the Oracle database
    connection = cx_Oracle.connect(username, password, dsn)

    return connection



def connect_to_oracle_test():
    hostname = "10.0.28.190"
    port = 1562
    service_name = "DEV"
    username = "TI_IIOT"
    password = "ti_iiotdev"

    # Construct the DSN string
    dsn = cx_Oracle.makedsn(hostname, port, service_name=service_name)

    # Connect to the Oracle database
    connection = cx_Oracle.connect(username, password, dsn)

    return connection



