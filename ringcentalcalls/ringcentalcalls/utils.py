from datetime import datetime
import dateutil


def month_diff(x: str, n: int) -> str:
    """
    Looks back n of months from the input month and returns that value in year-month format.

    Args:
        x: input month in Y-m-d format "2022-01-21"

    Returns:
        month in Y-m-d format
    """

    a_date = datetime.strptime(x, "%Y-%m-%d")
    a_month = dateutil.relativedelta.relativedelta(months=n)
    date_minus_month = a_date - a_month
    month = date_minus_month.strftime('%Y-%m-%d')

    return month

def conn_str() -> str:
    """Returns a connection string for databricks to connect to the edw.
    Returns:
        str: connection string
    """

    conn = (
        "P01_USERNAME = dbutils.secrets.get(scope='DataScienceProjects', key='P01_USERNAME');"
        "P01_PASSWORD = dbutils.secrets.get(scope='DataScienceProjects', key='P01_PASSWORD');"
        "P01_HOST = dbutils.secrets.get(scope='DataScienceProjects', key='P01_HOST');"
        "P01_DATABASE = dbutils.secrets.get(scope='DataScienceProjects', key='P01_DATABASE');"
        "P01_PORT = dbutils.secrets.get(scope='DataScienceProjects', key='P01_PORT');"
        "JDBC_CONNECTION = 'jdbc:sqlserver://{host}:{port};database={database};user={user};password={password};UseNTLMv2=true'.format("
        "host=P01_HOST,"
        "port=P01_PORT,"
        "database=P01_DATABASE,"
        "user=P01_USERNAME,"
        "password=P01_PASSWORD);")

    return conn

def qa_conn_str() -> str:
    """Returns a connection string for databricks to connect to the QA edw.
    Returns:
        str: connection string
    """

    conn = (
        "P01_USERNAME_Q_DB = dbutils.secrets.get(scope='DataScienceProjects', key='USERNAME_Q_DB');"
        "P01_PASSWORD_Q_DB = dbutils.secrets.get(scope='DataScienceProjects', key='PASSWORD_Q_DB');"
        "P01_HOST_Q_DB = dbutils.secrets.get(scope='DataScienceProjects', key='HOST_Q01_DB');"
        "P01_DATABASE_Q_DB = dbutils.secrets.get(scope='DataScienceProjects', key='DATABASE_Q_DB');"
        "P01_PORT_Q_DB = dbutils.secrets.get(scope='DataScienceProjects', key='PORT_Q_DB');"
        "JDBC_CONNECTION_Q01_DB = 'jdbc:sqlserver://{host}:{port};database={database};user={user};password={password};UseNTLMv2=true'.format("
        "host=P01_HOST_Q_DB,"
        "port=P01_PORT_Q_DB,"
        "database=P01_DATABASE_Q_DB,"
        "user=P01_USERNAME_Q_DB,"
        "password=P01_PASSWORD_Q_DB);")

    return conn
