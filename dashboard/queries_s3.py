import os
import awswrangler as wr
from dotenv import load_dotenv

load_dotenv()

DATABASE = os.environ["ATHENA_DB"]         
TABLE = os.environ["ATHENA_TABLE"]
ATHENA_OUTPUT = os.environ["ATHENA_OUTPUT"]


def get_available_dates():
    sql = f"""
    SELECT DISTINCT
      partition_0 AS yyyy,
      partition_1 AS mm,
      partition_2 AS dd,
      concat(partition_0, '-', partition_1, '-', partition_2) AS date
    FROM {TABLE}
    ORDER BY yyyy DESC, mm DESC, dd DESC
    """
    return wr.athena.read_sql_query(
        sql=sql,
        database=DATABASE,
        s3_output=ATHENA_OUTPUT,
    )


def get_data_for_date(date_str: str):
    yyyy, mm, dd = date_str.split("-")

    sql = f"""
    SELECT *
    FROM {TABLE}
    WHERE partition_0 = '{yyyy}'
      AND partition_1 = '{mm}'
      AND partition_2 = '{dd}'
    """
    return wr.athena.read_sql_query(
        sql=sql,
        database=DATABASE,
        s3_output=ATHENA_OUTPUT,
    )


def get_last_n_days(n: int = 14):
    sql = f"""
    WITH d AS (
      SELECT
        partition_0,
        partition_1,
        partition_2,
        date_parse(
          concat(partition_0, '-', partition_1, '-', partition_2),
          '%Y-%m-%d'
        ) AS dt,
        soil_mean,
        temp_mean,
        readings,
        plants
      FROM {TABLE}
    )
    SELECT
      dt,
      soil_mean,
      temp_mean,
      readings,
      plants
    FROM d
    ORDER BY dt DESC
    LIMIT {n}
    """
    return wr.athena.read_sql_query(
        sql=sql,
        database=DATABASE,
        s3_output=ATHENA_OUTPUT,
    )
