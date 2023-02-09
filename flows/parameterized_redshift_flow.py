from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_aws.s3 import S3Bucket
from prefect_sqlalchemy import SqlAlchemyConnector


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_from_s3(color: str, year: int, month: int) -> Path:
    "Download trip from S3 bucket"
    s3_block = S3Bucket.load("prefect-de-zoom-s3")
    s3_path = f"data/{color}"
    dataset_file = f"{color}_tripdata_{year}-{month:02}.parquet"
    localpath = Path(Path(__file__).parents[1], s3_path)
    localpath.mkdir(parents = True, exist_ok = True)
    s3_block.download_object_to_path(from_path=s3_path + "/" + dataset_file, to_path=Path(localpath, dataset_file))
    return localpath / dataset_file

@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def transform(path: Path) -> pd.DataFrame:
    """data cleaning"""
    df = pd.read_parquet(path)
    return df

@task(retries=3)
def write_redshift(df: pd.DataFrame, table_name) -> None:
    """write DataFrame to RedShift"""
    print(f"Length is {len(df)}")
    connection_block = SqlAlchemyConnector.load("aws-redshif")
    with connection_block.get_connection(begin=False) as engine:
        df.head().to_sql(name=table_name, con=engine, index=False, if_exists="replace", method="multi")
    print("Uploaded to redshift")


@flow(name='Subflow', log_prints=True)
def log_subflow(table_name: str):
    print(f'Logging subflow for: {table_name}')

@flow(log_prints=True)
def etl_s3_to_redshift(year: int, color: str, month: int, table_name: str):
    """Main ETL flow to load data into AWSRedshift"""
    
    path = extract_from_s3(color, year, month)
    df = transform(path)
    write_redshift(df, table_name)

@flow()
def redshift_parent_flow(
    months: list[int] = [1, 2], year: int = 2021, color: str = 'yellow'
):
    total_rows = 0
    for month in months:
        etl_s3_to_redshift(year, color, month)
        total_rows += len(etl_s3_to_redshift(year, color, month))
    print(f"Total rows processed: {total_rows}")


if __name__ == '__main__':
    color = 'yellow'
    month = [1,2,3]
    year = 2021
    etl_s3_to_redshift(year, color, month, table_name = f'{color}_taxi_data')
