from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_aws.s3 import S3Bucket
from prefect_aws import AwsCredentials
from prefect_sqlalchemy import SqlAlchemyConnector
from sqlalchemy import create_engine


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
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task(retries=3)
def write_redshift(df: pd.DataFrame, table_name) -> None:
    """write DataFrame to RedShift"""
    print(f"Length is {len(df)}")
    connection_block = SqlAlchemyConnector.load("aws-redshif")
    with connection_block.get_connection(begin=False) as engine:
        df.head(100_000).to_sql(name=table_name, con=engine, index=False, if_exists="replace", method="multi")
    print("Uploaded to redshift")
    # connection_block = SqlAlchemyConnector.load('aws-redshif')
    # with connection_block.get_connection(begin=False) as engine:
    #     df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    #     df.to_sql(name=table_name, con=engine, if_exists='append')
    # conn = create_engine('postgresql://awsuser:Nasslola_09@redshift-cluster-1.csz5joeiy7ou.us-east-1.redshift.amazonaws.com:5439/ny_taxi')
    # df.to_sql(table_name, conn, index=False)


@flow(name='Subflow', log_prints=True)
def log_subflow(table_name: str):
    print(f'Logging subflow for: {table_name}')

@flow(log_prints=True)
def etl_s3_to_redshift(table_name: str):
    """Main ETL flow to load data into AWSRedshift"""
    color = 'yellow'
    year=2021
    month=1

    path = extract_from_s3(color, year, month)
    df = transform(path)
    write_redshift(df, table_name)


if __name__ == "__main__":
    etl_s3_to_redshift('yellow_taxi_data')