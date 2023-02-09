from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_aws.s3 import S3Bucket


@task(log_prints=True)
def fetch(dataset_url: str) -> pd.DataFrame:
    """read taxi data into dataframe"""
    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df:pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df['lpep_pickup_datetime'] = pd.to_datetime(df.lpep_pickup_datetime)
    df['lpep_dropoff_datetime'] = pd.to_datetime(df.lpep_dropoff_datetime)
    print(df.head(2))
    print(f'rows: {len(df)}')
    print(f'columns: {df.dtypes}')
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    "write dataframe out locally as parquet file"
    path = Path(f'data/{color}/{dataset_file}.parquet')
    path.parents[0].mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, compression='gzip')
    return path


@task()
def write_s3(path: Path) -> None:
    """uploading local parquet file into s3bucket"""
    s3_block = S3Bucket.load("prefect-de-zoom-s3")
    s3_block.upload_from_path(f"{path}", f"{path}")
    return


@flow(log_prints=True)
def etl_web_to_s3():
    """the main ETL function"""
    color = 'yellow'
    year = 2021
    month = 1
    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    dataset_url =f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_s3(path)


if __name__ == '__main__':
    etl_web_to_s3()