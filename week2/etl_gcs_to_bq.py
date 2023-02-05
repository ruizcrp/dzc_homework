from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import os


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.csv.gz"
    gcs_block = GcsBucket.load("gcsbucketblock")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"")
    return Path(f"{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    os.system(f"gzip -d {path}")
    df = pd.read_csv(os.path.splitext(path)[0])
    #print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    #df["passenger_count"].fillna(0, inplace=True)
    #print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("credsblock")

    df.to_gbq(
        destination_table="dszoomcampusa.rides",
        project_id="data-eng-cr",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )

@flow()
def etl_subflow(year: int, month: int, color: str) -> None:
    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)
    return df.shape[0]

@flow(log_prints=True)
def etl_gcs_to_bq(
    months: list[int] = [2,3], year: int = 2019, color: str = "yellow"
):
    counter=0
    for month in months:
        temp=etl_subflow(year, month, color)
        print(f"Row count of {month:02}-{year}-{color}: {temp}")
        counter=counter+temp
    print(f"Final counter: {counter}") 



if __name__ == "__main__":
    etl_gcs_to_bq()
