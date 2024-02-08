import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/magic-zoomcamp/smiling-breaker-413716-703f6ba426ce.json"
bucket_name = 'mage-zoomcamp-eileenthedev-1'
object_key = 'green_ny_taxi_clean.parquet'
project_id = "smiling-breaker-413716"
table_name = "green_ny_taxi_data"
root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data_to_big_query(data, **kwargs) -> None:
    data['lpep_pickup_datetime'] = data['lpep_pickup_datetime'].dt.date
    table = pa.Table.from_pandas(data)
    gcs = pa.fs.GcsFileSystem()
    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_datetime'],
        filesystem=gcs
    )
