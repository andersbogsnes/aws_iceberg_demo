import pyarrow.parquet as pq
from rich.progress import Progress, MofNCompleteColumn, TimeElapsedColumn, SpinnerColumn

from aws_iceberg_demo.catalog import aws_catalog
from aws_iceberg_demo.tables import event_table_schema


def build_progress_bar() -> Progress:
    return Progress(
        SpinnerColumn(),
        "{task.description}",
        TimeElapsedColumn()
    )

if __name__ == '__main__':
    aws_catalog.create_namespace_if_not_exists("stores")
    event_table = aws_catalog.create_table_if_not_exists("stores.events",
                                                         event_table_schema,
                                       "s3://data-engineering-events/warehouse/events")
    files = ["data/parquet/2019-Nov.parquet"]
    progress = build_progress_bar()
    append_task = progress.add_task("Appending data...")
    with progress:
        df = pq.read_table("data/parquet/2019-Dec.parquet", schema=event_table.schema().as_arrow())
        event_table.overwrite(df)


