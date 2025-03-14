import asyncio
import pathlib
import tempfile
from typing import Annotated

import pyarrow.parquet as pq
import typer
from rich.console import Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, MofNCompleteColumn, BarColumn, SpinnerColumn

from aws_iceberg_demo import console

DATA_BASE_URL = "https://data.rees46.com/datasets/marketplace"
DATA_FILES = ["2019-Oct.csv.gz",
              "2019-Nov.csv.gz",
              "2019-Dec.csv.gz",
              "2020-Jan.csv.gz",
              "2020-Feb.csv.gz",
              "2020-Mar.csv.gz",
              "2020-Apr.csv.gz"]

app = typer.Typer()

DownloadOption = Annotated[int, typer.Option("--download-concurrency", "-d", help="Number of concurrent downloads")]
OutputDirOption = Annotated[str, typer.Option("--output-dir", "-o")]
ExtractOption = Annotated[int, typer.Option("--extract-concurrency", "-e", help="Number of concurrent gunzip extracts")]


@app.command(name="download")
def download_data(download_concurrency: DownloadOption = 5,
                  extract_concurrency: ExtractOption = 2,
                  output_dir: OutputDirOption = "data"):
    """Download and gunzip the data files into the `output_dir`."""
    from aws_iceberg_demo.downloads import download_files, extract_files
    from aws_iceberg_demo.data import convert_to_parquet, sample_parquet
    output_dir = pathlib.Path.cwd() / "data"
    output_dir.mkdir(exist_ok=True)

    existing_files = [file_name.name for file_name in output_dir.glob("*.csv")]
    new_files = [file_name for file_name in DATA_FILES if file_name[:-3] not in existing_files]

    async def _run():
        if new_files:
            # Set lower extraction concurrency to avoid memory issues
            with tempfile.TemporaryDirectory() as temp_dir_str:
                temp_dir = pathlib.Path(temp_dir_str)
                console.print(Panel.fit(
                    "[bold cyan]Step 1: Downloading files[/bold cyan]",
                    title="Demo File Downloader"
                ))
                downloaded_files = await download_files(
                    urls=new_files,
                    output_folder=temp_dir,
                    download_concurrency=download_concurrency,
                )

                console.print(Panel.fit(
                    "[bold cyan]Step 2: Extracting files[/bold cyan] "
                    f"(max {extract_concurrency} at a time)",
                    title="Demo File Downloader"
                ))

                extracted_files = await extract_files(downloaded_files,
                                                      output_dir,
                                                      extract_concurrency=extract_concurrency)
        else:
            extracted_files = list(output_dir.glob("*.csv"))
            console.print(f"[bold green] All CSVs downloaded - skipping steps 1 and 2")

        console.print(Panel.fit("[bold cyan]Step 3: Converting to Parquet", title="Demo File Downloader"))
        parquet_output_folder = output_dir / "parquet"
        parquet_output_folder.mkdir(exist_ok=True)

        parquet_files = await convert_to_parquet(extracted_files, parquet_output_folder)

        console.print(Panel.fit("[bold cyan]Step 4: Building Sampled Dataset", title="Demo File Downloader"))

        sampled_parquet_folder = parquet_output_folder / "sampled"
        sampled_parquet_folder.mkdir(exist_ok=True)

        await sample_parquet(parquet_files, sampled_parquet_folder)

        console.print("[bold green]Processing complete![/bold green]")

    asyncio.run(_run())


@app.command(name="bootstrap")
def initialize_bucket(location: str = "data-engineering-events"):
    """Initialize required resources to run the intro"""
    from aws_iceberg_demo.connections import get_fs
    fs = get_fs()
    try:
        fs.mkdir(location)
    except FileExistsError:
        pass

@app.command(name="load")
def full_load(input_files: list[pathlib.Path],
              location: Annotated[str, typer.Option("-l", "--location", help="Target S3 bucket in s3:// URI format")],
              table_name: Annotated[
                  str, typer.Option("-n", "--name", help="Name of the table to register")] = "events"):
    """Load data from INPUT_FILES into the target S3 bucket."""
    from aws_iceberg_demo.connections import get_fs
    from aws_iceberg_demo.catalog import get_catalog
    from aws_iceberg_demo.tables import monthly_event_partition, event_table_schema

    fs = get_fs()
    try:
        fs.mkdir(location)
    except FileExistsError:
        pass

    catalog = get_catalog()
    catalog.create_namespace_if_not_exists("store")
    table = catalog.create_table_if_not_exists(f"store.{table_name}",
                                               schema=event_table_schema,
                                               partition_spec=monthly_event_partition,
                                               location=f"{location}/{table_name}")

    overall_progress = Progress("[bold blue]{task.description}", BarColumn(), MofNCompleteColumn())
    file_progress = Progress(SpinnerColumn(finished_text="✅"), "[bold blue]{task.description}")
    progress_group = Group(overall_progress, file_progress)
    with Live(progress_group, console=console):
        overall_upload_task = overall_progress.add_task("Uploading files...", total=len(input_files))
        for file in input_files:
            file_task = file_progress.add_task(f"[bold green]Reading {file.name}...")
            df = pq.read_table(file, schema=table.schema().as_arrow())
            file_progress.update(file_task, description=f"[bold green]Uploading {file.name}...")
            table.append(df)
            file_progress.update(file_task, completed=True, description=f"{file.name} uploaded!")
            overall_progress.update(overall_upload_task, advance=1)
    console.print("[bold green]Uploading complete!")
