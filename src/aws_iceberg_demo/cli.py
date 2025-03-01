import asyncio
import pathlib
import tempfile
from typing import Annotated

import typer
from rich.panel import Panel

from aws_iceberg_demo import console
from aws_iceberg_demo.data import convert_to_parquet, sample_parquet

DATA_BASE_URL = "https://data.rees46.com/datasets/marketplace"
DATA_FILES = ["2019-Oct.csv.gz",
              "2019-Nov.csv.gz",
              "2019-Dec.csv.gz",
              "2020-Jan.csv.gz",
              "2020-Feb.csv.gz",
              "2020-Mar.csv.gz",
              "2020-Apr.csv.gz"]

app = typer.Typer()

DownloadOption = Annotated[int, typer.Option("--download-concurrency", "-d")]
OutputDirOption = Annotated[str, typer.Option("--output-dir", "-o")]
ExtractOption = Annotated[int, typer.Option("--extract-concurrency", "-e")]


@app.command(name="download")
def download_data(download_concurrency: DownloadOption = 5,
                  extract_concurrency: ExtractOption = 2,
                  output_dir: OutputDirOption = "data"):
    from aws_iceberg_demo.downloads import download_files, extract_files
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
def bootstrap_project():
    from aws_iceberg_demo.connections import get_fs
    fs = get_fs()
    try:
        fs.mkdir("data-engineering-events")
    except FileExistsError:
        pass
