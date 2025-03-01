import asyncio
import pathlib
from typing import Annotated

import rich.console
import typer

DATA_BASE_URL = "https://data.rees46.com/datasets/marketplace"
DATA_FILES = ["2019-Oct.csv.gz",
              "2019-Nov.csv.gz",
              "2019-Dec.csv.gz",
              "2020-Jan.csv.gz",
              "2020-Feb.csv.gz",
              "2020-Mar.csv.gz",
              "2020-Apr.csv.gz"]

console = rich.console.Console()

app = typer.Typer()

DownloadOption = Annotated[int, typer.Option("--download-concurrency", "-d")]
OutputDirOption = Annotated[str, typer.Option("--output-dir", "-o")]
ExtractOption = Annotated[int, typer.Option("--extract-concurrency", "-e")]

@app.command(name="download")
def download_data(download_concurrency: DownloadOption = 5,
                  extract_concurrency: ExtractOption = 2,
                  output_dir: OutputDirOption = "data"):
    from aws_iceberg_demo.downloads import process_csv_files
    output_dir = pathlib.Path.cwd() / "data"
    output_dir.mkdir(exist_ok=True)

    existing_files = [file_name.name for file_name in output_dir.glob("*.csv")]
    new_files = [file_name for file_name in DATA_FILES if file_name[:-3] not in existing_files]

    async def _run():
        if not new_files:
            console.print("[bold green]All files already downloaded")
            return
        # Set lower extraction concurrency to avoid memory issues
        await process_csv_files(
            new_files,
            output_dir,
            download_concurrency=download_concurrency,  # Download many files simultaneously
            extract_concurrency=extract_concurrency    # Extract only a few files at a time
        )

        # Display results using Rich
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
