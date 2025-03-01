import asyncio
import pathlib

import duckdb
from rich.console import Group
from rich.live import Live
from rich.progress import Progress, BarColumn, MofNCompleteColumn, TimeRemainingColumn, SpinnerColumn

from aws_iceberg_demo import console


async def convert_csv(in_path: pathlib.Path, out_path: pathlib.Path):
    sql = f"COPY '{in_path}' to '{out_path}' (FORMAT 'parquet')"
    duckdb.sql(sql)


async def convert_to_parquet(file_paths: list[pathlib.Path],
                             output_folder_path: pathlib.Path,
                             concurrency=2) -> list[pathlib.Path]:
    total_progress = Progress("[bold yellow]{task.description}",
                              BarColumn(),
                              MofNCompleteColumn(),
                              TimeRemainingColumn())
    task_progress = Progress(SpinnerColumn(finished_text="✅"), "[bold blue]{task.description}")
    overall_progress = total_progress.add_task("Writing parquet files...", total=len(file_paths))
    semaphore = asyncio.Semaphore(concurrency)
    progress_group = Group(total_progress, task_progress)

    async def _convert(file_path: pathlib.Path):
        async with semaphore:
            convert_task = task_progress.add_task(f"Converting {file_path}", total=1)
            out_path = output_folder_path / file_path.with_suffix(".parquet").name
            if out_path.exists():
                task_progress.update(convert_task, description=f"{file_path} already exists", advance=1)
                total_progress.update(overall_progress, advance=1)
                return out_path
            try:
                task_progress.update(convert_task, advance=1, description=f"{file_path} converted")
                total_progress.update(overall_progress, advance=1)
                return await convert_csv(file_path, out_path)
            except Exception as e:
                total_progress.update(overall_progress, advance=1)
                task_progress.update(convert_task, description=f"Exception occurred: {e}")
                raise e


    with Live(progress_group, console=console):
        tasks = [_convert(file_path) for file_path in file_paths]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        converted_files = []
        for result in results:
            if isinstance(result, Exception):
                console.print(f"[bold red]Extraction error:[/bold red] {result}")
                continue
            converted_files.append(result)
        return converted_files


async def sample(in_path: pathlib.Path, out_path: pathlib.Path):
    sql = f"COPY (SELECT * FROM read_parquet('{in_path}') using sample 10%) to '{out_path}' (FORMAT 'parquet')"
    duckdb.sql(sql)


async def sample_parquet(file_list: list[pathlib.Path], output_folder_path: pathlib.Path):
    total_progress = Progress("[bold yellow]{task.description}", BarColumn(), MofNCompleteColumn(),
                              TimeRemainingColumn())
    file_progress = Progress(SpinnerColumn(finished_text="✅"), "[bold blue]{task.description}")
    progress_group = Group(total_progress, file_progress)
    conversion_task = total_progress.add_task("Sampling parquet files...", total=len(file_list))
    semaphore = asyncio.Semaphore(3)

    async def _convert(file: pathlib.Path):
        async with semaphore:
            out_path = output_folder_path / file.name
            file_task = file_progress.add_task(f"Converting {file}...", total=1)
            if out_path.exists():
                file_progress.update(file_task, advance=1, description=f"{file} already exists")
                total_progress.update(conversion_task, advance=1)
                return
            await sample(file, out_path)

    with Live(progress_group):
        tasks = [_convert(file_name) for file_name in file_list]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        converted_files = []
        for result in results:
            if isinstance(result, Exception):
                console.print(f"[bold red]Extraction error:[/bold red] {result}")
            else:
                converted_files.append(result)
    return converted_files
