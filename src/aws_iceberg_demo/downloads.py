import asyncio
import gzip
import pathlib
import shutil
import tempfile
from pathlib import Path
from typing import List, Dict

import httpx
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import (Progress, TaskID, TextColumn, BarColumn, DownloadColumn,
                           TransferSpeedColumn, TimeRemainingColumn, MofNCompleteColumn)

DATA_BASE_URL = "https://data.rees46.com/datasets/marketplace"
DATA_FILES = ["2019-Oct.csv.gz",
              "2019-Nov.csv.gz",
              "2019-Dec.csv.gz",
              "2020-Jan.csv.gz",
              "2020-Feb.csv.gz",
              "2020-Mar.csv.gz",
              "2020-Apr.csv.gz"]

# Create a global console object
console = Console()

async def download_file(
        client: httpx.AsyncClient,
        url: str,
        progress: Progress,
        task_id: TaskID,
        temp_dir: pathlib.Path
) -> pathlib.Path:
    """
    Download a file asynchronously and track progress.

    Args:
        client: HTTP client
        url: URL to download
        progress: Rich progress instance
        task_id: ID of the progress task
        temp_dir: Directory to save the temporary gzipped file

    Returns:
        Path to the downloaded gzipped file
    """
    # Create a temporary filename
    filename = temp_dir / pathlib.Path(url).name

    # Start streaming the download
    async with client.stream("GET", url) as response:
        response.raise_for_status()

        # Get content length if available
        total_size = int(response.headers.get("Content-Length", 0))
        if total_size:
            progress.update(task_id, total=total_size)

        # Write the file as we receive it
        with filename.open("wb") as f:
            downloaded = 0
            async for chunk in response.aiter_bytes():
                f.write(chunk)
                downloaded += len(chunk)
                progress.update(task_id, completed=downloaded)
    return filename

def _extract_gzip_sync(gzip_path: Path, output_dir: Path) -> Path:
    """
    Synchronous function to extract a gzipped file.
    This will be called in a separate thread.
    """
    if gzip_path.suffix == ".gz":
        output_filename = gzip_path.stem
    else:
        raise ValueError("Not a gzip file")

    output_path = output_dir / output_filename

    # Extract the file
    with gzip.open(gzip_path, 'rb') as f_in:

        with open(output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    return output_path

async def extract_gzip(
        gzip_path: Path,
        output_dir: Path,
        progress: Progress,
        task_id: TaskID
) -> Path:
    """
    Extract a gzipped file to the output directory asynchronously.

    Args:
        gzip_path: Path to the gzipped file
        output_dir: Directory to extract the file to
        progress: Rich progress instance
        task_id: ID of the progress task

    Returns:
        Path to the extracted file
    """
    # Run CPU-bound extraction in a thread pool
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None,
        _extract_gzip_sync,
        gzip_path,
        output_dir
    )

    # Update progress
    progress.update(task_id, advance=1)

    return result

async def download_files(
        urls: list[str],
        temp_dir: pathlib.Path,
        download_concurrency: int = 5,
        timeout: int = 60
) -> Dict[str, pathlib.Path]:
    """
    Download multiple gzipped CSV files with progress display.

    Args:
        urls: List of URLs to download
        temp_dir: Directory to save the temporary files
        download_concurrency: Maximum number of concurrent downloads
        timeout: Connection timeout in seconds

    Returns:
        Dictionary mapping URLs to downloaded file paths
    """
    # Set up a semaphore to limit concurrency
    semaphore = asyncio.Semaphore(download_concurrency)

    download_progress = Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
            console=console
    )
    total_progress = Progress("[bold yellow]{task.description}",
                              BarColumn(),
                              MofNCompleteColumn(),
                              TimeRemainingColumn(),
                              console=console)
    progress_group = Group(total_progress, download_progress)
    # Set up progress display
    with Live(progress_group):
        # Create a task for overall progress
        overall_task = total_progress.add_task("[yellow]Overall download progress", total=len(urls))
        file_tasks = {}
        downloaded_files = {}

        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout), base_url=DATA_BASE_URL) as client:
            async def download_url(url):
                async with semaphore:
                    try:
                        # Create a task for this file
                        filename = pathlib.Path(url).name
                        task_id = download_progress.add_task(f"Downloading {filename}", total=0)
                        file_tasks[url] = task_id

                        # Download the file
                        file_path = await download_file(client, url, download_progress, task_id, temp_dir)

                        # Complete the task
                        download_progress.update(task_id, description=f"Downloaded {filename}")
                        total_progress.update(overall_task, advance=1)

                        return url, file_path

                    except Exception as e:
                        download_progress.update(task_id, description=f"Failed {filename}: {str(e)}")
                        raise e

            # Process all URLs concurrently
            tasks = [download_url(url) for url in urls]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Check for exceptions and build result dictionary
            for result in results:
                if isinstance(result, Exception):
                    console.print(f"[bold red]Download error:[/bold red] {result}")
                else:
                    url, file_path = result
                    downloaded_files[url] = file_path

    return downloaded_files

async def extract_files(
        downloaded_files: Dict[str, pathlib.Path],
        output_dir: pathlib.Path,
        extract_concurrency: int = 2
) -> List[Path]:
    """
    Extract downloaded gzipped files with limited concurrency.

    Args:
        downloaded_files: Dictionary mapping URLs to downloaded file paths
        output_dir: Directory to extract the files to
        extract_concurrency: Maximum number of concurrent extractions

    Returns:
        List of paths to the extracted files
    """
    # Set up a semaphore to limit extraction concurrency
    semaphore = asyncio.Semaphore(extract_concurrency)

    overall_progress = Progress("[yellow]{task.description}",
                                BarColumn(),
                                MofNCompleteColumn(),
                                TimeRemainingColumn(),
                                console=console)
    task_progress = Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TimeRemainingColumn(),
            console=console
    )
    progress_group = Group(overall_progress, task_progress)
    # Set up progress display
    with Live(progress_group) as progress:
        # Create a task for overall progress
        overall_task = overall_progress.add_task(
            "[yellow]Overall extraction progress",
            total=len(downloaded_files)
        )

        extracted_files = []

        async def extract_file(gzip_path):
            async with semaphore:
                try:
                    filename = gzip_path.name
                    task_id = task_progress.add_task(f"Extracting {filename}", total=1)

                    # Extract the file
                    extracted_path = await extract_gzip(gzip_path, output_dir, task_progress, task_id)

                    # Update task description
                    task_progress.update(task_id, description=f"Completed {filename}")
                    overall_progress.update(overall_task, advance=1)

                    return extracted_path

                except Exception as e:
                    task_progress.update(task_id,
                                         description=f"Extraction failed {filename}: {str(e)}")
                    raise e

        # Create extraction tasks
        tasks = [
            extract_file(file_path)
            for file_path in downloaded_files.values()
        ]

        # Run extraction tasks
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Check for exceptions
        for result in results:
            if isinstance(result, Exception):
                console.print(f"[bold red]Extraction error:[/bold red] {result}")
            else:
                extracted_files.append(result)

    return extracted_files

async def process_csv_files(
        urls: list[str],
        output_dir: pathlib.Path,
        download_concurrency: int = 5,
        extract_concurrency: int = 2,
        timeout: int = 60
) -> list[pathlib.Path]:
    """
    Download and extract multiple gzipped CSV files with controlled concurrency.

    Args:
        urls: List of URLs to download
        output_dir: Directory to save the extracted files
        download_concurrency: Maximum number of concurrent downloads
        extract_concurrency: Maximum number of concurrent extractions
        timeout: Connection timeout in seconds

    Returns:
        List of paths to the extracted files
    """
    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create a temporary directory for the gzipped files
    with tempfile.TemporaryDirectory() as temp_dir_str:
        temp_dir = pathlib.Path(temp_dir_str)

        # Step 1: Download all files with high concurrency
        console.print(Panel.fit(
            "[bold cyan]Step 1: Downloading files[/bold cyan]",
            title="CSV Processor"
        ))

        downloaded_files = await download_files(
            urls,
            temp_dir,
            download_concurrency,
            timeout
        )

        # Step 2: Extract files with limited concurrency
        console.print(Panel.fit(
            "[bold cyan]Step 2: Extracting files[/bold cyan] "
            f"(max {extract_concurrency} at a time)",
            title="CSV Processor"
        ))

        extracted_files = await extract_files(
            downloaded_files,
            output_dir,
            extract_concurrency
        )

    return extracted_files

async def main():
    # Example usage
    output_dir = pathlib.Path.cwd() / "data"
    existing_files = [file_name.name for file_name in output_dir.glob("*.csv")]
    new_files = [file_name for file_name in DATA_FILES if file_name[:-3] not in existing_files]

    if not new_files:
        console.print("[bold green]All files already downloaded")
        return
    # Set lower extraction concurrency to avoid memory issues
    await process_csv_files(
        new_files,
        output_dir,
        download_concurrency=5,  # Download many files simultaneously
        extract_concurrency=2    # Extract only a few files at a time
    )

    # Display results using Rich
    console.print("[bold green]Processing complete![/bold green]")

if __name__ == "__main__":
    asyncio.run(main())