"""A CLI wrapper with a single entry point.

This module acts as a command line interface around the batching logic
and consumes the API exposed in `batcher.py`.

This module should not be imported directly; import from `batcher.py`
instead.
"""

from datetime import datetime
import pathlib
from pprint import pprint
from typing import Annotated

from cyclopts import App, Parameter
from rich import print as richprint
from rich.panel import Panel

from batcher import BatchGenerator

__version__ = "1.0.0"

app = App(default_parameter=Parameter(negative=()))


@app.default
def main(database: str,
         collection: str,
         file_path: pathlib.Path,
         *,
         dry_run: Annotated[bool, Parameter(name=["--dry-run", "-d"])] = False,
         dump_stage: bool = False,
         ) -> int:
    """The DSpace Batching CLI

    Build a DSpace Simple Archive Format (SAF) batch directory from a
    set of raw data files and their metadata stored in MongoDB.

    For each item in the specified MongoDB collection, an item
    directory (**item_001**, **item_002**, ...) is created containing
    the bitstream, a contents manifest, and a Dublin Core metadata
    file (**dublin_core.xml**), as required by the DSpace SAF import
    specification.

    Before generating the batch, use **--dry-run** to verify that
    every record in the collection has a matching file in the raw data
    folder and to review the metadata values that will be written —
    without creating any output on disk.

    **EXAMPLES**

    The parameters **DATABASE**, **COLLECTION** and **FILE-PATH**
    should be passed in that order:

        python cli.py DATABASE COLLECTION FILE-PATH

    For the use in scripts, or in documentation, prefer using named
    parameters as they self-document the intended purpose of the
    values being passed:

        python cli.py
            --database DATABASE
            --collection COLLECTION
            --file-path FILE-PATH

    **FILE PATHS**

    On Linux and macOS, **FILE-PATH** is a POSIX path with forward
    slashes (/); on Windows either forward (/) or backward (\\\\)
    slashes may separate path components.

    **Linux / macOS**  : */data/batch/files*  or  *~/projects/batch/files*

    **Windows**        : *C:/Users/Curator/batch/files*  or  *C:\\Users\\Curator\\batch\\files*

    **STAGING DATA**

    A **--dry-run** will report any "hard" issues resulting from
    missing local files or records in MongoDB. It **can not**
    guarantee semantic correctness of the metadata; should you wish to
    inspect the state of the staged data that would be written to the
    archive, pass **--dump-stage** in addition to **--dry-run**.

    This will print a condensed version of the table to the terminal
    and write a CSV file to the current directory called
    *staging_debug_\\<NOW\\>.csv*, where *\\<NOW\\>* is the current
    date and time.


    Parameters
    ----------
    database : str
        Name of the MongoDB database holding the item metadata.

    collection : str
        Name of the collection within the database. Each document in
        the collection is expected to correspond to one item in the
        batch.

    file_path : pathlib.Path
        Path to the folder containing the raw bitstream files.

    dry_run : bool, default=False
        Validate the batch without writing any output. Checks that
        every database record has a matching file in **--file-path**
        and prints the staged metadata values to stdout. No
        directories or files are created. **Recommended before any
        full build.**

    dump_stage : bool, default=False
        Write the staged data to the terminal and a CSV file. Only
        used in conjunction with **--dry-run**.  **Note:** the
        terminal output can get very wide and long and may exceed the
        limits of your terminal window. In this case, it is
        recommended to inspect the CSV file. This file will have a
        prefix of **staging_debug_**, followed by the current
        date and time. The file will be created in the current
        directory.

    """
    if dry_run:
        richprint("Running in [bold]--dry-run[/bold] mode\n")

    if dump_stage and not dry_run:
        richprint("[yellow][bold]--dump-stage[/bold] has no effect without [bold]--dry-run[/bold][/yellow]\n")

    batch_gen = BatchGenerator(
        db_name=database,
        collection_name=collection,
        files_folder_path=file_path,
    )

    check = batch_gen.check_files_directory()
    folder_missing = check.get("folder_missing_list", [])
    mongo_missing = check.get("mongo_missing_list", [])

    if mongo_missing:
        items = [f"- {item}" for item in sorted(mongo_missing)]
        items = "\n".join(items)

        richprint(Panel(
            items,
            title="[bold yellow]WARN:[/bold yellow] Local files without a matching bitstream in MongoDB",
            subtitle="These items will be skipped - the batch [bold]can still proceed[/bold].",
            title_align="left",
            subtitle_align="left",
            expand=True
        ))
        richprint()

    if folder_missing:
        items = [f"- {item}" for item in sorted(folder_missing)]
        items = "\n".join(items)
        richprint(Panel(
            items,
            title="[bold red]ERR:[/bold red] Files missing from folder",
            subtitle="Add the missing files to [bold]--file-path[/bold] before running the batch.",
            title_align="left",
            subtitle_align="left",
            expand=True
        ))
        richprint("\n[bold red]Checks failed - batch cannot proceed until the above files are present.[/bold red]")
        return 1

    if not dry_run:
        result = batch_gen.create_batch_dir()
        richprint(f"[bold green]SUCCESS: batch has been created in {result.resolve()}.[/bold green]")
        return 0

    if dump_stage:
        stage = batch_gen.staged_data()
        richprint("[bold]Staged data:[/bold]")
        pprint(stage)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        stage.write_csv(file=f"./staging_debug_{ts}.csv")

    if mongo_missing:
        richprint(f"[yellow]{len(mongo_missing)} record(s) have no match in MongoDB and will be skipped[/yellow]")

    richprint("\n[bold green]Checks complete - no blocking issues found.[/bold green]")

    richprint("\nNo files were written due to [bold]--dry-run[/bold].")
    return 0


if __name__ == '__main__':
    app()
