"""
Consolidated script for DSpace SAF batch generation.

Workflow:
    Step 1 — Compare local filenames against MongoDB bitstream names.
             If mismatches are found, suggest fuzzy-matched renames and
             offer to apply them before proceeding.
    Step 2 — Preview the staged Dublin Core metadata extracted from MongoDB.
    Step 3 — Generate the DSpace Simple Archive Format (SAF) batch directory
             and compress it into a zip named <collection>_batches.zip.

Usage:
    Interactive (prompts for each value):
        python run.py

    With arguments:
        python run.py <db_name> <collection_name> <files_folder_path>
"""

import os
os.environ["POLARS_SKIP_CPU_CHECK"] = "1"  # Required on ARM64 Windows (Qualcomm)

import pathlib
import shutil
import sys
from difflib import SequenceMatcher

from auxiliary.auth_functions import fetch_collection
from batcher import BatchGenerator
import jmespath as jp
from rich.console import Console
from rich.table import Table
from rich.prompt import Confirm


def fuzzy_score(a: str, b: str) -> float:
    """Compute a similarity ratio (0.0–1.0) between two filenames.

    Uses SequenceMatcher which compares sequences of characters and returns
    a float in [0, 1] where 1.0 means identical strings. Comparison is
    case-insensitive.

    Args:
        a: First filename.
        b: Second filename.

    Returns:
        Similarity ratio between the two strings.
    """
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def check_and_fix_filenames(
    db_name: str, collection_name: str, folder: pathlib.Path, console: Console
) -> bool:
    """Compare local filenames with MongoDB bitstream names and offer to fix mismatches.

    Fetches the list of expected bitstream filenames from MongoDB and compares
    them against the files present in the local folder. For any mismatches,
    it computes fuzzy similarity scores and presents a table of suggested
    renames. The user can accept or decline the renames.

    Args:
        db_name:         MongoDB database name.
        collection_name: MongoDB collection name.
        folder:          Path to the local folder containing bitstream files.
        console:         Rich Console instance for formatted output.

    Returns:
        True if filenames are aligned (either already matching or successfully
        renamed) and it is safe to proceed to batch generation.
        False if unresolvable mismatches remain and the user chose to abort.
    """
    console.print("\n[bold]Step 1: Checking filenames...[/]")

    data = fetch_collection(db_name=db_name, collection_name=collection_name)
    mongo_names = set(jp.search("[?bitstream != ''].bitstream", data))
    local_names = {f.name for f in folder.glob("*") if f.is_file()}

    matched = mongo_names & local_names
    only_mongo = sorted(mongo_names - matched)
    only_local = sorted(local_names - matched)

    if not only_mongo and not only_local:
        console.print("[green]All filenames match.[/]")
        return True

    # For each MongoDB name without a local match, find the best fuzzy match
    # among the unmatched local files (minimum threshold: 50%).
    suggestions: list[tuple[str, str, float]] = []
    used_local: set[str] = set()

    for mongo_name in only_mongo:
        best_match = ""
        best_score = 0.0
        for local_name in only_local:
            if local_name in used_local:
                continue
            score = fuzzy_score(mongo_name, local_name)
            if score > best_score:
                best_score = score
                best_match = local_name
        if best_match and best_score >= 0.5:
            suggestions.append((best_match, mongo_name, best_score))
            used_local.add(best_match)

    # Display fuzzy-match suggestions in a Rich table
    if suggestions:
        table = Table(title="Fuzzy-Match Suggestions", show_lines=True)
        table.add_column("Local file (current)", style="red")
        table.add_column("MongoDB bitstream (rename to)", style="green")
        table.add_column("Score", justify="right")
        for local_name, mongo_name, score in suggestions:
            table.add_row(local_name, mongo_name, f"{score:.0%}")
        console.print(table)

    # Report files that could not be fuzzy-matched at all
    suggested_local = {s[0] for s in suggestions}
    suggested_mongo = {s[1] for s in suggestions}
    remaining_local = [f for f in only_local if f not in suggested_local]
    remaining_mongo = [m for m in only_mongo if m not in suggested_mongo]

    if remaining_local:
        console.print("\n[yellow]Local files with no match in MongoDB:[/]")
        for f in remaining_local:
            console.print(f"  - {f}")

    if remaining_mongo:
        console.print("\n[yellow]MongoDB bitstreams with no local file:[/]")
        for m in remaining_mongo:
            console.print(f"  - {m}")

    if not suggestions:
        console.print("\n[red]No fuzzy matches found — manual fixes needed.[/]")
        return False

    # Apply renames if the user confirms
    if Confirm.ask("\nApply these renames?"):
        for local_name, mongo_name, _ in suggestions:
            src = folder / local_name
            dst = folder / mongo_name
            src.rename(dst)
            console.print(f"  [green]Renamed:[/] {local_name} -> {mongo_name}")
        console.print("[green]Filenames fixed.[/]")
        return True
    else:
        console.print("[yellow]Renames skipped.[/]")
        return Confirm.ask("Continue to batch generation anyway?")


def run_batch(
    db_name: str, collection_name: str, folder: pathlib.Path, console: Console
) -> None:
    """Generate a DSpace SAF batch from MongoDB metadata and local files.

    Instantiates the BatchGenerator, displays a preview of the staged Dublin
    Core metadata, then (upon confirmation) writes the SAF directory structure
    and compresses it into a zip archive named <collection_name>_batches.zip.

    Args:
        db_name:         MongoDB database name.
        collection_name: MongoDB collection name.
        folder:          Path to the local folder containing bitstream files.
        console:         Rich Console instance for formatted output.
    """
    bat_gen = BatchGenerator(
        db_name=db_name,
        collection_name=collection_name,
        files_folder_path=folder,
    )

    # Preview staged Dublin Core metadata
    console.print("\n[bold]Step 2: Staged metadata preview[/]")
    stage = bat_gen.staged_data()

    # Summary: item count and column list
    console.print(f"  Staged [cyan]{stage.shape[0]}[/] items with [cyan]{stage.shape[1]}[/] metadata fields.")
    console.print(f"  Columns: {', '.join(stage.columns)}\n")

    # Readable terminal preview: filename, title, author
    preview_cols = ["filename", "dc.title", "dc.contributor.author"]
    preview_cols = [c for c in preview_cols if c in stage.columns]
    preview_table = Table(title="Preview (filename / title / author)", show_lines=True)
    preview_table.add_column("#", justify="right", style="dim")
    preview_table.add_column("Filename", style="cyan", max_width=40)
    preview_table.add_column("Title", style="white", max_width=50)
    preview_table.add_column("Author", style="white", max_width=30)
    for i, row in enumerate(stage.select(preview_cols).iter_rows(), 1):
        preview_table.add_row(str(i), *(str(v) if v else "" for v in row))
    console.print(preview_table)

    # Save full CSV for detailed inspection
    csv_path = folder.parent / f"{collection_name}_staging_preview.csv"
    stage.write_csv(csv_path)
    console.print(f"\n  Full preview saved to: [cyan]{csv_path}[/]")

    # Ask for confirmation before writing to disk
    if not Confirm.ask("\nGenerate DSpace batch?"):
        console.print("[yellow]Aborted.[/]")
        return

    # Write the SAF directory (one sub-folder per item with XML + bitstream)
    console.print("\n[bold]Step 3: Creating batch...[/]")
    output = bat_gen.create_batch_dir()
    console.print(f"[green]Batch created at:[/] {output}")

    # Compress the batch directory into a zip for easy upload to DSpace
    zip_path = output.parent / f"{collection_name}_batches"
    shutil.make_archive(str(zip_path), "zip", output)
    console.print(f"[green]Zipped to:[/] {zip_path}.zip")


def main() -> None:
    """Entry point: collect parameters then run the full workflow.

    Accepts database name, collection name, and files folder path either
    as CLI arguments or via interactive prompts. On Windows, the interactive
    prompts flush the keyboard buffer between inputs to prevent copy-paste
    artefacts from skipping fields.
    """
    if len(sys.argv) == 4:
        db_name, collection_name, folder = sys.argv[1], sys.argv[2], pathlib.Path(sys.argv[3])
    else:
        import msvcrt

        def flush_and_ask(prompt: str) -> str:
            """Drain any buffered keystrokes then prompt for input."""
            while msvcrt.kbhit():
                msvcrt.getwch()
            return input(prompt).strip()

        db_name = flush_and_ask("Database name: ")
        collection_name = flush_and_ask("Collection name: ")
        folder = pathlib.Path(flush_and_ask("Files folder path: "))

    console = Console()

    if not folder.is_dir():
        console.print(f"[red]Folder not found:[/] {folder}")
        sys.exit(1)

    # Step 1: fix filenames → Step 2: preview → Step 3: generate batch
    if check_and_fix_filenames(db_name, collection_name, folder, console):
        run_batch(db_name, collection_name, folder, console)


if __name__ == "__main__":
    main()
