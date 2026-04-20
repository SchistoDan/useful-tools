from pathlib import Path
import argparse


def rename_underscores(root: Path, old: str, new: str, dry_run: bool = False):
    """Rename files and directories, replacing old delimiter with new."""
    # Rename files first
    for f in root.rglob("*"):
        if f.is_file():
            parent = f.parent.name
            new_parent = parent.replace(old, new)
            if parent in f.name:
                new_name = f.name.replace(parent, new_parent)
                new_path = f.with_name(new_name)
                if new_path != f:
                    if dry_run:
                        print(f"[FILE] {f} -> {new_path}")
                    else:
                        f.rename(new_path)

    # Rename directories (bottom-up)
    for d in sorted(root.rglob("*"), reverse=True):
        if d.is_dir():
            new_name = d.name.replace(old, new)
            if new_name != d.name:
                new_path = d.with_name(new_name)
                if dry_run:
                    print(f"[DIR ] {d} -> {new_path}")
                else:
                    d.rename(new_path)


def rename_from_file_list(file_list: Path, old: str, new: str, dry_run: bool = False):
    """Rename specific files listed one-per-line in a text file."""
    with file_list.open() as fh:
        paths = [Path(line.strip()) for line in fh if line.strip()]

    for f in paths:
        if not f.exists():
            print(f"[WARN] Not found, skipping: {f}")
            continue
        if not f.is_file():
            print(f"[WARN] Not a file, skipping: {f}")
            continue

        new_name = f.name.replace(old, new)
        new_path = f.with_name(new_name)
        if new_path != f:
            if dry_run:
                print(f"[FILE] {f} -> {new_path}")
            else:
                f.rename(new_path)
        else:
            print(f"[SKIP] No rename needed: {f}")


def main():
    parser = argparse.ArgumentParser(
        description="Replace a delimiter in file/dir names."
    )

    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--input-dir", "-d",
        type=Path,
        help="Root directory to recurse through.",
    )
    source.add_argument(
        "--file-list", "-f",
        type=Path,
        help="Text file with one file path per line.",
    )

    parser.add_argument(
        "--old-delim", "-o",
        default="_",
        help="Delimiter to replace (default: '_').",
    )
    parser.add_argument(
        "--new-delim", "-e",
        default="-",
        help="Replacement delimiter (default: '-').",
    )
    parser.add_argument(
        "--dry-run", "-n",
        action="store_true",
        help="Print planned renames without making changes.",
    )

    args = parser.parse_args()

    if args.old_delim == args.new_delim:
        parser.error("--old-delim and --new-delim are the same — nothing to do.")

    if args.input_dir:
        if not args.input_dir.is_dir():
            parser.error(f"Not a directory: {args.input_dir}")
        rename_underscores(args.input_dir, args.old_delim, args.new_delim, dry_run=args.dry_run)

    elif args.file_list:
        if not args.file_list.is_file():
            parser.error(f"File list not found: {args.file_list}")
        rename_from_file_list(args.file_list, args.old_delim, args.new_delim, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
