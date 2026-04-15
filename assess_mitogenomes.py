#!/usr/bin/env python3
"""
Mitochondrial genome assembly quality assessment script, built for skim2mito.

Recursively finds all summary_contigs_mqc.txt files under one or more given
input directories, combines them, checks for duplicate sample IDs across files
(warns and exits if found), then produces:
  - A text quality report
  - A per-sample summary TSV
"""

import argparse
import csv
import sys
from collections import Counter, defaultdict
from pathlib import Path


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TARGET_FILENAME = "summary_contigs_mqc.txt"

LENGTH_THRESHOLDS_KB = [1, 3, 5, 8, 10, 12, 15]
LENGTH_THRESHOLDS_BP = [t * 1000 for t in LENGTH_THRESHOLDS_KB]

KEY_GENES = {
    "cox1", "cox2", "cox3",
    "atp6", "atp8",
    "cob",
    "nad1", "nad2", "nad3", "nad4", "nad4l", "nad5", "nad6",
    "rrnL", "rrnS",
}


# ---------------------------------------------------------------------------
# File discovery & parsing
# ---------------------------------------------------------------------------

def find_input_files(roots: list[Path]) -> list:
    """Recursively find all TARGET_FILENAME files under each root directory."""
    files = []
    for root in roots:
        files.extend(sorted(root.rglob(TARGET_FILENAME)))
    if not files:
        dirs_str = ", ".join(str(r) for r in roots)
        print(
            f"[ERROR] No '{TARGET_FILENAME}' files found under: {dirs_str}",
            file=sys.stderr,
        )
        sys.exit(1)
    return sorted(set(files))  # deduplicate in case of overlapping paths


def is_fail_row(row: dict) -> bool:
    """
    Detect failed assembly rows: Contig field is 'NA' or missing/empty,
    meaning the row has no real contig data.
    """
    contig = row.get("Contig", "").strip()
    return contig in ("", "NA") or contig is None


def parse_file(filepath: Path) -> tuple:
    """
    Parse a single summary_contigs_mqc.txt file.
    Returns (contig_rows, fail_ids) where contig_rows have a '_source_file'
    key injected for traceability.
    """
    contig_rows = []
    fail_ids = []
    seen_fail = set()

    with open(filepath, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            sid = row.get("ID", "").strip()
            row["_source_file"] = str(filepath)
            if is_fail_row(row):
                if sid and sid not in seen_fail:
                    fail_ids.append(sid)
                    seen_fail.add(sid)
            else:
                contig_rows.append(row)

    return contig_rows, fail_ids


def load_all_files(files: list) -> tuple:
    """
    Load and combine all input files.
    Checks for duplicate sample IDs across files and exits with an error if
    any are found. Duplicates within the same file (multi-contig samples)
    are expected and fine.

    Returns:
        all_contig_rows : combined list of all contig-level rows
        all_fail_ids    : deduplicated list of failed sample IDs
        files           : the list of files that were loaded
    """
    all_contig_rows = []
    all_fail_ids = []

    # Track which file(s) each sample ID appears in
    id_to_files = defaultdict(set)

    for fp in files:
        contig_rows, fail_ids = parse_file(fp)
        for row in contig_rows:
            sid = row.get("ID", "").strip()
            id_to_files[sid].add(str(fp))
        for sid in fail_ids:
            id_to_files[sid].add(str(fp))
        all_contig_rows.extend(contig_rows)
        all_fail_ids.extend(fail_ids)

    # Detect cross-file duplicates
    duplicates = {sid: fps for sid, fps in id_to_files.items() if len(fps) > 1}
    if duplicates:
        print(
            "[ERROR] Duplicate sample IDs found across multiple input files. "
            "Please resolve before re-running.\n",
            file=sys.stderr,
        )
        for sid, fps in sorted(duplicates.items()):
            print(f"  ID '{sid}' appears in:", file=sys.stderr)
            for fp in sorted(fps):
                print(f"    {fp}", file=sys.stderr)
        sys.exit(1)

    # Deduplicate fail IDs
    seen = set()
    deduped_fails = []
    for sid in all_fail_ids:
        if sid not in seen:
            deduped_fails.append(sid)
            seen.add(sid)

    return all_contig_rows, deduped_fails, files


# ---------------------------------------------------------------------------
# Data grouping
# ---------------------------------------------------------------------------

def group_samples(contig_rows: list) -> dict:
    """Group contig rows by sample ID."""
    samples = defaultdict(list)
    for row in contig_rows:
        sid = row.get("ID", "").strip()
        if sid:
            samples[sid].append(row)
    return dict(samples)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def safe_float(val, default=0.0):
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def safe_int(val, default=0):
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default


def is_circular(contig_name: str) -> bool:
    return "_circular" in contig_name.lower()


def sample_total_length(contigs: list) -> float:
    return sum(safe_float(c.get("Length", 0)) for c in contigs)


def sample_genes(contigs: list) -> set:
    genes = set()
    for c in contigs:
        gl = c.get("Genes list", "").strip()
        if gl and gl != "NA":
            for g in gl.split(","):
                g = g.strip()
                # normalise rrnL_0 / rrnL_1 -> rrnL
                if "_" in g and not g.startswith("nad"):
                    g = g.rsplit("_", 1)[0]
                if g:
                    genes.add(g)
    return genes


def sample_has_cox1(contigs: list) -> bool:
    return any(safe_int(c.get("Cox1", 0)) == 1 for c in contigs)


def sample_circular(contigs: list) -> bool:
    return any(is_circular(c.get("Contig", "")) for c in contigs)


def flag_non_insecta(contigs: list) -> list:
    """Return contig rows where Class != 'Insecta' (including no-hit)."""
    return [c for c in contigs if c.get("Class", "").strip() != "Insecta"]


# ---------------------------------------------------------------------------
# Report helpers
# ---------------------------------------------------------------------------

def section(title: str, width: int = 70) -> str:
    bar = "=" * width
    return f"\n{bar}\n  {title}\n{bar}"


def subsection(title: str, width: int = 70) -> str:
    return f"\n  {'-' * (width - 2)}\n  {title}\n  {'-' * (width - 2)}"


# ---------------------------------------------------------------------------
# Main analysis
# ---------------------------------------------------------------------------

def analyse(input_dirs, files, samples, fail_ids):

    lines = []
    summary_rows = []

    # -----------------------------------------------------------------------
    # Header
    # -----------------------------------------------------------------------
    lines.append(section("MITOGENOME ASSEMBLY QUALITY REPORT"))
    lines.append(f"  Input directories  : {len(input_dirs)}")
    for d in input_dirs:
        lines.append(f"    {d}")
    lines.append(f"  Files found        : {len(files)}")
    for fp in files:
        lines.append(f"    {fp}")
    lines.append(f"  Samples (non-fail) : {len(samples)}")
    lines.append(f"  Failed assemblies  : {len(fail_ids)}")
    if fail_ids:
        lines.append(f"  Failed IDs : {', '.join(fail_ids)}")

    # -----------------------------------------------------------------------
    # 1. Contig count distribution
    # -----------------------------------------------------------------------
    lines.append(section("1. CONTIG COUNT DISTRIBUTION"))

    bucket_ids = defaultdict(list)
    for sid, ctgs in samples.items():
        n = len(ctgs)
        key = str(n) if n <= 9 else "10+"
        bucket_ids[key].append(sid)

    for n in range(1, 10):
        key = str(n)
        ids = bucket_ids.get(key, [])
        lines.append(f"  {key} contig(s)  : {len(ids):4d} sample(s)")
        if ids:
            lines.append(f"    IDs: {', '.join(sorted(ids))}")

    ids_10plus = bucket_ids.get("10+", [])
    lines.append(f"  10+ contigs  : {len(ids_10plus):4d} sample(s)")
    if ids_10plus:
        lines.append(f"    IDs: {', '.join(sorted(ids_10plus))}")

    # -----------------------------------------------------------------------
    # 2. Total assembly length thresholds
    # -----------------------------------------------------------------------
    lines.append(section("2. TOTAL ASSEMBLY LENGTH THRESHOLDS"))
    lines.append("  (Summed across all contigs per sample)\n")

    for thresh_bp, thresh_kb in zip(LENGTH_THRESHOLDS_BP, LENGTH_THRESHOLDS_KB):
        passing = [sid for sid, ctgs in samples.items()
                   if sample_total_length(ctgs) >= thresh_bp]
        lines.append(f"  >{thresh_kb:>2d} kb : {len(passing):4d} sample(s)")

    # -----------------------------------------------------------------------
    # 3. Contig count + length combinations
    # -----------------------------------------------------------------------
    lines.append(section("3. CONTIG COUNT + LENGTH COMBINATIONS"))

    combos = [
        ("<5 contigs", lambda n: n < 5,  ">10 kb", 10_000),
        ("<5 contigs", lambda n: n < 5,  ">15 kb", 15_000),
        ("<3 contigs", lambda n: n < 3,  ">10 kb", 10_000),
        ("<3 contigs", lambda n: n < 3,  ">15 kb", 15_000),
    ]
    for contig_label, contig_test, len_label, len_thresh in combos:
        passing = [
            sid for sid, ctgs in samples.items()
            if contig_test(len(ctgs)) and sample_total_length(ctgs) >= len_thresh
        ]
        lines.append(f"  {contig_label} AND {len_label} : {len(passing):4d} sample(s)")
        if passing:
            lines.append(f"    IDs: {', '.join(sorted(passing))}")

    # -----------------------------------------------------------------------
    # 4. Complete (circular) assemblies
    # -----------------------------------------------------------------------
    lines.append(section("4. COMPLETE (CIRCULAR) ASSEMBLIES"))

    circular_ids = sorted(sid for sid, ctgs in samples.items() if sample_circular(ctgs))
    lines.append(f"  Complete circular assemblies : {len(circular_ids)}")
    if circular_ids:
        lines.append(f"  IDs: {', '.join(circular_ids)}")

    # -----------------------------------------------------------------------
    # 5. Gene content per sample
    # -----------------------------------------------------------------------
    lines.append(section("5. GENE CONTENT PER SAMPLE"))
    lines.append(f"  Key genes tracked : {', '.join(sorted(KEY_GENES))}\n")

    gene_count_dist = defaultdict(int)

    for sid, ctgs in sorted(samples.items()):
        genes = sample_genes(ctgs)
        n_genes = len(genes)
        has_cox1 = sample_has_cox1(ctgs)
        key_present = genes & KEY_GENES
        gene_count_dist[n_genes] += 1

        lines.append(f"  {sid}")
        lines.append(f"    Unique genes      : {n_genes}")
        lines.append(f"    Cox1 present      : {'Yes' if has_cox1 else 'No'}")
        lines.append(f"    Key genes present : {', '.join(sorted(key_present)) if key_present else 'none'}")
        lines.append(f"    All genes         : {', '.join(sorted(genes)) if genes else 'none'}")

    lines.append(subsection("Gene count distribution across samples"))
    for n_g in sorted(gene_count_dist):
        lines.append(f"  {n_g:>2d} unique gene(s) : {gene_count_dist[n_g]} sample(s)")

    cox1_count = sum(1 for ctgs in samples.values() if sample_has_cox1(ctgs))
    lines.append(f"\n  Samples with Cox1 : {cox1_count} / {len(samples)}")

    # -----------------------------------------------------------------------
    # 6. Taxonomic flagging
    # -----------------------------------------------------------------------
    lines.append(section("6. TAXONOMIC ASSIGNMENT & NON-INSECTA FLAGS"))

    all_flagged_contigs = []
    samples_with_flags = []

    for sid, ctgs in sorted(samples.items()):
        flagged = flag_non_insecta(ctgs)
        if flagged:
            samples_with_flags.append(sid)
            all_flagged_contigs.extend(flagged)

    lines.append(f"  Total contigs flagged (Class != Insecta) : {len(all_flagged_contigs)}")
    lines.append(f"  Samples with >=1 flagged contig          : {len(samples_with_flags)}")

    if all_flagged_contigs:
        lines.append(subsection("Flagged contigs (Class != Insecta or no-hit)"))
        lines.append(
            f"  {'Sample':<15} {'Contig':<35} {'Length':>8} {'Coverage':>10}  "
            f"{'Kingdom':<15} {'Phylum':<18} {'Class':<18} {'Species'}"
        )
        lines.append("  " + "-" * 130)
        for c in all_flagged_contigs:
            lines.append(
                f"  {c.get('ID',''):<15} {c.get('Contig',''):<35} "
                f"{safe_float(c.get('Length', 0)):>8.0f} "
                f"{safe_float(c.get('Coverage', 0)):>10.2f}  "
                f"{c.get('Kingdom',''):<15} {c.get('Phylum',''):<18} "
                f"{c.get('Class',''):<18} {c.get('Species','')}"
            )

    # -----------------------------------------------------------------------
    # 7. Per-sample taxonomic overview
    # -----------------------------------------------------------------------
    TAX_LEVELS = ["Superkingdom", "Kingdom", "Phylum", "Class", "Order", "Family", "Species"]

    lines.append(section("7. PER-SAMPLE TAXONOMIC OVERVIEW"))
    lines.append("  (Most common assignment per level across contigs, excl. no-hit)\n")

    for sid, ctgs in sorted(samples.items()):
        lines.append(f"  {sid}  ({len(ctgs)} contig(s), total {sample_total_length(ctgs)/1000:.1f} kb)")
        for level in TAX_LEVELS:
            vals = [
                c.get(level, "").strip() for c in ctgs
                if c.get(level, "").strip() not in ("", "no-hit", "NA")
            ]
            if vals:
                most_common = Counter(vals).most_common(1)[0][0]
                unique = sorted(set(vals))
                note = f" [multiple: {', '.join(unique)}]" if len(unique) > 1 else ""
                lines.append(f"    {level:<14}: {most_common}{note}")
            else:
                lines.append(f"    {level:<14}: no-hit")

    # -----------------------------------------------------------------------
    # 8. Additional quality metrics
    # -----------------------------------------------------------------------
    lines.append(section("8. ADDITIONAL QUALITY METRICS"))

    lines.append(subsection("Mean coverage per sample (length-weighted across contigs)"))
    for sid, ctgs in sorted(samples.items()):
        total_len = sample_total_length(ctgs)
        weighted_cov = (
            sum(safe_float(c.get("Coverage", 0)) * safe_float(c.get("Length", 0)) for c in ctgs) / total_len
            if total_len > 0 else 0.0
        )
        lines.append(f"  {sid:<20} : {weighted_cov:>8.2f}x")

    lines.append(subsection("Length-weighted mean GC content per sample"))
    for sid, ctgs in sorted(samples.items()):
        total_len = sample_total_length(ctgs)
        weighted_gc = (
            sum(safe_float(c.get("GC", 0)) * safe_float(c.get("Length", 0)) for c in ctgs) / total_len
            if total_len > 0 else 0.0
        )
        lines.append(f"  {sid:<20} : {weighted_gc:.4f} ({weighted_gc*100:.2f}%)")

    lines.append(subsection("Single-contig circular assemblies (best quality)"))
    single_circular = sorted(
        sid for sid, ctgs in samples.items()
        if len(ctgs) == 1 and sample_circular(ctgs)
    )
    lines.append(f"  Count : {len(single_circular)}")
    if single_circular:
        lines.append(f"  IDs   : {', '.join(single_circular)}")

    lines.append(subsection("Samples with no genes detected"))
    no_genes = sorted(sid for sid, ctgs in samples.items() if not sample_genes(ctgs))
    lines.append(f"  Count : {len(no_genes)}")
    if no_genes:
        lines.append(f"  IDs   : {', '.join(no_genes)}")

    lines.append(subsection("Highly fragmented assemblies (>5 contigs)"))
    fragmented = sorted(sid for sid, ctgs in samples.items() if len(ctgs) > 5)
    lines.append(f"  Count : {len(fragmented)}")
    if fragmented:
        lines.append(f"  IDs   : {', '.join(fragmented)}")

    # -----------------------------------------------------------------------
    # Build summary TSV rows
    # -----------------------------------------------------------------------
    for sid, ctgs in sorted(samples.items()):
        genes = sample_genes(ctgs)
        flagged = flag_non_insecta(ctgs)
        total_len = sample_total_length(ctgs)
        weighted_cov = (
            sum(safe_float(c.get("Coverage", 0)) * safe_float(c.get("Length", 0)) for c in ctgs) / total_len
            if total_len > 0 else 0.0
        )
        weighted_gc = (
            sum(safe_float(c.get("GC", 0)) * safe_float(c.get("Length", 0)) for c in ctgs) / total_len
            if total_len > 0 else 0.0
        )
        source_file = ctgs[0].get("_source_file", "")

        summary_rows.append({
            "ID":                    sid,
            "source_file":           source_file,
            "n_contigs":             len(ctgs),
            "circular":              sample_circular(ctgs),
            "total_length_bp":       int(total_len),
            "total_length_kb":       round(total_len / 1000, 2),
            "mean_coverage_x":       round(weighted_cov, 2),
            "mean_gc":               round(weighted_gc, 4),
            "n_unique_genes":        len(genes),
            "cox1_present":          sample_has_cox1(ctgs),
            "genes":                 ",".join(sorted(genes)),
            "n_flagged_contigs":     len(flagged),
            "fail":                  False,
        })

    for sid in fail_ids:
        summary_rows.append({
            "ID":                    sid,
            "source_file":           "",
            "n_contigs":             0,
            "circular":              False,
            "total_length_bp":       0,
            "total_length_kb":       0.0,
            "mean_coverage_x":       0.0,
            "mean_gc":               0.0,
            "n_unique_genes":        0,
            "cox1_present":          False,
            "genes":                 "",
            "n_flagged_contigs":     0,
            "fail":                  True,
        })

    lines.append(f"\n{'=' * 70}\n  END OF REPORT\n{'=' * 70}\n")
    return "\n".join(lines), summary_rows


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=(
            f"Recursively find all '{TARGET_FILENAME}' files under one or more "
            "input directories, combine them, and produce a mitogenome assembly "
            "quality report."
        )
    )
    parser.add_argument(
        "input_dirs",
        nargs="+",
        help=f"One or more root directories to search for '{TARGET_FILENAME}' files",
    )
    parser.add_argument(
        "--report", default=None,
        help="Path to write the text report (default: first INPUT_DIR/mitogenome_report.txt)",
    )
    parser.add_argument(
        "--summary_tsv", default=None,
        help="Path to write the summary TSV (default: first INPUT_DIR/mitogenome_summary.tsv)",
    )
    args = parser.parse_args()

    input_dirs = [Path(d).resolve() for d in args.input_dirs]
    for d in input_dirs:
        if not d.is_dir():
            print(f"[ERROR] Not a directory: {d}", file=sys.stderr)
            sys.exit(1)

    # Default output location uses the first input directory
    report_path = Path(args.report) if args.report else input_dirs[0] / "mitogenome_report.txt"
    summary_path = Path(args.summary_tsv) if args.summary_tsv else input_dirs[0] / "mitogenome_summary.tsv"

    # Discover and load files
    files = find_input_files(input_dirs)
    print(f"[✓] Found {len(files)} file(s) under {len(input_dirs)} director(y/ies)", file=sys.stderr)
    for fp in files:
        print(f"    {fp}", file=sys.stderr)

    all_contig_rows, all_fail_ids, files = load_all_files(files)
    samples = group_samples(all_contig_rows)

    print(
        f"[✓] Loaded {len(all_contig_rows)} contig rows across "
        f"{len(samples)} samples ({len(all_fail_ids)} failed)",
        file=sys.stderr,
    )

    # Run analysis
    report_text, summary_rows = analyse(input_dirs, files, samples, all_fail_ids)

    # Print to stdout
    print(report_text)

    # Write report
    report_path.write_text(report_text, encoding="utf-8")
    print(f"[✓] Report written to     : {report_path}", file=sys.stderr)

    # Write summary TSV
    if summary_rows:
        fieldnames = list(summary_rows[0].keys())
        with open(summary_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames, delimiter="\t")
            writer.writeheader()
            writer.writerows(summary_rows)
        print(f"[✓] Summary TSV written to: {summary_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
