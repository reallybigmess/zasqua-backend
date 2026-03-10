#!/usr/bin/env python3
"""
Generate a volume manifest CSV for ingest_dropbox_volumes.py.

Default mode: parse an inventory JSON to extract volume directories and
image counts. The inventory JSON is produced by the zasqua-preservation
backup audit tool (zasqua-preservation/inventory/output/copia-seguridad-ahrb.json).

Legacy mode (--legacy): scan rclone remote or local filesystem.

Output: CSV with columns fond,volume,image_dir,image_count

Usage (inventory JSON, recommended):
    python generate_volume_manifest.py \\
      --inventory ../zasqua-preservation/inventory/output/copia-seguridad-ahrb.json \\
      --output volumes.csv

    # With AHT exclusion (exclude already-tiled volumes):
    python generate_volume_manifest.py \\
      --inventory ../zasqua-preservation/inventory/output/copia-seguridad-ahrb.json \\
      --exclude-tiled \\
      --output volumes.csv

Usage (legacy rclone, for backward compatibility):
    python generate_volume_manifest.py \\
      --legacy \\
      --root "dropbox:/Archivos Comunes/Imagenes/Copia seguridad AHRB" \\
      --fonds AHRB_AHT AHRB_N1 AHRB_N2 AHRB_NVL \\
      --output volumes.csv
"""

import argparse
import csv
import json
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: The 5 in-scope fonds for the AHRB ingest.
INSCOPE_FONDS = [
    "AHRB_AHT",
    "AHRB_Cabildos",
    "AHRB_N1",
    "AHRB_N2",
    "AHRB_NVL",
]


# ---------------------------------------------------------------------------
# JSON-based manifest generation (new default mode)
# ---------------------------------------------------------------------------

def extract_volumes_from_inventory(inventory, fonds=None):
    """Extract volume information from an inventory JSON dict.

    Iterates over the ``images`` array and groups image paths by
    fond/volume directory. Only paths containing ``/proc/recortadas/``
    are counted toward image_count.

    Args:
        inventory: dict with ``images`` list (each item has a ``path`` key).
        fonds: list of fond codes to include. Defaults to INSCOPE_FONDS.

    Returns:
        List of dicts, each with keys:
            fond, volume, image_dir, image_count
        Sorted by fond (INSCOPE_FONDS order) then by volume (natural sort).
    """
    if fonds is None:
        fonds = INSCOPE_FONDS

    fonds_set = set(fonds)

    # Map: (fond, volume_dir) -> image count (recortadas only)
    image_counts = defaultdict(int)
    # Map: (fond, volume_dir) -> volume suffix
    volume_suffixes = {}

    for entry in inventory.get("images", []):
        path = entry.get("path", "")
        parts = path.split("/")

        # Expected: "Copia seguridad AHRB/{fond}/{fond}_{vol}/proc/recortadas/..."
        if len(parts) < 6:
            continue

        fond = parts[1]
        if fond not in fonds_set:
            continue

        vol_dir = parts[2]  # e.g. AHRB_AHT_003 or AHRB_Cabildos_unico

        # Extract volume suffix: everything after "{fond}_"
        prefix = fond + "_"
        if not vol_dir.startswith(prefix):
            continue

        volume = vol_dir[len(prefix):]  # e.g. "003", "unico", "024bis"
        key = (fond, vol_dir)
        volume_suffixes[key] = volume

        # Only count recortadas images
        if "/proc/recortadas/" in path:
            image_counts[key] += 1

    # Build result list
    result = []
    for (fond, vol_dir), volume in volume_suffixes.items():
        image_dir = f"{fond}/{vol_dir}/proc/recortadas"
        result.append({
            "fond": fond,
            "volume": volume,
            "image_dir": image_dir,
            "image_count": image_counts.get((fond, vol_dir), 0),
        })

    # Sort: by fond order (INSCOPE_FONDS), then natural sort on volume
    fond_order = {f: i for i, f in enumerate(fonds)}

    def sort_key(v):
        fond_idx = fond_order.get(v["fond"], 999)
        return (fond_idx, _natural_sort_key(v["volume"]))

    result.sort(key=sort_key)
    return result


def _natural_sort_key(s):
    """Return a sort key that sorts numeric parts numerically.

    e.g. "024bis" sorts after "024" and before "025".
    """
    parts = re.split(r"(\d+)", s)
    return [int(p) if p.isdigit() else p.lower() for p in parts]


def filter_aht_exclusions(volumes, r2_dirs):
    """Remove AHT volumes that already have tiles on R2.

    An AHT volume is considered tiled if any R2 directory name starts with
    ``co-ahrb-aht-{volume}-`` (document-level tile slugs).

    Args:
        volumes: list of volume dicts (fond, volume, image_dir, image_count).
        r2_dirs: list of directory names/paths from rclone lsf.

    Returns:
        (filtered_volumes, excluded_volumes) tuple where excluded_volumes
        is a list of volume strings that were removed.
    """
    # Build set of tiled AHT volume numbers from R2 dirs
    tiled_aht = set()
    # R2 dirs look like "co-ahrb-aht-003-0001/" or "co-ahrb-aht-003-0001"
    aht_pattern = re.compile(r"^co-ahrb-aht-(\d+[a-z]*)-\d+")
    for d in r2_dirs:
        d_clean = d.rstrip("/").split("/")[-1]  # strip path and trailing slash
        m = aht_pattern.match(d_clean)
        if m:
            tiled_aht.add(m.group(1))

    excluded = []
    filtered = []
    for v in volumes:
        if v["fond"] == "AHRB_AHT" and v["volume"] in tiled_aht:
            excluded.append(v["volume"])
        else:
            filtered.append(v)

    return filtered, sorted(excluded)


def query_r2_dirs(r2_remote):
    """Query R2 for tile directory names.

    Args:
        r2_remote: rclone remote path (e.g. ``r2:zasqua-iiif-tiles``).

    Returns:
        List of directory name strings, or empty list on failure.
    """
    cmd = ["rclone", "lsf", r2_remote, "--dirs-only"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(
            f"Warning: rclone lsf failed for {r2_remote}: {result.stderr.strip()}",
            file=sys.stderr,
        )
        return []
    return [line.strip() for line in result.stdout.strip().split("\n") if line.strip()]


# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------

def write_manifest_csv(volumes, output):
    """Write volumes to a CSV with columns fond,volume,image_dir,image_count.

    Args:
        volumes: list of dicts with fond, volume, image_dir, image_count.
        output: file-like object or path string. Use '-' for stdout.
    """
    fieldnames = ["fond", "volume", "image_dir", "image_count"]

    if hasattr(output, "write"):
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(volumes)
    elif output == "-":
        writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(volumes)
    else:
        with open(output, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(volumes)
        print(f"Written to {output}", file=sys.stderr)


def print_summary(volumes, excluded=None, file=sys.stderr):
    """Print per-fond counts to stderr."""
    by_fond = defaultdict(list)
    for v in volumes:
        by_fond[v["fond"]].append(v)

    print("\nVolume manifest summary:", file=file)
    total_volumes = 0
    total_images = 0
    for fond in INSCOPE_FONDS:
        vols = by_fond.get(fond, [])
        n_vols = len(vols)
        n_imgs = sum(v["image_count"] for v in vols)
        total_volumes += n_vols
        total_images += n_imgs
        print(f"  {fond}: {n_vols} volumes, {n_imgs} images", file=file)

    print(f"\nTotal: {total_volumes} volumes, {total_images} images", file=file)

    if excluded:
        print(
            f"\nExcluded AHT volumes (already tiled): {len(excluded)}",
            file=file,
        )
        for vol in excluded:
            print(f"  AHRB_AHT_{vol}", file=file)


# ---------------------------------------------------------------------------
# Legacy rclone-based functions (kept for --legacy backward compatibility)
# ---------------------------------------------------------------------------

def list_volumes_rclone(root, fond):
    """List volume directories from an rclone remote.

    Expects directories named like AHRB_AHT_003, AHRB_N1_001, etc.

    Returns:
        List of (fond, volume_number, image_dir) tuples, sorted.
    """
    remote_path = f"{root}/{fond}/"
    cmd = ["rclone", "lsd", remote_path]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(
            f"Warning: rclone lsd failed for {remote_path}: "
            f"{result.stderr.strip()}",
            file=sys.stderr,
        )
        return []

    volumes = []
    pattern = re.compile(rf"^{re.escape(fond)}_(\d+)$")

    for line in result.stdout.strip().split("\n"):
        if not line.strip():
            continue
        parts = line.strip().split()
        if len(parts) < 5:
            continue
        dirname = parts[-1]
        match = pattern.match(dirname)
        if match:
            vol_num = match.group(1)
            image_dir = f"{fond}/{dirname}/proc/recortadas"
            volumes.append((fond, vol_num, image_dir))

    return sorted(volumes, key=lambda x: x[1])


def list_volumes_local(root, fond):
    """List volume directories from local filesystem.

    Returns:
        List of (fond, volume_number, image_dir) tuples, sorted.
    """
    fond_dir = Path(root) / fond
    if not fond_dir.is_dir():
        print(f"Warning: directory not found: {fond_dir}", file=sys.stderr)
        return []

    volumes = []
    pattern = re.compile(rf"^{re.escape(fond)}_(\d+)$")

    for entry in sorted(fond_dir.iterdir()):
        if not entry.is_dir():
            continue
        match = pattern.match(entry.name)
        if match:
            vol_num = match.group(1)
            image_dir = f"{fond}/{entry.name}/proc/recortadas"
            volumes.append((fond, vol_num, image_dir))

    return sorted(volumes, key=lambda x: x[1])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate volume manifest CSV for Dropbox ingest",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Primary mode: inventory JSON
    parser.add_argument(
        "--inventory",
        help="Path to inventory JSON (copia-seguridad-ahrb.json)",
    )
    parser.add_argument(
        "--fonds",
        nargs="+",
        default=INSCOPE_FONDS,
        help=f"Fond codes to include (default: {' '.join(INSCOPE_FONDS)})",
    )
    parser.add_argument(
        "--output",
        default="-",
        help="Output CSV path (default: stdout)",
    )
    parser.add_argument(
        "--exclude-tiled",
        action="store_true",
        help="Exclude already-tiled AHT volumes by querying R2",
    )
    parser.add_argument(
        "--r2-remote",
        default="r2:zasqua-iiif-tiles",
        help="R2 remote for AHT exclusion check (default: r2:zasqua-iiif-tiles)",
    )

    # Legacy mode
    parser.add_argument(
        "--legacy",
        action="store_true",
        help="Use legacy rclone-based scanning instead of inventory JSON",
    )
    parser.add_argument(
        "--root",
        help="[Legacy] Root path (rclone remote or local directory)",
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="[Legacy] Scan local filesystem instead of rclone remote",
    )

    args = parser.parse_args()

    if args.legacy:
        # Legacy mode: rclone scan
        if not args.root:
            parser.error("--root is required in legacy mode")
        all_volumes = []
        for fond in args.fonds:
            if args.local:
                vols = list_volumes_local(args.root, fond)
            else:
                vols = list_volumes_rclone(args.root.rstrip("/"), fond)
            all_volumes.extend(vols)
            print(f"  {fond}: {len(vols)} volumes", file=sys.stderr)

        print(f"\nTotal: {len(all_volumes)} volumes", file=sys.stderr)

        # Write legacy CSV (no image_count)
        if args.output == "-":
            writer = csv.writer(sys.stdout)
            writer.writerow(["fond", "volume", "image_dir"])
            for fond, vol_num, image_dir in all_volumes:
                writer.writerow([fond, vol_num, image_dir])
        else:
            with open(args.output, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["fond", "volume", "image_dir"])
                for fond, vol_num, image_dir in all_volumes:
                    writer.writerow([fond, vol_num, image_dir])
            print(f"Written to {args.output}", file=sys.stderr)
        return

    # Primary mode: inventory JSON
    if not args.inventory:
        parser.error("--inventory is required (or use --legacy for rclone scanning)")

    print(f"Loading inventory: {args.inventory}", file=sys.stderr)
    # NOTE: The inventory JSON is ~350 MB / 5.3M lines with 894K image entries.
    # json.load() works fine on a machine with sufficient RAM (needs ~1-2 GB).
    # If memory is a concern, consider ijson for streaming, but json.load is simpler.
    with open(args.inventory) as f:
        inventory = json.load(f)

    print("Extracting volumes...", file=sys.stderr)
    volumes = extract_volumes_from_inventory(inventory, fonds=args.fonds)

    excluded = []
    if args.exclude_tiled:
        print(f"Querying R2 for tiled AHT volumes: {args.r2_remote}", file=sys.stderr)
        r2_dirs = query_r2_dirs(args.r2_remote)
        volumes, excluded = filter_aht_exclusions(volumes, r2_dirs)

    print_summary(volumes, excluded)

    write_manifest_csv(volumes, args.output)


if __name__ == "__main__":
    main()
