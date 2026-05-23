#!/usr/bin/env python3
"""Generate nascar_tracks_groupings.csv from FRCS.pro track page markdown or URL.

Parses per-track sections (Accupredict + NASCAR shape + NASCAR size) and emits a CSV
compatible with script/import-track-groupings.

Usage:
  ./script/generate-track-groupings-csv.py [--input PATH | --url] [--output PATH]
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
import urllib.request
from pathlib import Path

FRCS_TRACKS_URL = "https://frcs.pro/nascar/tracks/"
EXPECTED_TRACK_COUNT = 36

TRACK_HEADER_RE = re.compile(r"^#####\s+(.+?)\s*$", re.MULTILINE)

# Normalize scraped labels to canonical track_type names used by import-track-groupings.
ACCUPREDICT_NORMALIZE: dict[str, str] = {
    "flat": "Flat",
    "flat tracks": "Flat",
    "large oval": "Large Oval",
    "large oval tracks": "Large Oval",
    "restrictor plate": "Restrictor Plate",
    "restrictor plate / tapered spacer": "Restrictor Plate",
    "road course": "Road Course",
    "road courses": "Road Course",
    "steep": "Steep",
    "steep tracks": "Steep",
}

SHAPE_NORMALIZE: dict[str, str] = {
    "d-shaped oval": "D-Shaped Oval",
    "egg-shaped oval": "Egg-shaped Oval",
    "paper clip oval": "Paper Clip Oval",
    "paperclip oval": "Paper Clip Oval",
    "quad-oval": "Quad-oval",
    "rounded-off rectangle": "Rounded-off Rectangle",
    "rounded-off triangle": "Rounded-off Triangle",
    "stadium oval": "Stadium Oval",
    "tri-oval": "Tri-oval",
    "road course": "Road Course",
}

SIZE_NORMALIZE: dict[str, str] = {
    "intermediate": "Intermediate",
    "intermediate tracks": "Intermediate",
    "mile oval": "Mile Oval",
    "mile-oval": "Mile Oval",
    "mile-oval tracks": "Mile Oval",
    "short track": "Short Track",
    "short tracks": "Short Track",
    "street circuit": "Street Circuit",
    "superspeedway": "Superspeedway",
    "superspeedways": "Superspeedway",
    "road course": "Road Course",
}

# FRCS page names -> nascar.tracks.name (Cup importer / schedule canonical names).
TRACK_NAME_ALIASES: dict[str, str] = {
    "Chicago Street Circuit": "Chicago Street Race",
    "Charlotte Motor Speedway Road Course": "Charlotte Motor Speedway (Road Course)",
    "Autodromo Hermanos Rodriguez": "Autódromo Hermanos Rodríguez",
}


def _normalize(label: str, mapping: dict[str, str]) -> str:
    key = label.strip().lower()
    if key in mapping:
        return mapping[key]
    # Title-case fallback for unknown labels.
    return label.strip()


def _fetch_url(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "fantasy-sports-track-groupings/1.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _known_values(mapping: dict[str, str]) -> set[str]:
    return set(mapping.values())


def _extract_label(
    section: str,
    track_name: str,
    mapping: dict[str, str],
) -> str | None:
    """Return the standalone grouping label from a per-track subsection.

    Only accepts lines that normalize to a known FRCS grouping name so we do
    not confuse duplicate track-name lines or city/state locations for labels.
    """
    header = TRACK_HEADER_RE.search(section)
    if not header:
        return None
    body = section[header.end() :]
    known = _known_values(mapping)
    track_lower = track_name.lower()

    for line in body.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("*"):
            continue
        if stripped.lower() == track_lower:
            continue
        if stripped.endswith("Track Group") or stripped.endswith("Tracks"):
            continue
        if stripped.endswith(" Track") and stripped[:-6].strip().lower() in mapping:
            stripped = stripped[:-6].strip()
        if "Track Groupings" in stripped:
            continue
        if re.match(r"^[A-Za-z].*,", stripped):
            continue
        if re.match(r"^\d+(\.\d+)?\s+miles?$", stripped, re.I):
            continue
        if len(stripped) > 60:
            continue
        if ":" in stripped and not stripped.startswith("Turn"):
            continue

        normalized = _normalize(stripped, mapping)
        if normalized in known:
            return normalized
    return None


def _parse_track_block(block: str) -> dict[str, str] | None:
    """Parse one track's Accupredict + Shape + Size block."""
    header = TRACK_HEADER_RE.search(block)
    if not header:
        return None
    track_name = header.group(1).strip()
    track_name = TRACK_NAME_ALIASES.get(track_name, track_name)

    shape_split = re.split(r"NASCAR 'Shape' Groupings", block, maxsplit=1)
    if len(shape_split) < 2:
        return None
    accu_part, rest = shape_split[0], shape_split[1]

    size_split = re.split(r"NASCAR 'Size' Groupings", rest, maxsplit=1)
    if len(size_split) < 2:
        return None
    shape_part, size_part = size_split[0], size_split[1]

    accu_label = _extract_label(accu_part, track_name, ACCUPREDICT_NORMALIZE)
    shape_label = _extract_label(shape_part, track_name, SHAPE_NORMALIZE)
    size_label = _extract_label(size_part, track_name, SIZE_NORMALIZE)
    if not accu_label or not shape_label or not size_label:
        return None

    return {
        "track_name": track_name,
        "accupredict_track_grouping": _normalize(accu_label, ACCUPREDICT_NORMALIZE),
        "nascar_shape_grouping": _normalize(shape_label, SHAPE_NORMALIZE),
        "nascar_size_grouping": _normalize(size_label, SIZE_NORMALIZE),
    }


def parse_frcs_markdown(content: str) -> list[dict[str, str]]:
    """Parse FRCS tracks page content into grouping rows."""
    # Each track section starts with "Accupredict Track Groupings" (skip page intro).
    parts = re.split(r"\nAccupredict Track Groupings\n", content)
    rows: list[dict[str, str]] = []
    seen: set[str] = set()

    for part in parts[1:]:
        row = _parse_track_block("Accupredict Track Groupings\n" + part)
        if row is None:
            continue
        if row["track_name"] in seen:
            continue
        seen.add(row["track_name"])
        rows.append(row)

    rows.sort(key=lambda r: r["track_name"])
    return rows


def write_csv(rows: list[dict[str, str]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "track_name",
        "accupredict_track_grouping",
        "nascar_shape_grouping",
        "nascar_size_grouping",
    ]
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    default_out = root / "data" / "nascar_tracks_groupings.csv"
    default_in = Path("/home/jackalope/.cursor/projects/home-jackalope-bin/uploads/tracks-0.md")

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, help="FRCS page markdown file")
    parser.add_argument("--url", action="store_true", help="Fetch from FRCS tracks URL")
    parser.add_argument("--output", type=Path, default=default_out, help="Output CSV path")
    args = parser.parse_args()

    if args.url:
        content = _fetch_url(FRCS_TRACKS_URL)
    elif args.input:
        content = args.input.read_text(encoding="utf-8")
    elif default_in.is_file():
        content = default_in.read_text(encoding="utf-8")
    else:
        print("Provide --input PATH or --url", file=sys.stderr)
        return 1

    rows = parse_frcs_markdown(content)
    if len(rows) != EXPECTED_TRACK_COUNT:
        print(
            f"Expected {EXPECTED_TRACK_COUNT} tracks, parsed {len(rows)}",
            file=sys.stderr,
        )
        for row in rows:
            print(f"  {row['track_name']}", file=sys.stderr)
        return 1

    for row in rows:
        for key in (
            "accupredict_track_grouping",
            "nascar_shape_grouping",
            "nascar_size_grouping",
        ):
            if not row[key].strip():
                print(f"Missing {key} for {row['track_name']}", file=sys.stderr)
                return 1

    write_csv(rows, args.output)
    print(f"Wrote {len(rows)} tracks to {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
