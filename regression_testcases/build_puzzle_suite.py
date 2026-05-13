import argparse
import csv
import random
from pathlib import Path


KEEP_FIELDS = [
    "PuzzleId",
    "FEN",
    "Moves",
    "Rating",
    "RatingDeviation",
    "Popularity",
    "NbPlays",
    "Themes",
    "OpeningTags",
]


def parse_tags(value: str) -> set[str]:
    if not value:
        return set()

    return {
        tag.strip()
        for tag in value.split(",")
        if tag.strip()
    }


def load_puzzles(path: Path):
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def accept_row(
    row: dict,
    *,
    include_tags: set[str],
    exclude_tags: set[str],
    min_rating: int,
    max_rating: int,
    max_rd: int,
    min_popularity: int,
    min_nb_plays: int,
) -> bool:
    rating = int(row["Rating"])
    rd = int(row["RatingDeviation"])
    popularity = int(row["Popularity"])
    nb_plays = int(row["NbPlays"])
    themes = set(row["Themes"].split())

    if not (min_rating <= rating <= max_rating):
        return False

    if rd > max_rd:
        return False

    if popularity < min_popularity:
        return False

    if nb_plays < min_nb_plays:
        return False

    if include_tags and not themes.intersection(include_tags):
        return False

    if exclude_tags and themes.intersection(exclude_tags):
        return False

    return True


def reservoir_sample(iterator, count: int, rng: random.Random):
    sample = []
    seen = 0

    for row in iterator:
        seen += 1

        if len(sample) < count:
            sample.append(row)
            continue

        j = rng.randint(0, seen - 1)

        if j < count:
            sample[j] = row

    if len(sample) < count:
        raise RuntimeError(f"only {len(sample)} rows available, need {count}")

    return sample


def write_csv(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=KEEP_FIELDS)
        writer.writeheader()

        for row in rows:
            writer.writerow({field: row.get(field, "") for field in KEEP_FIELDS})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--puzzle-file", required=True)
    parser.add_argument("--include-tags", type=str, default="")
    parser.add_argument("--exclude-tags", type=str, default="veryLong,mateIn1")
    parser.add_argument("--count", type=int, required=True)
    parser.add_argument("--out", required=True)

    parser.add_argument("--min-rating", type=int, default=800)
    parser.add_argument("--max-rating", type=int, default=2400)
    parser.add_argument("--max-rd", type=int, default=150)
    parser.add_argument("--min-popularity", type=int, default=60)
    parser.add_argument("--min-nb-plays", type=int, default=30)
    parser.add_argument("--seed", type=int, default=20260510)

    args = parser.parse_args()

    puzzle_file = Path(args.puzzle_file)
    out_path = Path(args.out)

    include_tags = parse_tags(args.include_tags)
    exclude_tags = parse_tags(args.exclude_tags)

    rng = random.Random(args.seed)

    filtered_rows = (
        row for row in load_puzzles(puzzle_file)
        if accept_row(
            row,
            include_tags=include_tags,
            exclude_tags=exclude_tags,
            min_rating=args.min_rating,
            max_rating=args.max_rating,
            max_rd=args.max_rd,
            min_popularity=args.min_popularity,
            min_nb_plays=args.min_nb_plays,
        )
    )

    sample = reservoir_sample(filtered_rows, args.count, rng)
    write_csv(out_path, sample)

    print(f"wrote {out_path}: {len(sample)} puzzles")


if __name__ == "__main__":
    main()