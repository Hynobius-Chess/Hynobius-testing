import argparse
import csv
import re
from pathlib import Path


DEFAULT_TEMPLATE = (
    'name="{id}" '
    'fen="{fen}" '
    'bestmove="{bestmove}" '
    'pv="{pv}" '
    'score_cp={score_cp} '
    'score_mate={score_mate} '
    'rating={rating} '
    'themes="{themes}"'
)


def extract_mate_score_from_themes(themes: str) -> str:
    match = re.search(r"\bmateIn(\d+)\b", themes)

    if not match:
        return ""

    return match.group(1)


def parse_puzzle_row(
    row: list[str],
    fixed_score_cp: str,
    fixed_score_mate: str,
    auto_mate_score: bool,
) -> dict:
    """
    預期格式：
    0 PuzzleId
    1 FEN
    2 Moves
    3 Rating
    4 RatingDeviation
    5 Popularity
    6 NbPlays
    7 Themes
    """

    if len(row) < 8:
        raise ValueError(f"Row has too few columns: {row}")

    puzzle_id = row[0].strip()
    fen = row[1].strip()
    moves = row[2].strip()
    rating = row[3].strip()
    rating_deviation = row[4].strip()
    popularity = row[5].strip()
    nb_plays = row[6].strip()
    themes = row[7].strip()

    move_list = moves.split()
    premove = move_list[0] if move_list else ""
    bestmove = move_list[1] if move_list else ""

    score_cp = fixed_score_cp
    score_mate = fixed_score_mate

    if auto_mate_score and not score_mate:
        score_mate = extract_mate_score_from_themes(themes)
        
    return {
        "id": puzzle_id,
        "name": puzzle_id,
        "fen": fen,
        "moves": moves,
        "pv": moves,
        "premove": premove,
        "bestmove": bestmove,
        "score_cp": score_cp,
        "score_mate": score_mate,
        "rating": rating,
        "rating_deviation": rating_deviation,
        "popularity": popularity,
        "nb_plays": nb_plays,
        "themes": themes,
    }


def convert_file(
    input_path: Path,
    output_path: Path,
    template: str,
    skip_header: bool,
    fixed_score_cp: str,
    fixed_score_mate: str,
    auto_mate_score: bool,
) -> None:
    output_lines: list[str] = []

    with input_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)

        for line_no, row in enumerate(reader, start=1):
            if skip_header and line_no == 1:
                continue

            if not row or all(not col.strip() for col in row):
                continue

            try:
                data = parse_puzzle_row(
                    row=row,
                    fixed_score_cp=fixed_score_cp,
                    fixed_score_mate=fixed_score_mate,
                    auto_mate_score=auto_mate_score,
                )

                output_lines.append(template.format(**data))

            except Exception as e:
                print(f"[WARN] line {line_no}: {e}")

    output_path.write_text("\n".join(output_lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)

    parser.add_argument(
        "--template",
        default=DEFAULT_TEMPLATE,
        help="Custom output format template",
    )

    parser.add_argument(
        "--skip-header",
        action="store_true",
    )

    parser.add_argument(
        "--score-cp",
        default="",
        help="Fixed cp score for every test case. Example: --score-cp 80",
    )

    parser.add_argument(
        "--score-mate",
        default="",
        help="Fixed mate score for every test case. Example: --score-mate 2",
    )

    parser.add_argument(
        "--auto-mate-score",
        action="store_true",
        help="Extract score_mate from themes like mateIn2, mateIn3.",
    )

    args = parser.parse_args()

    convert_file(
        input_path=Path(args.input),
        output_path=Path(args.output),
        template=args.template,
        skip_header=args.skip_header,
        fixed_score_cp=args.score_cp,
        fixed_score_mate=args.score_mate,
        auto_mate_score=args.auto_mate_score,
    )


if __name__ == "__main__":
    main()