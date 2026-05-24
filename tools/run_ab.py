import argparse
import json
import subprocess
from pathlib import Path


def split_pgn_games(text: str) -> list[str]:
    games = []
    current = []

    for line in text.splitlines():
        if line.startswith("[Event ") and current:
            games.append("\n".join(current).strip() + "\n")
            current = []
        current.append(line)

    if current:
        games.append("\n".join(current).strip() + "\n")

    return [g for g in games if g.strip()]


def result_from_pgn(game: str) -> str | None:
    for line in game.splitlines():
        if line.startswith("[Result "):
            return line.split('"')[1]
    return None


def parse_score(pgn_path: Path) -> dict:
    text = pgn_path.read_text(encoding="utf-8", errors="replace")
    games = split_pgn_games(text)

    wins = losses = draws = unknown = 0

    for game in games:
        result = result_from_pgn(game)

        # 假設 cutechess 第一個 engine 是 baseline，第二個是 candidate。
        # 因為有 repeat，不能只靠 Result，要看 White/Black 是誰。
        white = None
        black = None

        for line in game.splitlines():
            if line.startswith("[White "):
                white = line.split('"')[1]
            elif line.startswith("[Black "):
                black = line.split('"')[1]

        if result == "1/2-1/2":
            draws += 1
        elif result == "1-0":
            if white == "candidate":
                wins += 1
            elif white == "baseline":
                losses += 1
            else:
                unknown += 1
        elif result == "0-1":
            if black == "candidate":
                wins += 1
            elif black == "baseline":
                losses += 1
            else:
                unknown += 1
        else:
            unknown += 1

    return {
        "games": len(games),
        "wins": wins,
        "losses": losses,
        "draws": draws,
        "unknown": unknown,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cutechess", default="cutechess-cli")
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--opening-file", required=True)
    parser.add_argument("--batch-index", type=int, required=True)
    parser.add_argument("--pairs-per-batch", type=int, default=20)
    parser.add_argument("--tc", default="10+0.1")
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    batch_name = f"batch_{args.batch_index:03d}"
    batch_openings = out_dir / f"{batch_name}_openings.pgn"
    batch_pgn = out_dir / f"{batch_name}.pgn"
    batch_json = out_dir / f"{batch_name}.json"

    opening_text = Path(args.opening_file).read_text(encoding="utf-8", errors="replace")
    openings = split_pgn_games(opening_text)

    start = args.batch_index * args.pairs_per_batch
    end = start + args.pairs_per_batch
    selected = openings[start:end]

    if len(selected) < args.pairs_per_batch:
        raise RuntimeError(
            f"Not enough openings: requested {start}..{end - 1}, "
            f"but only found {len(openings)} openings"
        )

    batch_openings.write_text("\n\n".join(selected) + "\n", encoding="utf-8")

    cmd = [
        args.cutechess,

        "-engine",
        "name=baseline",
        f"cmd={args.baseline}",
        "proto=uci",

        "-engine",
        "name=candidate",
        f"cmd={args.candidate}",
        "proto=uci",

        "-each",
        f"tc={args.tc}",

        "-openings",
        f"file={batch_openings}",
        "format=pgn",
        "order=sequential",
        "plies=999",

        "-repeat",
        "-games", "2",
        "-rounds", str(args.pairs_per_batch),

        "-pgnout", str(batch_pgn),
        "-recover",
        "-concurrency", "1",
    ]

    print("Running:")
    print(" ".join(cmd))

    completed = subprocess.run(cmd, check=False)

    stats = parse_score(batch_pgn) if batch_pgn.exists() else {
        "games": 0,
        "wins": 0,
        "losses": 0,
        "draws": 0,
        "unknown": 0,
    }

    result = {
        "batch_index": args.batch_index,
        "pairs_per_batch": args.pairs_per_batch,
        "expected_games": args.pairs_per_batch * 2,
        "tc": args.tc,
        "pgn": str(batch_pgn),
        "cutechess_returncode": completed.returncode,
        **stats,
    }

    batch_json.write_text(json.dumps(result, indent=2), encoding="utf-8")

    if completed.returncode != 0:
        raise RuntimeError(f"cutechess-cli failed with code {completed.returncode}")


if __name__ == "__main__":
    main()