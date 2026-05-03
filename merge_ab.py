import argparse
import json
import math
from pathlib import Path

def elo_from_score(score: float) -> float:
    """
    score: candidate score rate, 0.0 ~ 1.0
    returns approximate Elo difference.
    """
    eps = 1e-9
    score = max(eps, min(1.0 - eps, score))
    return -400.0 * math.log10(1.0 / score - 1.0)

def score_from_elo(elo: float) -> float:
    return 1.0 / (1.0 + 10.0 ** (-elo / 400.0))


def sprt_normal_approx(
    wins: int,
    losses: int,
    draws: int,
    elo0: float,
    elo1: float,
    alpha: float,
    beta: float,
) -> dict:
    """
    Simplified SPRT using normal approximation on score rate.

    Candidate perspective:
      win  = 1.0
      draw = 0.5
      loss = 0.0
    """
    games = wins + losses + draws

    lower_bound = math.log(beta / (1.0 - alpha))
    upper_bound = math.log((1.0 - beta) / alpha)

    if games <= 0:
        return {
            "elo0": elo0,
            "elo1": elo1,
            "alpha": alpha,
            "beta": beta,
            "llr": 0.0,
            "lower_bound": lower_bound,
            "upper_bound": upper_bound,
            "result": "CONTINUE",
        }

    score = (wins + 0.5 * draws) / games

    # Expected score under H0 / H1.
    p0 = score_from_elo(elo0)
    p1 = score_from_elo(elo1)

    # Trinomial score variance estimated from observed result distribution.
    ex2 = (wins * 1.0 + draws * 0.25) / games
    var = max(1e-9, ex2 - score * score)

    # Normal log-likelihood ratio for mean score.
    # LLR = log L(H1) - log L(H0)
    llr = games * (
        ((score - p0) ** 2 - (score - p1) ** 2)
        / (2.0 * var)
    )

    if llr >= upper_bound:
        result = "PASS"
    elif llr <= lower_bound:
        result = "FAIL"
    else:
        result = "CONTINUE"

    return {
        "elo0": elo0,
        "elo1": elo1,
        "alpha": alpha,
        "beta": beta,
        "llr": llr,
        "lower_bound": lower_bound,
        "upper_bound": upper_bound,
        "result": result,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--expected-batches", type=int, default=None)
    parser.add_argument("--sprt", action="store_true")
    parser.add_argument("--sprt-elo0", type=float, default=0.0)
    parser.add_argument("--sprt-elo1", type=float, default=10.0)
    parser.add_argument("--sprt-alpha", type=float, default=0.05)
    parser.add_argument("--sprt-beta", type=float, default=0.05)
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    json_files = sorted(input_dir.glob("batch_*.json"))

    if not json_files:
        raise RuntimeError(f"No batch_*.json found in {input_dir}")

    batches = []
    total_games = 0
    total_wins = 0
    total_losses = 0
    total_draws = 0
    total_unknown = 0
    failed_batches = []
    completed_batch_indices = set()

    merged_pgn_parts = []

    for json_path in json_files:
        data = json.loads(json_path.read_text(encoding="utf-8"))

        batch_index = int(data["batch_index"])
        completed_batch_indices.add(batch_index)
        batches.append(data)

        total_games += int(data.get("games", 0))
        total_wins += int(data.get("wins", 0))
        total_losses += int(data.get("losses", 0))
        total_draws += int(data.get("draws", 0))
        total_unknown += int(data.get("unknown", 0))

        if int(data.get("cutechess_returncode", 0)) != 0:
            failed_batches.append(batch_index)

        pgn_path = input_dir / f"batch_{batch_index:03d}.pgn"
        if pgn_path.exists():
            merged_pgn_parts.append(pgn_path.read_text(encoding="utf-8", errors="replace"))

    decisive_games = total_wins + total_losses
    score_points = total_wins + 0.5 * total_draws
    score_rate = score_points / total_games if total_games > 0 else 0.0
    elo = elo_from_score(score_rate) if total_games > 0 else 0.0
    draw_rate = total_draws / total_games if total_games > 0 else 0.0

    missing_batches = []
    if args.expected_batches is not None:
        for i in range(args.expected_batches):
            if i not in completed_batch_indices:
                missing_batches.append(i)

    sprt = None
    if args.sprt:
        sprt = sprt_normal_approx(
            total_wins,
            total_losses,
            total_draws,
            args.sprt_elo0,
            args.sprt_elo1,
            args.sprt_alpha,
            args.sprt_beta,
        )

    result = {
        "input_dir": str(input_dir),
        "batches_found": len(batches),
        "expected_batches": args.expected_batches,
        "missing_batches": missing_batches,
        "failed_batches": sorted(failed_batches),
        "games": total_games,
        "wins": total_wins,
        "losses": total_losses,
        "draws": total_draws,
        "unknown": total_unknown,
        "score_rate": score_rate,
        "draw_rate": draw_rate,
        "elo_diff": elo,
        "decisive_games": decisive_games,
        "sprt": sprt,
    }

    merged_json = out_dir / "merged_ab_result.json"
    merged_pgn = out_dir / "merged_ab_result.pgn"
    summary_md = out_dir / "summary.md"

    merged_json.write_text(json.dumps(result, indent=2), encoding="utf-8")
    merged_pgn.write_text("\n\n".join(merged_pgn_parts), encoding="utf-8")

    status = "complete"
    if missing_batches or failed_batches or total_unknown > 0:
        status = "incomplete_or_has_warnings"

    summary = f"""# AB Test Summary

Status: `{status}`

## Result

| Metric | Value |
| --- | --- |
| Games | {total_games} |
| Candidate wins | {total_wins} |
| Candidate losses | {total_losses} |
| Draws | {total_draws} |
| Unknown | {total_unknown} |
| Score rate | {score_rate:.3f} |
| Draw rate | {draw_rate:.3f} |
| Elo diff | {elo:+.1f} |

## Batch status

| Metric | Value |
| --- | --- |
| Batches found | {len(batches)} |
| Expected batches | {args.expected_batches if args.expected_batches is not None else "N/A"} |
| Missing batches | {", ".join(map(str, missing_batches)) if missing_batches else "None"} |
| Failed batches | {", ".join(map(str, sorted(failed_batches))) if failed_batches else "None"} |
"""

    if sprt is not None:
        summary += f"""
    
## SPRT

| Metric | Value |
|---|---:|
| Elo0 | {sprt["elo0"]:+.1f} |
| Elo1 | {sprt["elo1"]:+.1f} |
| Alpha | {sprt["alpha"]:.3f} |
| Beta | {sprt["beta"]:.3f} |
| LLR | {sprt["llr"]:+.3f} |
| Lower bound | {sprt["lower_bound"]:+.3f} |
| Upper bound | {sprt["upper_bound"]:+.3f} |
| Result | {sprt["result"]} |
"""

    summary_md.write_text(summary, encoding="utf-8")

    print(summary)
    print(f"Wrote: {merged_json}")
    print(f"Wrote: {merged_pgn}")
    print(f"Wrote: {summary_md}")


if __name__ == "__main__":
    main()