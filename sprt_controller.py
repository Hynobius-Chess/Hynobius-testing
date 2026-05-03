import argparse
import json
import math
import shutil
import subprocess
import time
from pathlib import Path
from datetime import datetime

# ============================================================
# Command helpers
# ============================================================

def run_cmd(cmd: list[str], *, capture: bool = False) -> str:
    print("+", " ".join(cmd), flush=True)

    if capture:
        completed = subprocess.run(
            cmd,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        return completed.stdout.strip()

    subprocess.run(cmd, check=True)
    return ""


# ============================================================
# GitHub Actions / gh helpers
# ============================================================

def trigger_workflow(
    *,
    workflow: str,
    workflow_ref: str | None,
    baseline_ref: str,
    candidate_ref: str,
    start_batch: int,
    num_batches: int,
    pairs_per_batch: int,
    tc: str,
) -> None:
    cmd = [
        "gh",
        "workflow",
        "run",
        workflow,
        "-f",
        f"baseline_ref={baseline_ref}",
        "-f",
        f"candidate_ref={candidate_ref}",
        "-f",
        f"start_batch={start_batch}",
        "-f",
        f"num_batches={num_batches}",
        "-f",
        f"pairs_per_batch={pairs_per_batch}",
        "-f",
        f"tc={tc}",
    ]

    if workflow_ref:
        cmd.extend(["--ref", workflow_ref])

    run_cmd(cmd)


def get_latest_run_id(workflow: str) -> str:
    run_id = run_cmd(
        [
            "gh",
            "run",
            "list",
            "--workflow",
            workflow,
            "--limit",
            "1",
            "--json",
            "databaseId",
            "--jq",
            ".[0].databaseId",
        ],
        capture=True,
    )

    if not run_id:
        raise RuntimeError(f"Cannot find latest run for workflow: {workflow}")

    return run_id


def get_run_progress(run_id: str, games_per_batch: int) -> dict:
    output = run_cmd(
        [
            "gh",
            "run",
            "view",
            run_id,
            "--json",
            "jobs",
        ],
        capture=True,
    )

    data = json.loads(output)
    jobs = data.get("jobs", [])

    # Matrix reusable workflow jobs usually contain "run-batches" in their names.
    # If your job name differs, adjust this filter.
    batch_jobs = [
        job for job in jobs
        if "run-batches" in job.get("name", "")
        or "Run AB batch" in job.get("name", "")
        or "run-ab-batch" in job.get("name", "")
    ]

    total = len(batch_jobs)
    completed = 0
    failed = 0
    in_progress = 0
    queued = 0

    for job in batch_jobs:
        status = job.get("status")
        conclusion = job.get("conclusion")

        if status == "completed":
            if conclusion == "success":
                completed += 1
            else:
                failed += 1
        elif status == "in_progress":
            in_progress += 1
        elif status == "queued":
            queued += 1

    completed_games_est = completed * games_per_batch
    total_games_est = total * games_per_batch

    return {
        "total_batches": total,
        "completed_batches": completed,
        "failed_batches": failed,
        "in_progress_batches": in_progress,
        "queued_batches": queued,
        "completed_games_est": completed_games_est,
        "total_games_est": total_games_est,
    }


def wait_for_run(run_id: str, poll_seconds: int, games_per_batch: int) -> str:
    while True:
        status = run_cmd(
            [
                "gh",
                "run",
                "view",
                run_id,
                "--json",
                "status,conclusion",
                "--jq",
                '.status + " " + (.conclusion // "")',
            ],
            capture=True,
        )

        progress = get_run_progress(run_id, games_per_batch)

        print(
            f"run {run_id}: {status} | "
            f"batches: {progress['completed_batches']}/{progress['total_batches']} done, "
            f"{progress['in_progress_batches']} running, "
            f"{progress['queued_batches']} queued, "
            f"{progress['failed_batches']} failed | "
            f"games est: {progress['completed_games_est']}/{progress['total_games_est']}",
            flush=True,
        )

        if status.startswith("completed"):
            parts = status.split(maxsplit=1)
            return parts[1] if len(parts) > 1 else ""

        time.sleep(poll_seconds)


def download_batch_artifacts(run_id: str, out_dir: Path) -> None:
    """
    Downloads all ab-batch-* artifacts from one workflow run.

    Output usually becomes:
      out_dir/
        ab-batch-0/
          batch_000.json
          batch_000.pgn
        ab-batch-1/
          batch_001.json
          batch_001.pgn
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    run_cmd(
        [
            "gh",
            "run",
            "download",
            run_id,
            "--pattern",
            "ab-batch-*",
            "--dir",
            str(out_dir),
        ]
    )


# ============================================================
# Chess AB statistics
# ============================================================

def normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def elo_from_score(score: float) -> float:
    eps = 1e-9
    score = max(eps, min(1.0 - eps, score))
    return -400.0 * math.log10(1.0 / score - 1.0)


def score_from_elo(elo: float) -> float:
    return 1.0 / (1.0 + 10.0 ** (-elo / 400.0))


def elo_slope_at_score(score: float) -> float:
    eps = 1e-9
    score = max(eps, min(1.0 - eps, score))
    return 400.0 / math.log(10.0) / (score * (1.0 - score))


def score_stats(wins: int, losses: int, draws: int) -> dict:
    """
    Candidate perspective:
      win  = 1.0
      draw = 0.5
      loss = 0.0

    Uses trinomial variance:
      Var(X) = E[X^2] - E[X]^2
    """
    games = wins + losses + draws

    if games <= 0:
        return {
            "games": 0,
            "score_rate": 0.0,
            "draw_rate": 0.0,
            "elo_diff": 0.0,
            "elo_error_95": 0.0,
            "los": 50.0,
        }

    score_rate = (wins + 0.5 * draws) / games
    draw_rate = draws / games

    ex2 = (wins * 1.0 + draws * 0.25) / games
    variance = max(0.0, ex2 - score_rate * score_rate)
    score_se = math.sqrt(variance / games)

    elo_diff = elo_from_score(score_rate)
    elo_se = elo_slope_at_score(score_rate) * score_se
    elo_error_95 = 1.96 * elo_se

    if elo_se > 0:
        los = normal_cdf(elo_diff / elo_se) * 100.0
    else:
        los = 50.0 if abs(elo_diff) < 1e-9 else (100.0 if elo_diff > 0 else 0.0)

    return {
        "games": games,
        "score_rate": score_rate,
        "draw_rate": draw_rate,
        "elo_diff": elo_diff,
        "elo_error_95": elo_error_95,
        "los": los,
    }


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

    This is not a full OpenBench/Fishtest implementation, but it is enough
    for your current fixed-games workflow controller.
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
    p0 = score_from_elo(elo0)
    p1 = score_from_elo(elo1)

    ex2 = (wins * 1.0 + draws * 0.25) / games
    variance = max(1e-9, ex2 - score * score)

    # log L(H1) - log L(H0)
    llr = games * (((score - p0) ** 2 - ((score - p1) ** 2)) / (2.0 * variance))

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


# ============================================================
# Accumulated merge inside controller
# ============================================================

def merge_batch_results(
    input_dir: Path,
    *,
    start_batch: int,
    expected_batches: int | None,
    sprt_elo0: float,
    sprt_elo1: float,
    sprt_alpha: float,
    sprt_beta: float,
    out_dir: Path | None = None,
) -> dict:
    json_files = sorted(input_dir.rglob("batch_*.json"))

    if not json_files:
        raise RuntimeError(f"No batch_*.json found under {input_dir}")

    completed_batch_indices: set[int] = set()

    total_games = 0
    total_wins = 0
    total_losses = 0
    total_draws = 0
    total_unknown = 0
    failed_batches = []
    duplicate_batches = []

    for json_path in json_files:
        data = json.loads(json_path.read_text(encoding="utf-8"))

        batch_index = int(data["batch_index"])

        # Prevent duplicated old artifacts from corrupting accumulated stats.
        if batch_index in completed_batch_indices:
            duplicate_batches.append(batch_index)
            print(f"Warning: duplicate batch {batch_index}; ignoring {json_path}")
            continue

        completed_batch_indices.add(batch_index)

        total_games += int(data.get("games", 0))
        total_wins += int(data.get("wins", 0))
        total_losses += int(data.get("losses", 0))
        total_draws += int(data.get("draws", 0))
        total_unknown += int(data.get("unknown", 0))

        if int(data.get("cutechess_returncode", 0)) != 0:
            failed_batches.append(batch_index)

    missing_batches = []
    if expected_batches is not None:
        for i in range(start_batch, start_batch + expected_batches):
            if i not in completed_batch_indices:
                missing_batches.append(i)

    stats = score_stats(total_wins, total_losses, total_draws)

    sprt = sprt_normal_approx(
        total_wins,
        total_losses,
        total_draws,
        sprt_elo0,
        sprt_elo1,
        sprt_alpha,
        sprt_beta,
    )

    result = {
        "input_dir": str(input_dir),
        "batches_found": len(completed_batch_indices),
        "start_batch": start_batch,
        "expected_batches": expected_batches,
        "completed_batches": sorted(completed_batch_indices),
        "missing_batches": missing_batches,
        "failed_batches": sorted(failed_batches),
        "duplicate_batches": sorted(set(duplicate_batches)),
        "games": total_games,
        "wins": total_wins,
        "losses": total_losses,
        "draws": total_draws,
        "unknown": total_unknown,
        "score_rate": stats["score_rate"],
        "draw_rate": stats["draw_rate"],
        "elo_diff": stats["elo_diff"],
        "elo_error_95": stats["elo_error_95"],
        "los": stats["los"],
        "sprt": sprt,
    }

    if out_dir is not None:
        write_controller_outputs(result, input_dir, out_dir)

    return result


def make_summary_markdown(result: dict) -> str:
    sprt = result["sprt"]

    status = "complete"
    if (
        result["missing_batches"]
        or result["failed_batches"]
        or result["duplicate_batches"]
        or result["unknown"] > 0
    ):
        status = "incomplete_or_has_warnings"

    return f"""# Accumulated AB Test Summary

Status: `{status}`

## Result

| Metric | Value |
|---|---:|
| Games | {result["games"]} |
| Candidate wins | {result["wins"]} |
| Candidate losses | {result["losses"]} |
| Draws | {result["draws"]} |
| Unknown | {result["unknown"]} |
| Score rate | {result["score_rate"]:.3f} |
| Draw rate | {result["draw_rate"]:.3f} |
| Elo diff | {result["elo_diff"]:+.1f} |
| Error 95% | ±{result["elo_error_95"]:.1f} |
| Elo range 95% | {result["elo_diff"] - result["elo_error_95"]:+.1f} ~ {result["elo_diff"] + result["elo_error_95"]:+.1f} |
| LOS | {result["los"]:.1f}% |

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

## Batch status

| Metric | Value |
|---|---:|
| Batches found | {result["batches_found"]} |
| Start batch | {result["start_batch"]} |
| Expected batches | {result["expected_batches"] if result["expected_batches"] is not None else "N/A"} |
| Missing batches | {", ".join(map(str, result["missing_batches"])) if result["missing_batches"] else "None"} |
| Failed batches | {", ".join(map(str, result["failed_batches"])) if result["failed_batches"] else "None"} |
| Duplicate batches | {", ".join(map(str, result["duplicate_batches"])) if result["duplicate_batches"] else "None"} |
"""


def write_controller_outputs(result: dict, input_dir: Path, out_dir: Path) -> None:
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    result_json = out_dir / f"{timestamp}_merged_ab_result.json"
    summary_md = out_dir / f"{timestamp}_summary.md"
    merged_pgn = out_dir / f"{timestamp}_merged_ab_result.pgn"

    latest_result_json = out_dir / "latest_result.json"
    latest_summary_md = out_dir / "latest_summary.md"
    latest_merged_pgn = out_dir / "latest_games.pgn"

    pgn_parts = []
    seen_batches = set()

    for pgn_path in sorted(input_dir.rglob("batch_*.pgn")):
        # Avoid duplicate PGN merge when artifacts are duplicated.
        stem = pgn_path.stem  # batch_000
        if stem in seen_batches:
            print(f"Warning: duplicate PGN {stem}; ignoring {pgn_path}")
            continue

        seen_batches.add(stem)
        pgn_parts.append(pgn_path.read_text(encoding="utf-8", errors="replace"))

    result_json.write_text(json.dumps(result, indent=2), encoding="utf-8")
    summary_md.write_text(make_summary_markdown(result), encoding="utf-8")
    merged_pgn.write_text("\n\n".join(pgn_parts), encoding="utf-8")

    latest_result_json.write_text(json.dumps(result, indent=2), encoding="utf-8")
    latest_summary_md.write_text(make_summary_markdown(result), encoding="utf-8")
    latest_merged_pgn.write_text("\n\n".join(pgn_parts), encoding="utf-8")

def print_accumulated_summary(data: dict) -> None:
    sprt = data["sprt"]

    print()
    print("Accumulated summary")
    print("-------------------")
    print(f"Batches: {data.get('batches_found')}")
    print(f"Games: {data.get('games')}")
    print(f"W/L/D: {data.get('wins')} / {data.get('losses')} / {data.get('draws')}")
    print(f"Score rate: {data.get('score_rate'):.3f}")
    print(f"Draw rate: {data.get('draw_rate'):.3f}")
    print(f"Elo: {data.get('elo_diff'):+.1f} ± {data.get('elo_error_95'):.1f}")
    print(f"LOS: {data.get('los'):.1f}%")
    print(f"SPRT LLR: {sprt.get('llr', 0.0):+.3f}")
    print(f"SPRT lower: {sprt.get('lower_bound', 0.0):+.3f}")
    print(f"SPRT upper: {sprt.get('upper_bound', 0.0):+.3f}")
    print(f"SPRT result: {sprt.get('result')}")

    if data.get("missing_batches"):
        print(f"Missing batches: {data['missing_batches']}")

    if data.get("failed_batches"):
        print(f"Failed batches: {data['failed_batches']}")

    if data.get("duplicate_batches"):
        print(f"Duplicate batches ignored: {data['duplicate_batches']}")

    print()


# ============================================================
# Main controller loop
# ============================================================

def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument("--workflow", default="ab-test-matrix.yml")
    parser.add_argument("--workflow-ref", default=None)

    parser.add_argument("--baseline-ref", default="main")
    parser.add_argument("--candidate-ref", default="dev")

    parser.add_argument("--start-batch", type=int, default=0)
    parser.add_argument("--batches-per-round", type=int, default=5)
    parser.add_argument("--max-batches", type=int, default=50)
    parser.add_argument("--pairs-per-batch", type=int, default=20)
    parser.add_argument("--tc", default="10+0.1")

    parser.add_argument("--sprt-elo0", type=float, default=0.0)
    parser.add_argument("--sprt-elo1", type=float, default=10.0)
    parser.add_argument("--sprt-alpha", type=float, default=0.05)
    parser.add_argument("--sprt-beta", type=float, default=0.05)

    parser.add_argument("--poll-seconds", type=int, default=20)
    parser.add_argument("--work-dir", default="sprt_runs")

    # By default, clean previous controller data so old artifacts cannot poison the test.
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Keep existing work-dir/all_results and continue from it.",
    )

    args = parser.parse_args()

    work_dir = Path(args.work_dir)
    all_results_dir = work_dir / "all_results"
    merged_dir = work_dir / "merged_accumulated"

    work_dir.mkdir(parents=True, exist_ok=True)

    if not args.resume:
        if all_results_dir.exists():
            shutil.rmtree(all_results_dir)
        if merged_dir.exists():
            shutil.rmtree(merged_dir)

    all_results_dir.mkdir(parents=True, exist_ok=True)

    current_batch = args.start_batch
    end_batch = args.start_batch + args.max_batches
    games_per_batch = args.pairs_per_batch * 2

    while current_batch < end_batch:
        remaining = end_batch - current_batch
        num_batches = min(args.batches_per_round, remaining)

        print()
        print("=" * 80)
        print(
            f"Starting round: batches "
            f"{current_batch}..{current_batch + num_batches - 1}"
        )
        print("=" * 80)

        trigger_workflow(
            workflow=args.workflow,
            workflow_ref=args.workflow_ref,
            baseline_ref=args.baseline_ref,
            candidate_ref=args.candidate_ref,
            start_batch=current_batch,
            num_batches=num_batches,
            pairs_per_batch=args.pairs_per_batch,
            tc=args.tc,
        )

        # Give GitHub a moment to register the new run.
        time.sleep(5)

        run_id = get_latest_run_id(args.workflow)
        print(f"Triggered run id: {run_id}")

        conclusion = wait_for_run(run_id, args.poll_seconds, games_per_batch)
        if conclusion != "success":
            raise RuntimeError(
                f"Workflow run {run_id} ended with conclusion: {conclusion}"
            )

        download_batch_artifacts(run_id, all_results_dir)

        completed_batches = current_batch + num_batches - args.start_batch

        data = merge_batch_results(
            all_results_dir,
            start_batch=args.start_batch,
            expected_batches=completed_batches,
            sprt_elo0=args.sprt_elo0,
            sprt_elo1=args.sprt_elo1,
            sprt_alpha=args.sprt_alpha,
            sprt_beta=args.sprt_beta,
            out_dir=merged_dir,
        )

        print_accumulated_summary(data)

        sprt_result = data["sprt"].get("result")

        if sprt_result in ("PASS", "FAIL"):
            print(f"Stopping: SPRT {sprt_result}")
            print(f"Final result written to: {merged_dir}")
            return

        if sprt_result != "CONTINUE":
            raise RuntimeError(f"Unknown SPRT result: {sprt_result}")

        current_batch += num_batches

    print("Stopping: reached max_batches without PASS/FAIL")
    print(f"Final result written to: {merged_dir}")


if __name__ == "__main__":
    main()