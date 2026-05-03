import argparse
import json
import shutil
import subprocess
import time
from pathlib import Path


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
    sprt_elo0: float,
    sprt_elo1: float,
    sprt_alpha: float,
    sprt_beta: float,
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
        "-f",
        "sprt=true",
        "-f",
        f"sprt_elo0={sprt_elo0}",
        "-f",
        f"sprt_elo1={sprt_elo1}",
        "-f",
        f"sprt_alpha={sprt_alpha}",
        "-f",
        f"sprt_beta={sprt_beta}",
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


def wait_for_run(run_id: str, poll_seconds: int) -> str:
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

        print(f"run {run_id}: {status}", flush=True)

        if status.startswith("completed"):
            parts = status.split(maxsplit=1)
            return parts[1] if len(parts) > 1 else ""

        time.sleep(poll_seconds)


def download_merged_result(run_id: str, out_dir: Path) -> None:
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    run_cmd(
        [
            "gh",
            "run",
            "download",
            run_id,
            "--name",
            "ab-test-merged-result",
            "--dir",
            str(out_dir),
        ]
    )


def find_merged_json(run_dir: Path) -> Path:
    candidates = list(run_dir.rglob("merged_ab_result.json"))

    if not candidates:
        raise RuntimeError(f"Cannot find merged_ab_result.json under {run_dir}")

    if len(candidates) > 1:
        print("Warning: multiple merged_ab_result.json files found. Using first one:")
        for path in candidates:
            print(f"  {path}")

    return candidates[0]


def read_result(run_dir: Path) -> dict:
    json_path = find_merged_json(run_dir)
    data = json.loads(json_path.read_text(encoding="utf-8"))

    sprt = data.get("sprt")
    if sprt is None:
        raise RuntimeError(f"No 'sprt' field found in {json_path}")

    return data


def print_round_summary(data: dict) -> None:
    sprt = data["sprt"]

    games = data.get("games", 0)
    wins = data.get("wins", 0)
    losses = data.get("losses", 0)
    draws = data.get("draws", 0)

    elo = data.get("elo_diff")
    error = data.get("elo_error_95")
    los = data.get("los")

    print()
    print("Round summary")
    print("-------------")
    print(f"Games: {games}")
    print(f"W/L/D: {wins} / {losses} / {draws}")

    if elo is not None and error is not None:
        print(f"Elo: {elo:+.1f} ± {error:.1f}")

    if los is not None:
        print(f"LOS: {los:.1f}%")

    print(f"SPRT LLR: {sprt.get('llr', 0.0):+.3f}")
    print(f"SPRT lower: {sprt.get('lower_bound', 0.0):+.3f}")
    print(f"SPRT upper: {sprt.get('upper_bound', 0.0):+.3f}")
    print(f"SPRT result: {sprt.get('result')}")
    print()


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

    args = parser.parse_args()

    work_dir = Path(args.work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)

    current_batch = args.start_batch
    end_batch = args.start_batch + args.max_batches

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
            sprt_elo0=args.sprt_elo0,
            sprt_elo1=args.sprt_elo1,
            sprt_alpha=args.sprt_alpha,
            sprt_beta=args.sprt_beta,
        )

        time.sleep(5)

        run_id = get_latest_run_id(args.workflow)
        print(f"Triggered run id: {run_id}")

        conclusion = wait_for_run(run_id, args.poll_seconds)
        if conclusion != "success":
            raise RuntimeError(
                f"Workflow run {run_id} ended with conclusion: {conclusion}"
            )

        round_dir = work_dir / f"run_{run_id}"
        download_merged_result(run_id, round_dir)

        data = read_result(round_dir)
        print_round_summary(data)

        sprt_result = data["sprt"].get("result")

        if sprt_result in ("PASS", "FAIL"):
            print(f"Stopping: SPRT {sprt_result}")
            return

        if sprt_result != "CONTINUE":
            raise RuntimeError(f"Unknown SPRT result: {sprt_result}")

        current_batch += num_batches

    print("Stopping: reached max_batches without PASS/FAIL")


if __name__ == "__main__":
    main()