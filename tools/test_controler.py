from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
import re


@dataclass
class TestJob:
    name: str
    test_path: Path
    required: bool = True


@dataclass
class TestJobResult:
    name: str
    test_path: Path
    exit_code: int
    stdout: str
    stderr: str
    passed_cases: int = 0
    failed_cases: int = 0

    @property
    def passed(self) -> bool:
        return self.exit_code == 0


@dataclass
class Summary:
    results: list[TestJobResult] = field(default_factory=list)

    @property
    def passed(self) -> int:
        return sum(1 for r in self.results if r.passed)

    @property
    def failed(self) -> int:
        return sum(1 for r in self.results if not r.passed)

    @property
    def total(self) -> int:
        return len(self.results)
    
    @property
    def passed_cases(self) -> int:
        return sum(r.passed_cases for r in self.results)

    @property
    def failed_cases(self) -> int:
        return sum(r.failed_cases for r in self.results)

    @property
    def total_cases(self) -> int:
        return self.passed_cases + self.failed_cases

def make_job_from_path(path: Path) -> TestJob:
    return TestJob(
        name=path.stem,
        test_path=path,
        required=True,
    )

def parse_summary(stdout: str) -> tuple[int, int]:
    passed_match = re.search(r"^Passed:\s+(\d+)", stdout, re.MULTILINE)
    failed_match = re.search(r"^Failed:\s+(\d+)", stdout, re.MULTILINE)

    if passed_match is None or failed_match is None:
        return 0, 0

    passed = int(passed_match.group(1))
    failed = int(failed_match.group(1))

    return passed, failed

def run_job(
    python_exe: str,
    engine_path: Path,
    job: TestJob,
    pass_rate: int,
    verbose: bool,
) -> TestJobResult:
    command = [
        python_exe,
        "-m",
        "tools.test_runner",
        "--engine",
        str(engine_path),
        "--test",
        str(job.test_path),
        "--pass-rate",
        str(pass_rate),
    ]

    if verbose:
        command.append("--verbose")

    completed = subprocess.run(
        command,
        text=True,
        capture_output=True,
    )

    passed_cases, failed_cases = parse_summary(completed.stdout)

    return TestJobResult(
        name=job.name,
        test_path=job.test_path,
        exit_code=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
        passed_cases=passed_cases,
        failed_cases=failed_cases,
    )


def print_plan(jobs: list[TestJob]) -> None:
    print("Test plan")
    print("---------")

    for i, job in enumerate(jobs, start=1):
        print(f"[{i:03d}] PENDING {job.name:<24} {job.test_path}")

    print()


def print_job_result(index: int, result: TestJobResult) -> None:
    status = "PASS" if result.passed else "FAIL"

    print(
        f"[{index:03d}] {status:<8} "
        f"{result.name:<50} {result.test_path}"
        f"cases: passed={result.passed_cases} failed={result.failed_cases}"
    )

    if not result.passed:
        print(f"      exit_code: {result.exit_code}")

        # 只印 stderr，避免失敗時畫面太爆。
        if result.stderr.strip():
            print("      stderr:")
            for line in result.stderr.strip().splitlines():
                print(f"        {line}")


def print_final_summary(summary: Summary) -> None:
    print()
    print("Final results")
    print("-------------")

    total_test = 0
    passed_test = 0
    failed_test = 0
    for i, result in enumerate(summary.results, start=1):
        status = "PASS" if result.passed else "FAIL"

        print(
            f"[{i:03d}] {status:<8} "
            f"{result.name:<50}"
            f"cases: passed={result.passed_cases} failed={result.failed_cases}"
        )

        total_test += result.passed_cases + result.failed_cases
        passed_test += result.passed_cases
        failed_test += result.failed_cases

    print()
    print("Summary")
    print("-------")
    print(f"Passed: {summary.passed}")
    print(f"Failed: {summary.failed}")
    print(f"Total : {summary.total}")
    print(f"Pass rate of all cases: {passed_test / total_test} ({passed_test} / {total_test})")

def collect_test_files(paths: list[Path]) -> list[Path]:
    test_files: list[Path] = []

    for path in paths:
        if not path.exists():
            raise FileNotFoundError(f"test path does not exist: {path}")

        if path.is_file():
            if path.suffix == ".txt":
                test_files.append(path)
            else:
                raise ValueError(f"test file must be .txt: {path}")

        elif path.is_dir():
            found = sorted(path.rglob("*.txt"))
            test_files.extend(found)

        else:
            raise ValueError(f"unsupported test path: {path}")

    unique_files: list[Path] = []
    seen: set[Path] = set()

    for file in test_files:
        resolved = file.resolve()

        if resolved in seen:
            continue

        seen.add(resolved)
        unique_files.append(file)

    return unique_files

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", required=True, type=Path)
    parser.add_argument(
        "--tests",
        nargs="+",
        type=Path,
        required=True,
        help="Test files or directories. Directories will be searched recursively for *.txt files.",
    )
    parser.add_argument("--pass-rate", required=True, type=int)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--show-output", action="store_true")

    args = parser.parse_args()

    try:
        test_files = collect_test_files(args.tests)
    except Exception as e:
        print(f"error: {e}")
        return 1

    if not test_files:
        print("No test files found.")
        return 1
    
    jobs = [make_job_from_path(path) for path in test_files]

    summary = Summary()

    print("Running")
    print("-------")

    for i, job in enumerate(jobs, start=1):
        if not job.test_path.exists():
            result = TestJobResult(
                name=job.name,
                test_path=job.test_path,
                exit_code=1,
                stdout="",
                stderr=f"test file does not exist: {job.test_path}",
            )
        else:
            result = run_job(
                python_exe=sys.executable,
                engine_path=args.engine,
                job=job,
                pass_rate=args.pass_rate,
                verbose=args.verbose,
            )

        summary.results.append(result)
        print_job_result(i, result)

        if args.show_output:
            print()
            print(f"Output: {result.name}")
            print("-" * (8 + len(result.name)))
            print(result.stdout)

            if result.stderr.strip():
                print("stderr:")
                print(result.stderr)

    print_final_summary(summary)

    return 0 if summary.failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())