from __future__ import annotations

import argparse
from pathlib import Path

from tools.test_format import (
    TestSuite,
    TestType,
    TestCase,
    load_test_suite,
)
from tools.uci_engine import (
    UciEngine,
    SearchResult,
    PerftResult,
)
from tools.cmd_output import Cmd

# ============================================================
# Compare helpers
# ============================================================

def compare_int_exact(
    errors: list[str],
    name: str,
    expected: int | None,
    actual: int | None,
) -> None:
    if expected is None:
        return

    if actual != expected:
        errors.append(f"{name}: expected {expected}, got {actual}")


def compare_int_min(
    errors: list[str],
    name: str,
    expected_min: int | None,
    actual: int | None,
) -> None:
    if expected_min is None:
        return

    if actual is None:
        errors.append(f"{name}: expected >= {expected_min}, got None")
        return

    if actual < expected_min:
        errors.append(f"{name}: expected >= {expected_min}, got {actual}")


def compare_bestmove_list(
    errors: list[str],
    expected_moves: list[str] | None,
    actual_bestmove: str,
) -> None:
    if expected_moves is None:
        return

    if actual_bestmove not in expected_moves:
        errors.append(
            f"bestmove: expected one of {expected_moves}, got {actual_bestmove}"
        )


# ============================================================
# Perft compare
# ============================================================

def compare_perft(case: TestCase, actual: PerftResult) -> list[str]:
    expected = case.expected
    errors: list[str] = []

    compare_int_exact(errors, "nodes", expected.nodes, actual.nodes)
    compare_int_exact(errors, "captures", expected.captures, actual.captures)
    compare_int_exact(errors, "en_passants", expected.enpassants, actual.en_passants)
    compare_int_exact(errors, "castles", expected.castles, actual.castles)
    compare_int_exact(errors, "promotions", expected.promotions, actual.promotions)
    compare_int_exact(errors, "checks", expected.checks, actual.checks)

    return errors


# ============================================================
# Search / bestmove compare
# ============================================================

def compare_search(case: TestCase, actual: SearchResult) -> list[str]:
    expected = case.expected
    errors: list[str] = []

    compare_bestmove_list(
        errors=errors,
        expected_moves=expected.bestmove,
        actual_bestmove=actual.bestmove,
    )

    compare_int_exact(errors, "score_cp", expected.score_cp, actual.cp_score)
    compare_int_exact(errors, "score_mate", expected.score_mate, actual.mate_score)

    return errors


# ============================================================
# Case runners
# ============================================================

def run_perft_case(engine: UciEngine, case: TestCase) -> list[str]:
    actual = engine.go_perft(
        fen=case.fen,
        depth=case.depth,
    )

    return compare_perft(case, actual)


def run_bestmove_case(engine: UciEngine, case: TestCase) -> list[str]:
    engine.new_game()
    engine.set_position(case.fen, case.premove)

    actual = engine.go_depth(case.depth)

    return compare_search(case, actual)


def run_bench_case(engine: UciEngine, case: TestCase) -> list[str]:
    engine.new_game()
    engine.set_position(case.fen, case.premove)

    actual = engine.go_depth(case.depth)

    return compare_search(case, actual)

def run_mate_case(engine: UciEngine, case:TestCase) -> list[str]:
    engine.new_game();
    engine.set_position(case.fen, case.premove)

    actual = engine.go_depth(case.depth)

    return compare_search(case, actual)

def run_case(
    suite: TestSuite,
    engine: UciEngine,
    case: TestCase,
) -> list[str]:
    if suite.spec.test_type == TestType.PERFT:
        return run_perft_case(engine, case)

    if suite.spec.test_type == TestType.BESTMOVE:
        return run_bestmove_case(engine, case)

    if suite.spec.test_type == TestType.BENCH:
        return run_bench_case(engine, case)
    
    if (suite.spec.test_type == TestType.MATE):
        return run_mate_case(engine, case)

    raise ValueError(f"unknown test type: {suite.spec.test_type}")


# ============================================================
# Suite runner
# ============================================================

def run_suite(
    engine_path: Path,
    test_path: Path,
    pass_rate: int,
    verbose: bool = False,
) -> int:
    cmd = Cmd()
    suite = load_test_suite(test_path)

    passed = 0
    failed = 0

    print(f"Test file : {test_path}")
    print(f"Test type : {suite.spec.test_type.value}")
    print(f"Cases     : {len(suite.cases)}")
    print()

    with UciEngine(engine_path) as engine:
        for case in suite.cases:
            try:
                errors = run_case(suite, engine, case)
            except Exception as e:
                errors = [f"exception: {type(e).__name__}: {e}"]

            if errors:
                failed += 1
                cmd.print_fail(f"[FAIL] {case.name}")

                for error in errors:
                    cmd.print_fail(f"  - {error}")

                if verbose:
                    print(f"  fen   : {case.fen}")
                    print(f"  depth : {case.depth}")

            else:
                passed += 1
                cmd.print_pass(f"[PASS] {case.name}")

    print()
    print("Summary")
    print("-------")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")

    return 0 if passed / (passed + failed) * 100 >= pass_rate else 1


# ============================================================
# Main
# ============================================================

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", required=True, type=Path)
    parser.add_argument("--test", required=True, type=Path)
    parser.add_argument("--pass-rate", required=True, type=int)
    parser.add_argument("--verbose", action="store_true")

    args = parser.parse_args()

    return run_suite(
        engine_path=args.engine,
        test_path=args.test,
        pass_rate=args.pass_rate,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    raise SystemExit(main())