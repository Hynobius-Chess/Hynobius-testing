import shlex
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
from pathlib import Path


class TestType(str, Enum):
    PERFT = "perft"
    BESTMOVE = "bestmove"
    MATE = "mate"
    BENCH = "bench"


@dataclass
class Expected:
    # UCI common
    bestmove: Optional[list[str]] = None
    score_cp: Optional[int] = None
    score_mate: Optional[int] = None

    # perft
    nodes: Optional[int] = None
    captures: Optional[int] = None
    enpassants: Optional[int] = None
    castles: Optional[int] = None
    promotions: Optional[int] = None
    checks: Optional[int] = None


@dataclass
class TestSpec:
    test_type: TestType
    timeout_sec: float = 10.0


@dataclass
class TestCase:
    name: str
    fen: str
    premove: str
    depth: int = 1
    expected: Expected = field(default_factory=Expected)
    raw: dict[str, str] = field(default_factory=dict)


@dataclass
class TestSuite:
    spec: TestSpec
    cases: list[TestCase]


def parse_kv_line(line: str) -> dict[str, str]:
    result: dict[str, str] = {}

    for token in shlex.split(line):
        if "=" not in token:
            continue

        key, value = token.split("=", 1)
        result[key] = value

    return result


def parse_optional_int(data: dict[str, str], key: str) -> Optional[int]:
    if key not in data:
        return None
    
    if data[key] == 'None':
        return None

    try:
        return int(data[key])
    except ValueError as e:
        raise ValueError(f"{key} must be int, got {data[key]!r}") from e


def parse_optional_float(data: dict[str, str], key: str) -> Optional[float]:
    if key not in data:
        return None

    try:
        return float(data[key])
    except ValueError as e:
        raise ValueError(f"{key} must be float, got {data[key]!r}") from e


def parse_expected(data: dict[str, str]) -> Expected:
    expected = Expected()

    if "bestmove" in data:
        expected.bestmove = data["bestmove"].split("|")

    expected.score_cp = parse_optional_int(data, "score_cp")
    expected.score_mate = parse_optional_int(data, "score_mate")

    expected.nodes = parse_optional_int(data, "nodes")
    expected.captures = parse_optional_int(data, "captures")

    expected.enpassants = parse_optional_int(data, "enpassants")

    expected.castles = parse_optional_int(data, "castles")
    expected.promotions = parse_optional_int(data, "promotions")
    expected.checks = parse_optional_int(data, "checks")

    return expected


def parse_test_case(data: dict[str, str]) -> TestCase:
    if "name" not in data:
        raise ValueError("case missing required field: name")

    if "fen" not in data:
        raise ValueError(f"case {data['name']} missing required field: fen")

    depth = parse_optional_int(data, "depth") or 1

    return TestCase(
        name=data["name"],
        fen=data["fen"],
        premove="" if "premove" not in data else data["premove"],
        depth=depth,
        expected=parse_expected(data),
        raw=data,
    )


def parse_header_line(line: str) -> tuple[str, str] | None:
    if not line.startswith("#"):
        return None

    header = line[1:].strip()

    if ":" not in header:
        return None

    key, value = header.split(":", 1)
    return key.strip(), value.strip()


def load_test_suite(path: Path) -> TestSuite:
    header_data: dict[str, str] = {}
    cases: list[TestCase] = []

    with open(path, "r", encoding="utf-8") as f:
        for line_no, raw_line in enumerate(f, start=1):
            line = raw_line.strip()

            if not line:
                continue

            if line.startswith("#"):
                parsed_header = parse_header_line(line)

                if parsed_header is not None:
                    key, value = parsed_header
                    header_data[key] = value

                continue

            try:
                data = parse_kv_line(line)
                cases.append(parse_test_case(data))
            except Exception as e:
                raise ValueError(f"{path}:{line_no}: {e}") from e

    if "type" not in header_data:
        raise ValueError(f"{path}: missing header '# type: ...'")

    try:
        test_type = TestType(header_data["type"])
    except ValueError as e:
        raise ValueError(f"unknown test type: {header_data['type']}") from e

    timeout_sec = parse_optional_float(header_data, "timeout") or 10.0

    spec = TestSpec(
        test_type=test_type,
        timeout_sec=timeout_sec,
    )

    return TestSuite(
        spec=spec,
        cases=cases,
    )

def print_test_info(test: TestSuite):
    print(f"Testing type: {test.spec.test_type}")
    print(f"Testcases loaded: {len(test.cases)}")