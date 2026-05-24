from __future__ import annotations

import subprocess
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class SearchResult:
    bestmove: str = ""
    mate_score: Optional[int] = None
    cp_score: Optional[int] = None
    depth: Optional[int] = None
    nodes: Optional[int] = None
    nps: Optional[int] = None
    pv: list[str] = field(default_factory=list)
    raw_lines: list[str] = field(default_factory=list)

@dataclass
class PerftResult:
    nodes: int
    captures: int
    en_passants: int
    castles: int
    promotions: int
    checks: int

class UciEngine:
    def __init__(self, engine_path: Path):
        self.engine_path = engine_path
        self.process: Optional[subprocess.Popen[str]] = None

    def __enter__(self) -> "UciEngine":
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.stop()

    def start(self) -> None:
        self.process = subprocess.Popen(
            [str(self.engine_path)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )

        self.send("uci")
        self.read_until("uciok")

        self.send("isready")
        self.read_until("readyok")

    def stop(self) -> None:
        if self.process is None:
            return

        try:
            self.send("quit")
            self.process.wait(timeout=5)
        finally:
            self.process = None

    def send(self, command: str) -> None:
        if self.process is None or self.process.stdin is None:
            raise RuntimeError("engine is not running")

        self.process.stdin.write(command + "\n")
        self.process.stdin.flush()

    def read_line(self) -> str:
        if self.process is None or self.process.stdout is None:
            raise RuntimeError("engine is not running")

        line = self.process.stdout.readline()

        if line == "":
            raise RuntimeError("engine closed stdout")

        return line.strip()

    def read_until(self, target: str) -> list[str]:
        lines: list[str] = []

        while True:
            line = self.read_line()
            lines.append(line)

            if line == target:
                return lines

    def new_game(self) -> None:
        self.send("ucinewgame")
        self.send("isready")
        self.read_until("readyok")

    def set_position(self, fen: str, premove: str) -> None:
        if len(premove) >= 0:
            self.send(f"position fen {fen} moves {premove}")
        else:
            self.send(f"position fen {fen}")

    def go_depth(self, depth: int) -> SearchResult:
        self.send(f"go depth {depth}")
        return self._read_search_result()

    def _read_search_result(self) -> SearchResult:
        result = SearchResult()

        while True:
            line = self.read_line()
            result.raw_lines.append(line)

            if line.startswith("info "):
                self._parse_info_line(line, result)

            if line.startswith("bestmove"):
                parts = line.split()
                if len(parts) >= 2:
                    result.bestmove = parts[1]
                return result

    def _parse_info_line(self, line: str, result: SearchResult) -> None:
        parts = line.split()

        for i, part in enumerate(parts):
            if part == "depth" and i + 1 < len(parts):
                result.depth = int(parts[i + 1])

            elif part == "nodes" and i + 1 < len(parts):
                result.nodes = int(parts[i + 1])

            elif part == "nps" and i + 1 < len(parts):
                result.nps = int(parts[i + 1])

            elif part == "score" and i + 2 < len(parts):
                score_type = parts[i + 1]
                score_value = int(parts[i + 2])

                if score_type == "cp":
                    result.cp_score = score_value
                    result.mate_score = None

                elif score_type == "mate":
                    result.mate_score = score_value
                    result.cp_score = None

            elif part == "pv" and i + 1 < len(parts):
                result.pv = parts[i + 1:]
                break

    def go_perft(self, fen: str, depth: int) -> PerftResult:
        self.send(f"go perft {depth}")
        return self._read_perft_result()

    def _read_perft_result(self) -> PerftResult:
        raw = {
            key: int(value)
            for key, value in re.findall(r"(\w+)=(\d+)", self.read_line())
        }

        return PerftResult(
            nodes=raw["nodes"],
            captures=raw["captures"],
            en_passants=raw["enPassants"],
            castles=raw["castles"],
            promotions=raw["promotions"],
            checks=raw["checks"],
        )