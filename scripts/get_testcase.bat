@echo off

python regression_testcases/build_puzzle_suite.py ^
  --puzzle-file lichess_db_puzzle.csv ^
  --count 200 ^
  --include-tags "veryLong" ^
  --out testcase.csv

python regression_testcases/transform_puzzle.py^
  --input testcase.csv ^
  --output hard_very_long_200.txt ^
  --template "name='{id}' fen='{fen}' depth=10 premove={premove} bestmove={bestmove}"