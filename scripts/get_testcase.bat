@echo off

python regression_testcases/build_puzzle_suite.py ^
  --puzzle-file lichess_db_puzzle.csv ^
  --count 100 ^
  --max-rating 1500 ^
  --out smoke_100.csv