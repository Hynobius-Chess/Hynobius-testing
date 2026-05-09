@echo off

set BASELINE=dev
set CANDIDATE=bugfix/tt
set START_BATCH=0
set BATCH_COUNT=5
set PAIRS_PER_BATCH=5
set MAX_BATCH_COUNT=100
set TC="10+0.1"

set /a GAMES_PER_ROUND=%BATCH_COUNT% * %PAIRS_PER_BATCH% * 2
set /a MAX_TOTAL_GAMES=%MAX_BATCH_COUNT% * %PAIRS_PER_BATCH% * 2

echo You are going to run an AB test with:

echo - baseline: %BASELINE% 
echo - candidate: %CANDIDATE%

echo - time control: %TC%
echo - games per round: %GAMES_PER_ROUND%
echo - max total games: %MAX_TOTAL_GAMES%

echo Are you sure to run this AB test?

pause

python sprt_controller.py ^
  --workflow ab-test-matrix.yml ^
  --workflow-ref main ^
  --baseline-ref %BASELINE% ^
  --candidate-ref %CANDIDATE% ^
  --start-batch %START_BATCH% ^
  --batches-per-round %BATCH_COUNT% ^
  --max-batches %MAX_BATCH_COUNT% ^
  --pairs-per-batch %PAIRS_PER_BATCH% ^
  --tc "%TC%" ^
  --sprt-elo0 0 ^
  --sprt-elo1 10 ^
  --sprt-alpha 0.05 ^
  --sprt-beta 0.05 ^
  --poll-seconds 10

pause