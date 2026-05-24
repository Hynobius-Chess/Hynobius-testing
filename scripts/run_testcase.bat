@echo off

python correctness_tests\test_controler.py ^
--engine Hynobius.exe ^
--tests ^
    correctness_tests\basic_tests\basic_deflection_1000.txt ^
    correctness_tests\basic_tests\basic_discover_attack_1000.txt ^
    correctness_tests\basic_tests\basic_fork_1000.txt ^
    correctness_tests\basic_tests\basic_hanging_piece_1000.txt ^
    correctness_tests\basic_tests\basic_pin_1000.txt ^
    correctness_tests\basic_tests\basic_skewer_1000.txt ^
--pass-rate 98

python correctness_tests\test_controler.py ^
--engine Hynobius.exe ^
--tests ^
    correctness_tests\hard_tests\hard_deflection_200.txt ^
    correctness_tests\hard_tests\hard_discover_attack_200.txt ^
    correctness_tests\hard_tests\hard_fork_200.txt ^
    correctness_tests\hard_tests\hard_hanging_piece_200.txt ^
    correctness_tests\hard_tests\hard_pin_200.txt ^
    correctness_tests\hard_tests\hard_skewer_200.txt ^
--pass-rate 70

python correctness_tests\test_controler.py ^
--engine Hynobius.exe ^
--tests ^
    correctness_tests\very_hard_tests\very_hard_very_long_40.txt ^
--pass-rate 0