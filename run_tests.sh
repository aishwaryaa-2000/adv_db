#!/bin/bash
# run_tests.sh - 
# Usage: ./run_tests.sh [test_number]
#   No argument: runs all tests
#   With number: runs specific test (e.g., ./run_tests.sh 18)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if repcrec executable exists
if [ ! -f "./repcrec" ]; then
    echo -e "${YELLOW}Building repcrec...${NC}"
    make
    if [ $? -ne 0 ]; then
        echo -e "${RED}Build failed!${NC}"
        exit 1
    fi
fi

# Function to run a single test
run_test() {
    local test_num=$1
    local test_name=$2
    local test_input=$3
    local expected=$4
    
    echo -e "${YELLOW}Test $test_num: $test_name ${NC}"
    result=$(echo "$test_input" | ./repcrec 2>&1)
    
    # Extract commit/abort lines
    outcome=$(echo "$result" | grep -E "(commits|aborts|x[0-9]+:.*[0-9]|waits|Retry)")
    echo "$outcome"
    echo ""
}

# Test 1
test1() {
    run_test "1" "First-Committer-Wins" 'begin(T1)
begin(T2)
W(T1,x1,101)
W(T2,x2,202)
W(T1,x2,102)
W(T2,x1,201)
end(T2)
end(T1)
dump()' "T2 commits, T1 aborts"
}

# Test 2
test2() {
    run_test "2" "Snapshot Isolation - Both Commit" 'begin(T1)
begin(T2)
W(T1,x1,101)
R(T2,x2)
W(T1,x2,102)
R(T2,x1)
end(T1)
end(T2)
dump()' "Both commit, T2 reads x1=10, x2=20"
}

# Test 3
test3() {
    run_test "3" "Site Failure Non-Critical" 'begin(T1)
begin(T2)
R(T1,x3)
fail(2)
W(T2,x8,88)
R(T2,x3)
W(T1,x5,91)
end(T2)
recover(2)
end(T1)' "Both commit"
}

# Test 3.5
test3_5() {
    run_test "3.5" "T2 Aborts - Wrote Before Failure" 'begin(T1)
begin(T2)
R(T1,x3)
W(T2,x8,88)
fail(2)
R(T2,x3)
W(T1,x4,91)
recover(2)
end(T2)
end(T1)' "T1 commits, T2 aborts"
}

# Test 3.7
test3_7() {
    run_test "3.7" "T2 Aborts - Site Failure" 'begin(T1)
begin(T2)
R(T1,x3)
W(T2,x8,88)
fail(2)
R(T2,x3)
recover(2)
W(T1,x4,91)
end(T2)
end(T1)' "T1 commits, T2 aborts"
}

# Test 4
test4() {
    run_test "4" "T1 Aborts - Write Site Failed" 'begin(T1)
begin(T2)
W(T1,x1,512)
fail(2)
W(T2,x8,88)
R(T2,x3)
R(T1,x5)
end(T2)
recover(2)
end(T1)' "T2 commits, T1 aborts"
}

# Test 5
test5() {
    run_test "5" "T1 Aborts - Replicated Write Site Failed" 'begin(T1)
begin(T2)
W(T1,x6,66)
fail(2)
W(T2,x8,88)
R(T2,x3)
R(T1,x5)
end(T2)
recover(2)
end(T1)' "T2 commits, T1 aborts"
}

# Test 6
test6() {
    run_test "6" "Recovery - Odd Variable Readable" 'begin(T1)
begin(T2)
fail(3)
fail(4)
R(T1,x1)
W(T2,x8,88)
end(T1)
recover(4)
recover(3)
R(T2,x3)
end(T2)
dump()' "Both commit"
}

# Test 7
test7() {
    run_test "7" "Multiversion Read - T2 reads x3=30" 'begin(T1)
begin(T2)
R(T2,x1)
R(T2,x2)
W(T1,x3,33)
end(T1)
R(T2,x3)
end(T2)' "Both commit, T2 reads x3=30"
}

# Test 8
test8() {
    run_test "8" "Multiversion - T3 reads 33, T2 reads 30" 'begin(T1)
begin(T2)
R(T2,x1)
R(T2,x2)
W(T1,x3,33)
end(T1)
begin(T3)
R(T3,x3)
R(T2,x3)
end(T2)
end(T3)' "All commit, T3 reads x3=33, T2 reads x3=30"
}

# Test 9
test9() {
    run_test "9" "Snapshot - T1 reads x2=20" 'begin(T3)
begin(T1)
begin(T2)
W(T3,x2,22)
W(T2,x4,44)
R(T3,x4)
end(T2)
end(T3)
R(T1,x2)
end(T1)' "All commit, T3 reads x4=40, T1 reads x2=20"
}

# Test 10
test10() {
    run_test "10" "Snapshot - T1 reads x2=22" 'begin(T2)
begin(T3)
W(T3,x2,22)
W(T2,x4,44)
R(T3,x4)
end(T2)
end(T3)
begin(T1)
R(T1,x2)
end(T1)' "All commit, T1 reads x2=22"
}

# Test 11
test11() {
    run_test "11" "Both Commit - Read then Write" 'begin(T1)
begin(T2)
R(T1,x2)
R(T2,x2)
W(T2,x2,10)
end(T1)
end(T2)' "Both commit"
}

# Test 12
test12() {
    run_test "12" "Both Commit - T1 ends before T2 writes" 'begin(T1)
begin(T2)
R(T1,x2)
R(T2,x2)
end(T1)
W(T2,x2,10)
end(T2)' "Both commit"
}

# Test 13
test13() {
    run_test "13" "FCW - Only T3 Commits" 'begin(T1)
begin(T2)
begin(T3)
W(T3,x2,10)
W(T2,x2,20)
W(T1,x2,30)
end(T3)
end(T2)
end(T1)' "T3 commits, T1/T2 abort"
}

# Test 14
test14() {
    run_test "14" "FCW - Only T1 Commits" 'begin(T1)
begin(T2)
begin(T3)
W(T3,x2,10)
W(T1,x2,20)
W(T2,x2,30)
end(T1)
end(T3)
end(T2)' "T1 commits, T2/T3 abort"
}

# Test 15
test15() {
    run_test "15" "Site Failure + FCW" 'begin(T5)
begin(T4)
begin(T3)
begin(T2)
begin(T1)
W(T1,x4,5)
fail(2)
W(T2,x4,44)
recover(2)
W(T3,x4,55)
W(T4,x4,66)
W(T5,x4,77)
end(T1)
end(T2)
end(T3)
end(T4)
end(T5)' "T1 aborts (site), T2 commits, T3/T4/T5 abort (FCW)"
}

# Test 16
test16() {
    run_test "16" "Snapshot Reads" 'begin(T3)
begin(T2)
W(T3,x2,22)
W(T2,x4,44)
R(T3,x4)
end(T2)
end(T3)
begin(T1)
R(T1,x2)
end(T1)' "All commit, T1 reads x2=22"
}

# Test 17
test17() {
    run_test "17" "T3 Aborts - Site 4 Failure" 'begin(T3)
begin(T2)
W(T3,x2,22)
W(T2,x3,44)
R(T3,x3)
end(T2)
fail(4)
end(T3)
begin(T1)
R(T1,x2)
end(T1)' "T2 commits, T3 aborts, T1 reads x2=20"
}

# Test 18
test18() {
    run_test "18" "RW Cycle - T5 Aborts" 'begin(T1)
begin(T2)
begin(T3)
begin(T4)
begin(T5)
R(T4,x4)
R(T5,x5)
R(T1,x1)
W(T1,x2,10)
R(T2,x2)
W(T2,x3,20)
R(T3,x3)
W(T3,x4,30)
W(T4,x5,40)
W(T5,x1,50)
end(T4)
end(T3)
end(T2)
end(T1)
end(T5)' "T1-T4 commit, T5 aborts (RW-cycle)"
}

# Test 19
test19() {
    run_test "19" "RW Cycle with Failure" 'begin(T1)
begin(T2)
begin(T3)
begin(T4)
begin(T5)
W(T3,x3,300)
fail(4)
recover(4)
R(T4,x4)
R(T5,x5)
R(T1,x6)
R(T2,x2)
W(T1,x2,10)
W(T2,x3,20)
W(T3,x4,30)
W(T5,x1,50)
end(T5)
W(T4,x5,40)
end(T4)
end(T3)
end(T2)
end(T1)' "T3 aborts (site), others commit"
}

# Test 20
test20() {
    run_test "20" "FCW - T2 Aborts" 'begin(T1)
begin(T2)
R(T2,x2)
W(T1,x2,202)
W(T2,x2,302)
end(T1)
end(T2)
dump()' "T1 commits, T2 aborts"
}

# Test 21
test21() {
    run_test "21" "Simple RW Cycle" 'begin(T1)
begin(T2)
R(T1,x2)
R(T2,x4)
W(T1,x4,30)
W(T2,x2,90)
end(T1)
end(T2)' "T1 commits, T2 aborts (RW-cycle)"
}

# Test 22
test22() {
    run_test "22" "WW Closes RW Cycle" 'begin(T1)
begin(T2)
W(T1,x2,80)
W(T1,x4,50)
R(T2,x4)
end(T1)
W(T2,x6,90)
begin(T3)
R(T3,x6)
W(T3,x2,70)
end(T2)
end(T3)' "T1/T2 commit, T3 aborts (RW-cycle)"
}

# Test 23
test23() {
    run_test "23" "No Valid Snapshot - T3 Aborts" 'begin(T1)
begin(T2)
fail(3)
fail(4)
R(T1,x1)
W(T2,x8,88)
end(T1)
recover(4)
recover(3)
R(T2,x3)
end(T2)
fail(1)
fail(2)
fail(5)
fail(6)
fail(7)
fail(8)
fail(9)
fail(10)
begin(T3)
R(T3,x8)' "T1/T2 commit, T3 aborts (no valid snapshot)"
}

# Test 24
test24() {
    run_test "24" "T4 Commit Not Visible to T3" 'begin(T1)
begin(T2)
fail(3)
fail(4)
R(T1,x1)
W(T2,x8,88)
end(T1)
recover(4)
recover(3)
R(T2,x3)
end(T2)
fail(1)
fail(2)
fail(5)
fail(6)
fail(7)
fail(8)
fail(9)
fail(10)
begin(T3)
begin(T4)
W(T4,x8,99)
end(T4)
R(T3,x8)' "T3 aborts (no valid snapshot)"
}

# Test 25
test25() {
    run_test "25" "T3 Waits for Site 2" 'begin(T1)
begin(T2)
fail(3)
fail(4)
R(T1,x1)
W(T2,x8,88)
end(T1)
recover(4)
recover(3)
R(T2,x3)
end(T2)
fail(1)
fail(5)
fail(6)
fail(7)
fail(8)
fail(9)
fail(10)
begin(T3)
fail(2)
begin(T4)
W(T4,x8,99)
end(T4)
R(T3,x8)
recover(2)
end(T3)' "T3 waits, reads x8=88, commits"
}

# Main execution
echo -e "${GREEN}RepCRec Test Suite - Tests mentioned in official website${NC}"
echo ""

if [ $# -eq 0 ]; then
    # Run all tests
    test1
    test2
    test3
    test3_5
    test3_7
    test4
    test5
    test6
    test7
    test8
    test9
    test10
    test11
    test12
    test13
    test14
    test15
    test16
    test17
    test18
    test19
    test20
    test21
    test22
    test23
    test24
    test25
    
    echo -e "${GREEN}All tests completed!${NC}"
else
    # Run specific test
    case $1 in
        1) test1 ;;
        2) test2 ;;
        3) test3 ;;
        3.5) test3_5 ;;
        3.7) test3_7 ;;
        4) test4 ;;
        5) test5 ;;
        6) test6 ;;
        7) test7 ;;
        8) test8 ;;
        9) test9 ;;
        10) test10 ;;
        11) test11 ;;
        12) test12 ;;
        13) test13 ;;
        14) test14 ;;
        15) test15 ;;
        16) test16 ;;
        17) test17 ;;
        18) test18 ;;
        19) test19 ;;
        20) test20 ;;
        21) test21 ;;
        22) test22 ;;
        23) test23 ;;
        24) test24 ;;
        25) test25 ;;
        *) echo "Unknown test: $1. Valid tests: 1-25, 3.5, 3.7" ;;
    esac
fi