#!/bin/bash
set -e

echo "======================================"
echo "EM-√ COMPREHENSIVE TEST SUITE"
echo "======================================"
echo ""

# Color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

FAILED_TESTS=()
PASSED_TESTS=()

run_test_suite() {
    local name=$1
    local command=$2
    
    echo -e "${YELLOW}Running: $name${NC}"
    if eval "$command" > /tmp/test_output.log 2>&1; then
        echo -e "${GREEN}✓ $name PASSED${NC}"
        echo ""
        PASSED_TESTS+=("$name")
        return 0
    else
        echo -e "${RED}✗ $name FAILED${NC}"
        echo -e "${RED}Last 20 lines of output:${NC}"
        tail -20 /tmp/test_output.log
        echo ""
        FAILED_TESTS+=("$name")
        return 1
    fi
}

# Print header
echo -e "${BLUE}Running with workspace: $(pwd)${NC}"
echo ""

# 1. Unit Tests
echo "======== PHASE 1: UNIT TESTS ========"
run_test_suite "SpillManager Tests" "cargo test --test spill_manager_tests --no-default-features"
run_test_suite "RowBatch Helper Tests" "cargo test --test rowbatch_helpers_tests --no-default-features"
run_test_suite "Memory Budget Tests" "cargo test --test memory_budget_tests --no-default-features"

# 2. Integration Tests
echo "======== PHASE 2: INTEGRATION TESTS ========"
run_test_suite "Full Pipeline Tests" "cargo test --test integration_tests --no-default-features"

# 3. Existing E2E Tests
echo "======== PHASE 3: E2E SMOKE TESTS ========"
run_test_suite "E2E Smoke Test" "cargo test --test e2e_smoke --no-default-features"

# 4. All other unit tests in crates
echo "======== PHASE 4: CRATE-LEVEL TESTS ========"
run_test_suite "All Crate Tests" "cargo test --all --no-default-features --lib"

# 5. Expression Engine Tests
echo "======== PHASE 5: EXPRESSION ENGINE TESTS ========"
run_test_suite "Expression Parsing Tests" "cargo test --test expression_tests --no-default-features"
run_test_suite "Expression Evaluation Tests" "cargo test --test expression_evaluation_tests --no-default-features"

# 6. Column Statistics Tests
echo "======== PHASE 6: COLUMN STATISTICS TESTS ========"
run_test_suite "Column Statistics Tests" "cargo test --test column_stats_tests --no-default-features"
run_test_suite "Cost Estimation with Stats Tests" "cargo test --test cost_estimation_tests --no-default-features"

# 7. Error Handling Tests
echo "======== PHASE 7: ERROR HANDLING TESTS ========"
run_test_suite "Error Context Tests" "cargo test --test error_handling_tests --no-default-features"
run_test_suite "Error Recovery Tests" "cargo test --test error_recovery_tests --no-default-features"

# 8. Operator Tests
echo "======== PHASE 8: OPERATOR TESTS ========"
run_test_suite "Merge Join Tests" "cargo test --test merge_join_tests --no-default-features"
run_test_suite "Filter with Expressions Tests" "cargo test --test filter_expression_tests --no-default-features"
run_test_suite "Grace Hash Join Tests" "cargo test --test grace_hash_join_tests --no-default-features"

# 9. Feature-Specific Tests (conditional)
echo "======== PHASE 9: FEATURE-SPECIFIC TESTS ========"
if cargo check --features parquet --no-default-features 2>/dev/null; then
    run_test_suite "Arrow Conversion Tests" "cargo test --test arrow_conversion_tests --features parquet --no-default-features"
    run_test_suite "Parquet I/O Tests" "cargo test --test parquet_io_tests --features parquet --no-default-features"
    run_test_suite "Parquet Pipeline Tests" "cargo test --test integration_tests --features parquet --no-default-features -- test_parquet"
else
    echo -e "${YELLOW}Skipping Parquet/Arrow tests (feature not available)${NC}"
fi

# 10. CLI Tests
echo "======== PHASE 10: CLI TESTS ========"
run_test_suite "CLI YAML Parsing Tests" "cargo test --test cli_yaml_tests --no-default-features"
run_test_suite "CLI Validation Tests" "cargo test --test cli_validation_tests --no-default-features"

# Summary
echo ""
echo "======================================"
echo "TEST SUMMARY"
echo "======================================"
echo ""

echo -e "${BLUE}Tests Passed: ${#PASSED_TESTS[@]}${NC}"
for test in "${PASSED_TESTS[@]}"; do
    echo -e "${GREEN}  ✓ $test${NC}"
done

echo ""

if [ ${#FAILED_TESTS[@]} -eq 0 ]; then
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}ALL TESTS PASSED! ✓${NC}"
    echo -e "${GREEN}========================================${NC}"
    exit 0
else
    echo -e "${RED}Tests Failed: ${#FAILED_TESTS[@]}${NC}"
    for test in "${FAILED_TESTS[@]}"; do
        echo -e "${RED}  ✗ $test${NC}"
    done
    echo ""
    echo -e "${RED}========================================${NC}"
    echo -e "${RED}SOME TESTS FAILED${NC}"
    echo -e "${RED}========================================${NC}"
    exit 1
fi

