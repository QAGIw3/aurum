#!/bin/bash
# Run integration tests for Aurum platform
# This script starts test databases and runs integration tests

set -e

echo "=================================="
echo "Aurum Integration Test Runner"
echo "=================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}Error: docker-compose not found${NC}"
    echo "Please install docker-compose to run integration tests"
    exit 1
fi

# Start test databases
echo -e "${YELLOW}Starting test databases...${NC}"
docker-compose -f docker-compose.test.yml up -d

# Wait for databases to be ready
echo -e "${YELLOW}Waiting for databases to be ready...${NC}"
sleep 10

# Check database health
echo -e "${YELLOW}Checking database health...${NC}"

# Check PostgreSQL
echo -n "PostgreSQL: "
if docker-compose -f docker-compose.test.yml exec -T postgres-test pg_isready -U aurum > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Ready${NC}"
else
    echo -e "${RED}✗ Not ready${NC}"
fi

# Check TimescaleDB
echo -n "TimescaleDB: "
if docker-compose -f docker-compose.test.yml exec -T timescale-test pg_isready -U aurum > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Ready${NC}"
else
    echo -e "${RED}✗ Not ready${NC}"
fi

echo ""

# Load test data
echo -e "${YELLOW}Loading test data fixtures...${NC}"
if [ -f "tests/integration/fixtures/test_data.sql" ]; then
    docker-compose -f docker-compose.test.yml exec -T postgres-test \
        psql -U aurum -d aurum_test < tests/integration/fixtures/test_data.sql
    echo -e "${GREEN}✓ Test data loaded${NC}"
else
    echo -e "${YELLOW}⚠ No test data fixtures found${NC}"
fi

echo ""

# Run integration tests
echo -e "${YELLOW}Running integration tests...${NC}"
echo ""

# Run all integration tests
pytest tests/integration/data/ -v -m integration "$@"

TEST_EXIT_CODE=$?

echo ""

# Cleanup
if [ "$1" != "--no-cleanup" ]; then
    echo -e "${YELLOW}Stopping test databases...${NC}"
    docker-compose -f docker-compose.test.yml down
    echo -e "${GREEN}✓ Test databases stopped${NC}"
else
    echo -e "${YELLOW}Test databases left running (--no-cleanup specified)${NC}"
    echo "To stop manually: docker-compose -f docker-compose.test.yml down"
fi

echo ""

# Summary
if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}=================================="
    echo -e "✓ Integration tests passed!"
    echo -e "==================================${NC}"
else
    echo -e "${RED}=================================="
    echo -e "✗ Integration tests failed!"
    echo -e "==================================${NC}"
    exit $TEST_EXIT_CODE
fi

