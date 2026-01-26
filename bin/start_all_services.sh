#!/bin/bash

###############################################################################
# Sage - Start All Services Script
# 
# This script launches all Sage components in the correct order:
# 1. Docker Compose (Kafka, Zookeeper, PostgreSQL)
# 2. Sage Server (HTTP API)
# 3. Sage Worker (Task processor)
# 4. Sage UI (Web interface)
#
# Features:
# - Kafka health checking with retry mechanism
# - Service status reporting
# - Graceful error handling
###############################################################################

set -e  # Exit on error

# Configuration
RETRY_INTERVAL=5  # Seconds between Kafka retry attempts
MAX_KAFKA_RETRIES=12  # Maximum number of Kafka connection attempts (60 seconds total)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$SCRIPT_DIR/log"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Ensure log directory exists
mkdir -p "$LOG_DIR"

# Log files
DOCKER_LOG="$LOG_DIR/docker-compose.log"
SERVER_LOG="$LOG_DIR/sage_server.log"
WORKER_LOG="$LOG_DIR/sage_worker.log"
UI_LOG="$LOG_DIR/sage_ui.log"

# PIDs for cleanup
declare -a SERVICE_PIDS=()

###############################################################################
# Helper Functions
###############################################################################

print_header() {
    echo -e "${CYAN}╔═══════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║          SAGE - Starting All Services                     ║${NC}"
    echo -e "${CYAN}╚═══════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

print_step() {
    echo -e "${BLUE}▶ $1${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_info() {
    echo -e "${CYAN}ℹ $1${NC}"
}

cleanup() {
    echo ""
    print_warning "Cleaning up..."
    
    # Kill all spawned processes
    for pid in "${SERVICE_PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            print_info "Stopping process $pid"
            kill "$pid" 2>/dev/null || true
        fi
    done
    
    exit 1
}

# Trap Ctrl+C and cleanup
trap cleanup SIGINT SIGTERM

###############################################################################
# Step 1: Clean and Start Docker Compose
###############################################################################

cleanup_old_containers() {
    print_step "Cleaning up old containers..."
    
    cd "$SCRIPT_DIR/environment"
    
    # Stop and remove old containers
    docker-compose down > /dev/null 2>&1 || true
    
    print_success "Old containers cleaned up"
}

start_docker_compose() {
    print_step "Starting Docker Compose (Kafka, Zookeeper, PostgreSQL, Kafka UI)..."
    
    cd "$SCRIPT_DIR/environment"
    
    if docker-compose up -d > "$DOCKER_LOG" 2>&1; then
        print_success "Docker Compose started successfully"
        return 0
    else
        print_error "Failed to start Docker Compose"
        cat "$DOCKER_LOG"
        return 1
    fi
}

###############################################################################
# Step 2: Check Kafka Health with Retry Logic
###############################################################################

check_kafka_health() {
    local attempt=1
    
    print_step "Checking Kafka health..."
    
    while [ $attempt -le $MAX_KAFKA_RETRIES ]; do
        print_info "Kafka health check attempt $attempt/$MAX_KAFKA_RETRIES"
        
        # Check if Kafka container is running using docker ps
        local kafka_status=$(docker ps --filter "name=kafka" --filter "status=running" --format "{{.Names}}" 2>/dev/null | grep -E "^kafka$")
        
        if [ ! -z "$kafka_status" ]; then
            print_info "Kafka container is running, testing connectivity..."
            
            # Try to list topics (this confirms Kafka is actually ready)
            if docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
                print_success "Kafka is healthy and ready!"
                return 0
            else
                print_warning "Kafka container running but not accepting connections yet..."
            fi
        else
            # Check if Kafka container exists but is not running
            local kafka_exists=$(docker ps -a --filter "name=kafka" --format "{{.Names}}" 2>/dev/null | grep -E "^kafka$")
            
            if [ ! -z "$kafka_exists" ]; then
                local kafka_state=$(docker ps -a --filter "name=kafka" --format "{{.Status}}" 2>/dev/null | head -1)
                print_warning "Kafka container exists but is not running. Status: $kafka_state"
                
                # If Kafka exited, show last 10 lines of logs and restart it
                if echo "$kafka_state" | grep -q "Exited"; then
                    print_error "Kafka container failed to start. Last logs:"
                    docker logs kafka --tail 10 2>&1 | sed 's/^/    /'
                    
                    print_info "Attempting to restart Kafka container..."
                    cd "$SCRIPT_DIR/environment"
                    docker-compose restart kafka > /dev/null 2>&1
                    print_success "Kafka restart initiated"
                fi
            else
                print_warning "Kafka container not found yet..."
            fi
        fi
        
        if [ $attempt -lt $MAX_KAFKA_RETRIES ]; then
            print_info "Waiting $RETRY_INTERVAL seconds before next check..."
            sleep $RETRY_INTERVAL
        fi
        
        ((attempt++))
    done
    
    print_error "Kafka failed to become healthy after $MAX_KAFKA_RETRIES attempts"
    print_info "Final container status:"
    docker ps -a --filter "name=kafka" 2>&1 | sed 's/^/    /'
    return 1
}

###############################################################################
# Step 3: Check PostgreSQL Health
###############################################################################

check_postgres_health() {
    print_step "Checking PostgreSQL health..."
    
    local attempt=1
    local max_attempts=10
    
    while [ $attempt -le $max_attempts ]; do
        if docker exec postgres pg_isready -U sage > /dev/null 2>&1; then
            print_success "PostgreSQL is healthy and ready!"
            return 0
        fi
        
        print_warning "PostgreSQL not ready yet (attempt $attempt/$max_attempts)..."
        sleep 2
        ((attempt++))
    done
    
    print_error "PostgreSQL failed to become healthy"
    return 1
}

###############################################################################
# Step 4: Start Sage Server
###############################################################################

start_sage_server() {
    print_step "Starting Sage Server (HTTP API)..."
    
    cd "$SCRIPT_DIR"
    
    # Check if server binary exists or needs compilation
    if [ ! -f "$SCRIPT_DIR/target/release/sage_server" ] && [ ! -f "$SCRIPT_DIR/target/debug/sage_server" ]; then
        print_info "Compiling Sage Server..."
        cargo build --bin sage_server >> "$SERVER_LOG" 2>&1
    fi
    
    # Start the server in background
    cargo run --bin sage_server > "$SERVER_LOG" 2>&1 &
    local server_pid=$!
    SERVICE_PIDS+=($server_pid)
    
    # Wait a moment for server to start
    sleep 3
    
    # Check if server is still running
    if kill -0 $server_pid 2>/dev/null; then
        print_success "Sage Server started (PID: $server_pid)"
        print_info "Server running at http://localhost:4000"
        print_info "Logs: $SERVER_LOG"
        return 0
    else
        print_error "Sage Server failed to start"
        print_info "Check logs: $SERVER_LOG"
        tail -n 20 "$SERVER_LOG"
        return 1
    fi
}

###############################################################################
# Step 5: Start Sage Worker
###############################################################################

start_sage_worker() {
    print_step "Starting Sage Worker..."
    
    cd "$SCRIPT_DIR"
    
    # Check if worker binary exists or needs compilation
    if [ ! -f "$SCRIPT_DIR/target/release/sage_worker" ] && [ ! -f "$SCRIPT_DIR/target/debug/sage_worker" ]; then
        print_info "Compiling Sage Worker..."
        cargo build --bin sage_worker >> "$WORKER_LOG" 2>&1
    fi
    
    # Start the worker in background
    cargo run --bin sage_worker > "$WORKER_LOG" 2>&1 &
    local worker_pid=$!
    SERVICE_PIDS+=($worker_pid)
    
    # Wait a moment for worker to start
    sleep 3
    
    # Check if worker is still running
    if kill -0 $worker_pid 2>/dev/null; then
        print_success "Sage Worker started (PID: $worker_pid)"
        print_info "Logs: $WORKER_LOG"
        return 0
    else
        print_error "Sage Worker failed to start"
        print_info "Check logs: $WORKER_LOG"
        tail -n 20 "$WORKER_LOG"
        return 1
    fi
}

###############################################################################
# Step 6: Start Sage UI
###############################################################################

start_sage_ui() {
    print_step "Starting Sage UI (Web Interface)..."
    
    cd "$SCRIPT_DIR/sage_ui"
    
    # Check if node_modules exists
    if [ ! -d "node_modules" ]; then
        print_info "Installing Node.js dependencies..."
        npm install >> "$UI_LOG" 2>&1
    fi
    
    # Start the UI in background
    npm run dev > "$UI_LOG" 2>&1 &
    local ui_pid=$!
    SERVICE_PIDS+=($ui_pid)
    
    # Wait a moment for UI to start
    sleep 4
    
    # Check if UI is still running
    if kill -0 $ui_pid 2>/dev/null; then
        print_success "Sage UI started (PID: $ui_pid)"
        print_info "UI available at http://localhost:5173"
        print_info "Logs: $UI_LOG"
        return 0
    else
        print_error "Sage UI failed to start"
        print_info "Check logs: $UI_LOG"
        tail -n 20 "$UI_LOG"
        return 1
    fi
}

###############################################################################
# Step 7: Report Service Status
###############################################################################

report_status() {
    echo ""
    echo -e "${CYAN}╔═══════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║                   ALL SERVICES ARE UP!                    ║${NC}"
    echo -e "${CYAN}╚═══════════════════════════════════════════════════════════╝${NC}"
    echo ""
    
    print_success "Service Status Summary:"
    echo ""
    echo -e "  ${GREEN}✓${NC} Docker Compose:  Running"
    echo -e "    - Kafka:           localhost:9092"
    echo -e "    - Kafka UI:        http://localhost:8080"
    echo -e "    - PostgreSQL:      localhost:5432"
    echo -e "    - Zookeeper:       localhost:2181"
    echo ""
    echo -e "  ${GREEN}✓${NC} Sage Server:     http://localhost:4000 (PID: ${SERVICE_PIDS[0]})"
    echo -e "  ${GREEN}✓${NC} Sage Worker:     Running (PID: ${SERVICE_PIDS[1]})"
    echo -e "  ${GREEN}✓${NC} Sage UI:         http://localhost:5173 (PID: ${SERVICE_PIDS[2]})"
    echo ""
    
    print_info "Log Files:"
    echo -e "    - Docker:  $DOCKER_LOG"
    echo -e "    - Server:  $SERVER_LOG"
    echo -e "    - Worker:  $WORKER_LOG"
    echo -e "    - UI:      $UI_LOG"
    echo ""
    
    print_info "Quick Test:"
    echo -e "    curl -X POST http://localhost:4000/tasks/v1/start \\"
    echo -e "      -H 'Content-Type: application/json' \\"
    echo -e "      -d '{\"requestor_id\": 1, \"task_name\": \"PrimeTask\", \"task_context\": \"{\\\"limit\\\": 1000}\"}'"
    echo ""
    
    print_warning "Press Ctrl+C to stop all services"
    echo ""
    
    # Keep script running
    wait
}

###############################################################################
# Main Execution Flow
###############################################################################

main() {
    print_header
    
    # Step 0: Clean up old containers
    cleanup_old_containers
    
    echo ""
    
    # Step 1: Start Docker Compose
    if ! start_docker_compose; then
        print_error "Failed at Step 1: Docker Compose"
        exit 1
    fi
    
    echo ""
    
    # Step 2: Check Kafka Health (with retry)
    if ! check_kafka_health; then
        print_error "Failed at Step 2: Kafka Health Check"
        print_info "You may want to check Docker logs:"
        echo "    docker logs kafka"
        exit 1
    fi
    
    echo ""
    
    # Step 3: Check PostgreSQL Health
    if ! check_postgres_health; then
        print_error "Failed at Step 3: PostgreSQL Health Check"
        print_info "You may want to check Docker logs:"
        echo "    docker logs postgres"
        exit 1
    fi
    
    echo ""
    
    # Step 4: Start Sage Server
    if ! start_sage_server; then
        print_error "Failed at Step 4: Sage Server"
        exit 1
    fi
    
    echo ""
    
    # Step 5: Start Sage Worker
    if ! start_sage_worker; then
        print_error "Failed at Step 5: Sage Worker"
        exit 1
    fi
    
    echo ""
    
    # Step 6: Start Sage UI
    if ! start_sage_ui; then
        print_error "Failed at Step 6: Sage UI"
        exit 1
    fi
    
    echo ""
    
    # Step 7: Report Status
    report_status
}

# Run main function
main
