#!/bin/bash

###############################################################################
# Sage - Stop All Services Script
# 
# This script stops all Sage components and Docker containers:
# - Sage Server (HTTP API)
# - Sage Worker (Task processor)
# - Sage UI (Web interface)
# - Docker Compose (Kafka, Zookeeper, PostgreSQL, Kafka UI)
#
# Features:
# - Graceful shutdown attempts
# - Force kill if graceful shutdown fails
# - Docker cleanup
###############################################################################

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$SCRIPT_DIR/log"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

###############################################################################
# Helper Functions
###############################################################################

print_header() {
    echo -e "${CYAN}╔═══════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║          SAGE - Stopping All Services                     ║${NC}"
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

###############################################################################
# Stop Functions
###############################################################################

stop_sage_processes() {
    print_step "Stopping Sage processes (Server, Worker, UI)..."
    
    local processes_found=false
    
    # Find and kill sage_server processes
    local server_pids=$(pgrep -f "sage_server" 2>/dev/null)
    if [ ! -z "$server_pids" ]; then
        processes_found=true
        print_info "Found sage_server processes: $server_pids"
        for pid in $server_pids; do
            kill $pid 2>/dev/null && print_success "Killed sage_server (PID: $pid)" || true
        done
        sleep 1
        # Force kill if still running
        for pid in $server_pids; do
            if kill -0 $pid 2>/dev/null; then
                kill -9 $pid 2>/dev/null && print_warning "Force killed sage_server (PID: $pid)" || true
            fi
        done
    fi
    
    # Find and kill sage_worker processes
    local worker_pids=$(pgrep -f "sage_worker" 2>/dev/null)
    if [ ! -z "$worker_pids" ]; then
        processes_found=true
        print_info "Found sage_worker processes: $worker_pids"
        for pid in $worker_pids; do
            kill $pid 2>/dev/null && print_success "Killed sage_worker (PID: $pid)" || true
        done
        sleep 1
        # Force kill if still running
        for pid in $worker_pids; do
            if kill -0 $pid 2>/dev/null; then
                kill -9 $pid 2>/dev/null && print_warning "Force killed sage_worker (PID: $pid)" || true
            fi
        done
    fi
    
    # Find and kill npm/vite processes (Sage UI)
    local ui_pids=$(pgrep -f "vite.*sage_ui" 2>/dev/null)
    if [ ! -z "$ui_pids" ]; then
        processes_found=true
        print_info "Found sage_ui processes: $ui_pids"
        for pid in $ui_pids; do
            kill $pid 2>/dev/null && print_success "Killed sage_ui (PID: $pid)" || true
        done
        sleep 1
        # Force kill if still running
        for pid in $ui_pids; do
            if kill -0 $pid 2>/dev/null; then
                kill -9 $pid 2>/dev/null && print_warning "Force killed sage_ui (PID: $pid)" || true
            fi
        done
    fi
    
    # Also kill any cargo run processes related to sage
    local cargo_pids=$(pgrep -f "cargo run.*sage" 2>/dev/null)
    if [ ! -z "$cargo_pids" ]; then
        processes_found=true
        print_info "Found cargo processes: $cargo_pids"
        for pid in $cargo_pids; do
            kill $pid 2>/dev/null && print_success "Killed cargo process (PID: $pid)" || true
        done
        sleep 1
        # Force kill if still running
        for pid in $cargo_pids; do
            if kill -0 $pid 2>/dev/null; then
                kill -9 $pid 2>/dev/null && print_warning "Force killed cargo process (PID: $pid)" || true
            fi
        done
    fi
    
    if [ "$processes_found" = true ]; then
        print_success "All Sage processes stopped"
    else
        print_info "No Sage processes found running"
    fi
}

stop_docker_containers() {
    print_step "Stopping Docker Compose containers..."
    
    cd "$SCRIPT_DIR/environment"
    
    # Check if containers are running
    local running_containers=$(docker-compose ps -q 2>/dev/null)
    
    if [ -z "$running_containers" ]; then
        print_info "No Docker containers found running"
        return 0
    fi
    
    print_info "Stopping containers..."
    if docker-compose down > /dev/null 2>&1; then
        print_success "Docker Compose containers stopped and removed"
    else
        print_warning "docker-compose down failed, trying to stop containers manually..."
        
        # Try to stop individual containers
        local containers=("kafka" "zookeeper" "postgres" "kafka-ui")
        for container in "${containers[@]}"; do
            if docker ps -q -f name=$container > /dev/null 2>&1; then
                docker stop $container > /dev/null 2>&1 && print_success "Stopped $container" || print_warning "Failed to stop $container"
                docker rm $container > /dev/null 2>&1 || true
            fi
        done
    fi
}

verify_cleanup() {
    print_step "Verifying cleanup..."
    
    local issues_found=false
    
    # Check for remaining Sage processes
    local remaining_procs=$(pgrep -f "sage_server|sage_worker|vite.*sage_ui|cargo run.*sage" 2>/dev/null)
    if [ ! -z "$remaining_procs" ]; then
        issues_found=true
        print_warning "Some Sage processes still running: $remaining_procs"
    fi
    
    # Check for remaining Docker containers
    cd "$SCRIPT_DIR/environment"
    local remaining_containers=$(docker-compose ps -q 2>/dev/null)
    if [ ! -z "$remaining_containers" ]; then
        issues_found=true
        print_warning "Some Docker containers still running"
        docker-compose ps 2>/dev/null | sed 's/^/    /'
    fi
    
    if [ "$issues_found" = false ]; then
        print_success "All services successfully stopped"
    else
        print_warning "Some services may still be running. You can manually check with:"
        echo "    ps aux | grep -E 'sage_server|sage_worker|sage_ui'"
        echo "    docker ps -a"
    fi
}

###############################################################################
# Main Execution Flow
###############################################################################

main() {
    print_header
    
    # Step 1: Stop Sage processes
    stop_sage_processes
    echo ""
    
    # Step 2: Stop Docker containers
    stop_docker_containers
    echo ""
    
    # Step 3: Verify cleanup
    verify_cleanup
    echo ""
    
    echo -e "${CYAN}╔═══════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║              All Services Stopped Successfully            ║${NC}"
    echo -e "${CYAN}╚═══════════════════════════════════════════════════════════╝${NC}"
}

# Run main function
main
