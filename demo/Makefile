COMPOSE ?= podman compose          # or: make up COMPOSE="docker compose"
.PHONY: up down reset backend sidecar frontend dev check full load load-report

up:       ; $(COMPOSE) up -d postgres mongo && ./scripts/wait-db.sh
down:     ; $(COMPOSE) down
reset:    ; $(COMPOSE) down -v      # WARNING: erases EVERYTHING, including the event journal (dev only)
backend:  ; cargo run -p encore-backend
sidecar:  ; cargo run -p encore-sidecar
frontend: ; cd front-end && npm run dev
dev:      ; ./scripts/dev.sh        # up + backend + sidecar + frontend, one Ctrl+C stops all
check:    ; cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings \
            && cargo test --workspace && cd front-end && npm run typecheck && npm run lint && npm test
full:     ; $(COMPOSE) --profile app up -d --build
load:     ; ./load/matrix.sh        # k6 load tests on an isolated stand (load/README.md); one run: ./load/run.sh <test> VUS=…
load-report: ; ./load/report.sh
