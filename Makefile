.PHONY: up down install test demo check status audit
install:   ; pip install -r requirements.txt
up:        ; docker compose up -d && sleep 8
down:      ; docker compose down -v
test:      ; python -m pytest tests -q
demo:      ; python -m obs.cli --offline seed && python -m obs.cli --offline check
check:     ; python -m obs.cli migrate && python -m obs.cli check --fail-on-alert
audit:     ; python -m obs.cli registry-audit
status:    ; streamlit run app/status_app.py
status-offline: ; OBS_BACKEND=offline streamlit run app/status_app.py
