#!/usr/bin/env bash
# ============================================================
# GLOBAL EARLY WARNING OSINT MONITOR (GEWOM) v2.4 — Lanzador
# ============================================================

set -euo pipefail

cd "$(dirname "$0")"

if [ -d "venv" ]; then
    # shellcheck disable=SC1091
    source venv/bin/activate
elif [ -d ".venv" ]; then
    # shellcheck disable=SC1091
    source .venv/bin/activate
else
    echo "[WARN] No se detectó venv. Usando Python del sistema."
fi

if [ -f ".env" ]; then
    set -a
    # shellcheck disable=SC1091
    . ./.env
    set +a
fi

if [ ! -f "GEWOM.py" ]; then
    echo "[ERR] No se encontró GEWOM.py en $(pwd)" >&2
    exit 1
fi

echo "▶ Arrancando GEWOM.py ..."
exec python GEWOM.py "$@"
