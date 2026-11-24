#!/usr/bin/env bash
# ============================================================
# SentinelMar / GEWOM v2.4 — Lanzador
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

MAIN=""
for f in SentinelMar.py GEWOM.py sentinelmar.py; do
    if [ -f "$f" ]; then
        MAIN="$f"
        break
    fi
done

if [ -z "$MAIN" ]; then
    echo "[ERR] No se encontró SentinelMar.py (ni GEWOM.py)."
    exit 1
fi

echo "▶ Arrancando $MAIN ..."
exec python "$MAIN" "$@"
