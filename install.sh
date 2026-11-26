
#!/usr/bin/env bash
# ============================================================
# GLOBAL EARLY WARNING OSINT MONITOR (GEWOM) v2.4 — Instalador
# ============================================================

set -euo pipefail

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

info()  { echo -e "${BLUE}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[ OK ]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()   { echo -e "${RED}[ERR ]${NC}  $*"; }

info "Detectando Python..."
PYTHON=""
for candidate in python3.12 python3.11 python3.10 python3.9 python3 python; do
    if command -v "$candidate" >/dev/null 2>&1; then
        version=$("$candidate" -c 'import sys; print("%d.%d" % sys.version_info[:2])')
        major=$(echo "$version" | cut -d. -f1)
        minor=$(echo "$version" | cut -d. -f2)
        if [ "$major" -ge 3 ] && [ "$minor" -ge 9 ]; then
            PYTHON="$candidate"
            ok "Python $version en $candidate"
            break
        fi
    fi
done

if [ -z "$PYTHON" ]; then
    err "No se encontró Python 3.9+."
    exit 1
fi

if [ ! -d "venv" ]; then
    info "Creando venv..."
    "$PYTHON" -m venv venv
    ok "venv creado"
else
    warn "venv ya existe, reutilizando"
fi

# shellcheck disable=SC1091
source venv/bin/activate
ok "venv activado"

info "Actualizando pip..."
pip install --quiet --upgrade pip setuptools wheel
ok "pip actualizado"

if [ -f "requirements.txt" ]; then
    info "Instalando dependencias..."
    pip install --quiet -r requirements.txt
    ok "Dependencias instaladas"
else
    warn "No hay requirements.txt, instalando core..."
    pip install --quiet flask requests beautifulsoup4 urllib3
fi

read -r -p "¿Instalar Telethon para búsqueda nativa de Telegram? [s/N] " ans || ans=""
if [[ "$ans" =~ ^[SsYy]$ ]]; then
    info "Instalando Telethon..."
    pip install --quiet telethon
    ok "Telethon instalado"
fi

if [ ! -f ".env" ]; then
    if [ -f ".env.example" ]; then
        cp .env.example .env
        ok ".env creado a partir de .env.example"
    fi
else
    warn ".env ya existe, no se sobreescribe"
fi

if [ -f "run.sh" ]; then
    chmod +x run.sh
fi

echo ""
echo -e "${GREEN}════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  Instalación completada${NC}"
echo -e "${GREEN}════════════════════════════════════════════════════${NC}"
echo ""
echo "  Arrancar:"
echo -e "    ${YELLOW}./run.sh${NC}"
echo ""
echo "  O manualmente:"
echo -e "    ${YELLOW}source venv/bin/activate && python GEWOM.py${NC}"
echo ""
echo "  Abrir en el navegador:"
echo -e "    ${BLUE}http://127.0.0.1:5000${NC}"
echo ""
