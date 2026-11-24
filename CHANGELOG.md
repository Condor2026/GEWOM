# Changelog

Todos los cambios notables de este proyecto se documentan aquí.
Formato basado en [Keep a Changelog](https://keepachangelog.com/es/1.1.0/),
versionado [SemVer](https://semver.org/lang/es/).

## [2.4.0] — 2026-XX-XX

### Añadido
- Fuentes sísmicas: USGS, EMSC, GEOFON (detección heurística de explosiones).
- Correlación de explosiones con alertas de ataque (≤150 km, ±30 min).
- Fusión de sismos duplicados entre redes (±120 s, ≤100 km).
- `SECURITY.md`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`.
- Caché de traducciones (`_tr_cache`) y `/api/translate`.
- Pestaña BUSCAR con búsqueda nativa de Telegram (Telethon opcional).

### Cambiado
- `run_parallel()` respeta `workers` por fuente.
- Mejor gestión de estado de fuentes (`ok` / `warn` / `error` / `skipped`).
- Migración automática de BD v2.1 → v2.4.

### Corregido
- Duplicados de alertas al recargar el panel.

## [2.3.0] — 2026-XX-XX

### Añadido
- Comunicados oficiales (Casa Blanca, Kremlin, OTAN, OIEA, ONU, FCDO, UE).
- Medios estatales con etiqueta `estatal` y credibilidad `C`.
- Clasificación de comunicados especiales (guerra, nuclear, ley marcial…).

## [2.2.0] — 2026-XX-XX

### Añadido
- Canales de Telegram ampliados (UA Air Force, DeepState, etc.).
- Búsqueda de canales de Telegram.
- Vista previa de canales.

## [2.1.0] — 2026-XX-XX

### Añadido
- Histórico SQLite.
- Exportación CSV / JSON.
- Panel web con pestañas.

## [2.0.0] — 2026-XX-XX

- Primera versión pública.
