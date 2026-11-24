# Contribuir a GEWOM

¡Gracias por tu interés en contribuir! Este documento explica cómo hacerlo.

## Antes de empezar

1. Lee el [Código de Conducta](CODE_OF_CONDUCT.md).
2. Lee el [DISCLAIMER](DISCLAIMER.md) — este proyecto tiene límites importantes.
3. Revisa los [issues abiertos](https://github.com/Condor2026/GEWOM/issues)
   para evitar duplicados.

## Cómo reportar un bug

Abre un [issue](https://github.com/Condor2026/GEWOM/issues/new) con:

- **Versión de GEWOM** (`/api/health`).
- **Sistema operativo** y versión de Python.
- **Pasos para reproducir** el problema.
- **Comportamiento esperado vs. observado**.
- **Logs relevantes** (sin datos sensibles).

> ⚠️ **No incluyas** tokens, claves API, IPs privadas ni datos personales.

## Cómo proponer una mejora

Abre un issue describiendo:

- Qué problema resuelve.
- Cómo lo resolverías.
- Alternativas consideradas.

Espera feedback **antes** de abrir un PR grande para evitar trabajo perdido.

## Cómo añadir una nueva fuente OSINT

1. Crea una función `fetch_<nombre>()` que devuelva `List[dict]` con la
   estructura de alerta estándar (mira `fetch_usgs()` como referencia).
2. Añádela a la lista `SOURCES` al final del archivo.
3. Asigna la credibilidad correcta (A/B/C/D) en `CRED_RULES` si aplica.
4. Documenta la fuente en el README.
5. **Nunca** añadas fuentes que requieran scraping agresivo o violen ToS.

## Pull Requests

- Un PR = un cambio lógico.
- Sigue el estilo del código existente (PEP 8, type hints, `logger`).
- Actualiza el README si cambias comportamiento visible.
- Los PRs que añadan dependencias nuevas deben justificarlo.

## Estilo de código

- Python 3.9+.
- `snake_case` para funciones y variables.
- `PascalCase` para clases.
- Docstrings en funciones públicas.
- No uses `print()` en el servidor; usa `logger`.

## Licencia

Al contribuir aceptas que tu código se publique bajo la licencia MIT del
proyecto (ver [LICENSE](LICENSE)).
