# Política de Seguridad

## Versiones soportadas

| Versión | Soportada |
|---------|-----------|
| 2.4.x   | ✅        |
| < 2.4   | ❌        |

## Reportar una vulnerabilidad

**NO abras un issue público para vulnerabilidades de seguridad.**

Envía un correo a: **security@example.com** *(reemplaza con tu contacto real)*

Incluye:

- Descripción de la vulnerabilidad.
- Pasos para reproducirla.
- Impacto potencial.
- Versión afectada.
- Sugerencia de mitigación (si la tienes).

### Tiempo de respuesta

- **Acuse de recibo:** ≤ 72 h.
- **Evaluación inicial:** ≤ 7 días.
- **Parche o mitigación:** según gravedad.

## Alcance

Este proyecto:

- **NO** procesa datos personales en servidores propios (todo es local).
- **NO** envía datos a terceros más allá de las APIs públicas que consulta.
- Almacena un histórico local SQLite (`alerts_history.db`) que **tú controlas**.

### Vulnerabilidades relevantes

- **XSS** en el panel web.
- **SSRF** en las peticiones a feeds configurables.
- **Inyección SQL** en el histórico.
- **Path traversal** en endpoints de exportación.
- **Exposición de tokens** en logs o respuestas.

### Fuera de alcance

- Contenido publicado por las fuentes externas.
- Bloqueos o límites de tasa de APIs de terceros.
- Cambios de comportamiento en servicios externos.
- Ingeniería social contra mantenedores.

## Buenas prácticas para usuarios

- Ejecuta GEWOM en `127.0.0.1` (por defecto) o detrás de un proxy con auth.
- **Nunca** expongas el puerto a Internet sin autenticación.
- Rota tus claves API (`NASA_FIRMS_KEY`, `GDELT_API_KEY`, etc.) periódicamente.
- Mantén `.env` fuera del control de versiones (`.gitignore` ya lo hace).
- Revisa `alerts_history.db` antes de compartirlo — puede contener datos.

Gracias por ayudar a mantener GEWOM seguro.
