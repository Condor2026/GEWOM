---
name: ♿ Problema de accesibilidad
about: Algo del panel no es usable con teclado, lector de pantalla, zoom o contraste
title: "[A11Y] "
labels: accessibility
assignees: ''
---

<!--
  Consulta ACCESSIBILITY.md antes de abrir este issue.
  🚫 NO incluyas datos personales ni capturas con información sensible.
-->

## Tipo de problema

- [ ] Navegación por teclado (no se puede alcanzar / activar)
- [ ] Foco no visible o ilógico
- [ ] Contraste de color insuficiente
- [ ] Lector de pantalla (NVDA / VoiceOver / Orca)
- [ ] Zoom al 200% rompe el layout
- [ ] Texto alternativo faltante o incorrecto
- [ ] Movimiento / animación sin respetar `prefers-reduced-motion`
- [ ] Formularios sin `<label>` asociado
- [ ] Pestañas / tabs sin roles ARIA correctos
- [ ] Mapa no accesible (falta tabla equivalente)
- [ ] Otro: ___________

## Descripción

Qué elemento del panel no es accesible y por qué.

## Dónde ocurre

- **Pestaña:** (ALERTAS / ERRORES / HISTÓRICO / BUSCAR / FUENTES / otra)
- **Elemento:** (botón, mapa, tabla, formulario…)
- **URL o ruta:** `/` · `/api/...`

## Pasos para reproducir

1. Ir a '...'
2. Hacer clic en '...'
3. Ver error

## Comportamiento esperado

Qué debería poder hacer un usuario con esa necesidad.

## Comportamiento observado

Qué ocurre realmente.

## Entorno

- **Versión de GEWOM:** (ver `/api/health`)
- **Sistema operativo:** (Windows 11 / macOS 14 / Ubuntu 24.04...)
- **Navegador y versión:** (Chrome 131 / Firefox 133 / Safari 18...)
- **Tecnología de asistencia:** (NVDA 2024.4 / VoiceOver / Orca / ninguna)
- **Zoom aplicado:** (100% / 150% / 200%)

## Criterio WCAG afectado (si lo sabes)

- Ej.: WCAG 2.1 — 1.4.3 Contraste (mínimo), 2.1.1 Teclado, 2.4.7 Foco visible…

## Captura o grabación

Si es posible, adjunta imagen o vídeo mostrando el problema.

## Contexto adicional

Cualquier otra información útil.
