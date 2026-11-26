# Accesibilidad — GEWOM

GEWOM se compromete a ser usable por el mayor número de personas posible,
incluidas aquellas que usan tecnologías de asistencia. Este documento
describe el estado actual y las pautas para contribuir.

## Objetivo

Alinear el panel web con **WCAG 2.1 nivel AA** en la medida de lo posible.

## Estado actual

| Área | Estado |
|------|--------|
| Contraste de color (texto normal) | ✅ ≥ 4.5:1 |
| Contraste de color (texto grande) | ✅ ≥ 3:1 |
| Navegación por teclado | ⚠️ Parcial (pestañas OK, mapa pendiente) |
| Foco visible | ✅ |
| Etiquetas `aria-*` en controles | ⚠️ Parcial |
| Texto alternativo en iconos | ✅ (emoji con `aria-label`) |
| Zoom hasta 200% sin pérdida | ✅ |
| Respeto a `prefers-reduced-motion` | ⚠️ Pendiente |
| Modo oscuro / alto contraste | ⚠️ Pendiente |

> Los elementos marcados como parciales o pendientes están en la hoja de ruta.

## Pautas para contribuir

### Contraste

- **Texto normal:** mínimo **4.5:1** sobre el fondo.
- **Texto grande (≥18pt o ≥14pt bold):** mínimo **3:1**.
- **Elementos no textuales** (bordes, iconos informativos): mínimo **3:1**.

Herramientas: [WebAIM Contrast Checker](https://webaim.org/resources/contrastchecker/),
[Colour Contrast Analyser](https://www.tpgi.com/color-contrast-checker/).

### No uses el color como único indicador

Un estado `critical` no debe distinguirse **solo** por el rojo. Añade también
un icono, texto o patrón.

### Semántica HTML

- Usa `<button>` para acciones, `<a>` para navegación.
- Un solo `<h1>` por página; jerarquía `h1 → h2 → h3` sin saltos.
- Tablas con `<th scope="col">`.
- Formularios con `<label for="...">` asociado.

### Teclado

- Todo debe ser alcanzable y operable **sin ratón**.
- `Tab` navega, `Enter`/`Espacio` activan, `Esc` cierra.
- **Nunca** elimines el foco visible (`outline: none` sin sustituto).
- Orden de tabulación lógico.

### Texto alternativo

```html
<!-- Bien -->
<button aria-label="Recargar alertas">🔄</button>

<!-- Mal -->
<button>🔄</button>
