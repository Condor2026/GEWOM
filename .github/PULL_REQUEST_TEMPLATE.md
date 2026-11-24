## Descripción

Qué cambia este PR y por qué.

## Tipo de cambio

- [ ] 🐛 Bug fix
- [ ] ✨ Nueva funcionalidad
- [ ] 📡 Nueva fuente OSINT
- [ ] 🎯 Nueva regla de clasificación / credibilidad
- [ ] 🧠 Cambio en traducción / IA
- [ ] 📝 Documentación
- [ ] 🎨 Refactor / estilo
- [ ] ⚡ Rendimiento
- [ ] 🔒 Seguridad
- [ ] ♿ Accesibilidad

## Checklist

- [ ] He leído [CONTRIBUTING.md](CONTRIBUTING.md)
- [ ] El código sigue el estilo del proyecto (PEP 8, `logger` en vez de `print()`)
- [ ] He actualizado la documentación si aplica (README, CRED_LABELS, CHANGELOG…)
- [ ] He probado los cambios localmente (`/api/alerts`, `/api/health`, panel web)
- [ ] No incluyo tokens, claves API ni datos personales
- [ ] Si añado una fuente, tiene `_ok` / `_warn` / `_fail` y credibilidad asignada
- [ ] Si toco la UI, los cambios son accesibles (contraste, teclado, `prefers-reduced-motion`)
- [ ] Si toco el histórico SQLite, la migración es compatible con bases v2.1+
- [ ] No rompo la caché de 15 s ni el `_fetch_lock`

## Issues relacionados

Closes #
