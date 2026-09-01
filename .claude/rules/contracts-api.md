---
paths:
  - "api/**"
---

# Contrats API

**Les contrats sont la source de vérité** : implémente pour conformer
`api/openapi-public.yaml` et `api/openapi-admin.yaml`, jamais l'inverse. Le
contrat se déclare **avant** l'implémentation.

Les contrats sont publiés comme package npm versionné
(`@martialanouman/gateway-api-contracts`) et consommés par le tableau de bord
(dépôt séparé). Tout changement d'un `api/openapi-*.yaml` — **un endpoint Admin
neuf compris** — exige un bump de `api/package.json`, **majeur** si `oasdiff`
classe la rupture `ERR`. `make contracts` le vérifie.

Procédure complète (les cinq étapes, le tableau correctif/mineur/majeur,
`make contracts-types`) : `api/README.md`.

`api/collections/admin-api.yaml` est un **artefact dérivé**, gardé par
`internal/adminapi/collection_test.go` — il se régénère, il ne s'édite pas.
