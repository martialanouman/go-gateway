# step-208 — Dérouler la checklist de mise en production (go-live)

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-201, step-203, step-204, step-206, step-207 · **Bloque :** —

## But
Clore M12 : dérouler la checklist de mise en production (guide d'ingénierie §15), consigner l'état de
chaque item et matérialiser la porte de go-live.

## Périmètre (ce que fait CETTE PR)
- `deploy/PRODUCTION-CHECKLIST.md` : la checklist §15 du guide d'ingénierie, chaque item coché avec la
  preuve (PR/step de référence : charge, chaos, sécurité, auth, manifests).
- Récapitulatif des NFR tenus (débit, latence) et des politiques de panne vérifiées.

## Points d'implémentation clés
- La checklist référence les livrables : NFR (step-200/201), chaos par politique (step-202/203),
  sécurité (step-204/205), auth opérateur réelle (step-206), manifests (step-207).
- Vérifier une dernière fois les **4 invariants** (a/b/c/d) verts sur l'ensemble avant go-live.
- Item explicite : **auth opérateur réelle active** (le stub M1 n'est plus câblé).
- Artefact documentaire (pas de code) : ne PAS inventer d'items — reprendre §15 du guide.

## Tests (écrits dans la même PR)
- Aucun test de code nouveau ; la CI complète (build + `go test -race` + govulncheck + gosec) est la preuve.
- Revue : chaque item de la checklist pointe une preuve vérifiable.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck · gosec verts
- [ ] les 4 invariants (a/b/c/d) re-vérifiés verts · checklist §15 entièrement cochée avec preuves
- [ ] auth opérateur réelle active · NFR et politiques de panne consignés

## Hors périmètre
DR inter-région (RPO/RTO) — non-objectif (§16). Fin de M12 et du plan.
