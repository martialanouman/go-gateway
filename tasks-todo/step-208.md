# step-208 — Dérouler la checklist de mise en production (go-live)

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-201b, step-203, step-204, step-206, step-207, step-213 · **Bloque :** —

## But
Clore M12 : dérouler la checklist de mise en production (guide d'ingénierie §15), consigner l'état de
chaque item et matérialiser la porte de go-live.

## Périmètre (ce que fait CETTE PR)
- `deploy/PRODUCTION-CHECKLIST.md` : la checklist §15 du guide d'ingénierie, chaque item coché avec la
  preuve (PR/step de référence : charge, chaos, sécurité, auth, manifests).
- Récapitulatif des NFR tenus (débit, latence) et des politiques de panne vérifiées.

## Points d'implémentation clés
- La checklist référence les livrables : NFR (step-200/201/**201b**), chaos par politique (step-202/203),
  sécurité (step-204/205), auth opérateur réelle (step-206), manifests (step-207).
- **Le verdict NFR vient de step-201b, pas de step-201.** step-201 a livré les leviers, les instruments
  de mesure et un run de référence à la borne basse du modèle par-worker (§2.5) ; le débit soutenu
  8 000 SMS/s **traversant** ne peut se prononcer que sur un environnement représentatif. Ne pas cocher
  l'item NFR sur la foi du run local de step-201 : il mesurait une machine de développement.
- **Dette du harnais de charge, à solder ici au plus tard** — deux points relevés en revue de step-200
  et laissés ouverts parce qu'ils ne coûtent rien tant que la patte sortante est un simulateur. Ils
  redeviennent bloquants au **premier run contre un SMSC réel**, c'est-à-dire au plus tard ici :
  1. **Aucun verrou d'envoi.** `make load BASE_URL=<passerelle réelle>` avec une clé valide envoie
     ~500 messages en profil `smoke`, ~480 000 en `sustained`, vers des numéros `+22507000xxxx` — un
     préfixe Orange CI actif ; il n'existe aucune plage réservée aux tests en `+225`. Restreindre le
     tirage ne suffit pas : c'est l'envoi qui doit devenir délibéré (opt-in explicite refusé par défaut
     hors boucle locale).
  2. **Le mot de passe de bind transite par `argv`** — désormais **deux** occurrences :
     `smpp-bindgen -password …` et `smsc-ceiling -password …` (ajouté en step-201). Visible dans `ps`
     pour tout utilisateur de la machine pendant tout le run, et dans l'historique du shell. Le lire
     dans l'environnement, flag conservé en repli documenté comme non sûr — **les deux binaires**.
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
- [ ] **le contrat publié ne ment pas** : toute opération d'`openapi-admin.yaml` et d'`openapi-public.yaml`
      est servie, ou différée avec sa raison et sa step (garde de step-213). Le contrat part en package
      npm versionné vers le tableau de bord : une opération déclarée et non servie devient un client
      typé qui appelle un 404.
- [ ] dette du harnais soldée : verrou d'envoi en place, aucun secret de bind sur `argv`

## Hors périmètre
DR inter-région (RPO/RTO) — non-objectif (§16). Fin de M12 et du plan.
