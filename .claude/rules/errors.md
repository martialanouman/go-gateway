---
paths:
  - "internal/platform/errors/**"
---

# Ajouter un code d'erreur — trois endroits, en même temps

`code` est un **contrat partagé** : la même valeur circule dans la réponse REST,
les `command_status` SMPP et `cdr.error_code`. En ajouter un touche :

1. `internal/platform/errors` — la sentinelle **et** son mapping HTTP/SMPP ;
2. l'`enum` du champ `code` des **deux** `api/openapi-*.yaml`, si le code a un
   statut HTTP — `TestErrorCodeEnumMatchesTheCatalogue` (adminapi, restapi) le
   garde dans les deux sens ; le couplage contrat s'applique donc aussi : bump
   de `api/package.json` ;
3. la **§11.3** de `docs/guide-ingenierie-passerelle-sms.md` (catalogue, mapping
   unifié REST ↔ SMPP).

Le modèle reste plat : `{ code, message, errors[] }` en `application/json`,
via la surcharge `huma.NewError`. Gardé par
`internal/adminapi/contract_test.go` (`TestErrorSchemaIsTheFlatContractModel`).
Cycle de vie du `code` : guide d'ingénierie §11.2 ; invariant CDR : §11.5.
