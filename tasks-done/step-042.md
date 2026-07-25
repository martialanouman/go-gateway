# step-042 — Mapping de corrélation DLR à l'envoi (dlrmap Redis, §1.11)

> **Jalon :** M4 (§8 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-044

## But
À chaque `submit_sm`, mémoriser le mapping `smsc_msg_id → message_id` pour pouvoir corréler les DLR ultérieurs (§1.11).

## Périmètre (ce que fait CETTE PR)
- Dans `connector-pool-svc` : après réception du `submit_sm_resp` (avec l'ID SMSC), écrire `dlrmap:{connector_id}:{smsc_msg_id} → message_id` dans Redis, TTL = `validity_period` + marge.
- Stocker aussi le `trace_id` (corrélation observabilité au DLR).
- Ajouter une dépendance Redis au `connector-pool-svc` (client `go-redis`).

## Points d'implémentation clés
- **`ctx7`** avant d'utiliser `go-redis` (`SET` avec `EX`).
- `connector_id` fait partie de la clé : deux SMSC peuvent réutiliser le même `smsc_msg_id`.
- TTL dérivé de `validity_period` de l'enveloppe (défaut raisonnable si absent).
- Écriture best-effort mais journalisée : un échec d'écriture du mapping ne doit pas perdre le message (il est déjà `enroute`), mais doit être compté (un DLR arrivera sans mapping → géré step-044).
- **Invariant (a)** : la valeur stockée ne contient jamais le corps.

## Tests (écrits dans la même PR)
- Intégration Redis : après un `submit_sm` simulé (fakesmsc renvoyant un ID), le mapping existe avec le bon TTL.
- Clé scoping par `connector_id` correcte.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] mapping écrit avec TTL dérivé de la validité

## Hors périmètre
Réception/classification des `deliver_sm` (step-043) ; résolution du DLR (step-044).
