---
paths:
  - "internal/**/*.go"
  - "cmd/**/*.go"
---

# Code Go — les règles qu'un compilateur vert ne garantit pas

Ce que le linter fait échouer n'est pas répété ici. Détail des patterns :
`docs/guide-codage-go.md` ; style : `docs/convention-style-go.md`.

- **JAMAIS le corps d'un message dans un log, un span ou un label** *(invariant a)*.
  Type `Body` masquant, `Reveal()` pour le clair. Réf : guide de codage §11.
- **Aucune goroutine sans condition d'arrêt** — une goroutine sans condition
  d'arrêt est un bug. Réf : guide de codage §5.
- **Opérations Redis atomiques en Lua** (token-bucket, réserve/capture de crédit).
  Jamais un read-modify-write côté Go. Réf : guide de codage §7.2.
- **Facturation idempotente par `message_id`** *(invariant c)* ; désactivée = zéro
  appel réseau (contrôle booléen en cache). Réf : guide de codage §7.4.
- **Secrets : d'abord se demander si on le VÉRIFIE ou si on le REJOUE.** La
  réponse décide de la forme de stockage, et confondre les deux a produit deux
  défauts (step-295, ADR-0016).
  - **Vérifié** (mot de passe de bind *entrant*, clé API) → **hash**, révélé une
    seule fois à la création/rotation. Comparaison en temps constant — **sauf la
    clé API**, cherchée par son hash, donc comparée par PostgreSQL et non en Go.
    Réf : guide de codage §11 ; le *pourquoi* argon2id/SHA-256 et cette
    exception : plan d'exécution §1.9.
  - **Rejoué vers un tiers** (mot de passe de bind *sortant*, identifiants d'un
    fournisseur externe, clé de signature d'un webhook) → **scellé** par
    `ConfigSecrets` (`content-key-svc`), jamais haché : un hash ne se dé-hache
    pas, et un bind sortant met son mot de passe en clair dans la PDU. Colonne
    `*_sealed bytea` + `*_kms_key_ref`.
  - **Descellé sur un chemin chaud, l'échec se classe** (step-295b) : un service
    injoignable est transitoire et l'enregistrement se redélivre ; un chiffré
    inouvrable est déterministe et part au dead-letter. Les confondre bloque une
    partition entière sur une seule ligne.
  - Dans les deux cas le secret **ne ressort pas** de l'API. Masquer n'est pas
    déchiffrer : la sentinelle de lecture est une constante.
- **Modèle d'erreur plat** `{ code, message, errors[] }` en `application/json`
  (surcharge `huma.NewError`). Réf : guide d'ingénierie §11.
- Tout le code métier vit sous `internal/` ; `cmd/<service>/main.go` ne fait que
  câbler (guide de codage §2). Interfaces définies **côté consommateur**
  (guide de codage §6).

## Bibliothèques : la doc via `ctx7` avant d'écrire, jamais de mémoire

chi, huma, pgx, franz-go, go-redis, goja. La procédure est une règle globale ; ce
qui est propre à ce dépôt, c'est que **pgx v4→v5 et huma v1→v2 ont cassé**, et
qu'un usage périmé **compile parfois**.
