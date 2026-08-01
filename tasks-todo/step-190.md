# step-190 — Reaper de réservations orphelines (rattrapage du fail-open MT)

> **Jalon :** Audit pré-production (§6.9) · **Statut :** À FAIRE
> **Dépend de :** step-146 · **Bloque :** step-208 (go-live) — **BLOQUANTE**

## But
Implémenter le **reaper** que `internal/connectorpool/settle` référence déjà dans ses commentaires (l. 7,
101, 117, 130, 144) mais **qui n'existe pas**. Sans lui, tout le chemin de règlement MT est fail-open sans
filet : un `Release` échoué (billing-svc lent au-delà de `BILLING_SETTLE_TIMEOUT` 200 ms, ou down) laisse
le client **sur-facturé définitivement**, et un `Capture` échoué laisse un écart d'audit permanent.

> ⚠️ Ne pas confondre avec `billing.Reconciler` (`internal/billing/reconcile.go`) : homonyme trompeur, il
> traite la réconciliation de facturation **externe** (§6.10, écart local/provider) et ne touche jamais aux
> réservations. Le reaper est un composant distinct.

## Périmètre (ce que fait CETTE PR)
- `internal/billing/reaper.go` : détection des réservations orphelines + résolution capture/release.
- Repo Postgres : requête de détection des réservations ouvertes au-delà d'un seuil d'âge.
- Lecture de l'issue du message dans le CDR (ClickHouse, lecture seule).
- Câblage dans `cmd/billing-svc/main.go` comme composant supervisé (même moule que `runReconcile`).
- Métriques : réservations rattrapées (par issue) + réservations **non résolvables** (celles-ci doivent alerter).

## Points d'implémentation clés
- **Détection via `control_plane.billing_idempotency`, PAS via `billing_ledger`.** Le ledger est
  partitionné par jour : son index d'idempotence ne franchit pas les partitions. `billing_idempotency` est
  la garde autoritative cross-partition, non partitionnée, PK `(message_id, entry_type)`. Une réservation
  orpheline = un `message_id` avec `entry_type='reserve'` **sans** ligne `capture` ni `release`, dont
  `created_at` dépasse le seuil. Récupérer ensuite `owner_type`/`owner_id`/`customer_id`/`account_id`/
  `credits` dans `billing_ledger` (`billing_idempotency` ne les porte pas).
- **Détection pilotée par le grand livre (l'autorité monétaire), décision pilotée par le CDR.** Le ledger
  dit *quelles* réservations sont ouvertes ; seul le CDR dit *quelle issue* a eu le message. Mapping des
  statuts `cdr.status` — il n'y a pas deux camps mais **trois** :
  | Statut CDR | Décision |
  |---|---|
  | `enroute`, `delivered` | **capture** — soumis au SMSC avec succès (ESME_ROK), la prestation est due |
  | `failed`, `expired`, `rejected`, `cancelled` | **release** — jamais livré |
  | `accepted`, `rerouted` | **transitoire — ne rien faire**, repasser au tick suivant |
  `accepted` signifie « accepté en ingestion, pas encore soumis » et `rerouted` est un état de transit :
  les traiter comme terminaux libérerait des messages encore en vol.
- **Lire la DERNIÈRE version du CDR.** `cdr` est un `ReplacingMergeTree` porteur d'une colonne `version`
  (rang de cycle de vie) : une lecture naïve renvoie une ligne périmée — typiquement l'`accepted` initial
  d'un message depuis passé à `delivered`. Résoudre le statut par version maximale, jamais par première
  ligne trouvée.
- **NE JAMAIS release aveuglément.** Une réservation orpheline dont l'issue est introuvable ou ambiguë dans
  le CDR doit être **laissée intacte**, loguée et **comptée sur une métrique qui alerte** — jamais libérée
  par défaut. Libérer un message réellement envoyé = livraison gratuite = perte de revenu.
- **Passer par l'`Accountant` in-process** (billing-svc possède le ledger), pas par un RPC vers soi-même :
  l'idempotence et l'exclusion mutuelle capture/release vivent déjà dans `resolveTerminal`. Le reaper
  n'écrit jamais le ledger en direct.
- **Seuil d'âge configurable**, largement supérieur à la durée de vie normale d'un envoi (le message doit
  avoir eu le temps d'atteindre son issue terminale) — sinon le reaper court après le chemin nominal.
- **Ticker supervisé** calqué sur `runReconcile` : une passe en erreur est loguée, pas fatale ; le contexte
  est propagé pour un drain propre à l'arrêt.
- billing-svc gagne une dépendance **lecture seule** ClickHouse (il ne l'importe pas encore). Assumé : c'est
  le propriétaire du ledger, donc le bon hôte sémantique ; l'alternative (un `cmd/` dédié) dupliquerait tout
  le câblage Postgres/Redis pour un job périodique.
- **`ctx7`** avant toute API `clickhouse-go/v2` de requête.

## Tests (écrits dans la même PR)
- Une réservation orpheline dont le CDR dit `enroute` ou `delivered` est **capturée** ; le solde est correct
  après passe.
- Une réservation orpheline dont le CDR dit `failed` (et une par `expired`/`rejected`/`cancelled`) est
  **libérée** ; le client est remboursé.
- Une réservation dont le CDR dit `accepted` ou `rerouted` est **laissée intacte** et reprise au tick suivant.
- **Un CDR portant plusieurs versions pour le même `message_id`** (ex. `accepted` v1 puis `delivered` v2)
  est résolu sur la version maximale — ce test échoue si la lecture oublie le `ReplacingMergeTree`.
- Une réservation orpheline **sans ligne CDR** n'est ni capturée ni libérée, et incrémente la métrique d'alerte.
- Une réservation **déjà réglée** (capture ou release présent) n'est jamais retouchée.
- Une réservation **plus jeune que le seuil** est ignorée (pas de course avec le chemin nominal).
- Deux passes consécutives ne double-facturent pas (idempotence, invariant c).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · **invariant (c)** respecté
- [ ] métrique d'orphelines non résolvables exposée et documentée comme devant alerter
- [ ] les commentaires « reconcile via reaper » de `settle.go` pointent le composant réel

## Alternative écartée (tranchée le 2026-08-01)
Publier l'issue sur un topic durable `billing.settle-retry` depuis le connector-pool — qui la connaît déjà
au moment où le settle échoue — puis la rejouer via un consumer pacé, sur le modèle de step-192. Séduisant :
aucune dépendance ClickHouse, aucune interprétation de statut, rattrapage en secondes plutôt qu'en minutes.
**Écarté parce qu'incomplet** : si le pod connector-pool meurt avant de publier, la réservation redevient
invisible pour toujours. Le balayage ledger × CDR couvre ce cas résiduel et est correct **seul** ; le topic
ne l'est pas. Il reste ajoutable plus tard comme optimisation de latence, par-dessus le balayage.

## Hors périmètre
Colonne CDR `billable` (distinguer « facturation désactivée » de « capture échouée ») et disjoncteur sur le
client billing → suivis distincts. Re-réservation au replay dead-letter → step-129/145. Topic
`billing.settle-retry` → voir ci-dessus.
