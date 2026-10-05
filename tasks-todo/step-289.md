# step-289 — Un débit par sender ID, refusé à l'admission

> **Jalon :** ADR-0021 §3 · **Statut :** LIVRÉE
> **Dépend de :** step-283, step-288 · **Bloque :** la colonne « limite » de l'écran sender IDs du tableau de
> bord (go-gateway-bo step-067)
> Décision humaine du 05/10/2026 ; unité faute de multiple de dix libre.

## Pourquoi
ADR-0021 §3 : le débit d'un flux est l'engagement contractuel de son expéditeur. Il plafonne une fraude au
trafic gonflé sans toucher aux autres flux du client. Aujourd'hui, `rate_limits.entity_type` ne connaît que
`smpp_account` et `connector` (migration 0023), et aucune opération du contrat ne lit ni n'écrit une limite.

## Design arrêté
- **Schéma** : la migration ajoute `sender_id` au `CHECK` de `rate_limits.entity_type`, avec
  `entity_id = sender_ids.id`. Le DDL de `db/schema_passerelle_sms.sql:528` est mis à jour en même temps.
- **Snapshot** : `ratelimit.LoadSnapshot` joint `sender_ids` et indexe les limites par
  `(customer_id, address)`. L'admission ne connaît en effet que `env.CustomerID` et `env.From`, brut,
  comparé octet à octet comme le fait `Authorize`. Un expéditeur non enregistré n'a pas de seau : il passe
  l'admission et le routeur le rejette.
- **Admission**, à `internal/ingest/ingest.go:66`, le seul point par lequel passent REST et SMPP :
  - le seau du sender ID est débité **avant** celui du compte, avec le même coût en segments ;
  - un refus rend `ErrRateLimited` : 429 avec `Retry-After`, ou `ESME_RTHROTTLED` ;
  - aucun record n'est produit sur `mt.inbound`, donc aucun CDR.
- **Aucun remboursement** si le seau du compte refuse après celui du sender ID. Le code le marque d'un
  commentaire `ponytail:`, qui nomme le plafond : au plus le coût d'un message perdu par refus.
- **Rechargement à chaud** : `rest-api-svc` et `smpp-server-svc` s'abonnent au watcher de config
  (`config.NewWatcher`, le même patron que `cmd/router-svc/wiring.go:722-800`). Aujourd'hui le snapshot est
  figé au démarrage (`enforcer.go:45-46`), et une limite réglée au tableau de bord ne s'appliquerait
  jamais. Les limites de compte en profitent.
- **Suppression d'un sender ID** : sa ligne `rate_limits` est supprimée dans la même transaction. La
  colonne est polymorphe et sans FK.
- **Contrat Admin, bump MINEUR** :
  - en lecture, `SenderId.rate_limit` vaut `{max_per_sec, burst_capacity}` ou `null` (`null` = aucun
    plafond propre, celui du compte s'applique). Il est servi par `list-sender-ids`, sans appel par ligne ;
  - en écriture, `PUT /admin/customers/{id}/sender-ids/{senderId}/rate-limit` (200, rend le `SenderId`) et
    `DELETE` sur le même chemin (204). Tous deux rendent 404 si le sender ID est inconnu ou appartient à un
    autre client, et exigent le scope d'écriture (`scopeSecurity`) ;
  - pas de `PATCH` avec `null` sur `update-sender-id` : les pièges de nullable de huma
    (`huma-contract-quirks`) ;
  - bornes : `max_per_sec` de 1 à 2147483647 (`int4`, aucun plafond métier n'existe). `burst_capacity`
    est optionnel, au minimum 1 et au plus 2147483647, et vaut `max_per_sec` par défaut ;
  - `max_per_day` n'est **pas** exposé (`debts/max-per-day-expose-jamais-applique.md`).

## Tests rouges attendus
- Un envoi au-delà du débit de son sender ID est refusé avant l'ACK, en REST (429) et en SMPP
  (`ESME_RTHROTTLED`), alors que le compte a de la marge. Aucun record n'est produit.
- Un second sender ID du même compte n'est pas touché (isolation des flux).
- Un sender ID sans limite n'est plafonné que par son compte.
- Une limite posée par l'API Admin s'applique **sans redémarrage** du pod d'ingestion.
- `list-sender-ids` rend `rate_limit` pour chaque ligne en une seule requête Postgres.
- La suppression d'un sender ID ne laisse aucune ligne `rate_limits` orpheline.
- `TestRateLimitRepoList` couvre le cas `sender_id`.

## Hors périmètre
Le topic par catégorie et le choix du topic à l'ingestion (ADR-0021 §1-§2) : **step-292**.

## Livré
- Contrat 7.1.0 (mineure) : `SenderId.rate_limit` (nullable, lu dans la liste par un LEFT JOIN, sans appel
  par ligne), `set-sender-id-rate-limit` (PUT, rafale = `max_per_sec` par défaut) et
  `delete-sender-id-rate-limit` (DELETE, idempotent).
- Migration 0030 : `sender_id` dans le `CHECK` ; **un trigger** supprime la limite d'un sender ID supprimé.
  *Amendé à l'implémentation* : le design prévoyait une suppression dans la même transaction, mais un
  sender ID part aussi par la cascade de son client, qu'aucun chemin applicatif ne voit.
- `Enforcer.Admit(account, customer, from, segments)` remplace `AdmitAccount` : le sender d'abord, puis
  le compte. Le snapshot est remplaçable à chaud (`Enforcer.Reload`), câblé sur l'invalidation de
  configuration dans `rest-api-svc` et `smpp-server-svc`.
- SMPP : même `Ingestor`, même mapping `ErrRateLimited` → `ESME_RTHROTTLED` (déjà prouvé par
  `TestOnSubmitIngestErrorMapsToCommandStatus`) ; le test d'intégration du sender est côté REST.
- Mutations tuées : contrôle du sender sauté, sender non indexé, compte débité d'abord, rechargement qui
  ne remplace pas, `From` perdu à l'ingestion, trigger retiré, défaut de rafale, PUT d'un autre client
  (trouvé par la mutation : le 404 venait de la relecture, l'écriture avait eu lieu), DELETE d'un autre
  client, adresse du sender perdue au repository.
- Reste non gardé : la ligne `g.Add("rate-limit watcher", …)` de chaque `main.go`
  (`debts/supervision-des-services-gardee-par-aucun-test.md`, déclencheur atteint).
