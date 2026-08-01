# ADR-0011 : La garde des clés de contenu vit dans un service dédié (`content-key-svc`)

**Status:** Accepted
**Date:** 2026-08-01
**Deciders:** Équipe plateforme
**Réf spec:** §6.14 ; §14 (« `content_keys` hébergé par `billing-svc` ») ; step-161 → step-167

## Context

Le chiffrement du contenu est une enveloppe : une clé de données (DEK) AES-256 par client scelle le corps
des messages, et une clé maître (KEK, détenue par la KMS) scelle la DEK. La propriété de sécurité visée est
que **la KEK vive dans le moins de process possible**.

Le plan d'exécution §14 prescrivait « `content_keys` hébergé par `billing-svc` », et step-161 l'a suivi :
`billing-svc` détenait la KMS et servait le service gRPC `ContentKeys`.

La revue de conception de step-161 a bien comparé deux options — `billing-svc` détenteur unique **contre**
`admin-api-svc` écrivant en base avec sa propre KMS (soit la KEK dans **deux** process) — et a retenu la
première. Mais elle n'a **jamais évalué un service dédié**. La décision n'a donc pas été confrontée à sa
véritable alternative, et l'hébergement dans `billing-svc` s'est retrouvé acquis par défaut plutôt que par
comparaison.

À l'usage, cet hébergement pose quatre problèmes :

1. **Cohésion nulle.** La comptabilité de crédits et le chiffrement de contenu n'ont aucun rapport
   fonctionnel. `billing-svc` portait deux responsabilités sans lien.
2. **Rayon d'explosion.** La KEK vivait dans un process du chemin chaud (reserve/capture à 8 000 msg/s) dont
   la surface de dépendances est large : Redis, provider de facturation externe en HTTP, ledger Postgres,
   réconciliation périodique. Chacune est une voie potentielle vers la clé maître.
3. **Couplage de disponibilité**, dans les deux sens : une panne de `billing-svc` emportait la rotation, la
   lecture gardée et le crypto-shred ; une opération de clés lourde pesait sur le chemin de facturation.
4. **Conformité.** « Le dépositaire des clés est un service dédié, minimal et audité » est une affirmation
   nettement plus tenable devant un auditeur que « notre service de facturation détient aussi la clé
   maître » — d'autant que l'attestation d'effacement RGPD (step-166) est un document légal.

Le coût de la bascule était par ailleurs faible, parce que le découpage logique existait déjà :
`ContentKeyServer` ne dépendait que de `content.KMS` et d'une interface de store (aucun couplage
facturation), `service ContentKeys` était déjà distinct de `service Billing` dans le proto, et seuls deux
binaires l'appelaient.

## Decision

**La garde des clés de contenu devient un service dédié, `content-key-svc` (gRPC :7002), seul détenteur de
la KMS.**

- `service ContentKeys` sort de `billing.proto` vers `api/proto/contentkeys.proto`, package `contentkeys`,
  `go_package` `internal/contentkeys/pb`.
- Le serveur déménage de `internal/billing/` vers `internal/contentkeys/`. Aucune logique ne change.
- `cmd/content-key-svc` déclare la surface la plus étroite possible : **Postgres + gRPC**. Pas de Redis, pas
  de Kafka, pas de HTTP, aucun client sortant. Chaque dépendance qu'il n'a pas est une voie de moins vers la
  KEK.
- `billing-svc` perd la KMS, l'enregistrement du service et `CONTENT_KMS_MASTER_KEY`.
- `admin-api-svc` et `router-svc` dialent `CONTENT_KEY_ADDR` (défaut `localhost:7002`).
- Le mapping erreur→statut gRPC, jusque-là privé à `internal/billing`, devient
  `internal/platform/errors/grpcerr` — pendant de `humaerr` pour HTTP — pour que deux services ne dérivent
  pas sur ce que « not found » signifie au client.

### Runbook de bascule (sans coupure)

L'ordre compte, parce que la version précédente de `billing-svc` sert encore `ContentKeys` :

1. Déployer `content-key-svc` avec **le même** `CONTENT_KMS_MASTER_KEY` que `billing-svc`. Les deux process
   servent alors les mêmes clés (même KEK, même table).
2. Déployer `admin-api-svc` et `router-svc` : ils dialent désormais `CONTENT_KEY_ADDR`.
3. Déployer `billing-svc` : il perd la KMS.

Entre les étapes 1 et 3 la clé maître est **transitoirement présente dans deux process**. C'est borné et
assumé ; l'ordre inverse (retirer d'abord) casserait le chiffrement et la lecture de contenu.

Trois précautions, parce que le **chemin de méthode gRPC change** (`/billing.ContentKeys/*` →
`/contentkeys.ContentKeys/*`) :

- **Provisionner `CONTENT_KEY_ADDR` avant l'étape 2.** Sans lui, `admin-api-svc` et `router-svc` refusent de
  démarrer en production (le validateur rejette le défaut loopback). L'échec est sain, mais il ressemble à un
  mauvais déploiement si on ne l'attend pas.
- **L'étape 3 est une porte à sens unique.** Une fois `billing-svc` dépouillé, un *rollback* d'un client vers
  l'image précédente le renvoie sur `billing.ContentKeys`, que plus personne ne sert : côté `router-svc` la
  récupération de DEK échoue en silence et le corps est simplement retiré du CDR. Tenir l'étape 3 jusqu'à la
  fin du déploiement **et** de la fenêtre de rollback des clients.
- **Surveiller `accepted_content_dropped_total` entre les étapes 2 et 3.** C'est le seul signal si un client
  n'atteint pas le service de clés — le mode de défaillance est un compteur, pas une erreur bruyante.

## Options Considered

### Option A (retenue) : service dédié `content-key-svc`
**Pros :** surface minimale et auditable autour de la KEK ; cohésion rétablie ; découplage de disponibilité ;
histoire de conformité simple. Coût faible car le découpage logique préexistait.
**Cons :** un binaire et une unité de déploiement de plus ; dévie de §14 (d'où cet ADR) ; un saut réseau
supplémentaire pour la récupération de DEK — amorti par le cache TTL côté plan de données (step-162a).

### Option B : statu quo — `billing-svc` héberge la KMS
**Pros :** aucune nouvelle unité de déploiement ; conforme à §14.
**Cons :** les quatre problèmes ci-dessus. Le coût de la bascule ne fait qu'augmenter à mesure que des
services en dépendent, donc « plus tard » signifie « plus cher ».

### Option C : bibliothèque KMS embarquée dans chaque appelant
**Cons :** la KEK dans `admin-api-svc` **et** `router-svc` **et** l'ingestion. C'est exactement l'option que
la revue de step-161 avait déjà écartée, en pire.

## Consequences

- **Plus facile :** verrouiller l'accès à la KEK par politique réseau (peu d'appelants, un seul port) ;
  auditer le dépositaire (code court, peu de dépendances) ; raisonner sur la disponibilité.
- **Plus difficile / limites :** une unité de déploiement de plus à exploiter ; `CONTENT_KEY_ADDR` devient
  une configuration obligatoire en production pour `admin-api-svc` et `router-svc` (le validateur refuse le
  défaut loopback) ; la bascule doit suivre l'ordre du runbook.
- **Inchangé :** la crypto, le cycle de vie des clés, les RPC, le schéma `content_keys`. Aucune migration de
  données. Le fournisseur KMS réel (AWS/GCP/Vault) reste hors périmètre, derrière `content.KMS`.
- **Rupture proto assumée :** retirer `service ContentKeys` et ses messages de `billing.proto` est une
  rupture au sens de `buf breaking` (`buf.yaml` déclare `use: FILE`). Aucun job CI ne lance `buf breaking`
  aujourd'hui, donc rien ne casse ; c'est consigné ici au cas où cette barrière serait câblée plus tard —
  la rupture est délibérée et couverte par le runbook ci-dessus.
- **Traçabilité :** `tasks-done/step-167.md` ; §14 du plan amendé pour ne plus désigner `billing-svc` ;
  §5.3 de la spec et §3.7bis du guide d'ingénierie décrivent le nouveau service ; ADR-0008 (contenu chiffré
  par clé client) reste valable, seul l'hébergeur change.
