# step-403 — Le trafic par canal d'entrée et les sessions SMPP actives dans les métriques Admin

> ⛓ step-380, step-402 — demandée par le tableau de bord (écran Trafic de `go-gateway-bo`) ; unité faute de
> multiple de dix libre

## But

L'écran Trafic ne sait ni par où le trafic entre (HTTP ou SMPP) ni combien de binds SMPP sont ouverts :
`get-traffic-metrics` n'a pas de dimension canal, la CDR ne porte pas le canal, et `active_sessions` est
toujours absent (debts/sessions-actives-sans-compteur-global.md).

## Design arrêté

Arbitré par Fable le 2026-10-11 (aucun point remonté à l'humain).

**A. Le canal dans la CDR.** `pipeline.InboundMT.Channel` (`pipeline.ChannelHTTP = "http"`,
`pipeline.ChannelSMPP = "smpp"`), posé par `restapi` et `smppserver`, voyage sur mt.inbound
(`ingress_channel`, omitempty). Colonne CH `ingress_channel LowCardinality(String) DEFAULT ''`
(migration 0009 + appendice A du schéma). Seules les lignes de rang 0 la portent : `ingest.AcceptedRow`,
`router.rejectedRow`, et le cancelled du Canceller, qui recopie l'instantané (`cdrAggregate` gagne la
colonne). Un segment (rang ≥ 1) ne remplace jamais la ligne de rang 0 (clé de tri, 0002). Lecture :
`max(ingress_channel)` aux deux niveaux de `metricsMessages`, exact puisqu'un message n'a qu'un canal. Un MO
garde `''`.

*Corrigé au rouge :* un rejet ou un cancelled de rang 0 partage la clé de tri de l'accepted, et
ReplacingMergeTree le remplace physiquement : `max()` ne rattrape pas un rejet écrit sans canal. Chaque
écrivain de rang 0 doit donc le porter ; pendant le déploiement, un rejet écrit par un ancien routeur tombe
sous `unknown`.

*Écart assumé :* le contrat Admin dit « rest » (`rest_enabled`), le ticket du tableau de bord impose
`http` ; on suit le ticket.

**B. `groupBy=channel`.** MT seulement : clés `http`, `smpp`, et `unknown` pour un MT sans canal (lignes
antérieures à la migration, et la réponse automatique au STOP, émise par la passerelle sans ligne de
rang 0), pour que la somme des séries égale `submitted`. Le MO n'est sous aucune clé :
il entre par un connecteur, pas par un client ; `mo_received` y vaut 0. Trois clés au plus, jamais
`other`.

**C. Latence d'admission : reste `null`.** La CDR ne peut pas la porter (la ligne accepted est projetée
d'un enregistrement produit *avant* l'ACK qu'elle chronométrerait) et `ingest_duration_seconds` n'est
observé nulle part (debts/budget-d-ingestion-non-mesurable.md, non payée ici). Pas d'objet `by_channel` :
les comptes par canal sont dans `groupBy=channel`. Refus d'admission : comptés nulle part → fiche de dette.

**D. `active_sessions`.** ZSET global `sess:live`, membre = bind_id, score = expiration (celle de la clé du
compte), écrit dans le même pipeline que le ZADD de `sess:idx` (best-effort, rejoué à chaque
rafraîchissement), retiré par `Unindex`. RPC `CountSessions` : purge des expirés puis ZCARD. Écarts bornés
par la TTL, comme la clé du compte. L'API Admin l'appelle par `SessionDirectory` ; une erreur laisse le
champ absent, jamais 0. Pas de ventilation par type de bind : le registre ne l'agrège pas.

**E. Temps réel.** session-manager n'a pas d'émetteur : `smpp_sessions_active` → fiche de dette.
`MetricSample` documente la forme d'un histogramme sur le flux (`<kind>_count`, `<kind>_sum`, deltas en
secondes, moyenne seule). `ingest_duration_seconds` n'y est pas émis : dette existante (`budget-d-ingestion-non-mesurable`).

## Tests

- ClickHouse : une fenêtre qui mêle MT HTTP, MT SMPP, MT antérieur et MO compte chaque canal à sa place,
  le MO hors des clés.
- Registre : `CountSessions` compte un bind, ne recompte pas un rafraîchissement, oublie un unbind et un
  expiré.
- Handler de synthèse : `active_sessions` reflète le registre, absent quand il échoue.
- Mutations : filtre de canal retiré / inversé, repli « absent » remplacé par 0.
