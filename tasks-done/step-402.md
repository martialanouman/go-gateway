# step-402 — Débit MO par tranche : `mo_received` dans `TrafficMetrics` et compteur MO temps réel

> **Jalon :** M9, demandé par le tableau de bord (go-gateway-bo step-081) · **Statut :** LIVRÉE
> **Dépend de :** step-380 · **Bloque :** go-gateway-bo step-081

## Le constat
L'écran Trafic annonce des « compteurs/graphiques MT/MO » (spécification du tableau de bord §26), mais
`get-traffic-metrics` ne porte que `submitted`, `delivered` et `failed`, avec un filtre
`direction = 'mt'` (`internal/storage/clickhouse/metrics.go`). Le flux `stream-metrics` n'a aucun
compteur MO.

## Design arrêté (arbitrage Fable, 2026-10-11)
1. **Contrat (mineur, 7.6.0 → 7.7.0).** `mo_received` (integer) est ajouté aux points, hors de `required`.
2. **Une seule requête.** `Traffic` compte les deux directions. Comme `MetricsSummary`, chacun des
   compteurs MT est restreint à `direction = 'mt'`, et `mo_received` vaut `countIf(direction = 'mo')`.
   Sans cette restriction, un MO `delivered` compterait dans `delivered`.
3. **Rattachement.**
   - Par connecteur : le connecteur qui a reçu le MO. `metricsMessages` prend
     `anyIf(connector_id, direction = 'mo' OR dispatched)`, pour que la règle ne dépende pas du statut
     qu'écrira l'écrivain MO.
   - Par client : le client du compte destinataire.
   - Par groupe : le groupe COURANT de ce client, sinon `ungrouped`.
4. **MO non routés : dans aucune série.** Ils n'ont ni client ni ligne CDR. Ils sont déjà comptés par
   `UnroutedMetric` et listés par `list-unrouted-mo`. `other` et `ungrouped` gardent leur sens.
5. **Classement du top 20 : `submitted + mo_received`.** Une clé qui ne reçoit que du MO garde sa
   série.
6. **`maxRows` inchangé.** La requête reste groupée par (tranche, clé).
7. **Temps réel.** `mo_received_total{connector_id}` est ajouté au catalogue. Il est incrémenté dans
   `connectorpool.handleDeliver`, sur la branche MO, après la publication réussie sur `mo.inbound`. Il
   est émis sur le flux et sur Prometheus, sur le modèle de `observeSubmit`. L'unité est le
   `deliver_sm` : un MO en plusieurs morceaux compte une fois par morceau, comme `submits_total` côté MT.
   Aucune adresse ni aucun corps ne figure dans les labels.
8. **L'écrivain CDR MO** : d'abord différé, puis payé dans la même PR (§ suivant).

## Tests
- ClickHouse : une fenêtre qui mêle MT et MO, avec des MO dans des tranches sans MT, compte chaque
  direction à sa place, par connecteur et par client.
- Handler : `mo_received` est sérialisé ; une clé qui ne reçoit que du MO entre dans le top 20.
- connector-pool : un MO accepté incrémente `mo_received_total` ; une DLR ne l'incrémente pas, et une
  publication en échec non plus.
- Écrivain : la projection de `mo.routed` (champs, corps scellé sous le bon client, enregistrement
  corrompu sauté), `segment_count` de bout en bout, l'explorateur qui relit connecteur et segments, le
  lot gardé sur un arrêt en pleine écriture, le câblage de router-svc (depuis le LATEST).
- Mutations : le filtre de direction retiré ou inversé fait échouer un test nommé, comme chaque
  mutation de l'écrivain.

## Design arrêté — l'écrivain CDR MO, dette payée dans la même PR (arbitrage Fable, 2026-10-11)
Demandé par l'utilisateur : payer `debts/mo-jamais-ecrit-au-cdr.md` dans cette PR.

1. **Écrivain.** C'est une projection depuis `mo.routed`, dans mo-dlr-router-svc, sur le groupe dédié
   `mo-dlr-router-svc-mo-cdr` (`NewConsumerFromLatest`). Elle ne passe jamais par `MODeliveryRouter` : une
   panne ClickHouse y rejouerait la remise, donc livrerait le MO deux fois. `AcceptedConsumer` reçoit sa
   projection en paramètre, et une seule logique lot / commit / enregistrement corrompu sert les deux
   directions.
2. **Statut : `accepted`, une ligne par MO.** Il signifie « reçu et routé vers le compte ». `delivered`
   mentirait pour un MO parti en dead-letter. L'issue de la remise (bind, webhook, dead-letter) n'est pas
   projetée → `debts/issue-de-remise-mo-non-projetee.md`, et `MessageStatus` (contrat public) le dit.
   **Écart avec la spec §554**, qui place « CDR écrit » après la remise : l'intention, un CDR par MO
   imputable au connecteur et au client, est tenue, et l'ordre inverse exposerait à la double remise.
3. **Idempotence.** Le `message_id` déterministe du MO (`moMessageID`) et le rang fixe font qu'une
   relivraison de `mo.routed` est dédupliquée par la ReplacingMergeTree.
4. **Champs de la ligne.**
   - `submitted_at` = `ReceivedAt`, `segment_seq` = 1, `connector_id` = le connecteur récepteur.
   - Compte et client : ceux du compte résolu. `source` = `From`, `dest` = `To` (déjà normalisées).
   - `encoding` = `EncodingOf`, `billed` = false.
   - `segment_count` vient du `concat.Total` du réassemblage, porté par un nouveau
     `MORouted.SegmentCount` : 1 sans UDH, et 1 au décodage pour un enregistrement en vol qui ne le porte
     pas. On ne le recalcule jamais à partir du corps, qui n'est pas de l'UTF-8.
5. **Corps.** Il est scellé par `ingest.ContentSealer` selon la politique du client, avec la clé de
   données tirée de content-key-svc. La politique est chargée au démarrage seulement, comme le reste du
   service, qui n'a aucun watcher → `debts/politique-de-contenu-mo-rechargee-au-boot.md`.
6. **Contrat.** La description de `mo_received` (Admin) et `MessageStatus` (public) changent, sans autre
   bump : 7.7.0 n'est pas encore publié. La fiche de dette passe à PAYÉE.

### Corrections de la revue de l'écrivain (2026-10-11)
- **Hôte : router-svc, pas mo-dlr-router-svc (bloquant).** content-key-svc refuse
  `GetContentEncryptionKey` à mo-dlr-router-svc, et c'est voulu (step-295b, ADR-0016) : tout MO d'un client
  `stored_encrypted` aurait perdu son corps. router-svc a ce droit, héberge déjà la projection `accepted`
  et recharge la politique de contenu à chaque invalidation. Le projecteur MO y partage le sceller et la
  politique, sur son propre groupe (`router-svc-mo-cdr`, depuis le LATEST). La fiche « politique relue au
  démarrage » n'a plus d'objet.
- **Lecture agrégée.** L'explorateur, l'export et l'API publique rattachent le MO à son connecteur et
  lisent son `segment_count`, comme le font déjà les métriques.
- **Arrêt en plein lot.** Une écriture annulée ne valide plus le lot : il est relu au redémarrage et
  dédupliqué. Le défaut existait déjà dans les projections `accepted` et `outcome`.
- **Hors périmètre, avec une fiche :** l'Admin rend le corps révélé en `string(bytes)`, quel que soit son
  codage (`debts/corps-revele-sans-decodage.md`). C'était déjà le cas du MT soumis en SMPP UCS2 ; avec le
  MO, ça devient le cas courant.
