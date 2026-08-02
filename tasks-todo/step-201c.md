# step-201c — Le CDR sortant devient une projection (le goulot du débit traversant)

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-201 · **Bloque :** step-201b

## But
Lever le goulot que le run de référence de step-201 a mesuré : le `connector-pool-svc` sort **192–330
`submit_sm/s`** quand l'ingestion en accepte 1 200, et le backlog `mt.routed` monte de ~900 rec/s.

## Le constat, mesuré
`processOne` appelle `CDR.Insert` **par message** au retour du `submit_sm_resp`
(`internal/connectorpool/connectorpool.go:980`). `Insert` est littéralement `InsertBatch` d'une ligne, et
`InsertBatch` touche deux tables — `cdr` puis `cdr_events` — soit **quatre allers-retours ClickHouse par
message**, synchrones, sur le chemin de consommation, **avant le commit d'offset**. Un message coûte
exactement ce que coûterait un batch de 5 000.

Micro-banc isolant l'écriture seule : **154 insert/s à 1 writer, 548 à 4**, puis **effondrement** à 278
(16) et 301 (64) avec des `i/o timeout`. **Aucun levier de step-201 `D5` ne le déplace** : ×4 sur
`bind_pool_size` achète ×1,39, ×6 sur le pool ClickHouse achète ×1,11. Un goulot insensible à la
concurrence n'est pas une sérialisation par shard.

Le pair n'y est pour rien : le faux SMSC calibre à 218 000–255 000/s, le simulateur à 34 872/s (`D3`).

---

## Design arrêté (2026-08-02)

### D1 — Le CDR sortant devient une projection, il n'est plus écrit sur le chemin d'envoi
Le pool **cesse d'écrire ClickHouse**. Il publie l'issue du message sur Kafka — un produce déjà sur son
chemin — et un **consommateur dédié** projette la ligne CDR, en batchant par poll. C'est le patron que
le dépôt applique déjà à la ligne `accepted`, projetée par un groupe dédié depuis `mt.inbound`.

**Raison — pourquoi pas simplement batcher sur place.** Batcher l'écriture après la boucle ferait
redélivrer **tout le batch** en cas d'échec du `Send`, donc re-soumettre au SMSC des messages **déjà
envoyés**. Ordre de grandeur mesuré depuis les défauts réels (`FetchMaxPartitionBytes` = 1 MiB jamais
réglé, ~0,7–1 Ko par record, 12 partitions) : **~1 000 records par partition, jusqu'à ~10 000 par poll**.
Et le pire cas est corrélé au pire moment : la cause d'un échec de `Send` est une saturation ClickHouse,
celle-là même qui creuse le backlog et remplit les polls. On multiplierait le rayon de duplication par
10³ précisément quand il se déclenche.

**Raison — pourquoi pas le fail-open, malgré le précédent `settle`.** `settle` est délibérément
fail-open et le dit : « a propagated error would redeliver the record and re-submit the SMS (a
duplicate) ». Mais ce fail-open n'est acceptable **que parce qu'un reaper le rattrape** — et
`billing.Reaper` (step-190) « settles each against the message's **recorded CDR outcome** »
(`reaper.go:29-32`). Sans ligne CDR, `TestReaperNeverReleasesBlind` prouve que la réservation est
**laissée intacte** : du crédit client bloqué à vie. Un fail-open sur le CDR détruirait donc le filet
qui rend le fail-open de `settle` acceptable. Il n'y a par ailleurs aucun reaper CDR.

**Ce que E gagne, et que les autres options ne gagnent pas :**
- le batching atterrit **là où la redélivrance est inoffensive**, donc il peut être agressif sans
  arbitrage de sûreté — la correction de débit et la correction de risque sont le même geste ;
- la seule chose fail-closed après le `submit_sm` devient un produce Kafka, dont le domaine de panne
  est **décorrélé** de la saturation ClickHouse (Kafka down ⇒ le pool ne consomme plus rien de toute
  façon) ;
- **aucune ligne n'est jamais perdue** : Kafka est le spool durable, la projection est at-least-once, et
  `cdr` est un `ReplacingMergeTree(version)` qui dédoublonne le rejeu ;
- **un amplificateur de gravité disparaît** : aujourd'hui un `i/o timeout` ClickHouse n'est pas reconnu
  par `isLinkDrop`, fait tomber tout le cycle de dial, et **parque le pod** jusqu'à un reconfigure
  Admin. Une panne d'écriture d'observabilité qui met un pod du plan de données hors service est un
  défaut de conception, pas un réglage.

Écartées : **batch + fail-closed** (×10³ sur le rayon) · **batch + fail-open** (casse le reaper) ·
**batch + garde Redis « déjà envoyé »** (garde ClickHouse en section critique, ajoute un RTT sur le
chemin chaud, et n'a pas de bonne réponse quand Redis tombe) · **réduire les allers-retours sans
batcher** (au mieux ×2 quand il en manque ×4).

### D2 — Le rayon de duplication est borné à ~250 SMS par partition et par crash
Plafonner « la taille du batch CDR » ne bornerait rien : sous `D1` le pool n'écrit plus de CDR. La
duplication est bornée par le **nombre de `submit_sm` effectués depuis le dernier commit d'offset**,
c'est-à-dire par la taille d'un poll. Le levier est `FetchMaxPartitionBytes`, **jamais réglé
aujourd'hui** — donc au défaut franz-go de 1 MiB (`kgo/config.go:676`), soit ~1 000 records par
partition à ~0,7–1 Ko le record.

**Le cap est posé à 256 KiB**, exposé en `KAFKA_FETCH_MAX_PARTITION_BYTES` : environ **250 SMS
dupliqués par partition et par crash de pod**. À la cible NFR (8 000 msg/s sur 12 partitions, soit
~667 msg/s par partition), c'est **moins d'une demi-seconde de trafic** par partition.

**Raison.** La fenêtre résiduelle de `D1` — crash entre le `submit_sm` et l'ack du produce — n'est pas
nulle. Un `submit_sm` n'est transactionnel avec aucun store : aucune conception ne l'élimine, seule sa
**borne** est un choix. La laisser implicite, c'est étendre une garantie que la spec n'a jamais chiffrée.

Le coût de la borne est négligeable : ~2,6 polls/s par partition au lieu de 0,7. Sous `D1` le pool
n'est plus limité par ClickHouse mais par le SMSC, et le batching de projection vit dans un autre
consommateur, où la taille de poll n'est pas contrainte par ce cap. Diviser le rayon par 4 ne coûte
donc rien d'observable. *(Arbitré avec l'utilisateur.)*

**Honnêteté du chiffre** : le cap est une borne en **octets**, le « ~250 » une conversion à ~1 Ko le
record. Un trafic majoritairement UCS-2 ou portant de longues `fallback_chain` donnera moins de records
pour les mêmes octets — jamais plus. La borne en SMS est donc un **majorant**, ce qui est le bon sens
pour un rayon de duplication.

### D3 — `cdr_events` doit devenir idempotent, sinon la projection duplique la timeline
`cdr` est un `ReplacingMergeTree(version)` : un rejeu y collapse. **`cdr_events` est un `MergeTree`
append-only** (`migrations/clickhouse/0006_cdr_events.up.sql:19`). Une projection at-least-once y
insérerait donc des événements en double.

Deux issues, à trancher en phase 5 sur le code : horodatage `at` **déterministe** (porté par
l'événement, jamais `now()` à l'insertion) plus dédoublonnage à la lecture ; ou passage en
`ReplacingMergeTree` avec une clé qui ne collapse que les vrais doublons — le commentaire de la
migration n'interdit que le collapse *inter-stages*, pas le dédoublonnage intra-stage.

**Raison.** Sans ça, `D1` n'est idempotent que sur `cdr`. La timeline est exposée par un endpoint livré
(get-message-trace) : des étapes en double y seraient visibles par le client.

> **Sans objet — vérifié sur le code (2026-08-02). La première des deux issues est DÉJÀ en place.**
>
> - **`at` est déjà déterministe** : `appendEvents` pose `at = row.SubmittedAt`, ou `*row.DeliveredAt`
>   quand il est non nil (`cdr.go:196-199`). Les deux sont portés par l'événement ; jamais `now()`.
>   Sur le chemin `enroute` que `D1` déplace, `DeliveredAt` est nil, donc `at` vaut le `SubmittedAt`
>   immuable.
> - **La lecture dédoublonne déjà, et délibérément** : `Timeline` fait `GROUP BY segment_seq, status`
>   avec `max(version)` / `min(at)` / `argMax(...)` (`cdr.go:656-669`), sous un commentaire qui nomme
>   exactement ce cas — « the data plane is at-least-once, so one stage can be written more than once,
>   and two identical spans read as two events that never happened ».
> - **Un test le prouve déjà** : `TestTimelineDeduplicatesRedeliveredStages`
>   (`timeline_integration_test.go:85`) écrit **trois fois** la même ligne et exige une seule milestone.
>
> `D3` — et l'alerte qui l'a produite — **surestimaient le problème** : le moteur append-only est bien
> réel, mais il n'a jamais été le chemin par lequel un doublon atteint le client.
>
> **Ce qui reste vrai, et qui est une note de stockage, pas de correction** : une redélivrance écrit
> bien une ligne de plus dans `cdr_events`, sans collapse. C'est borné par le TTL de 90 jours et sans
> effet sur ce qui est lu. À surveiller si une campagne produit des redélivrances massives — pas à
> corriger ici.
>
> **Conséquence sur le plan** : `D3` ne demandait aucun code, donc **PR1 n'est plus un prérequis
> bloquant de PR2**. PR1 se réduit à `D5`, et garde son intérêt propre.

### D4 — Le statut `enroute` devient asynchrone : best-effort, alerté au-delà de 30 s
L'OpenAPI publique documente le statut comme « **dernière projection, pas état temps réel** ». Une
métrique expose le lag du groupe de projection, alertée à 30 s. **Aucun chiffre n'est promis au client.**

**Raison.** C'est une latence, pas un mensonge : le treillis de statuts est monotone, on montre un état
antérieur **vrai**, jamais un état faux. Et le contrat ne change pas de classe : la ligne `accepted` est
**déjà** une projection asynchrone depuis `mt.inbound`, `delivered`/`failed` arrivent par DLR. Aucun
client ne peut aujourd'hui supposer un statut synchrone. Un SLO ferme contraindrait le dimensionnement
du consommateur au-delà de ce que M12 prévoit, pour une garantie qu'aucune autre partie du système ne
tient. *(Arbitré avec l'utilisateur.)*

### D8 — `Deps.Producer` devient obligatoire : `New` panique sur nil
Le défaut no-op disparaît, **y compris sa variante sélective**. Un `discardProducer` explicite est câblé
dans les tests qui n'assertent rien sur la voie retour, et un test de câblage exerce `wiring.go`
jusqu'à `New`.

**Raison — le no-op sélectif échoue au mauvais moment.** J'avais d'abord gardé le défaut en lui faisant
refuser le seul topic d'issue, pour qu'un pool mal câblé « échoue bruyamment à son premier envoi ».
C'est faux : le produce arrive **après** le `submit_sm`. Concrètement, un pool sans producteur envoie
réellement le SMS, le no-op refuse, l'offset ne commite pas, le record est redélivré — et **le même SMS
repart, en boucle, jusqu'à intervention humaine**. Le correctif ne rendait pas l'erreur visible : il la
convertissait en incident de duplication permanent, visible par les abonnés.

**Le critère est donc temporel** : une erreur de câblage doit devenir visible **avant** que le processus
puisse accomplir un effet externe irréversible. Seul un échec au démarrage le satisfait.

**Pourquoi un `panic` et pas une erreur retournée.** Un `Producer` nil est une erreur de programmation,
pas une condition d'exploitation : aucun opérateur ne peut la provoquer par configuration, aucun
appelant ne peut la traiter utilement. Changer la signature de `New` pour une erreur que 30 sites ne
feraient que propager serait du bruit. Le dépôt a le précédent exact — `Deps.CDR` n'a pas de défaut et
panique si nil — plus deux précédents de panique sur erreur de câblage (garde de cardinalité des
métriques, stub HTTP sur mode inconnu).

**« Ça casse 13 tests » n'est pas un argument de conception** : c'est un coût mécanique, opposé à un
mode de défaillance permanent. Et l'explicitation est un gain — chaque test dira désormais qu'il jette
les issues, au lieu que le constructeur le décide pour lui.

**La règle générale, à écrire dans le godoc de `Deps`** : *un défaut no-op est légitime si et seulement
si le service qui en résulte est un mode d'exploitation voulu et cohérent.* Les cinq autres la passent —
`Billing` nil est la facturation opt-out, une règle d'or du projet ; `Throttle`/`DeadLetter` nil sont de
l'observation en moins. `Producer` la passait **avant** `D1` (un `deliver_sm` non publié était le
comportement M2 assumé) et ne la passe plus : un pool qui envoie des SMS sans en garder trace n'est le
mode voulu de personne.

### D12 — Le cap est 56 KiB, parce que la borne porte sur des octets COMPRESSÉS
`KAFKA_FETCH_MAX_PARTITION_BYTES` passe de 256 KiB à **56 KiB**. La promesse d'ADR-0012 — ~250 messages
par partition et par crash — est tenue par le code au lieu d'être seulement écrite.

**Raison — `D2` reposait sur une conversion fausse.** `max.partition.fetch.bytes` borne les batches
**stockés, donc compressés**. franz-go compresse en **snappy par défaut** (`kgo/config.go:659`) et le
producteur du dépôt ne le surcharge pas. Mesuré sur 500 records `mt.routed` réalistes (UUID distincts,
MSISDN distincts, corps GSM-7 de 130 caractères) : **621 o/record brut, 221 o/record compressé, ratio
2,81×**. Donc 256 KiB portaient **~1 187 messages**, pas 250 — un facteur 4,7 d'erreur sur un chiffre
**déjà ratifié**.

Corollaire : le défaut franz-go de 1 MiB ne portait pas ~1 000 records mais **~4 750**. Le levier divise
donc bien le rayon par ~4 comme annoncé ; c'est la valeur absolue qui était fausse, aux deux bouts.

**56 KiB ≈ 250 records** aux 221 o/record mesurés, soit ~2,7 polls/s par partition à la cible NFR —
exactement le coût annoncé lors de l'arbitrage de `D2`, et négligeable puisque le pool n'est plus limité
par ClickHouse. *(Arbitré avec l'utilisateur : tenir la promesse ratifiée plutôt que réécrire
l'engagement.)*

**Ce que le chiffre vaut, honnêtement.** Il dépend du taux de compression, donc du trafic : des corps
plus répétitifs compressent mieux et le rayon monte, de l'UCS-2 ou du binaire compressent mal et il
baisse. Ce n'est ni un majorant ni un minorant — c'est une estimation pour un trafic typique, et l'ADR
doit le dire au lieu de prétendre le contraire.

### D9 — Le groupe de projection démarre au début du topic, pas à la fin
`NewConsumer` (AtStart), pas `NewConsumerFromLatest`.

**Raison.** `NewConsumerFromLatest` est documenté pour un group id **par instance**, où un groupe neuf
ne doit pas rejouer un topic accumulé depuis des mois. Ni l'un ni l'autre ne s'applique : ce groupe est
fixe et couvre toute la flotte, et `mt.outcome` est un topic **neuf** — au déploiement qui l'introduit
il n'y a rien à rejouer, donc l'argument de la tempête d'écritures est sans objet.

Les coûts d'erreur sont très asymétriques. Au début : une rafale de réécritures que le
`ReplacingMergeTree` collapse. À la fin : les issues produites avant que le consommateur rejoigne — et
**rien n'ordonne** le déploiement de `connector-pool-svc` et de `router-svc`. Ces messages restent
`accepted` à vie, et `billing.Reaper` règle contre l'issue CDR enregistrée, donc leur réservation est
retenue pour de bon. Sans log, sans métrique, sans erreur. Le commentaire d'origine s'appuyait sur un
runbook qui **n'existe pas** dans le dépôt.

Même raisonnement sur `OffsetOutOfRange` : un projecteur arrêté plus longtemps que la rétention sauterait
sinon droit à la fin.

### D10 — `FETCH_MAX_PARTITION_BYTES` au-dessus de `FETCH_MAX_BYTES` est refusé
**Raison.** franz-go **rabaisse silencieusement** un plafond de partition supérieur au plafond de
réponse (`kgo/config.go:235-237`). Le silence est le problème : ce réglage est la borne de duplication
d'ADR-0012, donc une valeur qui ne prend pas effet est une borne à laquelle un opérateur croit sans
l'avoir — exactement le défaut que l'ADR corrige.

### D11 — Le run de référence embarque la projection
La pile de `reference_test.go` câble le producteur du pool et démarre le projecteur, et `mt.outcome`
entre dans les topics dont le lag est suivi.

**Raison.** Le run est le gate de la DoD. Sans producteur, le chemin post-envoi du pool ne fait rien :
il publierait comme débit de la step celui d'une passerelle qui n'enregistre pas ce qu'elle envoie,
pendant que la production paie un produce synchrone acké par message. Et le lag de projection est
précisément le backlog que `D1` introduit — la clause « lag plat » de `D2` doit le voir, sans quoi un
`mt.routed` plat masquerait un `mt.outcome` qui monte.

### D5 — La divergence commentaire/code d'`appendEvents` est tranchée dans le sens du commentaire
`internal/storage/clickhouse/cdr.go:186-188` dit qu'un échec de la timeline « must not fail » le CDR —
mais `cdr.go:180` **propage l'erreur**. Le code suivra le commentaire : un échec `cdr_events` est logué
et compté, jamais propagé.

**Raison.** Le CDR est l'autorité de facturation et de reporting ; la timeline est du confort de tableau
de bord. Faire échouer la première pour la seconde inverse la hiérarchie que le commentaire énonce. Il
n'y a de toute façon **aucune atomicité** entre les deux tables : quand `appendEvents` échoue, les
lignes `cdr` sont déjà durables, donc propager l'erreur fait rejouer un travail déjà fait.

### D6 — Les trois autres sites d'écriture CDR du pool ne bougent pas dans cette step
Annulation (`connectorpool.go:843`), reroute (`reroute.go:109`) et dead-letter (`reroute.go:150`)
écrivent tous **avant** l'effet irréversible : leur échec ne peut pas dupliquer un SMS. Ils basculeront
sur la même projection par uniformité, **sans urgence**, hors de cette step.

**Raison.** Le sujet de la step est le goulot, et le goulot est le site post-submit. Élargir aux trois
autres triple la surface de revue pour un gain de débit nul — ces chemins sont rares par construction.

### D7 — Un ADR amende §6.7, à faire ratifier
La spec autorise le duplicata par implication (« remise **au moins une fois** au SMSC », « l'exactement-
une-fois n'est pas garanti ») mais **ne le documente jamais explicitement, ne le borne pas, et ne le
mitige pas**. Les deux mitigations qu'elle nomme ne couvrent pas ce cas : les clés d'idempotence client
protègent du rejeu *du client* à la frontière REST ; l'idempotence de facturation protège l'argent, pas
le combiné.

L'ADR nomme la fenêtre résiduelle de `D1`, la borne de `D2`, et assume l'engagement. **À ratifier avant
merge** : c'est un engagement vis-à-vis des opérateurs et des abonnés, pas une décision de code.

> **Rédigé (2026-08-02) : `docs/adr/0012-duplication-submit-sm-bornee.md`, statut `Proposed`.**
> Il passe `Accepted` après revue, selon la convention de `docs/adr/0000-index.md`. L'index était en
> retard de trois entrées (0010, 0011 manquaient) — rattrapé au passage.
>
> Le texte de §6.7 et du guide §10 n'est **pas encore amendé** : ils renverront à l'ADR une fois celui-ci
> accepté, pour ne pas modifier la spec sur la foi d'une décision `Proposed`.

---

## Plan — trois PRs

| PR | Unités | Fichiers | Dépend de |
|---|---|---|---|
| **1 — le socle** | ~~U1 `cdr_events` idempotent~~ **sans objet, déjà en place** (`D3`) · U2 `appendEvents` fail-open (`D5`) | `internal/storage/clickhouse/cdr.go` | — |
| **2 — la projection** | U3 topic + encodage de l'issue · U4 le pool produit au lieu d'écrire · U5 le consommateur de projection batché | `internal/storage/kafka/topics.go`, `internal/pipeline/wire.go`, `internal/connectorpool/`, nouveau paquet de projection, `cmd/` | PR1 |
| **3 — les garanties et la preuve** | U6 métrique de lag + alerte + doc OpenAPI (`D4`) · U7 cap d'in-flight (`D2`) · U8 ADR §6.7 (`D7`) · U9 run de référence relancé | catalogue de métriques, `api/openapi-public.yaml`, `docs/adr/`, `test/load/README.md` | PR2 |

**PR1 devait précéder PR2** parce que la projection at-least-once était censée dupliquer la timeline.
**Vérification faite, elle ne la duplique pas** : la lecture dédoublonne déjà (cf. l'encadré de `D3`).
PR1 se réduit donc à `D5` et n'est plus bloquante — elle reste livrée en premier parce qu'elle est
petite, autonome, et qu'elle retire une propagation d'erreur que PR2 rendrait de toute façon caduque.

**U4 et U5 ne sont pas parallélisables** : U5 consomme ce que U4 produit, et le contrat entre les deux
est justement ce qu'un relecteur doit pouvoir refuser d'un bloc.

## Périmètre
- Publier l'issue du message sur Kafka au lieu d'écrire ClickHouse, au site post-submit uniquement.
- Un consommateur de projection dédié, batché par poll, at-least-once.
- `cdr_events` idempotent (`D3`).
- `appendEvents` fail-open (`D5`).
- Cap d'in-flight non commités documenté (`D2`), métrique + alerte de lag (`D4`).
- Re-lancer le run de référence de step-201 (`make load-reference`) et **consigner le nouveau chiffre**.

## Points d'implémentation clés
- **Le produce doit être acké avant le commit d'offset** — c'est ce qui remplace l'ancienne garantie.
- Le sharding par bind traite les records d'un batch **en parallèle par shard**, et un échec **halte le
  reste de son shard** (`errShardHalted`). Le commit est calculé **par partition Kafka**, pas par shard :
  un échec coupe tous les offsets supérieurs de la même partition, y compris ceux d'autres shards.
- **`TestBindPoolKeepsSegmentsOnOneBindInOrder` est le test qui contraint le plus** : l'ordre des
  `submit_sm` doit rester inchangé.
- **`errShardHalted` n'est couvert par aucun test aujourd'hui** — grep sur tout le dépôt : zéro
  résultat. À couvrir puisque cette step y touche.
- **`chtest` laisse le pool ClickHouse à zéro**, que le driver lit comme « non réglé » : toute la suite
  d'intégration tourne sur les défauts de la bibliothèque, jamais sur les leviers de step-201 `D5`.
- **`ingest_duration_seconds` est déclarée et observée nulle part** — le défaut exact de
  `message_e2e_duration_seconds` avant step-201 PR2 — et ses bornes encadrent mal ses seuils NFR
  (p50 < 50 ms entre 0,032 et 0,064 ; p99 < 250 ms entre 0,128 et 0,256).

## Tests
- Un échec de projection **ne fait pas** re-soumettre un message déjà envoyé.
- La projection est idempotente sous redélivrance, **sur les deux tables** (`D3`).
- Un échec `cdr_events` ne fait pas échouer l'écriture `cdr` (`D5`).
- L'ordre des segments sur un bind est inchangé.
- Le run de référence est relancé : sortie ≈ acceptation, lag plat, à un débit consigné.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] aucun SMS re-soumis sur un échec de projection, **testé**
- [ ] projection idempotente sur `cdr` **et** `cdr_events`
- [x] cap d'in-flight **tranché** : `KAFKA_FETCH_MAX_PARTITION_BYTES` à 256 KiB ≈ 250 SMS par partition
      et par crash (`D2`) — reste à exposer et câbler en PR3
- [x] ADR-0012 rédigé (`D7`) — statut `Proposed`, à ratifier avant merge
- [ ] statut documenté comme projection dans l'OpenAPI · métrique de lag exposée et alertée (`D4`)
- [ ] run de référence relancé, sortie ≈ acceptation et lag plat, nouveau chiffre consigné

## Hors périmètre
Verdict NFR pleine échelle → step-201b (que cette step débloque). Les trois autres sites d'écriture CDR
du pool → `D6`. Chaos → step-202/203.
