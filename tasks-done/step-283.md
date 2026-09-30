# step-283 — Le débit se refuse avant l'ACK, jamais après

> **Jalon :** M12 · **Statut :** FAIT
> **Dépend de :** — · **Bloque :** step-287
> Décision humaine du 29/09/2026 ; unité faute de multiple de dix libre.

## Pourquoi

Un message acquitté peut être perdu pour une simple question de débit.
- Le contrôle de débit n'a lieu qu'au routeur, **après** l'ACK durable. `Enforcer.Check` vérifie tour à tour
  le compte, la route et le connecteur (`internal/pipeline/ratelimit/enforcer.go:96`), et rend
  `ErrRateLimited`.
- Le routeur traite toute erreur codée comme un rejet définitif (`internal/router/router.go:182`) : CDR
  `rejected`, offset commité. Le client avait reçu un 200 ou un `ESME_ROK`.
- Le seau du **connecteur** est partagé : un gros client qui le vide fait rejeter les messages des petits
  clients routés sur ce connecteur.
- La spec §6.4 promet l'inverse : « à l'approche du plafond, `connector-pool-svc` ralentit la consommation
  Kafka ; les messages restent durablement en file ».
- Aucun contrôle de débit n'existe sur la soumission : REST et SMPP acquittent tout ce qui arrive.

## Décision humaine

1. **Admission à l'ingestion, par compte.** Le seau `rate_limits` du compte s'applique **avant l'ACK** :
   `429` en REST, `ESME_RTHROTTLED` en SMPP. Un client ne fait plus entrer dans la file partagée plus que son
   contrat.
2. **Le routeur ne rejette plus pour débit.** Le plafond du connecteur devient de la backpressure dans le
   pool : on ralentit, on ne jette pas.

## À arbitrer (spec → Fable → humain)

- **Où vit le plafond du connecteur** une fois sorti du routeur. `Enforcer.AllowConnector` existe déjà
  pour le drainer (step-126) : le pool attend-il le jeton avant chaque `submit_sm` ? Et que devient la fenêtre
  SMPP pendant cette attente ?
- **Le seau `route`** (`entity_type = 'route'`) : il se paie au routeur ou au pool, mais ne peut plus rejeter.
  Sinon, le supprimer.
- **Le compte au routeur** : il est retiré, sinon le même segment serait décompté deux fois.
- **Le coût d'admission** : une soumission REST ou SMPP de N segments coûte N jetons. L'ingestion ne
  segmente pas aujourd'hui. Faut-il estimer le nombre de segments à l'entrée, ou compter un jeton par
  message ? Chiffrer l'écart sur un trafic UCS-2.
- **La politique de panne à l'ingestion** : le plafond local par pod (`localCeiling`) multiplié par le
  nombre de pods REST et SMPP. Écrire la borne réelle.
- **`MaxPerDay`**, chargé mais jamais appliqué : l'admission le rend-elle applicable, ou reste-t-il une
  dette ?
- **Le reroute et la file de parking** passent déjà par `AllowConnector` : vérifier qu'aucun chemin ne
  reste où `ErrRateLimited` devient un rejet.

## Definition of Done

- [x] un test prouve qu'un message au-delà du débit du compte est aujourd'hui **rejeté après l'ACK** —
      rouge lu sur le code actuel
- [x] un test prouve qu'un connecteur saturé par un compte ne produit **aucun** CDR `rejected` pour un
      autre compte — rouge lu sur le code actuel
- [x] `429` REST et `ESME_RTHROTTLED` SMPP au-delà du débit du compte, sans écriture sur `mt.inbound`
- [x] spec §6.4 et guide alignés ; le guide §4.1 corrigé au passage (`mt.routed` est clé par
      `message_id`, pas par `(connector_id, shard)`)
- [x] l'ordre du pipeline de `CLAUDE.md` et de la spec (§4, §4.2, §6.1 — la « §5.1 » visée est le guide) dit que le débit se contrôle avant l'ACK
- [x] les quatre invariants verts

## Design arrêté

Arbitrages du 29/09/2026 : la spec tranche S1-S5, Fable tranche F1-F6 sans heurter la spec.

**Tranché par la spec**
- **S1 — Admission par compte dans `ingest.Ingestor.Accept`**, avant l'encodage et le produce : le seul
  chemin partagé REST/SMPP. `ErrRateLimited` y devient `429` (humaerr) et `ESME_RTHROTTLED`
  (`SMPPStatusForError`). Rien n'est écrit sur `mt.inbound`. En REST idempotent, le créneau est libéré et le
  client peut réessayer.
- **S2 — Le plafond du connecteur passe au pool.** Chaque `submit_sm` attend son jeton du seau `connector`
  (§6.4 backpressure ; ADR-0021 §5 « chaque envoi consomme un jeton du seau du connecteur »).
  L'attente précède l'écriture du PDU, donc aucune place de la fenêtre SMPP n'est occupée. La boucle
  sérielle du shard et la barrière de lot calent, et la consommation Kafka ralentit : c'est la backpressure.
- **S3 — L'étape rate-limit du pipeline disparaît** : le compte passe à l'ingestion, le connecteur au pool,
  la route est supprimée (F1). `pipeline.Deps.RateLimiter` et `Enforcer.Check` sont retirés. Le routeur ne
  peut donc plus produire de CDR `rejected` pour cause de débit.
- **S4 — Le parking et le draineur restent** : la spec §6.15 veut éviter une tempête de republication.
- **S5 — Le seau `sender_id` n'est pas traité ici** : il revient à la step qui porte ADR-0021.

**Tranché par Fable**
- **F1 — Suppression du seau `route`.** Il n'est écrivable que par SQL, jamais par l'Admin. Le payer au pool
  bloquerait en tête de ligne les autres routes du shard. Changements : migration `0023` (CHECK sans
  `'route'`), schéma, spec §6.4 « compte/connecteur ». La migration **ne supprime aucune ligne** : si une
  ligne `route` existe, l'`ADD CONSTRAINT` échoue et l'opérateur décide (écart assumé avec Fable, qui
  proposait un `DELETE`).
- **F2 — Un seau de republication distinct.** `AllowConnector` (porte de parking, draineur) consomme la
  fenêtre `"reroute"` au débit de la cible. L'envoi est seul à payer la fenêtre `"sec"`. Sans cela, un
  message rerouté paierait deux fois.
- **F3 — Le coût d'admission est le nombre exact de segments.** `pipeline.SegmentCount(in)` reprend la
  détection d'encodage et le `Split` des étapes 6-7 : ce sont les mêmes fonctions, et un test vérifie la
  parité. Avec un jeton par message, un UCS-2 de 160 caractères (3 segments) coûterait 1, et le client
  enverrait 3× son contrat.
- **F4 — Position de l'attente au pool** : après le max-age, avant le claim d'annulation. Un message en
  backpressure reste ainsi annulable. Si le disjoncteur est ouvert, le jeton est perdu, mais ce budget ne
  sert à personne pendant la panne.
- **F5 — `MaxPerDay` reste une dette**, avec une fiche dans `debts/` : GET /account l'expose sans qu'il soit
  appliqué.
- **F6 — Politique de panne** : pas de code. La borne s'écrit dans §6.4 : débit du compte × (pods REST +
  pods SMPP portant un bind du compte, au plus min(`max_sessions`, pods SMPP)).

**API de `ratelimit.Enforcer`** : `AdmitAccount(ctx, account, segments) error` ·
`WaitConnector(ctx, connector) error` · `AllowConnector(ctx, connector) bool` (fenêtre `"reroute"`).
Côté pool, un enregistrement de `mt.routed` est un segment, donc un `submit_sm`, donc un jeton : la revue a
retiré le paramètre `segments`, qui ne valait jamais que 1. `WaitConnector` refait l'essai après
max(1/débit, 5 ms) et rend `ctx.Err()` dès que le ctx est mort, même si le repli par pod accorde le jeton.

**Déploiement** : le routeur **d'abord**. Un ancien routeur derrière une ingestion neuve ferait payer le
compte deux fois et rejetterait après l'ACK. Dans l'autre ordre, la fenêtre est brève et sans limite de
compte. Il n'y a pas de production (environnement de test seulement).

**Tests** : R1 est un rouge REST par le câblage réel (`newHTTPServer`) : aujourd'hui 202, attendu 429. R2 est
un rouge routeur par `newPipelineStack` : un connecteur vidé par le compte A rejette aujourd'hui le compte B
en `rate_limited`. S'y ajoutent l'ingestion (rien produit au-delà du débit ; chaos Redis déplacé du routeur
vers l'ingestion), le pool (attente avant le claim et avant le submit), l'Enforcer (attente, fenêtres
disjointes) et la parité des segments.

## Revue (30/09/2026)

Trois axes : mécanisme, tests, doc et code en trop. Aucun constat bloquant n'est resté ouvert.
- **Corrigés** :
  - `WaitConnector` sur un ctx mort ;
  - `Retry-After: 1` sur le 429 (le contrat le déclarait déjà) ;
  - le paramètre `segments`, retiré ;
  - la réconciliation inverse du chaos ;
  - les tests à 1/s rendus robustes à un premier produce lent, et les lignes `rate_limits` nettoyées ;
  - la spec : §6.1, §6.6, §6.7, §6.9, §6.11 ; le glossaire, le guide de codage, le guide §3.1 et §3.2.
- **Mis en dette** après arbitrage de Fable :
  - `debts/rebalancement-pendant-un-lot-ralenti.md` ;
  - `debts/instantane-de-debit-charge-au-boot.md` ;
  - `debts/reservation-expiree-sous-backpressure.md`.
- **Écartés, avec raison** :
  - Les gardes de câblage SMPP et pool, jugées « en trop » : ce sont les seuls tests qu'une mutation du
    câblage fait tomber.
  - Le helper commun de chargement : il ferait dépendre `ratelimit` de `postgres`, pour trois lignes.
  - Le max-age revérifié après l'attente : l'écart est borné par la contention.
  - Le 503 sur une clé idempotente en attente pendant un 429 : c'est le comportement existant de toute
    erreur d'`Accept`.
