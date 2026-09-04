# step-260g — Trois affirmations fausses corrigées, et `query_sm` a enfin sa fiche

> **Jalon :** Audit du 2026-09-03 (correctifs) · **Statut :** EN COURS (2026-09-04)
> **Dépend de :** — · **Bloque :** —

## Pourquoi cette fiche existe

La Definition of Done dit : « la PR qui périme une affirmation la corrige ». L'audit du 2026-09-03 a
trouvé trois affirmations que step-201c (ADR-0012) et step-130 ont périmées sans relecture, et une
promesse de spec (§6.22) que le code ne tient pas et qu'aucune fiche ne portait :

1. **Le CDR `enroute` serait écrit par `connector-pool-svc`.** Faux depuis step-201c : le pool publie
   l'issue de chaque `submit_sm_resp` sur `mt.outcome` et **commite** ; le projecteur
   `internal/outcome`, hébergé par `router-svc`, est le **seul** écrivain de la ligne `enroute`/`failed`
   (`cmd/router-svc/main.go:83-85` le dit en toutes lettres). Le pool n'écrit plus directement que les
   lignes qui **précèdent** l'effet irréversible : `cancelled`, `rerouted`, le `failed` de dead-letter
   (`internal/connectorpool/connectorpool.go:62-65`).
2. **Le simulateur SMSC serait « non disponible pour l'instant ».** Faux depuis step-130 : projet
   compagnon épinglé `v0.7.0` par `make smsc-sim` (`Makefile:92-97`), construit en CI (`ci.yml:70`),
   piloté par `internal/testutil/smscsim`, requis par les tests de résilience. `CLAUDE.md` raconte déjà
   ce que cette phrase a coûté : deux jalons de tests interdits par un document faux.
3. **`query_sm` répond toujours `UNKNOWN`.** `internal/smppserver/ops.go:36-40` renvoie `ESME_ROK` +
   `MessageStateUnknown` ; l'aveu est dans `internal/smpp/smpp.go:58-59` (« while the real state lookup
   is unimplemented »). La spec §6.22 exige « résolu contre le magasin de statut/CDR » ; step-390
   déclare la résolution hors de son périmètre. Personne ne portait la dette.

## Ce que l'exploration a établi

- Emplacements de l'affirmation 1 (recensés par `grep -rn enroute docs/ internal/config/config.go`) :
  `guide-ingenierie §3.4` (`:90`), diagramme `§5.1` (`:167`), `spec §4.2` (`:534`),
  `plan-execution §1.10` (`:137`) et le périmètre M2 (`:266`), godoc du type `config.ClickHouse`
  (`config.go:333-335`). `guide §16` (`:371`) est déjà juste. `strategie-de-test:102` décrit le test
  bout-en-bout M2 (« `POST /messages` → CDR `enroute` → `GET` ») sans attribuer l'écriture : juste.
- Emplacements de l'affirmation 2 : `strategie-de-test §2` (`:29` et `:39`), `glossaire:123`. La règle
  de choix du pair (`.claude/rules/tests.md:19-25`) est déjà juste : on y renvoie plutôt que de la
  recopier.
- Pour `query_sm`, tout est déjà câblé dans `smpp-server-svc` : `config.SectionClickHouse` déclarée
  (`main.go:55`), `clickhouse.NewCDRReader(st.ch)` construit pour `cancel_sm` (`wiring.go:231`), le
  throttle dédié §6.22 posé (`wiring.go:241-249`). La lecture scopée tenant existe :
  `CDRReader.Current(ctx, customerID, accountID, messageID)` (`cdr.go:422`). `ByMessageID` et
  `MessageStatus` (`cdr.go:409,440`) sont cross-tenant et hors chemin chaud : à ne **jamais** utiliser
  depuis une session.
- Enum CDR (`db/schema_passerelle_sms.sql:759`) : `accepted, enroute, delivered, failed, expired,
  rejected, rerouted, cancelled`. États SMPP §5.2.28 : `ENROUTE 1, DELIVERED 2, EXPIRED 3, DELETED 4,
  UNDELIVERABLE 5, ACCEPTED 6, UNKNOWN 7, REJECTED 8`.

## Design arrêté

**A. CDR `enroute`.** Une phrase par emplacement, même vérité partout : le pool publie sur `mt.outcome`,
`router-svc` projette la ligne `enroute`/`failed` (ADR-0012). `§3.3` du guide gagne la phrase
symétrique (router-svc héberge les projecteurs `accepted` et `outcome`). `plan-execution:266` décrit le
périmètre **de M2** : on n'y réécrit pas l'histoire, on ajoute la parenthèse « déplacé vers le
projecteur de router-svc par step-201c ». Le godoc de `config.ClickHouse` nomme les écrivains réels.

**B. Simulateur.** `strategie §2` : « projet compagnon, épinglé `v0.7.0` par `make smsc-sim`, piloté par
`internal/testutil/smscsim`, requis par les tests de résilience depuis step-130 » ; « réservé au vrai
simulateur (M8) » → « porté par le vrai simulateur depuis step-130 ». Renvoi à la règle de choix du pair.
`glossaire:123` : le faux SMSC sert aux réponses applicatives scriptées ; le simulateur porte
l'injection de pannes. Plus aucun « pas prêt / non disponible ».

**C. Fiche `tasks-todo/step-390b.md`.** Une fiche, pas une implémentation. Numéro **390b** : même
territoire que 390 (§6.22, `smpp-server-svc`), ne dépend d'aucune step, ne bloque pas le go-live
(répondre `UNKNOWN` est légal SMPP : promesse non tenue, pas panne). Elle consigne : les faits ci-dessus,
le passage du reader au `Listener` via `l.opts`, le mapping statut CDR → `message_state`, l'arbitrage
sur le **lag de projection** (ADR-0012 : un message soumis il y a 200 ms n'a que sa ligne `accepted` ;
répondre `ACCEPTED` est honnête), et `ESME_RINVMSGID` pour un ID inconnu **du tenant**. Ligne dans
INDEX sous « Écart contrat ↔ implémentation ».

## Chaîne de preuves

Une PR documentaire n'a pas de rouge ; sa preuve est la relecture croisée :

1. `grep -rn "enroute" docs/*.md internal/config/config.go` : plus aucune ligne n'attribue l'écriture au
   pool.
2. `grep -rn -i "pas prêt\|non disponible\|pas encore prêt" docs/*.md` : vide pour le simulateur (la
   citation historique de `CLAUDE.md` reste : elle raconte le coût, elle n'affirme pas).
3. `make check` vert (le godoc Go passe le lint).
4. `tasks-todo/step-390b.md` existe avec ses faits, son mapping et son arbitrage.

## Commits

1. Cette fiche.
2. `docs` : CDR `enroute` (5 fichiers docs + godoc `config.ClickHouse`).
3. `docs` : simulateur SMSC (2 fichiers).
4. `tasks` : fiche step-390b + ligne INDEX.
5. Fiche → `tasks-done/`.

## Definition of Done

- [ ] `make check` vert
- [ ] plus aucune source de vérité n'attribue la ligne `enroute` au pool (grep 1)
- [ ] plus aucune ne dit le simulateur indisponible (grep 2)
- [ ] `step-390b.md` présent, avec faits, mapping et arbitrage sur le lag

## Hors périmètre

L'implémentation de `query_sm` (step-390b). `tasks-done/step-114.md`, `tasks-done/step-201c.md` et les
ADR : documents datés, on ne les réécrit pas.
