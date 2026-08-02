# step-201c — Écritures CDR par batch dans le connector pool (le goulot du débit traversant)

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-201 · **Bloque :** step-201b

## But
Lever le goulot que le run de référence de step-201 a mesuré : le `connector-pool-svc` sort **192–330
`submit_sm/s`** quand l'ingestion en accepte 1 200, et le backlog `mt.routed` monte de ~900 rec/s.

## Le constat, mesuré
`processOne` appelle `CDR.Insert` **par message** au retour du `submit_sm_resp`
(`internal/connectorpool/connectorpool.go:980`), et `InsertBatch` touche deux tables — `cdr` puis
`cdr_events` — soit **quatre allers-retours ClickHouse par message**, synchrones, sur le chemin de
consommation, **avant le commit d'offset**.

Micro-banc isolant l'écriture CDR seule : **154 insert/s à 1 writer, 548 à 4**, puis **effondrement** à
278 (16 writers) et 301 (64) avec des `i/o timeout`. Les chiffres encadrent exactement la sortie
observée du pool.

**Aucun levier de `D5` ne le déplace** : ×4 sur `bind_pool_size` achète ×1,39 ; ×6 sur le pool
ClickHouse achète ×1,11. Un goulot insensible à la concurrence n'est pas une sérialisation par shard.

Le pair n'y est pour rien : le faux SMSC calibre à 218 000–255 000/s, et le simulateur à 34 872/s
(step-201 `D3`).

## Pourquoi une fiche à part
Le correctif touche **l'alignement `poll = insert = commit`**, l'une des six frontières de contrat de
step-201 `D6`. `D8` interdit tout buffer **côté client** parce qu'il désalignerait commit Kafka et
flush : il faudrait alors tracer quels offsets sont couverts par quel flush — une machine à états neuve
sur le chemin de durabilité du CDR. Ce n'est pas un cinquième sujet à glisser dans une PR qui en porte
déjà quatre.

## Périmètre
- Faire écrire le pool **par batch de poll**, comme le fait déjà le projecteur `accepted`
  (`internal/ingest/accepted.go`) : un `PrepareBatch`/`Send` pour tout le batch, pas un par message.
- **Préserver l'alignement** : le commit d'offset ne doit avoir lieu qu'après le `Send` qui couvre le
  batch. Un message envoyé au SMSC dont le CDR n'est pas écrit est une perte de traçabilité ; un offset
  commité avant le `Send` est pire.
- Le sharding par bind (`shardIndex`) traite les records d'un batch **en parallèle par shard** et un
  échec **halte le reste de son shard** (`errShardHalted`) : le batch CDR doit composer avec ça, pas le
  contourner.
- Re-lancer le run de référence de step-201 (`make load-reference`) et **consigner le nouveau chiffre**.

## Points d'implémentation clés
- **Les deux tables restent deux `Send`**, mais par batch et non par message : à quelques batches par
  seconde, c'est exactement le régime que `D8` décrivait — et croyait déjà atteint ici.
- **`async_insert` serveur reste interdit** en levier (`D6`) : `wait_for_async_insert=0` désaligne
  silencieusement l'ACK d'insert et le commit Kafka. Si le batching applicatif ne suffit pas, rouvrir
  `D6` explicitement plutôt que de contourner.
- Le chemin `cancelledRow` (`connectorpool.go:843`) écrit lui aussi par message — même traitement.
- **`chtest` laisse le pool ClickHouse à zéro**, que le driver lit comme « non réglé » et remplace par
  ses propres 5/10 : toute la suite d'intégration du dépôt tourne sur les défauts de la bibliothèque,
  jamais sur les leviers de step-201 `D5`. À corriger ici, sinon aucun test ne mesure ce qu'il croit.
- **`ingest_duration_seconds` est déclarée et observée nulle part** — le même défaut que
  `message_e2e_duration_seconds` avant step-201 PR2 — et ses bornes encadrent mal les seuils NFR (p50
  < 50 ms tombe entre 0,032 et 0,064 ; p99 < 250 ms entre 0,128 et 0,256). Deux lignes, même patron.

## Tests
- Le batch CDR couvre **tous** les records du poll, y compris ceux d'un shard halté avant lui.
- Un échec du `Send` **ne commite pas** les offsets du batch : la redélivrance rejoue, et le CDR n'est
  pas perdu.
- L'idempotence du CDR sous redélivrance (`ReplacingMergeTree` versionné) tient toujours.
- Le run de référence de step-201 est relancé : sortie ≈ acceptation, lag plat, à un débit consigné.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] alignement `poll = insert = commit` **préservé et testé** (frontière `D6`)
- [ ] run de référence relancé, sortie ≈ acceptation et lag plat, nouveau chiffre consigné
- [ ] `chtest` applique les leviers ClickHouse · `ingest_duration_seconds` alimentée et décidable

## Hors périmètre
Verdict NFR pleine échelle → step-201b (que cette step débloque). Chaos → step-202/203.
