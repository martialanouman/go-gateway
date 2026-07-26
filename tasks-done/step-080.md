# step-080 — Poser le socle Redis + moteur de scripts Lua (EVALSHA atomique)

> **Jalon :** M6 (§10 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-084, step-101, step-105, step-122

## But
Créer le paquet `internal/storage/redis` (inexistant) : client go-redis v9 + un moteur qui charge un script Lua une fois et l'exécute par `EVALSHA` (rechargement transparent sur `NOSCRIPT`). Socle atomique commun au débit (M6), aux Bloom/exactroute (M7) et au disjoncteur (M8).

## Périmètre (ce que fait CETTE PR)
- `internal/storage/redis/redis.go` : constructeur du client (`*redis.Client`), options depuis `internal/config` (nouvelle `config.SectionRedis` : URL/DB/pool/timeouts).
- `internal/storage/redis/script.go` : type `Script` qui encapsule `source` + `sha1` ; méthode `Run(ctx, keys, args)` = `EVALSHA` puis fallback `EVAL`/`SCRIPT LOAD` sur `NOSCRIPT`.
- `internal/config/config.go` : ajouter `SectionRedis` (adresse Dragonfly de `make up`).
- Ping/`/readyz` : helper `Ping(ctx)` — mais Redis reste **non vital** pour `router-svc` (fail-closed, §1.5).

## Points d'implémentation clés
- **API go-redis v9 via `ctx7`** (constructeur, `EvalSha`/`ScriptLoad`, détection `redis.Nil`, parsing `NOSCRIPT`). Ne pas deviner la signature.
- **Jamais de read-modify-write côté Go** pour l'état atomique : toute la logique atomique vit dans un `Script` (règle d'or CLAUDE.md). Ce paquet ne fait qu'offrir le mécanisme.
- Un seul chargement paresseux du SHA par script ; concurrence-safe.
- Aucun corps de message ne transite ici (invariant a) — clés/compteurs uniquement.

## Tests (écrits dans la même PR)
- Intégration `testcontainers-go` (Dragonfly/Redis) : `Script.Run` renvoie le résultat ; après `SCRIPT FLUSH`, la 2e exécution se recharge sans erdeur (couvre le chemin `NOSCRIPT`).
- Unitaire : calcul du SHA1 identique à `SCRIPT LOAD`.
- `t.Skip` guidé par `DOCKER_HOST` (OrbStack) comme les autres tests d'intégration.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] `SectionRedis` documentée ; version go-redis figée via `ctx7` puis `make tidy`

## Hors périmètre
Les scripts métier (token-bucket, reserve/capture, breaker) — chacun dans sa step. Le pub/sub config-sync → step-105.
