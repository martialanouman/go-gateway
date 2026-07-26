# step-065 — Anti-spam : moteur + règles contenu & doublons (étape MT activée)

> **Jalon :** M5 (§9 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-066

## But
Activer l'étape STUB `pipeline.anti_spam` avec un moteur de règles et deux familles : contenu (regex précompilées) et doublons (Redis TTL), actions `block`/`flag`/`throttle`.

## Périmètre (ce que fait CETTE PR)
- Créer `internal/pipeline/antispam` : chargement des `control_plane.antispam_rules` (résolution `smpp_account → customer → global`), moteur d'évaluation.
- Règles **contenu** (`content_blacklist`) : regex précompilées depuis `config_json`, évaluées **en mémoire** sur le corps.
- Règles **doublons** (`duplicate`) : empreinte (hash) du couple (destinataire, corps) en Redis avec TTL.
- Remplacer `stubStage(ctx, "pipeline.anti_spam")` par l'étape réelle (span conservé, ordre §6.1).
- Actions : `block` → `errs.ErrContentBlocked` (CDR `rejected`) ; `flag` → annotation non bloquante ; `throttle` → signal de ralentissement.

## Points d'implémentation clés
- **`ctx7`** avant d'utiliser `go-redis` (empreinte doublon : `SET NX EX`).
- **Invariant (a)** : le contenu est lu **en clair uniquement en mémoire** ; **rien** n'est stocké ni loggé (test dédié qui renforce l'invariant a). L'empreinte doublon = **hash**, jamais le corps.
- Regex **précompilées** au chargement (pas de compilation par message).
- Résolution de portée la plus spécifique d'abord (`account` > `customer` > `global`).

## Tests (écrits dans la même PR)
- Contenu blacklisté → `block` (`content_blocked`, CDR `rejected`) ; `flag` n'interrompt pas.
- Doublon dans la fenêtre TTL → action configurée.
- Test « rien n'est stocké ni loggé » (le corps ne fuit pas) — renforce l'invariant (a).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] STUB `pipeline.anti_spam` remplacé, span conservé

## Hors périmètre
Vélocité + réputation (step-066) ; Admin anti-spam (step-067) ; hot reload (M7).
