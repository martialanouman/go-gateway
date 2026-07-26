# step-063 — Détection STOP côté MO : suppression scopée + auto-réponse (jamais facturée)

> **Jalon :** M5 (§9 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-061 · **Bloque :** —

## But
Sur un MO, détecter un mot-clé STOP (`opt_out_keywords`) : écrire une suppression scopée sur le numéro entrant, envoyer l'auto-réponse (MT **jamais facturé**), tout en **remettant quand même** le MO au compte.

## Périmètre (ce que fait CETTE PR)
- Dans `mo-dlr-router-svc` (chemin MO, step-045) : matcher le corps du MO contre `opt_out_keywords` (par `country_code`, `match_type`, `action`).
- `action=suppress` → écrire une `control_plane.suppressions` scope `inbound_number` (source `mo_stop`) ; `unsuppress` → retirer ; `help` → auto-réponse info.
- Envoyer l'auto-réponse (`auto_reply_template`) comme MT marqué **non facturable** (drapeau propagé jusqu'à la facturation M9).
- Le MO est **toujours** remis au compte (step-048) — la détection STOP n'interrompt pas la remise.

## Points d'implémentation clés
- **Invariant (a)** : le matching lit le corps **en mémoire**, jamais loggé/stocké en clair.
- MSISDN normalisé E.164 à l'écriture de la suppression.
- « MT jamais facturé » : aujourd'hui la facturation est un STUB (opt-in M9) ; poser le drapeau `billable=false` sur l'auto-réponse dès maintenant pour que M9 le respecte.
- Idempotence : un STOP répété ne crée pas de doublon (contrainte `suppressions_uq`).

## Tests (écrits dans la même PR)
- Un STOP crée une suppression scope `inbound_number` ; le MO est **quand même remis**.
- L'auto-réponse est émise avec `billable=false`.
- STOP répété → pas de doublon (conflit géré).
- Le corps n'apparaît dans aucun log.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] STOP → suppression + MO remis + auto-réponse non facturée

## Hors périmètre
Étape opt-out MT (step-062) ; Admin opt-out (step-064) ; facturation réelle (M9).
