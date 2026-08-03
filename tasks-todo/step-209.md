# step-209 — `cancelled` ne doit plus enterrer un message réellement livré

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-201c · **Bloque :** —

## But
Fermer la course entre l'annulation et l'envoi : aujourd'hui un `cancel_sm` accepté pendant que le
statut projeté est encore `accepted` écrit une ligne `cancelled` de rang **60**, qui enterre
définitivement les `enroute` (20) et `delivered` (40) écrits ensuite. `GET /messages/{id}` affiche donc
`cancelled` **pour toujours** sur un message livré à l'abonné et facturé.

## Le constat
- `internal/cancel/cancel.go:86-92` décide l'annulation sur le **statut lu** dans le CDR : `accepted` ⇒
  autorisé, `enroute` ou terminal ⇒ `ErrCancelFailed`.
- `internal/storage/clickhouse/cdr.go:39-53` : `cancelled` vaut **60**, au-dessus de `delivered` (40) et
  `failed` (50). Le `ReplacingMergeTree` garde le rang max **quel que soit l'ordre d'insertion**.
- La course **pré-existe** et était assumée (`cancel.go:65-68`). Ce que step-201c a changé, c'est sa
  **taille** : l'`enroute` n'est plus écrit synchrones par le connecteur mais projeté depuis
  `mt.outcome`. Fenêtre mesurée à ~30 ms en régime normal (backlog `mt.outcome` 20 → 33 sur le run de
  référence), mais bornée seulement par l'alerte de lag à 30 s sous saturation ClickHouse.

L'argent n'est pas touché : la facturation suit le grand livre réserve/capture, idempotent par
`message_id`, qui ne lit jamais ce statut. Le dommage est un dommage **d'affichage** — mais il est
définitif et visible du client, et le contrat public le documente désormais comme une limite connue
(`api/openapi-public.yaml`, schéma `MessageStatus`).

## Points d'implémentation clés
- **Le rang n'est pas le levier qu'il paraît.** Poser `cancelled` à 45 le laisse **au-dessus** de
  `delivered` (40) : le scénario central n'est pas corrigé. Le descendre sous 40 change le sens de
  `cancelled` dans le cas « course sans DLR », où le message a bien quitté la file. Et le rang étant la
  version du `ReplacingMergeTree`, le changer **rejuge toutes les lignes historiques**.
- **La vraie autorité est le connecteur**, pas la projection — `cancel.go` le dit déjà. Deux directions à
  arbitrer : le connecteur publie une issue corrective quand le drapeau d'annulation arrive après son
  propre contrôle ; ou la projection refuse d'écraser un `cancelled` par un `enroute` postérieur, ce qui
  demande une règle de résolution plus riche qu'un rang scalaire.
- Trancher d'abord **ce que `cancelled` signifie** quand le connecteur a dispatché malgré tout. C'est une
  décision de spec (§6.22), pas un choix d'implémentation — elle passe par l'échelle spec → Fable →
  arbitrage humain, et probablement par un ADR.

## Tests (écrits dans la même PR)
- Une annulation acceptée sur un statut `accepted` périmé, suivie d'un `enroute` puis d'un `delivered`,
  ne laisse pas le message en `cancelled`.
- Le cas légitime reste intact : un message réellement annulé avant dispatch lit bien `cancelled`.
- Aucune régression sur la résolution des lignes historiques si le rang bouge.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] la course est fermée, **testée** par le scénario complet accepted → cancel → enroute → delivered
- [ ] le sens de `cancelled` sous course est tranché et écrit (spec §6.22 ou ADR)
- [ ] le contrat public est mis à jour si la limite documentée disparaît (bump `api/package.json`)

## Hors périmètre
Le lag de projection lui-même (step-201c). L'annulation REST — elle n'existe pas : `cancel_sm` est
protocol-only (ADR-0009).
