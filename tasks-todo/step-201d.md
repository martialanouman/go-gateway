# step-201d — Le routeur est le goulot suivant du débit traversant

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-201c · **Bloque :** step-201b

## But
Lever le goulot que le run de référence du 03/08/2026 a mesuré **après** step-201c : `router-svc`
consomme `mt.inbound` moins vite que l'ingestion ne l'alimente, et c'est désormais la seule étape qui
accumule du retard.

## Le constat, mesuré
Run `make load-reference` aux défauts livrés, 1 200 msg/s visés, 60 s mesurés
(`test/load/README.md`, « Mesure du 03/08/2026 ») :

```
mt.inbound  3 486 -> 22 403     → +291 rec/s : le routeur ne suit pas
mt.routed       7 ->     12     → plat : le pool de connecteurs suit
mt.outcome     20 ->     33     → plat : la projection de CDR suit
```

- Sortie **892 `submit_sm/s`** pour 1 200 acceptés — 25,7 % d'écart, tolérance 2 %.
- p99 bout-en-bout entre **10,2 et 20,5 s**, moyenne 11,2 s, quand l'ingestion répond en **11 ms** au
  p99. L'écart est donc de l'**attente en file**, pas du temps de traitement.
- Le pair est hors de cause : le faux SMSC a été calibré à **236 274 `submit_sm/s`** dans le run même.

step-201c a levé le goulot précédent (le pool sortait 192–330/s parce qu'il écrivait le CDR par
message) : `mt.routed` est passé de +631 rec/s à plat. Le facteur limitant a simplement changé d'étape.

## Périmètre (ce que fait CETTE PR)
- **Mesurer avant de corriger** : situer le coût dans le pipeline MT (§6.1) étape par étape. Le pipeline
  a déjà un span par étape et un histogramme `pipeline_duration_seconds` — commencer par les lire plutôt
  que par supposer.
- Lever le goulot identifié, et le prouver par un run de référence relancé.

## Points d'implémentation clés
- **Ne pas supposer le coupable.** Les candidats plausibles sont nombreux et de natures différentes :
  allers-retours Redis du débit et de l'anti-spam, réservation de crédit, résolution de route,
  encodage/segmentation, ou simplement le parallélisme de consommation de `mt.inbound`. La leçon de
  step-201 est qu'un goulot insensible à la concurrence n'est pas ce qu'on croit : `TestCDRWriteCeiling`
  avait tranché en isolant l'écriture. Prévoir le micro-banc équivalent avant de toucher au pipeline.
- **L'ordre du pipeline MT n'est pas réordonnable** (CLAUDE.md, §6.1). Un gain obtenu en déplaçant une
  étape de conformité n'est pas un gain, c'est une régression d'invariant.
- **`ingest_duration_seconds` est déclarée et observée nulle part** (relevé en step-201c) et ses bornes
  encadrent mal ses seuils NFR — à corriger si la mesure passe par elle.
- Le run de référence tourne dans **un seul processus** : un goulot de parallélisme peut être un artefact
  du harnais autant qu'une propriété du service. Le vérifier avant d'en faire une conclusion.

## Tests (écrits dans la même PR)
- Un micro-banc isolant l'étape suspectée, comme `TestCDRWriteCeiling` l'a fait pour l'écriture CDR.
- Run de référence relancé : `mt.inbound` plat, ou le nouveau chiffre consigné avec le goulot suivant
  nommé.
- Les 4 invariants restent verts — en particulier b) : un message routé par numéro exact traverse toutes
  les étapes de conformité.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] le goulot est **nommé et isolé par une mesure**, pas déduit
- [ ] run de référence relancé et consigné dans `test/load/README.md`
- [ ] ordre du pipeline inchangé, invariants verts

## Hors périmètre
Verdict NFR pleine échelle → step-201b. Le goulot du pool de connecteurs → step-201c (livré).
