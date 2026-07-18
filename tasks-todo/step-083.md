# step-083 — Réassembler les MO concaténés (multipart)

> **Jalon :** M6 (§10 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-081 · **Bloque :** —

## But
Reconstituer un MO multipart (plusieurs `deliver_sm` portant un UDH de concaténation) en un message logique unique côté voie retour, avec les bonnes frontières.

## Périmètre (ce que fait CETTE PR)
- `internal/pipeline/encoding/reassemble.go` : buffer de réassemblage clé `(source, dest, ref)`, complétion quand les `total` segments sont reçus, TTL/éviction des segments orphelins.
- Intégration côté `mo-dlr-router-svc` / `internal/pipeline` : un MO complet est émis vers `mo.inbound` une fois réassemblé ; segments incomplets stockés en état volatil (Redis `internal/storage/redis`, socle step-080) plutôt qu'en mémoire par pod.
- Décodage GSM-7/UCS-2 en s'appuyant sur `internal/smpp` + step-081.

## Points d'implémentation clés
- Utiliser `smpp.ParseUDH` pour extraire `ref/total/seq`.
- Réassemblage **borné** : pas de goroutine sans condition d'arrêt ; éviction sur TTL. État partagé → Redis atomique, pas de RMW Go.
- Le corps réassemblé n'apparaît jamais dans un log/span (invariant a).

## Tests (écrits dans la même PR)
- Réassemblage aux frontières 160/153 (GSM-7) et 70/67 (UCS-2) : 2–3 segments dans l'ordre **et** dans le désordre → même message.
- Segment manquant → pas d'émission, éviction après TTL (test avec horloge injectée).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] frontières 160/153 et 70/67 réassemblées ; désordre géré

## Hors périmètre
La corrélation DLR (§1.11, déjà M4). Le routage MO → account (M4).
