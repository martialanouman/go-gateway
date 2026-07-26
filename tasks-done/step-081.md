# step-081 — Détection d'encodage GSM-7/UCS-2/8-bit + calcul du nombre de segments

> **Jalon :** M6 (§10 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-082, step-083

## But
Remplacer la résolution d'encodage naïve de M2 par une vraie détection GSM-7/UCS-2/8-bit et le calcul exact de `segment_count`, sans encore découper. Base du message long correct.

## Périmètre (ce que fait CETTE PR)
- `internal/pipeline/encoding/detect.go` : détection du jeu représentable (table GSM 03.38 de base + extensions), respect de `data_coding_default` du connecteur et de l'override client (§6.6).
- Calcul `segment_count` aux frontières : GSM-7 **160/153**, UCS-2 **70/67**, 8-bit **140/134** (les variantes segmentées réservent l'espace UDH).
- Étend `internal/platform/encoding/encoding.go` (`Resolve`) ou ajoute une fonction `DetectAndCount` sans casser l'API M2.
- Aucune modification des enveloppes Kafka ici (le champ `SegmentCount` de `pipeline.RoutedMT` existe déjà).

## Points d'implémentation clés
- `Encoding = auto` déclenche la détection ; `gsm7|ucs2|binary` forcent (contrat `internal/platform/encoding`).
- Réutiliser le codec `internal/smpp` (charset GSM-7) plutôt que de dupliquer une table.
- Pur, sans I/O, sans corps loggé (invariant a) — travaille sur `msg.Body` via `Reveal()` dans la fonction, jamais au-delà.
- Contrats : `data_coding_default` (`db/schema_passerelle_sms.sql` §9 `smsc_connectors`).

## Tests (écrits dans la même PR)
- Table-driven aux frontières exactes : 160 → 1 seg, 161 → 2 seg à 153 ; 70/71 UCS-2 ; caractères d'extension GSM-7 (`€`, `{`, `}`) comptent double.
- **Fuzz** `FuzzDetect` : ne panique jamais, `segment_count ≥ 1`, encodage ∈ {gsm7,ucs2,binary}.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] frontières 160/153, 70/67, 140/134 couvertes ; fuzz vert

## Hors périmètre
La découpe effective en segments UDH → step-082. Le réassemblage MO → step-083.
