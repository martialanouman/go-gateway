# step-194 — Découper `connectorpool.go` (extraction du mapping SMPP/CDR)

> **Jalon :** Audit pré-production (structure) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** —

## But
Ramener `internal/connectorpool/connectorpool.go` (**1165 lignes**, le plus gros fichier non généré du
dépôt) à une taille inspectable, en séparant trois responsabilités aujourd'hui mélangées : l'orchestration
(`Run`/`runOnce`/`processOne`), les battements de cœur (statut, disjoncteur) et les **helpers purs de
mapping** vers SMPP et le CDR. Ces derniers (l. 984-1137 : `buildSubmit`, `cdrRow`, `cancelledRow`,
`dataCoding`, `submitDataCoding`, `outcome`, `sourceAddr`, `segmentCount`, `segmentSeq`) sont sans état et
sortent sans rien casser.

Refactoring pur, à comportement constant.

## Périmètre (ce que fait CETTE PR)
- Nouveau `internal/connectorpool/mapping.go` : les helpers purs de construction `submit_sm` et de lignes CDR.
- `connectorpool.go` conserve orchestration, heartbeats et déclarations d'interfaces.
- Les tests de mapping existants suivent dans un `mapping_test.go` dédié (`connectorpool_test.go` fait
  1107 lignes — le scinder au passage améliore autant la lisibilité).

## Points d'implémentation clés
- **Découpage à comportement identique** : simple déplacement, aucune signature ni logique modifiée.
- **Ne pas toucher à `processOne` dans cette PR.** C'est le vrai point chaud (184 lignes, 22 branches) mais
  il porte la sémantique de règlement, de dead-letter et de commit d'offset : le retoucher dans une PR de
  déplacement de code rendrait la revue impossible. À traiter séparément, avec sa propre justification.
- Rester **dans le même package** : l'objectif est la lisibilité du fichier, pas une nouvelle frontière de
  package. Un sous-package forcerait à exporter des helpers aujourd'hui privés — ce serait relocaliser la
  complexité en l'augmentant.
- Ne pas profiter du déplacement pour « améliorer » un helper au passage : toute modification de logique
  doit faire l'objet d'une PR distincte avec son test.

## Tests (écrits dans la même PR)
- Aucun test nouveau attendu : les tests existants doivent passer **inchangés** (c'est la preuve du
  comportement constant). Seule leur répartition entre fichiers évolue.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] **aucun test existant modifié dans son contenu** · godoc sur l'exporté
- [ ] aucun invariant (a/b/c/d) violé · `connectorpool.go` nettement sous 1000 lignes
- [ ] `git diff` du déplacement lisible (déplacements purs, pas de réécriture)

## Hors périmètre
Simplification de `processOne` → PR distincte. Fichiers générés `*.pb.go` : non concernés.
