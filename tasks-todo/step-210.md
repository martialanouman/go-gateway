# step-210 — Les rejets du flux temps réel redeviennent visibles

> **Jalon :** M11, dette découverte après coup (§15 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-184 · **Bloque :** —

## But
Exposer les rejets de l'`EventPublisher`, aujourd'hui comptés et lus par personne, et séparer les raisons
de rejet pour qu'un bug ne se cache pas dans le bruit d'un plafond attendu.

## Le constat
`EventPublisher` compte ses rejets (`internal/metricstream/events.go:36`) et **aucun appelant ne lit ce
compteur**. La métrique `metrics_stream_dropped_total` des quatre câblages est branchée sur
`streamProducer.Dropped()` seul — le transport Kafka.

Conséquence : le plafond de 50 événements de session par seconde et par instance (`events.go:90`) tronque
**en silence**. Un drain de pod à 3 000 binds laisse l'opérateur devant un flux sessions incomplet, sans
aucun signal qu'un événement a été jeté — pendant l'incident même qu'il regarde. C'est aussi ce qui rend le
flux inutilisable comme journal : on ne peut pas reconstituer un décompte de sessions en additionnant
`bound`/`unbound`, le champ `sessions` (valeur absolue) et le registre Redis font seuls foi.

L'`Emitter` a une variante plus discrète du même défaut. Ses rejets voyagent dans le `Snapshot`
(`dropped_since_start`, `emitter.go:304`), choix documenté — mais ce chemin **se dissimule lui-même** : un
instantané qui échoue à se sérialiser (`emitter.go:241`) ne peut pas porter le compteur qui dirait qu'il a
échoué, et un flux que personne ne consomme ne rapporte rien du tout. Un nom de label hors vocabulaire
(`emitter.go:217`) jette alors chaque échantillon en silence côté `/metrics`.

## Périmètre (ce que fait CETTE PR)
- `internal/observability/metrics/streamdrops.go` : `DropCounter` (interface côté consommateur),
  `DropCounterFunc`, `StreamDropCollector(reason, d)`.
- `internal/metricstream/events.go` : deux atomiques ; `DroppedRateCapped()` / `DroppedUnserializable()`
  remplacent `Dropped()`.
- Chaque service déclare les raisons qu'il peut réellement produire, dans une fonction `streamDropCollectors`
  **nommée et testable** : smpp-server-svc `buffer`/`rate_cap`/`encode`, billing-svc `buffer`/`encode`,
  router-svc et connector-pool-svc `buffer`/`refused`.
- Les commentaires qui décrivent la limite `low_balance` renvoient à step-211.

## Design arrêté
- **DN1 — un label `reason`, pas un compteur agrégé.** `metrics_stream_dropped_total` gagne un label
  **constant** : `buffer` (tampon plein / broker injoignable), `rate_cap` (plafond de débit), `encode`
  (record insérialisable). Un rejet de plafond est un signal d'exploitation attendu sous charge, un rejet
  d'encodage est un bug ; les fondre dans une seule série ferait disparaître le second dans le bruit du
  premier — exactement la faute qu'on corrige. `reason` est dans l'allowlist
  (`internal/observability/metrics/labels.go:42`) ; `Desc.id` hachant fqName + labels constants, plusieurs
  `CounterFunc` de même nom coexistent **tant que le Help est identique**.
- **DN2 — scission des compteurs.** Aucun appelant hors tests, donc pas de compatibilité à préserver.
- **DN3 — un helper à interface côté consommateur**, pas une closure recopiée dans chaque câblage. Le
  package `metrics` ne dépend ainsi ni de `kafka` ni de `metricstream`, et **le lien compteur → exposition
  devient testable sans broker** : c'est le seul point qui empêche le défaut de se reproduire.
- **DN4 — `metricstream` reste sans dépendance Prometheus.** Le bornage des labels vit dans `metrics`, pas
  dans le domaine.
- **DN5 — chaque service n'expose que les raisons qu'il peut produire.** Une fabrique unique à trois raisons
  imposerait à billing-svc une série `rate_cap` figée à zéro : le plafond ne garde que les événements de
  session, et `Alerted` en est exempt. Une série constamment nulle se lit comme une garantie — l'opérateur
  conclurait que ses alertes sont plafonnées. Le prix est une fabrique par service ; c'est aussi ce qui rend
  chaque câblage scrutable par un test.

## Tests (écrits dans la même PR)
- Gather réel : la valeur exposée suit la source, et les trois `reason` coexistent sous le même `fqName`
  dans le registre gardé.
- Dépasser le plafond incrémente `DroppedRateCapped` et **pas** `DroppedUnserializable`.
- **Exposition scrutée au niveau du câblage**, pas seulement du helper : c'est là que le défaut a vécu.
- **Mutations vues tomber**, toutes au niveau du câblage : (i) ne garder que le collector `buffer` — le bug
  d'origine réintroduit ; (ii) inverser les étiquettes `rate_cap` et `encode` ; (iii) réintroduire la série
  morte `rate_cap` sur billing-svc. Une assertion jamais vue échouer n'en est pas une.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] un rejet de plafond apparaît sur `/metrics` (test), la mutation a été vue tomber

## Hors périmètre
Le seuil `low_balance` et la source durable de détection → step-211. Le contrat
`api/openapi-admin.yaml` n'est pas touché : sa description de la limite est exacte, la modifier coûterait un
bump de `api/package.json` sans rien clarifier.
