# step-201 — Tuning de débit : partitions Kafka, batch ClickHouse, pool pgx

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-200 · **Bloque :** —

## But
Atteindre le débit soutenu (8000 SMS/s) en réglant les leviers de capacité : partitions Kafka, taille
de batch ClickHouse, dimensionnement du pool `pgx` — pilotés par config, mesurés par le harnais de charge.

## Périmètre (ce que fait CETTE PR)
- `internal/config` : exposer les leviers (partitions par topic, batch/flush ClickHouse, `pgxpool`
  max/min conns) via env, avec des valeurs par défaut de prod raisonnées.
- Ajustements côté `internal/storage/{kafka,clickhouse,postgres}` pour honorer ces leviers.
- Re-run du harnais (step-200) documentant les réglages tenant les NFR.

## Points d'implémentation clés
- **Établir le plafond du pair de test AVANT de régler quoi que ce soit.** Le run de référence se fait
  contre le simulateur SMSC (`internal/testutil/smscsim`, `make smsc-sim`) : c'est lui qui borne la
  mesure. Rien ne dit aujourd'hui qu'il tient 8 000 `submit_sm/s`, encore moins 15 000 — M8 l'a éprouvé
  sur l'injection de pannes, jamais sur le débit. Si son plafond est en dessous de la cible, chaque
  chiffre produit ici mesure le simulateur et non la passerelle, et le tuning vise une contrainte
  artificielle. Le dépôt n'a rien pour le vérifier : `smpp-bindgen` ouvre des binds mais **ne soumet
  rien**. Trancher comment on établit ce plafond (injecteur `submit_sm`, plusieurs instances du
  simulateur, ou lecture de la saturation) fait partie du design de cette step.
- **Mesurer aussi le chemin `Idempotency-Key`.** `internal/restapi/messages.go` bascule sur
  `submitIdempotent` quand l'en-tête est présent, ce qui ajoute deux allers-retours Redis (`Reserve`,
  `Finalize`) autour de la publication Kafka. Régler les leviers sans cet en-tête optimise un chemin que
  les clients qui retentent n'empruntent pas : les NFR seraient déclarés tenus sur le cas favorable.
  Le script k6 de step-200 ne l'émet pas encore — l'ajouter en option, désactivée par défaut.
  **Piège :** la clé doit être unique par itération (128 caractères max, cf. contrat). Une clé constante
  ferait retourner le résultat mémorisé de la première requête et mesurerait le cache d'idempotence, pas
  le chemin idempotent.
- `mt.routed` est shardé (`shard_index = hash(message_key) % bind_pool_size`, §1.6) : le nombre de
  partitions doit couvrir le parallélisme cible sans surdécoupage.
- ClickHouse : batch/flush pour éviter la mutation par message (§1.10) — équilibrer latence CDR vs débit.
- `pgxpool` : dimensionner selon la charge billing/contrôle ; éviter l'épuisement sous pic.
- **`ctx7`** avant d'ajuster une API `franz-go` / `clickhouse-go/v2` / `pgxpool`.
- Aucun réglage ne doit affaiblir un invariant (idempotence, ordre, non-fuite).

## Tests (écrits dans la même PR)
- Config : les leviers se parsent et s'appliquent (test unitaire config).
- Le harnais (step-200) tient le débit soutenu avec les réglages (run de référence documenté).
- Le plafond du pair de test est **mesuré et consigné**, et le run de référence se situe en dessous :
  un run de référence au niveau du plafond du simulateur ne prouve rien de la passerelle.
- Avec l'en-tête activé, deux itérations émettent deux clés d'idempotence différentes ; désactivé,
  aucun en-tête n'est émis.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] débit soutenu tenu, budgets de latence respectés (disjoncteur fermé)

## Hors périmètre
Chaos → step-202/203. Sécurité → step-204+.
