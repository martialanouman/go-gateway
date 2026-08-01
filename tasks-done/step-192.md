# step-192 — Topic `webhook.retry` différé (sortir les retries du chemin chaud)

> **Jalon :** Audit pré-production (§6.12/M4) · **Statut :** À FAIRE
> **Dépend de :** step-047, step-048 · **Bloque :** —

## But
Sortir les réessais de webhook de la goroutine du consumer de remise. Aujourd'hui
(`cmd/mo-dlr-router-svc/main.go`) `Send` tourne **en ligne** sur le consumer, qui traite les records en
série : un endpoint client lent bloque tout le trafic retour de sa partition (head-of-line blocking). Le
garde-fou actuel — timeout court, plafond d'essais, chute en dead-letter — borne le dégât mais **au prix de
la livraison** : un endpoint temporairement indisponible perd ses événements en dead-letter au lieu d'être
réessayé. Le commentaire du code nomme déjà ce correctif comme le suivi attendu.

## Périmètre (ce que fait CETTE PR)
- Nouveau topic `webhook.retry` (aligné sur `webhook.dead-letter` existant).
- Le sender publie l'événement sur `webhook.retry` au lieu de boucler en ligne sur un échec **transitoire**.
- Consommateur dédié **pacé** dans `mo-dlr-router-svc`, drainant `webhook.retry` avec back-off ; l'épuisement
  des essais parque toujours sur `webhook.dead-letter`.
- Les bornes du chemin chaud (timeout, plafond) restent en place pour le **premier** essai.

## Points d'implémentation clés
- **Distinguer transitoire et permanent.** Un 5xx / timeout / erreur réseau ⇒ `webhook.retry`. Un 4xx
  définitif (endpoint qui refuse la charge utile) ⇒ dead-letter directement, sans réessayer inutilement.
- **Le consumer de retry doit être pacé et isolé** : c'est tout l'intérêt de la PR. S'il partage la cadence
  du consumer de remise, on a juste déplacé le head-of-line blocking.
- **Préserver la signature HMAC-SHA256** (step-047) à travers le cycle de retry : l'événement republié doit
  produire une signature vérifiable par le client, et l'horodatage signé ne doit pas rendre une livraison
  différée invalide côté client.
- **Back-off borné + âge maximum** : un événement ne doit pas tourner indéfiniment sur `webhook.retry`.
  Réutiliser la logique de fenêtre temporelle de step-129 plutôt qu'un compteur Redis.
- Ordre non garanti entre un événement réessayé et un événement frais du même client : c'est **assumé et à
  documenter** (les webhooks sont déjà at-least-once et non ordonnés).
- **`ctx7`** avant toute API `franz-go` de production/consommation avec pacing.

## Tests (écrits dans la même PR)
- Un échec transitoire republie sur `webhook.retry` et **ne bloque pas** le consumer de remise.
- Un endpoint lent n'affecte pas le délai de remise des événements suivants de la même partition.
- Un 4xx définitif va en dead-letter sans passer par `webhook.retry`.
- Un événement qui dépasse l'âge maximum est parqué en dead-letter.
- La signature HMAC d'un événement réessayé reste vérifiable.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] absence de head-of-line blocking démontrée par un test avec endpoint lent
- [ ] le commentaire de `cmd/mo-dlr-router-svc/main.go` reflète l'implémentation réelle

## Hors périmètre
Politique de réessai par client (configurable via Admin) → suivi distinct.
