# step-211 — `billing.events` durable : de l'affichage à la détection

> **Jalon :** M11, dette découverte après coup (§15 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-143, step-184 · **Bloque :** l'alerting métier du tableau de bord (dépôt séparé)

## But
Produire un flux **durable** d'événements de facturation, pour que le BFF du tableau de bord puisse
**détecter** une transition et pas seulement l'afficher.

## Le constat
step-184 promettait `stream-billing-alerts` avec « solde bas / plancher MO / disjoncteur ouvert ». Seul
`mo_floor_reached` a été livré : `low_balance` et le disjoncteur n'ont **aucun seuil configuré**, ni en base
ni en config. Ils ont été retirés du contrat plutôt que laissés comme une promesse creuse.

La vérification a élargi le problème. `docs/specification-technique-tableau-de-bord.md:416` :

> **Métriques de domaine métier** (`account.reputation`, `billing.mo_floor_reached`) — `evaluation_owner =
> bff`. […] Évaluées sur une **source durable** (topic Kafka `billing.events` ou pull réconciliateur depuis
> `billing-svc`) avec un **curseur/offset persisté**, de sorte qu'un redémarrage/basculement rejoue les
> transitions manquées au lieu de les perdre ; le flux WS sert l'affichage, jamais l'unique détection.

Deux conséquences :

1. **Le seuil ne nous appartient pas.** Il vit par règle dans `alert_rules.condition_json`, côté BFF —
   table absente de `db/schema_passerelle_sms.sql`. En inventer un dans la passerelle créerait un second
   lieu de vérité pour la même alerte.
2. **Mais la source durable, si — et elle n'existe pas.** Le topic `billing.events` n'est déclaré nulle part
   (`internal/storage/kafka/topics.go`). Donc `mo_floor_reached`, que nous émettons pourtant, ne peut pas
   davantage servir de détection : `metrics.stream` est best-effort par construction (producteur séparé,
   `MaxBufferedRecords(256)`, rejet plutôt que blocage — et c'est délibéré, une alerte ne doit jamais
   retarder un appel de facturation).

Ce qui manque n'est donc pas un seuil. C'est un chemin durable.

## Arbitrages à trancher (dans la fiche, avant tout code)
- **Outbox transactionnel vs producteur direct.** L'événement naît d'une transition constatée dans le cœur
  Lua/Postgres. Un producteur direct depuis `billing-svc` peut perdre l'événement si le pod meurt entre la
  transition et la production ; un outbox en base le garantit au prix d'un relais. Le curseur du BFF rejoue
  ce qui est **dans** le topic — il ne rattrape pas ce qui n'y est jamais entré.
- **Ce que porte l'événement** : la transition seule, ou l'état (solde) au moment de la transition ? Un
  consommateur qui rejoue depuis un vieil offset ne doit pas réagir à un solde périmé.
- **`mo_floor_reached` migre-t-il, ou reste-t-il dupliqué ?** Le garder sur les deux flux donne l'affichage
  immédiat (WS) et la détection fiable (topic), au prix d'une double émission à dédoublonner côté BFF.
- **Rétention du topic vs curseur du BFF** : une rétention plus courte que la fenêtre de panne tolérée
  reperd exactement ce que le curseur devait sauver.
- **Périmètre des événements** : plancher MO seul, ou aussi réserve refusée (`insufficient_credit`),
  changement de `balance_scope`, top-up ? Chaque ajout est un contrat de plus.

## Hors périmètre
Le seuil `low_balance` côté passerelle (il appartient à `alert_rules`, côté BFF). Les règles Alertmanager
pour les métriques d'infrastructure (§6.8, `evaluation_owner = alertmanager`). La visibilité des rejets du
flux temps réel → step-210.
