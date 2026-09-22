# `content-key-svc` est sur le chemin de la remise sans être une dépendance de readiness

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-295b · **Portée par :** —

Depuis step-295b, `mo-dlr-router-svc` ne peut signer aucune remise de webhook sans `content-key-svc` : il
lui demande d'ouvrir le secret de signature à chaque événement. Mais `newOpsServer` n'enregistre comme
dépendances vitales que Kafka, le producteur Kafka, ClickHouse et Postgres. `content-key-svc` n'y figure
pas, et `grpctls.NewClient` est paresseux — le pod démarre sans jamais joindre le service.

Conséquence : si l'adresse est fausse, ou si l'allowlist mTLS de `content-key-svc` ne nomme pas encore
`mo-dlr-router-svc`, chaque ouverture échoue en `Unavailable`. C'est classé transitoire — à raison, c'est
la seule classification qui ne détruit pas de backlog — donc `Deliver` rend l'erreur, le consommateur Kafka
la remonte, et le superviseur abat **tout le groupe** : la voie retour entière s'arrête, DLR et CDR compris,
alors que seuls les webhooks sont en cause. Le pod redémarre, paie son `DRAIN_DELAY`, et recommence.
`/readyz` n'aura rien dit.

**Ce qu'on a fait à la place.** L'échéance de cinq secondes sur le saut vers `content-key-svc` a été
ajoutée par step-295b, ce qui borne le blocage d'une goroutine de remise mais ne change rien au verdict de
readiness ni à la mise à mort du groupe.

**Pourquoi.** Deux raisons distinctes. La première est que le comportement « une erreur de traitement abat
le groupe » est **antérieur et général** : une panne durable de Postgres produit déjà exactement cela sur ce
service. Le corriger est une décision sur le superviseur, pas sur cette step. La seconde est qu'ajouter une
sonde de readiness gRPC demande de choisir ce qu'on sonde — un `Open` factice révélerait une identité, un
health check gRPC n'existe sur aucun des services du dépôt — et ce choix engage les quatre autres clients
gRPC du dépôt, pas seulement celui-ci.

**Ce qu'il en coûte.** Un défaut de configuration ou un ordre de déploiement malheureux ne se présente pas
comme « les webhooks ne partent plus » mais comme un `CrashLoopBackOff` sans cause lisible, avec toute la
voie retour arrêtée. Le diagnostic passe par les journaux d'un pod qui redémarre, pas par une sonde.

**À quoi on reconnaîtra qu'il faut la payer.** Au premier déploiement où l'ordre des images compte — c'est
exactement le scénario que la revue de step-295b a décrit — ou au premier `CrashLoopBackOff` dont la cause
mettra plus de dix minutes à se nommer. Le payer veut dire : décider si `content-key-svc` est vital pour ce
service (et alors le sonder, comme les quatre autres dépendances), ou rendre l'échec de remise de webhook
non fatal pour les autres jambes du même pod.

**Deux trous voisins, laissés avec lui.** `TLS_ALLOWED_CLIENTS` de `deploy/k8s/content-key-svc.yaml` n'est
lu par aucun test — `allowlist_test.go` compose sa propre liste, et `internal/deploy` ne regarde pas cette
clé. Une entrée oubliée dans le manifeste est donc invisible jusqu'au premier webhook. Et l'`up`/`down`/`up`
de la migration 0018 n'a pas de garde automatisée propre : le job de CI l'exerce sur une base vide, donc les
deux `RAISE EXCEPTION` qui refusent une table peuplée ne sont exercés par rien (0017 est dans le même état).

Sources : `cmd/mo-dlr-router-svc/wiring.go` (`newOpsServer`, les quatre dépendances vitales ;
`newDeliveryLeg`, le dial paresseux), `internal/platform/supervisor/supervisor.go` (la première erreur abat
le groupe), `internal/modlrrouter/secret_opener.go` (`openTimeout`).
