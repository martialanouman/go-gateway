# step-303 — Le cache de connexions par pod ne se vide jamais

> **Jalon :** Dette ouverte par step-302 · **Statut :** À FAIRE
> **Dépend de :** step-302 · **Bloque :** step-410 (go-live)

## Pourquoi cette fiche existe

step-302 a rendu la remise MO/DLR par pod réellement fonctionnelle. Ce faisant, elle a rendu
**observable** un défaut qui lui préexiste et qu'elle n'aggrave pas : `PodClients` garde une
`*grpc.ClientConn` par pod joint, et ne la relâche jamais.

## Le constat

`internal/modlrrouter/poddeliverer.go` — `PodClients.conns` est une `map[string]*grpc.ClientConn`
alimentée par `conn()` et vidée **uniquement** par `Close()`, à l'arrêt du service. Rien n'évince une
entrée dont le pod n'existe plus.

La clé est l'adresse du pod depuis step-302 ; c'était son `pod_id` avant. **Le changement de clé n'y
change rien** : les deux sont éphémères, parce que ni le nom ni l'IP d'un pod de `Deployment` ne
survivent à un redéploiement. Le cache est donc borné par les adresses **vues** sur la durée de vie du
processus, jamais par les pods vivants.

`mo-dlr-router-svc` est un processus long. Chaque redéploiement de `smpp-server-svc` (3 à 10 répliques)
y laisse autant de `ClientConn` orphelines. Une `ClientConn` gRPC n'est pas inerte : elle retente la
connexion sur son propre backoff, indéfiniment, avec la goroutine qui va avec.

Ce n'était pas visible avant step-302 parce que la voie retour ne dialait jamais avec succès — le
défaut était masqué par un défaut plus grave.

## Le second symptôme, trouvé en revue : la péremption sur réutilisation d'adresse

Le plafond mémoire n'est pas le pire. Le cache survit **bien au-delà** du TTL de session, donc une
adresse réattribuée est servie depuis la connexion de son ancien occupant :

- t=0 — le pod A (10.0.1.5) sert le compte X ; le routeur met en cache la `ClientConn` de `10.0.1.5:7000`.
- t=100 s — le pod A meurt sans `Unbind` (perte de nœud). La connexion passe `TRANSIENT_FAILURE`,
  backoff gRPC jusqu'à 120 s.
- t=300 s — Kubernetes réattribue 10.0.1.5 au pod C, qui bind le compte X et publie `10.0.1.5:7000`.
- `Lookup` rend cette adresse → **cache hit** → la connexion de l'ancien occupant, encore en backoff.
  Sans `wait-for-ready`, l'RPC échoue aussitôt en `Unavailable` → bind sauté → webhook/dead-letter,
  pendant ~2 min, **alors que le pod est joignable et le bind vivant**.

L'analyse de rollout de step-302 ne couvrait que le TTL de 60 s et concluait « sémantiquement identique
à un bind disparu » : vrai pour le routage, faux pour la disponibilité.

## Ce qu'il faut trancher

Quelle discipline d'éviction, sachant qu'aucune des deux évidentes n'est bonne telle quelle :

- **Évincer sur erreur de transport** (`Unavailable`) détruirait des connexions saines lors d'un pic
  transitoire, et paierait une reconnexion complète sur le chemin critique de la voie retour.
- **Un simple plafond LRU** évincerait une connexion vivante sous charge, pour la rouvrir aussitôt.

Une piste à peser : évincer sur l'**absence**, pas sur l'échec — une entrée dont l'adresse n'est
apparue dans aucun `Lookup` depuis une fenêtre supérieure au TTL de session (60 s) ne peut plus
correspondre à un bind vivant. C'est l'information que le registre porte déjà.

## Design arrêté

Arbitrage : Fable (A sur les quatre points), puis arbitrage humain le 2026-09-23 pour la reformulation
de la DoD 3.

- **Éviction sur l'absence, mesurée au dernier usage.** `conn()` horodate l'adresse servie ; une entrée
  non servie depuis `W = 2 × session.DefaultSessionTTL` (120 s) est retirée **et fermée**. Un pod mort
  est donc relâché au plus tard 60 s (registre) + 120 s après sa mort. `PodDeliverer` reste inchangée.
- **Balayage paresseux dans `conn()`**, sous le mutex déjà pris (n ≈ pods vivants) ; les `Close()` se
  font hors verrou. Plafond assumé : sans aucun trafic, les orphelines vivent jusqu'au prochain
  `Deliver`. Aucune goroutine.
- **Adresse réattribuée : sur un hit en `TransientFailure`, `ResetConnectBackoff()`.** Non bloquant :
  `pick_first` garde un TF collant (A62), donc l'RPC en cours échoue vite comme aujourd'hui, et la
  tentative de reconnexion part aussitôt au lieu d'attendre un backoff jusqu'à 120 s. La fenêtre de
  péremption passe de ~2 min à la durée d'une reconnexion : **un** message peut encore être sauté
  (bind suivant, webhook ou dead-letter), la remise n'est pas condamnée. Rejeté : redialer une conn
  neuve — une conn IDLE met l'RPC en attente jusqu'au connect timeout (~20 s) pour chaque message vers
  un pod réellement mort, en tête de ligne.
  Contrepartie : tant que le registre liste un pod mort (≤ 60 s), chaque message vers lui relance
  une tentative de connexion (jamais deux en parallèle), hors du chemin critique.
- **Coût sur le chemin critique :** un parcours O(n) de la map et un `GetState()` par `Deliver` ; un
  redial (handshake TLS, quelques ms) pour un pod vivant resté W sans être servi.
- **Écarts assumés à la lettre de la fiche :** (1) un pod vivant jamais servi pendant W voit sa
  connexion fermée puis rouverte au besoin ; (2) une RPC en vol depuis plus de W (ctx sans échéance,
  pod muet) est annulée par la fermeture — `tryBinds` passe au bind suivant. L'échéance par RPC qui
  supprimerait ce cas n'est pas l'objet de cette fiche.

## Definition of Done

- [ ] Une discipline d'éviction tranchée et écrite sous `## Design arrêté`, avec ce qu'elle coûte au
      chemin critique de la remise.
- [ ] Un test qui échouerait sur le code actuel : un cache qui a vu N adresses successives n'en retient
      pas N. Il doit constater la **fermeture** de la connexion évincée, pas seulement son retrait de la
      map — une `ClientConn` retirée mais non fermée continue de retenter.
- [ ] Aucune éviction d'une connexion servie dans la fenêtre W (reformulé par arbitrage humain le
      2026-09-23 : l'absence se mesure au dernier usage, cf. `## Design arrêté`).
- [ ] Le scénario de l'adresse réattribuée est exercé : une connexion en échec pour une adresse donnée
      ne doit pas condamner la remise vers le pod qui occupe désormais cette adresse.
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` verts

## Hors périmètre

L'adressage lui-même : tranché en step-302, et cette fiche n'y revient pas. Le défaut est antérieur au
choix de la clé et survivrait à n'importe lequel.
