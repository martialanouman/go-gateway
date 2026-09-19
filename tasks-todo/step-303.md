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

## Ce qu'il faut trancher

Quelle discipline d'éviction, sachant qu'aucune des deux évidentes n'est bonne telle quelle :

- **Évincer sur erreur de transport** (`Unavailable`) détruirait des connexions saines lors d'un pic
  transitoire, et paierait une reconnexion complète sur le chemin critique de la voie retour.
- **Un simple plafond LRU** évincerait une connexion vivante sous charge, pour la rouvrir aussitôt.

Une piste à peser : évincer sur l'**absence**, pas sur l'échec — une entrée dont l'adresse n'est
apparue dans aucun `Lookup` depuis une fenêtre supérieure au TTL de session (60 s) ne peut plus
correspondre à un bind vivant. C'est l'information que le registre porte déjà.

## Definition of Done

- [ ] Une discipline d'éviction tranchée et écrite sous `## Design arrêté`, avec ce qu'elle coûte au
      chemin critique de la remise.
- [ ] Un test qui échouerait sur le code actuel : un cache qui a vu N adresses successives n'en retient
      pas N. Il doit constater la **fermeture** de la connexion évincée, pas seulement son retrait de la
      map — une `ClientConn` retirée mais non fermée continue de retenter.
- [ ] Aucune éviction d'une connexion dont un bind vivant dépend encore.
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` verts

## Hors périmètre

L'adressage lui-même : tranché en step-302, et cette fiche n'y revient pas. Le défaut est antérieur au
choix de la clé et survivrait à n'importe lequel.
