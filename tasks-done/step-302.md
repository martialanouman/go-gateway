# step-302 — La remise MO/DLR par pod n'a pas de nom DNS à joindre

> **Jalon :** Dette ouverte par step-300b · **Statut :** FAIT
> **Dépend de :** — · **Bloque :** step-410 (go-live)

## Pourquoi cette fiche existe

step-300b devait faire vérifier, côté client, l'identité du pod joint par la voie retour. En
cartographiant ce dial, elle a trouvé plus bas que TLS : **l'adresse composée ne résout pas**. Le défaut
est antérieur à step-300 et indépendant d'elle — l'arbitrage de 300b épingle le nom vérifié sur le
`Deployment`, si bien que le correctif choisi ici ne touchera pas les certificats.

## Le constat

`mo-dlr-router-svc` remet un `deliver_sm` au pod `smpp-server-svc` qui détient le bind du client. Il
compose son adresse depuis un gabarit (`internal/config/config.go`) :

```
SMPP_POD_ADDR_TEMPLATE, défaut "%s.smpp-server-headless:7000"     # %s = pod_id
```

`pod_id` vaut `metadata.name` (`deploy/k8s/smpp-server-svc.yaml`, via `fieldRef`), et
`smpp-server-headless` est bien un `Service` de `clusterIP: None` dans le même fichier. Il manque
pourtant l'enregistrement A.

**Un nom DNS par pod derrière un service headless n'existe que si le pod porte `spec.hostname`**, et un
`spec.subdomain` égal au nom du service headless. Le contrôleur d'endpoints ne recopie dans
`EndpointSlice` que `spec.hostname` ; il ne le déduit pas de `metadata.name`. Or `spec.hostname` est un
champ du *template* de pod : un `Deployment` ne peut y mettre qu'**une seule chaîne**, identique pour
toutes ses répliques — ce qui ne donne pas un nom par pod, et casserait l'unicité si on essayait. Seul
un `StatefulSet` fabrique ces noms, un par pod, parce que c'est le contrôleur qui les pose.

`smpp-server-svc` est un `Deployment`. **Chaque `Deliver` échoue donc sur la résolution du nom**, avant
tout handshake, et l'appelant le lit comme un pod injoignable : `PodClients.Deliver` traduit l'échec de
dial en `codes.Unavailable`, que `deliverer.go` range avec « le bind n'est plus là » et qui arrête la
marche **sans erreur**, pour basculer sur le webhook puis sur la dead-letter (step-048).

**Rien n'est perdu, et c'est le problème :** un client qui tient un bind SMPP actif reçoit ses MO par
webhook s'il en a déclaré un, et les voit s'empiler en dead-letter sinon — dans les deux cas sans qu'une
seule erreur nomme la cause. Le canal SMPP retour est éteint, silencieusement.

Rien ne l'a signalé : les tests d'intégration de la remise par pod dialent un serveur local sur
`127.0.0.1`, jamais un nom de service.

## Ce qu'il faut trancher

Deux voies, et elles ne coûtent pas la même chose :

1. **`smpp-server-svc` devient un `StatefulSet`.** Les noms de pods deviennent stables
   (`smpp-server-svc-0`, `-1`, …) et résolvent naturellement sous le service headless. Le gabarit ne
   bouge pas. Bénéfice second : le registre de sessions garde des `pod_id` stables entre redémarrages,
   ce qui rend la trace d'un bind lisible. Coût : un `StatefulSet` déploie ses répliques en série par
   défaut (`podManagementPolicy: OrderedReady`) — à passer en `Parallel`, sans quoi le temps de
   déploiement grandit avec le nombre de pods —, et l'HPA, la PDB et le drain sont à relire sous ce
   contrôleur.
2. **`pod_id` devient l'adresse IP du pod** (`status.podIP`), gabarit `"%s:7000"`. Le DNS disparaît du
   chemin. Coût : le `pod_id` cesse d'être un identifiant stable — une IP est réattribuée —, or il est
   écrit dans le registre de sessions et sert à tracer un bind ; et l'autorité du dial devient une IP,
   forme sous laquelle **seul** l'épinglage de `ServerName` retenu par 300b vérifie quoi que ce soit.

La voie 1 corrige aussi la stabilité du `pod_id` ; la voie 2 la sacrifie. C'est ce que l'arbitrage doit
peser, pas la taille du diff.

## Design arrêté (Fable, 2026-09-19 — voie 3, validée par l'utilisateur avant le code)

**Ni l'une ni l'autre : on dissocie l'identifiant de l'adresse.** `pod_id` reste `metadata.name` ; le
pod publie lui-même son adresse de dial dans le registre de sessions ; `mo-dlr-router-svc` dial
l'adresse que `Lookup` lui rend. `SMPP_POD_ADDR_TEMPLATE` disparaît, et le DNS sort du chemin.
`smpp-server-svc` reste un `Deployment`. L'épinglage `ServerName = "smpp-server-svc"` de 300b est
inchangé — c'est exactement le cas qu'il avait prévu.

Le code l'annonçait déjà : le commentaire de `PodAddrTemplate` (`internal/config/config.go`) dit « real
at-scale pod discovery is M12 ». Nous y sommes.

### Pourquoi pas la voie 1 — le drain la tue, avant même le coût de contrôleur

L'enregistrement A par pod d'un service headless **est retiré dès que le pod passe `NotReady`**. Or le
contrat de drain de ce dépôt ([[graceful-drain-contract]]) commence par `OnDrain` → `/readyz` 503 : le
pod devient `NotReady` *alors que ses binds sont encore vivants et que la remise doit continuer*. Un
`StatefulSet` rejouerait donc le défaut exact de cette fiche pendant toute la fenêtre de drain, à moins
d'ajouter `publishNotReadyAddresses: true` sur le headless — un troisième réglage à tenir d'accord.

S'y ajoutent, et chacun suffirait : le contrôleur `StatefulSet` **ne recrée pas** un pod dont l'objet
est `Terminating` sur un nœud injoignable (un `Deployment` le recrée tout de suite) — perdre un tiers
de 5–20 k binds pour une durée non bornée sur une partition réseau, contre rien ; un `RollingUpdate`
strictement sériel, ordinal décroissant, sans `maxSurge`, soit ≥ 25 min à 10 répliques avec
`terminationGracePeriodSeconds: 150` ; et l'extension de la garde `internal/deploy` à un `kind` neuf.

Quant au bénéfice annoncé — un `pod_id` stable —, il vaut zéro **ici** : aucune opération du registre
n'est « libérer ce que détenait le pod X » (le drain unbind bind par bind, le TTL balaie le reste), un
bind ne survit jamais à un redémarrage puisque c'est un socket TCP, et la clé de trace d'un bind est
`bind_id` (un UUID). Pire, il tromperait : deux incarnations successives de `smpp-server-svc-2`
deviendraient indiscernables dans le registre et dans les logs — précisément ce que le commentaire de
`SMPP_POD_ID` dans le manifest cherche à éviter.

### Pourquoi pas la voie 2 — elle est minée sous IPv6

`internal/session/registry.go:41` pose `memberSep = ":"` (« pod names and bind ids never contain it »)
et `Lookup` relit le membre par `strings.Cut` (l. 162). Un `status.podIP` **IPv6**, sur un cluster
dual-stack, casse la lecture du registre au premier `:` ; et le gabarit `"%s:7000"` produit une adresse
invalide sans crochets. À quoi s'ajoute la confusion identité/adresse que la fiche avait déjà vue.

### Le mécanisme retenu

- **Manifest** : un env de plus, `SMPP_POD_ADDR` ← `fieldRef: status.podIP`. `SMPP_POD_ID` ne bouge pas.
- **Config** : `SMPP.PodAddr` (`POD_ADDR`, sans défaut) ; `SMPP.PodAddrTemplate` **supprimé**. Le
  câblage compose l'adresse annoncée par `net.JoinHostPort(cfg.SMPP.PodAddr, GRPCPort)` — IPv6-sûr.
- **Registre, écriture** : `session.Bind` porte un champ `Addr`. `Registry.Bind` écrit
  `SET sess:pod:{pod_id} <addr> EX <ttl+1s>` **hors du script Lua**, en une commande à part.
  *Deux raisons de ne pas l'ajouter en `KEYS[2]` de `bind.lua`* : les clés du registre portent un hash
  tag Redis Cluster (`sess:{account_id}`, cf. le commentaire de `key()`), donc un script à deux clés de
  slots différents serait refusé en `CROSSSLOT` ; et `bind.lua` porte l'**invariant d** — on n'y touche
  pas pour une donnée qui n'a aucun besoin d'atomicité. `unbind.lua`, `touch.lua` et `lookup.lua` ne
  bougent pas non plus.
- **Un seul point d'écriture** : le rafraîchissement du jeton passe par `Bind` (`refreshLoop`,
  `internal/smppserver/listener.go:481`), pas par `Touch` — qui n'a aucun appelant en production.
  L'adresse est donc posée *et* rafraîchie par le même appel, et expire avec la session.
- **Registre, lecture** : `Lookup` lit les adresses des `pod_id` distincts par un **pipeline de `GET`**
  (jamais un `MGET` : multi-slot en cluster), au plus un par pod, et remplit `Bind.Addr`.
- **Proto** : champ additif `Session.pod_addr = 6`. `BindRequest` porte déjà `Session` : rien à y
  ajouter. Les deux sites de `listener.go` qui composent une `Session` le remplissent.
- **Routeur** : `LiveBind` gagne `Addr` ; `PodClients` dial `Addr` et cache la connexion **par adresse**,
  en logguant `pod_id`. `AddrResolver`, `templateResolver` et `NewTemplateResolver` sont **supprimés** —
  une interface à une seule implémentation dont le seul appelant disparaît.
- **Adresse vide** (un pod d'avant le rollout) → le bind est sauté, comme aujourd'hui : webhook, puis
  dead-letter. Borné à un déploiement.
- **Le `Service` `smpp-server-headless` est supprimé** : plus rien ne le résout. Un pod est joignable
  sur son IP et le port de son conteneur sans qu'aucun `Service` existe. `deploy/README.md` perd la
  « singularité de topologie » correspondante.
- **Le silence** que le constat dénonce : le `default:` de `tryBinds`
  (`internal/modlrrouter/deliverer.go:196`) passe au bind suivant sans un mot. Il logge désormais la
  cause en `Debug` — un `Warn` par bind mort à chaque MO serait du bruit, et la dead-letter porte déjà
  la raison `bind_exhausted`.

### Ce que ça coûte au drain, à l'HPA et à la PDB

**Rien.** C'est le fond du choix : le contrôleur ne change pas. Le drain garde son contrat
`OnDrain → /readyz 503 → DRAIN_DELAY → composants` et n'a plus de dépendance aux `EndpointSlice` pour la
voie retour, puisqu'on dial une IP. L'HPA (`Deployment` 3→10, CPU 70 %) et la PDB (`maxUnavailable: 1`)
sont inchangés, et le commentaire de `deploy/README.md` sur « un pod redémarré porte un nouveau
`pod_id` » reste vrai.

### Ce qui reste à surveiller

- **Fenêtre de rollout** : tant que d'anciens pods n'ont pas publié d'adresse, leurs binds tombent au
  webhook — le comportement d'aujourd'hui, borné à un déploiement.
- **Réattribution d'une IP** dans les 60 s de TTL : `Deliver` répond `NotFound` sur le `bind_id`, on
  passe au bind suivant. Le pin TLS garantit qu'on a au moins parlé à un pod de `smpp-server-svc`.
  Sémantiquement identique à un bind disparu.
- `status.podIP` est l'IP **primaire** : sur un cluster dual-stack, routeur et pod partagent la même
  famille primaire, ce qui est toujours vrai dans un même cluster.

Écartés : encoder l'adresse dans le membre (`pod_id@addr:bind_id` — format mixte pendant 60 s au
rollout, parsing plus malin pour rien) ; la résolution par l'API Kubernetes (client-go + RBAC pour dix
pods) ; un `Service` par pod (incompatible HPA) ; la remise par pub/sub Redis à la manière de
`Disconnect` (fire-and-forget : on perdrait l'ack `delivered` qui pilote la bascule webhook/dead-letter).

## Definition of Done

*Réécrite après l'arbitrage : les items 2 à 4 étaient rédigés pour les voies 1 et 2, dont l'une parle
d'un gabarit qui n'existe plus et l'autre d'un `kind` qui ne change pas.*

- [x] Une voie tranchée et écrite sous `## Design arrêté`, avec ce qu'elle coûte au drain et à l'HPA.
- [x] Un test qui échouerait sur le code actuel : la remise par pod exercée **contre l'adresse lue dans
      le registre**, un pod qui s'y est enregistré avec la sienne — et aucune adresse configurée côté
      routeur. Le `stubResolver` vers `127.0.0.1` de `internal/smppserver/return_leg_integration_test.go`
      disparaît avec lui.
- [x] `internal/deploy` tient le lien à sa nouvelle place : plus de gabarit à tenir d'accord avec le DNS,
      mais une assertion que `smpp-server-svc` injecte bien `SMPP_POD_ADDR` depuis `status.podIP` et
      `SMPP_POD_ID` depuis `metadata.name`. `EnvVarSource.FieldRef` est déjà décodé par `deploy.go`.
      La règle neuve doit figurer dans `TestTheGuardCatchesWhatItClaimsTo` et mordre sur
      `testdata/broken`, sans quoi rien ne prouve qu'elle puisse échouer.
- [x] Le gabarit est parti **partout** : `config.SMPP.PodAddrTemplate`, `AddrResolver`,
      `templateResolver`, `NewTemplateResolver`, le `Service` `smpp-server-headless`, et les deux
      paragraphes de `deploy/README.md` qui en font une singularité de topologie.
- [x] gofmt/goimports · golangci-lint (0 issue) · `go test -race ./...` (exit 0) · `make manifests`
      (35 ressources, 0 invalide) · `buf generate` verts

## Ce que la revue a trouvé

**Premier tour : la revue par sous-agents n'a pas pu avoir lieu** — les trois relecteurs lancés sur des
axes disjoints (mécanisme · valeur probante des tests · manifests & contrats) ont été interrompus par
une limite de dépense de l'API, sans produire un seul constat. La relecture a d'abord été celle de
l'auteur. **Second tour, une fois le quota revenu : les trois relecteurs ont tourné et rendu leurs
constats**, repris plus bas. Aucun bloquant sur aucun des trois axes.

Ce que l'auto-relecture avait trouvé, et qui est corrigé ici :

- **Une violation parasite dans la fixture.** L'objet ajouté à `internal/deploy/testdata/broken/`
  déclenchait aussi `pdb-per-deployment`, son pod template n'ayant pas le label que `coveredByPDB`
  cherche — un futur rouge sur cet objet n'aurait pas été attribuable à une règle. Corrigé : il ne viole
  plus que `downward-api-fields`, sur ses deux variables.

Ce qu'elle a vérifié et trouvé sain :

- `pipe.Exec` sur un pipeline vide rend `nil, nil` (go-redis v9.21, `pipeline.go:104`) : un compte sans
  bind ne produit pas d'erreur, et `TestReturnLegDeadLettersWithoutBindOrWebhook` couvre ce chemin.
- Le rafraîchissement (30 s, `defaultRefreshInterval` = TTL/2) renouvelle l'adresse à la moitié de son
  TTL (61 s) : pas de fenêtre où une session vivante perd son adresse.
- `NewDeliverer` remplace un logger nil par `slog.Default` : le log ajouté à `tryBinds` ne peut pas
  paniquer.
- Aucune référence résiduelle au gabarit ni au `Service` headless hors commentaires historiques, qui
  portent le *pourquoi* et sont gardés à dessein.
- `.claude/rules/contracts-api.md` ne vise que `api/openapi-*.yaml` : un `.proto`, non publié en npm,
  n'entraîne pas de bump de `api/package.json`.

Ce que la reprise de la revue (les trois relecteurs, une fois le quota revenu) a trouvé et **corrigé
ici** — aucun bloquant sur les trois axes :

- **`podAddrs` masquait une panne Redis derrière une dégradation normale.** `Pipeline.Exec` rend la
  PREMIÈRE erreur (`cmdsFirstErr`), donc le `redis.Nil` d'un pod sans adresse couvrait une erreur de
  transport sur le `GET` du pod suivant, lu ensuite comme « pas d'adresse ». Les erreurs sont désormais
  inspectées **par commande**.
- **Un commentaire porteur affirmait une sûreté que le code n'a pas.** `r.rdb` est un `*redis.Client` :
  seul un `*redis.ClusterClient` route un pipeline slot par slot. Le choix du pipeline reste bon comme
  forme qui survivra au cluster, mais le commentaire le disait déjà acquis ([[invented-mechanism-in-comments]]).
- **`Touch` était devenu un piège.** La doc du paquet invite à le câbler ; il rafraîchissait le jeton
  sans renouveler l'adresse, donc il aurait gardé un bind vivant pendant que la voie retour perdait le
  chemin — la mort silencieuse que cette fiche corrige. Il renouvelle les deux, et un test le pin.
- **`SMPP_POD_ADDR` n'était ni validé ni journalisé.** Une valeur portant déjà un port donnait
  `[1.2.3.4:9000]:7000`, cible acceptée par `grpc.NewClient` puis échouant à chaque RPC. Elle est
  refusée au démarrage, et l'adresse composée est loggée — pour une step dont le sujet est une voie
  retour muette, ce bouton n'avait pas le droit d'être sans garde.
- **Trous de test comblés** : le *code* de statut d'un bind sans adresse (un `InvalidArgument` y aurait
  coupé la remise vers **tous** les binds du compte, pas seulement le sien) ; la clé de cache par
  adresse, qu'aucun test n'atteignait ; `podAddr` et ses crochets IPv6, seule ligne du dépôt à les
  poser alors que les tests du registre les écrivaient à la main ; l'isolation des `pod_id` littéraux
  sous `-count=2` ; et les deux entrées de `downwardAPIFields`, dont une seule était pinnée.
- **Mineurs** : `Close()` laissait le client re-dialer et fuir une connexion ; la doc de
  `grpctls.Dialer` contredisait son unique appelant ; `tasks-todo/step-300.md` — une fiche **à faire** —
  décrivait au présent le code que cette step vient de supprimer ; trois docs décrivaient la table du
  registre sans `pod_addr`.

Ce qui part en fiche plutôt qu'ici :

- **Le cache de connexions de `PodClients` ne s'évince jamais** → **step-303**, enrichie en revue d'un
  second symptôme plus grave que le plafond mémoire : une IP réattribuée est servie depuis la connexion
  en backoff de son ancien occupant, soit ~2 min de webhook pour un pod joignable et un bind vivant.
- **`Canceled`/`DeadlineExceeded` sont classés « bind mort » par `tryBinds`**, sans timeout par bind :
  un seul pod lent consomme le deadline de l'appelant, les binds suivants échouent instantanément, et
  la marche conclut `bind_exhausted` alors qu'un bind vivant existait et que la cause était notre propre
  échéance. Antérieur à cette step, mais c'est elle qui met ce chemin en production pour la première
  fois. **Non traité ici**, faute de fiche : à arbitrer avant le go-live.
- **Amplification d'écriture** : le `SET` d'adresse part à chaque bind ET à chaque refresh, soit
  ~170–670 SET/s supplémentaires à la cible de charge, tous écrivant la même valeur sur la même clé —
  et il double les aller-retours Redis sur le chemin chaud du bind. Un seul écrivain par pod suffirait.
  **Non traité ici** : mesurable, non bloquant, et le regrouper avec le script demande un pipeline dont
  l'ordre d'échec est moins clair que la séquence actuelle.
- **Le cadencement refresh/TTL tient par une constante, pas par un contrat** : le refresh dérive d'une
  constante compilée dans `smpp-server`, le TTL vit côté `session-manager` et est réglable
  (`WithSessionTTL`). L'alignement 30/60/61 est juste aujourd'hui et non câblé, donc rien ne le
  rattraperait s'il divergeait. Dette antérieure au
  choix de la clé (les `pod_id` d'un `Deployment` sont tout aussi éphémères qu'une IP), invisible
  jusqu'ici parce que la voie retour ne dialait jamais avec succès. Le plafond est nommé dans le code.

## Hors périmètre

L'identité TLS du pod joint : tranchée en step-300b, et volontairement indépendante de l'adressage —
c'est le `Deployment` (ou le `StatefulSet`) qui est vérifié, jamais le pod. Une identité **par pod**
demanderait des certificats par pod, donc une infrastructure que ce dépôt ne déploie pas.

Le **contrôleur** : `smpp-server-svc` reste un `Deployment`, et c'est un choix, pas un statu quo — ses
motifs sont sous `## Design arrêté` (recréation immédiate sur perte de nœud, rollout parallèle, et un
drain qui ne dépend d'aucun `EndpointSlice`). Ne pas rouvrir sans un fait neuf.

*(Levé après la revue.)* `Registry.Touch` et `touch.lua` étaient morts en production depuis que
`refreshLoop` rafraîchit par `Bind`. Cette step les avait d'abord laissés en place en leur ajoutant le
renouvellement d'adresse, pour qu'ils ne deviennent pas un piège — c'est-à-dire du code ajouté à du code
mort. Ils sont **supprimés**, et leurs deux tests basculés sur le chemin réel (`TestRebindRefreshesTTL`,
`TestRebindRenewsThePodAddress`) : le rafraîchissement par re-`Bind` n'était couvert par **aucun** test
jusque-là, alors que c'est le seul que la production emprunte.
