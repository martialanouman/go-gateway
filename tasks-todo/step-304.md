# step-304 — La voie retour raconte mal ses échecs, et écrit plus qu'il ne faut

> **Jalon :** Dettes ouvertes par step-302 · **Statut :** À FAIRE
> **Dépend de :** step-302 · **Bloque :** step-410 (go-live)

## Pourquoi cette fiche existe

step-302 a rendu la remise MO/DLR par pod réellement fonctionnelle. Deux défauts du chemin qu'elle vient
d'allumer ont été constatés en revue, **tous deux antérieurs à elle**, et aucun n'entrait dans son
périmètre. Ils sont réunis ici parce qu'ils vivent dans la même boucle : `tryBinds` et le chemin chaud
du bind.

## Constat 1 — une annulation n'est pas un bind mort

`internal/modlrrouter/deliverer.go` — le `default:` de `tryBinds` range ensemble tout ce qui n'est ni
`OK` ni `InvalidArgument` : un transmitter (`FailedPrecondition`), un bind disparu (`NotFound`,
`Unavailable`)… **et une annulation du contexte de l'appelant** (`Canceled`, `DeadlineExceeded`).

Or la boucle n'a **aucun timeout par bind** : le même `ctx` traverse les N binds, puis le webhook, puis
la dead-letter. D'où la séquence :

1. le premier bind interrogé est sur un pod lent et consomme l'échéance de l'appelant ;
2. chaque bind suivant échoue **instantanément** sur le `ctx` mort, et le `default:` les compte comme
   morts un par un ;
3. la marche se termine `hadBinds=true`, `delivered=false` → raison **`bind_exhausted`**, alors qu'un
   bind vivant existait et que la cause était notre propre échéance.

**Rien n'est perdu** — vérifié : le `Produce` de la dead-letter reçoit le même `ctx` mort, échoue, et
`Deliver` rend une erreur, donc le record est retraité. Le défaut est l'**attribution**, pas la perte :
la métrique `UndeliveredInc(…, "bind_exhausted")` et le `Warn` accusent les binds du client d'un
problème qui est le nôtre. Un opérateur qui suit cette métrique cherche au mauvais endroit.

C'est structurellement antérieur à step-302, mais c'est elle qui met ce chemin en production pour la
première fois — avant, aucun `Deliver` n'aboutissait.

## Constat 2 — le `SET` d'adresse part à chaque rafraîchissement

`internal/session/registry.go` — `Registry.Bind` publie l'adresse du pod avant le script de quota. Or
`refreshLoop` (`internal/smppserver/listener.go`) re-`Bind` **chaque connexion** toutes les
`defaultRefreshInterval` = 30 s.

Donc, par pod : une écriture Redis toutes les 30 s **par bind**, toutes portant la **même valeur** sur
la **même clé** `sess:pod:{pod_id}`, pour un TTL qu'une seule suffirait à renouveler. Aux 5 000–20 000
sessions simultanées visées : **~170 à 670 `SET`/s** de plus, dont ~67/s sur une seule clé pour un pod
tenant 2 000 binds. Et cela **double les aller-retours Redis** sur le chemin chaud du bind, là où le
reste du dépôt se bat pour les réduire.

Non bloquant, mesurable, et à trancher avant la mise en charge de step-280.

## Ce qu'il faut trancher

**Pour le constat 1 :** distinguer l'annulation de l'échec de bind coûte peu (`errors.Is(ctx.Err(), …)`
ou un `case` sur les deux codes), mais que faire ensuite est le vrai choix — abandonner la marche en
rendant une erreur (le record est retraité, comportement déjà effectif via la dead-letter qui échoue),
ou borner chaque bind par son propre timeout pour qu'un pod lent ne condamne pas ses voisins. Les deux
ne s'excluent pas ; le second change le budget de latence de la voie retour et mérite d'être chiffré.

**Pour le constat 2 :** l'écriture ne doit plus partir à chaque refresh. Une piste à peser — le pod
tient l'échéance de sa dernière publication en mémoire et ne réécrit qu'à mi-TTL, ce qui ramène le coût
à une écriture par pod et par demi-TTL au lieu d'une par bind. Attention à ce que la reprise après une
panne Redis ne laisse pas l'adresse absente jusqu'à la prochaine échéance locale.

## Design arrêté

Arbitrage : Fable, le 2026-09-23. Deux écarts à la lettre de la fiche, dits ci-dessous.

**Constat 1 — une échéance par bind, et l'annulation de l'appelant arrête la marche.**

- Le récit de la fiche suppose une échéance de l'appelant qui **n'existe pas** : le ctx vient du
  consommateur Kafka, sans échéance, annulé seulement à l'arrêt. Le vrai défaut du chemin est l'inverse :
  un pod qui accepte TCP sans répondre gèle la partition sans borne
  (`debts/remise-au-pod-sans-echeance-par-rpc.md`, payée ici).
- `tryBinds` borne chaque `pods.Deliver` par `bindDeliverTimeout` = 15 s. Un `DeadlineExceeded` du
  ctx FILS est l'échec de CE bind : on passe au suivant, qui reçoit une échéance neuve.
- Après un échec, si le ctx PARENT est mort, la marche s'arrête et rend l'erreur : le record est
  retraité, sans `UndeliveredInc`, sans `bind_exhausted`, sans dead-letter. On lit `ctx.Err()` du
  parent, jamais le code du status (un `context.Canceled` brut se lit `codes.Unknown`).
- **15 s** : au-dessus du `ResponseTimeout` du pod (10 s), pour que le cas courant (ESME muet, fenêtre
  libre) soit rendu `Unavailable` par le pod, qui journalise le bind ; seul un bind à fenêtre pleine
  et ESME lent tombe sur notre échéance, et il étouffe déjà. L'échéance se propage au pod, dont `Send`
  libère sur `ctx.Done()`.
- **Budget de latence** : au pire N × 15 s par record, N = binds vivants du compte (5 binds sourds =
  75 s de tête de ligne). Aujourd'hui : non borné. Couper la marche au premier `DeadlineExceeded` est
  refusé : c'est exactement un pod lent qui condamne ses voisins.
- L'échéance est dans `tryBinds` et non dans `PodClients` : c'est une règle de routage, testable
  contre un faux `PodDeliverer`. Elle est réglable par `DelivererDeps.BindTimeout` (zéro = 15 s),
  pour le test seulement ; le câblage ne la fixe pas.

**Constat 2 — le `Registry` saute un `SET` d'adresse publié il y a moins de TTL/4.**

- Le `SET` se fait dans `session-manager-svc`, pas dans le pod : la mémoire vit dans le `Registry`,
  qui sait lequel des deux pas a échoué. Le protocole pod → registre ne change pas, et le pod envoie
  toujours son adresse à chaque refresh (invariant step-302 intact).
- Mémoire `podID → {addr, at}`. On saute si même adresse ET `now − at < TTL/4` (15 s). Adresse
  différente ou entrée absente → `SET`. La mémoire n'est mise à jour **qu'après un `SET` réussi**,
  avec le `now` pris avant lui (la clé est crue plus vieille qu'elle n'est).
- **Pourquoi TTL/4 et pas la mi-TTL de la fiche** : un saut à 29,9 s suivi du refresh suivant à
  59,9 s laisse 1 s avant l'expiration à 61 s. Avec TTL/4, quelle que soit la réplique qui sert le
  Bind (mémoires indépendantes) : tout saut a lieu ≤ 15 s après un `SET`, un bind vivant se
  rafraîchit ≤ 30 s + 5 s d'appel, donc l'âge de la clé reste ≤ 50 s < 61 s.
- **Coût** : de ~67 `SET`/s sur la clé d'un pod à 2 000 binds, à au plus un par 15 s et par réplique.
- **Reprise après panne Redis** : un `SET` en échec ne touche pas la mémoire, le Bind suivant réécrit.
  Une perte de données sans erreur (Redis redémarré à vide) laisse l'adresse absente ≤ 15 s ; les
  sessions elles-mêmes ne reviennent qu'à leur refresh (≤ 30 s). Pendant ce temps, un bind revenu
  sans adresse est sauté vers le webhook — la dégradation déjà prévue par `podAddrs`.
- La mémoire n'est qu'une suppression d'écritures, jamais une source de vérité : élaguée des entrées
  de plus d'un TTL au moment d'une publication, pour ne pas grossir à chaque déploiement.

## Definition of Done

- [ ] Les deux voies tranchées et écrites sous `## Design arrêté`, celle du constat 2 avec le coût de
      la reprise après panne Redis.
- [ ] Un test qui échouerait aujourd'hui pour le constat 1 : un premier bind qui consomme l'échéance ne
      doit pas faire compter les binds suivants comme morts, ni produire la raison `bind_exhausted`.
- [ ] Un test qui échouerait aujourd'hui pour le constat 2 : N rafraîchissements d'un même pod ne
      produisent pas N écritures de son adresse.
- [ ] L'adresse reste publiée en continu sous un bind vivant — c'est l'invariant que step-302 a posé et
      que ce constat ne doit pas défaire : un pod dont l'adresse expire sous une session vivante éteint
      la voie retour SMPP en silence.
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` verts

## Hors périmètre

Le cache de connexions de `mo-dlr-router-svc` (éviction, et péremption sur adresse réattribuée) :
step-303.

L'adressage lui-même : tranché en step-302. Les deux constats ci-dessus sont antérieurs au choix de
l'adresse et survivraient à n'importe lequel.
