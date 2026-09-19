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
