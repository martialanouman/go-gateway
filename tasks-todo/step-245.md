# step-245 — un jeton d'annulation gagné sans sa ligne CDR laisse un message rejouable

> **Jalon :** M12, dette découverte pendant step-240 · **Statut :** À FAIRE
> **Dépend de :** step-209 (jeton d'annulation), step-240 (garde du rejeu) · **Bloque :** —

## But

Fermer le dernier résiduel par lequel un message annulé peut encore repartir : celui où l'annulation
a **gagné le jeton** mais n'a **jamais écrit sa ligne CDR**, de sorte que la garde du rejeu — qui lit
le CDR — ne voit rien à refuser.

## Le constat

`internal/cancel/cancel.go` pose le jeton **avant** d'écrire la ligne, et c'est délibéré : le jeton est
ce qui empêche réellement le dispatch, donc il doit être durable avant que `Cancel` ne rapporte un
succès (commentaire l.93-95).

```go
holder, err := c.flags.Claim(ctx, messageID, HolderCancel)   // l.122 — TTL 72 h
...
if err := c.writer.Insert(ctx, cancelledRow(row)); err != nil { // l.138
    return errs.ErrInternal                                   // ← la ligne n'existe pas
}
```

Si l'écriture ClickHouse échoue, l'état **« jeton pris, ligne absente » est collant** : à la tentative
suivante du client, la lecture rend encore `accepted`, le `Claim` retrouve **notre propre** jeton, et
`cancel.go:126-128` retourne succès **sans rien écrire**.

```go
case holder == HolderCancel:
    // Our own earlier intent, whose CDR row has not been projected back to us yet. Idempotent.
    return nil
```

**Le défaut n'est pas ce commentaire — il décrit un cas réel.** Une tentative précédente a pu écrire sa
ligne sans qu'elle soit encore visible à cette lecture ; `cancel.go:94` l'assume d'ailleurs déjà (« the
row is the visible state and **follows** »). Le défaut est que **cette branche ne peut pas distinguer les
deux causes** de ce qu'elle observe :

| ce que la branche voit | cause A | cause B |
|---|---|---|
| jeton à nous, pas de ligne `cancelled` | la ligne a été écrite, pas encore visible | l'écriture a échoué, la ligne n'existe pas |

Elle traite les deux comme un succès idempotent. Pour A c'est juste ; pour B c'est un message qui restera
sans ligne `cancelled` pour toujours, et que la garde du rejeu ne pourra donc pas reconnaître.

### La conséquence, et pourquoi step-240 ne la couvre pas

Le chemin de **dispatch** rattrape ce cas : voyant un jeton `HolderCancel`, le connecteur écrit
lui-même la ligne `cancelled` (`connectorpool.go:883`) — c'est écrit noir sur blanc dans son
commentaire, « closes the window where the Canceller crashed after claiming but before writing ».

La branche d'**expiration** est le seul chemin qui saute ce rattrapage : elle retourne avant le
`Claim` (`connectorpool.go:845`). Le message part donc sur `mt.dead-letter` avec une ligne `failed`,
et **rien nulle part ne porte l'annulation**. Au rejeu, la garde de step-240 lit `failed`, laisse
passer, et au-delà de 72 h le connecteur trouve un jeton libre et envoie.

Fenêtre : annulation **+ échec d'écriture ClickHouse** + expiration avant dispatch + rejeu tardif. Plus
étroite que celle de step-240 — mais c'est le même dommage, et step-240 a fermé tout le reste.

## Points d'implémentation clés

- **Deux gestes possibles, à arbitrer, pas à cumuler.**
  1. *Côté pool* — consulter le jeton dans la branche d'expiration (`Peek`, une lecture seule qui
     n'existe pas encore sur `cancel.RedisFlags`) et, s'il est détenu par autre chose que
     `HolderDispatched`, prendre la **branche d'annulation existante** au lieu de dead-letterer. Le
     message ne va alors plus du tout sur `mt.dead-letter`, la ligne manquante est écrite, et le bruit
     du compteur `delivery_expired` disparaît par construction.
  2. *Côté Canceller* — écrire la ligne aussi sur le chemin « le jeton est déjà à nous » (idempotent
     sous `ReplacingMergeTree` : même clé de tri, même rang 60). Trois lignes, mais ne guérit que si le
     client retente son `cancel_sm`.
- **L'objection d'ADR-0013 ne s'applique pas au geste 1, et il faut écrire pourquoi.** L'ADR dit que
  « revendiquer, pas lire » est ce qui rend la décision décisive. C'est vrai quand la lecture conditionne
  un **envoi**. Dans la branche d'expiration, on a déjà décidé de ne pas envoyer : une lecture racée ne
  peut que mal classer un message qui ne partira pas. Le `Peek` est admissible exactement là où le
  `Claim` ne l'est pas.
- **Le geste 1 change le comportement du chemin de dispatch**, que la fiche step-240 et ADR-0013
  tenaient tous deux hors périmètre. C'est la raison pour laquelle il a été sorti de step-240 plutôt
  qu'oublié.
- **Ne pas allonger le TTL du jeton** pour compenser : step-240 a déjà tranché que le rejeu décide, pas
  la durée de vie d'une clé.

## Tests (écrits dans la même PR)

- Un message périmé dont le jeton est détenu en `HolderCancel` n'est **pas** dead-letteré : une ligne
  CDR `cancelled`, la réservation libérée, `dl.count("delivery_expired") == 0`.
- Le cas ordinaire (jeton libre, message périmé) reste dead-letteré en `delivery_expired` — pas de
  régression sur step-129.
- Une erreur Redis sur le `Peek` ne bloque pas la branche : fail-open comme le `Claim` juste en dessous,
  la garde du rejeu restant le filet.
- Aucun test du dépôt ne combine aujourd'hui `MaxMessageAge` et un `CancelFlags` non-noop : les deux
  harnais (`runWithFlags`, `deadLetterDeps`) sont disjoints. Il faudra les faire se rencontrer.

## Definition of Done

- [ ] `make check` vert (lint · `test -race` · govulncheck · contrats)
- [ ] un message annulé dont la ligne CDR n'a jamais été écrite ne peut plus repartir par le rejeu
- [ ] le geste retenu est tranché et écrit avec sa raison, y compris pourquoi ADR-0013 ne s'y oppose pas
- [ ] la branche `holder == HolderCancel` distingue « ligne écrite, pas encore visible » de « ligne
      jamais écrite », ou explique pourquoi elle n'a pas à le faire (le geste 2 rend la question sans
      objet : elle écrit dans les deux cas)
- [ ] si le geste 1 est retenu : l'imprécision du compteur `delivery_expired`, documentée par step-240
      dans `connectorpool.go`, est retirée puisqu'elle n'a plus lieu d'être

## Hors périmètre

La garde du rejeu elle-même (step-240, livrée). L'ordre des deux contrôles dans `processOne` — il n'est
pas inversé ici, seulement complété. La sémantique de `cancelled` (ADR-0013).
