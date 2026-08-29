# step-240 — le rejeu d'un dead-letter ne doit pas remettre sur le fil un message annulé

> **Jalon :** M12, dette découverte pendant step-209 · **Statut :** LIVRÉE
> **Dépend de :** step-129 (dead-letter + rejeu), step-209 (jeton d'annulation) · **Bloque :** —

## But
Fermer la dernière porte par laquelle un message annulé peut encore partir : le **rejeu manuel** d'un
message annulé qui avait péri avant d'être dispatché.

## Le constat

`internal/connectorpool/connectorpool.go` teste l'**expiration avant l'annulation** :

```go
if s.expired(routed) {                      // ~l.840 — max-age SLA (step-129)
    return s.deadLetterWith(ctx, routed, errs.ErrDeliveryExpired)
}
holder, err := s.deps.CancelFlags.Claim(...) // ~l.861 — jeton d'annulation (step-209)
```

Un message annulé puis périmé part donc sur `mt.dead-letter` **sans que le connecteur ait revendiqué
le jeton**, et sans passer par la branche d'annulation.

### Ce qui n'est PAS le problème (vérifié, à ne pas re-dériver)

Le statut visible est correct. `deadLetterWith` écrit une ligne **`failed`** (rang 50, raison
`delivery_expired`) — pas `expired` — et le `cancelled` (rang 60) que le Canceller a déjà écrit la
supplante sous `ReplacingMergeTree`. `GET /messages/{id}` lit donc bien `cancelled`, ce qui est exact
au sens d'ADR-0013 : le message n'est jamais parti. La réservation est libérée une fois, par
`deadLetterWith`. L'ordre des deux contrôles est **bénin pour l'affichage et pour l'argent**.

### Ce qui l'est

Le message dort sur `mt.dead-letter`, et l'outil de rejeu (`internal/connectorpool/replay.go`)
**rebase l'horloge de max-age** en estampillant `replayed_at`. Le message redevient donc envoyable.

Au rejeu il repasse par la revendication du jeton, donc :

- **dans les 72 h** (TTL de `HolderCancel`) → le jeton tient, l'annulation est honorée, rien ne part ;
- **au-delà de 72 h** → le jeton a expiré, la clé est libre, le connecteur la revendique comme
  `dispatched` et **envoie le message annulé**.

Et le dommage ne s'arrête pas à l'envoi : la ligne `cancelled` (60) écrite par le Canceller est
toujours là, donc elle enterre l'`enroute` (20) puis le `delivered` (40) qui suivent. C'est
**exactement le symptôme que step-209 a fermé**, réintroduit par la porte du rejeu.

Fenêtre réelle : un message doit avoir été annulé, avoir péri avant dispatch, et être rejoué plus de
72 h après. Étroit — mais le rejeu est précisément l'outil qu'on sort après un incident long, donc les
trois conditions se rencontrent au pire moment.

**Bruit d'observabilité, à traiter ou à assumer dans la même PR :** `DeadLetter.Inc("delivery_expired")`
compte une annulation comme une expiration. Un opérateur qui enquête sur un pic d'expirations y trouve
des messages que des clients avaient annulés.

## Points d'implémentation clés

- **Inverser les deux contrôles n'est pas gratuit.** Revendiquer le jeton avant le test d'expiration
  ferait poser un jeton `dispatched` (TTL 5 min) sur un message qu'on s'apprête à dead-letterer, donc
  refuserait pendant 5 min des `cancel_sm` légitimes sur un message qui ne partira jamais. À arbitrer
  contre l'alternative : consulter l'annulation sans revendiquer, uniquement dans la branche
  d'expiration.
- **Le TTL du jeton ne peut pas être la réponse.** L'aligner sur la durée de rétention du dead-letter
  ferait vivre 72 h → N jours une clé par message annulé, et ne bornerait toujours rien pour un rejeu
  plus tardif. Le rejeu doit décider, pas la durée de vie d'une clé.
- **Le rejeu est déjà un point de décision.** `replay.go` réécrit l'enveloppe (il retire l'en-tête
  `dead_letter_reason` et estampille `replayed_at`) : c'est l'endroit naturel pour refuser un message
  dont le CDR lit `cancelled`, plutôt que de faire porter la garde au connecteur.
- **Trancher d'abord qui porte la garde** — le rejeu ou le connecteur — avant d'écrire une ligne. Le
  précédent existant (`step-129`, « le rejeu re-réserve : suite documentée ») dit que le rejeu a déjà
  des décisions non tranchées ; les traiter ensemble vaut mieux que de les empiler.

## Tests (écrits dans la même PR)
- Un message annulé, périmé, puis rejoué **au-delà** du TTL du jeton n'est pas soumis au SMSC.
- Le rejeu d'un message ordinaire (jamais annulé) reste inchangé — pas de régression sur step-129.
- ~~Le compteur de dead-letter ne classe pas une annulation en `delivery_expired`.~~ Le pool n'a, dans
  la branche d'expiration, aucune information sur l'annulation : ce test ne peut pas être écrit sans le
  geste de step-245. L'imprécision est documentée à la place, ce que la DoD autorise.

## Definition of Done
- [x] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [x] un message annulé ne peut plus être remis sur le fil par un rejeu, **quel que soit le délai** —
      sous une réserve nommée et fichée : voir le résiduel ci-dessous
- [x] le porteur de la garde (rejeu ou connecteur) est tranché et écrit avec sa raison
- [x] le classement du compteur de dead-letter est corrigé, ou son imprécision est documentée

## Ce qui a été tranché, et pourquoi

**La garde vit dans le rejeu, et lit le CDR.** Le jeton ne pouvait pas la porter : son TTL de 72 h est
exactement ce qui expire dans le scénario. Le CDR le peut, et pour une raison **structurelle** — pas
temporelle. ADR-0013 refuse d'arbitrer une annulation vivante sur le CDR parce que la projection lague ;
mais ce qui lague, ce sont `enroute`/`delivered`, projetés depuis `mt.outcome` depuis step-201c. La ligne
`cancelled` est écrite **en synchrone par son auteur**, et c'est la seule que cette garde lit.

Une marque posée par le connecteur sur l'enveloppe aurait été strictement dominée : elle n'aurait pas
couvert le **backlog déjà garé** — précisément la population que l'outil existe pour drainer.

**Trois verdicts, pas deux.** `cancelled`/`rejected` → refus, offset commité. Erreur de lecture → refus
**sans** commit, l'outil s'arrête et un relancement reprend sur ce même enregistrement : c'est gratuit,
puisque le consommateur ne commite que le préfixe traité. Ligne CDR absente → refus, offset commité,
compteur séparé — `deadLetterWith` écrit la ligne `failed` **avant** de produire sur `mt.dead-letter`,
donc tout enregistrement garé a une ligne dans le scope exact que la garde lit ; une absence ne peut
venir que d'une purge de rétention ou d'un effacement RGPD, et rejouer y serait pire que refuser.

**`rejected` avec `cancelled`, et pas `delivered`.** L'agrégat multi-segments teste `rejected` **avant**
`cancelled` : ne tester que `cancelled` aurait fait dépendre la garde d'une propriété d'un autre service.
`delivered`, lui, est inatteignable — tout enregistrement garé porte une ligne `failed`, testée avant —
donc l'y ajouter aurait été du code mort qu'aucun rouge honnête n'exerce.

**Un lecteur nil refuse.** Un binaire mal câblé s'arrête au premier enregistrement au lieu de rejouer des
annulations en silence.

## Le résiduel, nommé et fiché — step-245

Le Canceller gagne le jeton **avant** d'écrire sa ligne. Si l'écriture ClickHouse échoue, l'état « jeton
pris, ligne absente » est **collant** : la tentative suivante retrouve son propre jeton et retourne succès
sans rien écrire. Un tel message lit `failed` au rejeu, et la garde le laisse passer. Le chemin de
dispatch rattrape ce cas en écrivant la ligne lui-même ; la branche d'expiration est le seul chemin qui
saute ce rattrapage.

Le fermer exige de toucher le jeton et le chemin de dispatch, que cette fiche **et** ADR-0013 mettent hors
périmètre. Précédent du dépôt : ADR-0013 DN7 documente un résiduel fail-open sans le poursuivre. D'où
`tasks-todo/step-245.md`, créée dans le même commit — un commentaire de code n'est pas un backlog.

## Ce que la step a trouvé en chemin

**Aucun test ClickHouse du dépôt ne mentionnait `StatusCancelled`.** Toute la garde repose sur
`cancelled_cnt > 0` battant `failed_cnt > 0` dans l'agrégat message ; les faux lecteurs des tests
unitaires court-circuitent ce SQL. Sans le test d'intégration ajouté ici, la garde entière reposait sur
une lecture de requête. La mutation qui échange les deux le prouve : elle passe tous les tests unitaires
et ne fait tomber que celui-là.

## Hors périmètre
La sémantique de `cancelled` et le jeton lui-même (step-209, ADR-0013) — d'où step-245. La
re-réservation au rejeu, suite déjà documentée de step-129. Le max-age SLA lui-même. La collapse des
lectures pour les N segments d'un même message : un mémo à une entrée le ferait, mais le gain dépend
entièrement du nombre de segments — une optimisation à mesurer, pas à supposer. Le coût est écrit dans
le godoc.
