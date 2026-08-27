# step-240 — le rejeu d'un dead-letter ne doit pas remettre sur le fil un message annulé

> **Jalon :** M12, dette découverte pendant step-209 · **Statut :** À FAIRE
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
- Le compteur de dead-letter ne classe pas une annulation en `delivery_expired`.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] un message annulé ne peut plus être remis sur le fil par un rejeu, **quel que soit le délai**
- [ ] le porteur de la garde (rejeu ou connecteur) est tranché et écrit avec sa raison
- [ ] le classement du compteur de dead-letter est corrigé, ou son imprécision est documentée

## Hors périmètre
La sémantique de `cancelled` et le jeton lui-même (step-209, ADR-0013). La re-réservation au rejeu,
suite déjà documentée de step-129. Le max-age SLA lui-même.
