# Le drain de retry compte `retried` un événement qu'il a garé au dead-letter

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-192, relevée par la revue de step-295b · **Portée par :** —

`WebhookRetryRunner.Handle` fait `r.metric.Handled("retried")` dès que `Sender.Retry` rend `nil`. Or `Retry`
rend `nil` dans **quatre** cas différents : l'événement a été remis, il a été garé sur un rejet permanent,
il a été garé parce que le budget d'essais est épuisé, ou parce que la borne d'âge de six heures est
dépassée. Depuis step-295b il y en a un cinquième : garé parce que son secret ne s'ouvre pas. Un seul de
ces cinq est un « retried ».

`webhook_retry_handled_total{outcome}` est le seul signal par issue de ce drain. Le label `parked` existe,
mais il ne sert qu'à la branche `webhook_disabled`, que le runner décide lui-même. Tout ce que le sender
gare est donc compté `retried`, et l'histogramme d'âge est observé au passage comme si la remise avait eu
lieu.

**Ce qu'on a fait à la place.** Rien dans step-295b, dont le sujet est le scellement du secret. Le
mauvais étiquetage est antérieur : il vient de step-192, quand le chemin différé a été introduit.

**Pourquoi.** Le corriger demande que `Retry` dise **quel** état terminal il a atteint — un type d'issue
rendu au lieu d'un `error` nu — donc de toucher `internal/webhook`, l'interface `RetrySender` du runner et
son double. C'est un changement de signature sur le chemin chaud de la voie retour, pour un défaut
d'observabilité, dans une PR qui livrait déjà un changement de schéma, un nouveau client gRPC et une
correction d'autorisation. La revue de step-295b l'a explicitement rangé ici plutôt que de l'emporter.

**Ce qu'il en coûte.** Un dead-letter de masse est invisible. C'est précisément le mode de panne que la
même revue a trouvé sur la classification des échecs d'ouverture : un décalage de rollout, ou un chiffré
que la KMS refuse, peut vider un backlog entier dans le dead-letter tandis que le compteur affiche un flux
propre de `retried`. La classification a été corrigée ; la cécité, non. C'est la différence entre « un
opérateur le voit en quelques minutes » et « un opérateur l'apprend quand un client appelle ».

**À quoi on reconnaîtra qu'il faut la payer.** Au premier incident où l'on cherche pourquoi des MO ou des
DLR manquent chez un client sans qu'aucune métrique n'ait bougé. Ou à la première alerte qu'on voudra
écrire sur `webhook_retry_handled_total{outcome="parked"}` — elle ne peut pas fonctionner aujourd'hui. Le
payer veut dire : faire rendre par `Retry` l'issue atteinte, et étiqueter les cinq cas.

Sources : `internal/modlrrouter/webhook_retry_runner.go:162-168` (`Handled("retried")` inconditionnel),
`internal/webhook/retry.go` (`deliverOnce` : les quatre `nil`), `internal/webhook/secret.go`
(`onOpenFailure`, le cinquième).
