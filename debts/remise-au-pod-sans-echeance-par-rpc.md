# La remise d'un MO/DLR à un pod n'a pas d'échéance par RPC

> **Statut :** PAYÉE le 2026-09-23 (step-304) · **Nature :** technique
> **Née de :** step-303 · **Payée par :** step-304

**Payée.** `tryBinds` borne chaque bind par `bindDeliverTimeout` (15 s,
`internal/modlrrouter/deliverer.go`) : un pod muet coûte 15 s puis la marche passe au bind suivant, avec
une échéance neuve. Au pire N × 15 s par record, N = binds vivants du compte.

**Ce qu'on a fait à la place.** `PodClients.Deliver` (`internal/modlrrouter/poddeliverer.go`) appelle
`SessionRegistry.Deliver` avec le ctx du consommateur Kafka, qui n'a pas d'échéance. Step-303 borne
seulement le cas par ricochet : une connexion non servie depuis `evictAfter` (120 s) est fermée, ce
qui annule une RPC restée en vol aussi longtemps.

**Pourquoi.** Le sujet de step-303 était l'éviction du cache. Une échéance par RPC change le
comportement de la remise elle-même : il faut choisir sa valeur, et ce qu'on fait d'un
`DeadlineExceeded`.

**Ce qu'il en coûte.** Un pod qui accepte TCP et ne répond jamais bloque le consommateur, qui traite
en série. Toute la voie retour de la partition reste figée jusqu'à 120 s, et non le seul compte
touché.

**À quoi on reconnaîtra qu'il faut la payer.** Du lag sur les groupes MO/DLR alors que les pods sont
joignables, ou une remise qui reprend par paliers d'environ 2 minutes.
