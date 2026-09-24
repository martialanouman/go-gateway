# La réécriture de sender ID ne sert que le MT : une règle `direction='mo'` est refusée

> **Statut :** OUVERTE · **Nature :** produit
> **Née de :** step-350 (PR1, design arrêté, point 1) · **Portée par :** —

**Ce qu'on a fait à la place.** `create-sender-rewrite-rule` répond 422 à `direction: mo`. La colonne
l'autorise, l'enum `Direction` du contrat aussi, mais aucun moteur ne l'évalue : le moteur de step-350
PR2 vit dans `connector-pool-svc`, juste avant l'envoi, et le MO ne passe jamais par là.

**Pourquoi.** Une règle que personne n'évalue ment à l'opérateur : elle s'affiche, elle est active, et
elle ne s'applique jamais. La refuser à la création est la seule réponse honnête tant qu'il n'y a pas de
moteur MO — et la §6.16 ne dit pas où il vivrait (`mo-dlr-router-svc`, avant la résolution du compte ?
après ?).

**Ce qu'il en coûte.** Le cas d'usage « normalisation reply-to MO » que la §6.16 liste n'existe pas.
Un opérateur qui en a besoin n'a aucun levier.

**À quoi on reconnaîtra qu'il faut la payer.** Le premier client dont l'adresse de réponse MO doit être
normalisée avant d'atteindre son webhook.

Sources : `internal/adminapi/sender_rewrite.go:180` (refus de `mo`) ·
`docs/specification-technique-passerelle-sms.md:941` (cas d'usage)
