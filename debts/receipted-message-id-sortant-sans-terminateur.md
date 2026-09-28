# Le DLR remis aux clients porte un receipted_message_id sans son terminateur

> **Statut :** OUVERTE · **Nature :** conformité protocole
> **Née de :** le premier smoke bout-en-bout de l'environnement de test (2026-09-28) · **Portée par :** —

**Ce qu'on a fait à la place.** `receipted_message_id` est une C-Octet String (SMPP v3.4 §5.3.2.12) :
sa valeur se termine par un octet NUL. Le correctif du 2026-09-28 fait accepter ce terminateur au
DLR **entrant** (`internal/connectorpool/deliver.go`, `parseReceipt`). Le DLR **sortant**, celui que
la passerelle remet à ses clients ESME, l'omet toujours : `internal/modlrrouter/build.go:43` pose
l'UUID du message sans NUL.

**Pourquoi.** Hors du périmètre du correctif, qui visait la perte des DLR ; la voie sortante n'a
cassé aucun échange observé, et le smoke lit l'id dans le `short_message`, pas dans le TLV.

**Ce qu'il en coûte si on ne la paie jamais.** Un ESME client strict qui lit le TLV comme une
C-Octet String peut rejeter le `deliver_sm` ou lire au-delà de la valeur, et ne jamais corréler ses
accusés.

**À quoi on reconnaîtra qu'il faut la payer.** Avant le premier client SMPP réel (step-410), ou dès
qu'un client signale des accusés non corrélés.
