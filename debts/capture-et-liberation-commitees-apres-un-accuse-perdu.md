# Une capture et une libération du même message peuvent commiter toutes les deux après un accusé perdu

> **Statut :** OUVERTE · **Nature :** technique (argent)
> **Née de :** step-285b (revue, 04/10/2026) · **Portée par :** — (à trancher avant le go-live, step-410)

**Ce qu'on a fait à la place.** Capture et libération sont deux `entry_type` distincts : ni la réclamation
d'idempotence ni l'index du grand livre ne les excluent l'une de l'autre. Seuls le verrou terminal Redis et
les deux lectures préalables de `resolveTerminal` le font. Quand le COMMIT d'une issue terminale part sans
réponse (coupure, échéance), `RecordDurable` rend une erreur et `withTerminalLock` relâche le verrou alors
que la transaction peut encore aboutir. Une issue opposée qui prend le verrou dans cette fenêtre ne voit pas
encore la première, passe ses contrôles et écrit : le message est capturé **et** remboursé, livré gratuit.
step-285b lève l'ambiguïté sur le chemin groupé (`pg_xact_status`), pas sur le chemin unitaire : capture
d'un hold expiré (`no_reservation`), libération à froid, réparation d'une réserve rejouée.

**Pourquoi.** Antérieur à step-285b. La revue l'a nommé en mesurant ce que le lot élargissait ; le remède
structurel touche à l'exclusion mutuelle, hors du périmètre d'une step de débit.

**Ce qu'il en coûte.** Il faut une capture et une libération concurrentes du même message, dans la fenêtre
d'un COMMIT sans réponse (de l'ordre de la milliseconde), sur un chemin qui suit déjà une panne. Jamais
observé ; rien ne l'empêche. Chaque occurrence est un message livré non facturé, invisible tant qu'une
réconciliation ne compare pas le grand livre aux CDR.

**À quoi on reconnaîtra qu'il faut la payer.** Avant le go-live ; ou un message qui porte à la fois une
entrée `capture` et une entrée `release` au grand livre. Remède : une réclamation partagée par les deux
terminaux (par exemple `(message_id, 'terminal')` dans `billing_idempotency`), qui fait refuser la seconde
issue par la base — le même qui paierait `capture-lit-le-grand-livre-trois-fois-avant-d-ecrire.md`.

Sources : `internal/billing/billing.go` (`resolveTerminal`, `withTerminalLock`) ·
`internal/storage/postgres/billing.go` (`RecordDurable`, COMMIT) · `internal/connectorpool/settle/settle.go`
(échec ignoré, réconcilié par le reaper)
