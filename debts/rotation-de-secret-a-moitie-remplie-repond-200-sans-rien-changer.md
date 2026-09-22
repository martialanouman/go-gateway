# Une paire scellée à moitié remplie fait répondre 200 à une rotation qui ne change rien

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-295, relevée par la revue de step-295b · **Portée par :** —

`sealedPair` (`internal/storage/postgres/convert.go`) rend `nil, nil` dès qu'une des deux moitiés manque —
chiffré vide **ou** référence de clé vide. Sous les deux `COALESCE` de l'`UPDATE`, les deux colonnes gardent
alors leur ancienne valeur. C'est **volontaire** et c'est la bonne protection de la ligne : les deux
arguments sont asymétriques, un chiffré nil devient `NULL` et `COALESCE` conserve, tandis qu'une référence
vide devient `''` — pas `NULL` — et `COALESCE` écrase. Une écriture partielle laisserait donc un chiffré à
côté de la référence d'une autre clé : une ligne qui s'ouvre aujourd'hui et qu'une rotation de clé maîtresse
ne saurait plus placer. `TestConnectorRepoIgnoresAHalfFilledSealedPassword` l'énonce et le garde.

Ce qui manque n'est pas la protection, c'est le **signal**. Une paire incomplète est indiscernable d'une
paire absente : l'appelant reçoit 200, la rotation n'a pas eu lieu, et l'opérateur croit l'ancienne clé
morte alors qu'elle signe toujours. C'est la surface « répond 200 à un réglage sans effet » que
`debts/ancre-de-confiance-par-connecteur.md` reproche déjà ailleurs.

**Ce qu'on a fait à la place.** Rien, et c'est un choix de la revue de step-295b. Le correctif tenait en
quatre lignes — rendre une erreur au lieu de `nil, nil` — mais il renverse une décision écrite d'une autre
step, dans une step dont le sujet est autre chose, et sans repasser par l'échelle d'arbitrage. Le correctif
a été écrit, testé, puis **annulé** pour cette raison : il faisait échouer le test qui porte la décision de
step-295, et réécrire ce test aurait été trancher en silence.

**Pourquoi ce n'est pas urgent.** Une paire incomplète est inatteignable aujourd'hui : `Seal` refuse un
secret vide, et `content.KeyRefOf` refuse une référence vide côté serveur. Le trou n'existe que le jour où
un vrai fournisseur KMS remplace `LocalKMS` — `internal/configsecrets/server.go` dit explicitement que
`content.KMS` **ne promet pas** une `KeyRef` non vide, « seule `LocalKMS` refuse la vide, et c'est
l'implémentation qu'un vrai fournisseur remplace ».

**Ce qu'il en coûte.** Trois surfaces d'administration — mot de passe de connecteur, identifiants de
fournisseur, secret de webhook — peuvent confirmer une rotation qui n'a pas eu lieu. Pour le webhook c'est
le plus visible : le récepteur continue de vérifier avec l'ancienne clé, et personne ne le sait avant de
comparer les colonnes à la main.

**À quoi on reconnaîtra qu'il faut la payer.** Au branchement d'un fournisseur KMS réel (AWS, GCP, Vault)
derrière `content.KMS` — c'est ce jour-là que la garde disparaît. Le payer veut dire : rendre une erreur sur
une paire incomplète plutôt qu'un no-op, et reformuler la décision de step-295 — « ignorer » devient
« refuser » — dans son test et dans son énoncé.

Sources : `internal/storage/postgres/convert.go:161-166` (`sealedPair`),
`internal/storage/postgres/connectors_integration_test.go:184-214` (la décision et sa garde),
`internal/configsecrets/server.go` (`content.KMS` ne promet pas de `KeyRef`).
