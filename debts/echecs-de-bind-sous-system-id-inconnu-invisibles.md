# Un bind sous un system_id inconnu n'apparaît dans aucun diagnostic

> **Statut :** OUVERTE (assumée) · **Nature :** produit
> **Née de :** step-286b (`tasks-todo/step-286b.md`, « Écarté ») · **Portée par :** —

**Ce qu'on a fait à la place.** `list-account-bind-failures` ne liste que les refus dont le `system_id`
résout un identifiant (révoqué compris). Un `system_id` inconnu n'écrit rien, nulle part.

**Pourquoi.** Un inconnu n'appartient à aucun compte, et l'écrire sous une clé dérivée de ce qu'envoie
le client laisserait un attaquant fabriquer des clés Redis sans borne. Le contrat le dit, pour que
l'écran ne promette pas l'exhaustivité.

**Ce qu'il en coûte.** La faute de frappe sur le `system_id` est la première cause d'un ESME qui ne
binde pas : l'opérateur voit une liste vide, à raison, et ne sait rien de plus que l'ESME
(`ESME_RINVPASWD`, §11.3). Le journal `smpp bind rejected` ne porte pas le `system_id` (§1.9).

**À quoi on reconnaîtra qu'il faut la payer.** Un ticket « mon ESME ne binde pas » où le tableau de
bord est vide. Piste : un journal par IP source, plafonné globalement.

Sources : `internal/smppserver/bind.go` `authorize` (`!found` → motif vide) ·
`internal/smppserver/listener.go` `recordRefusal` (`reason == ""`)
