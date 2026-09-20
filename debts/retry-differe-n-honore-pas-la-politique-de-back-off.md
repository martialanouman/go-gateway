# Le retry différé des webhooks n'honore pas le back-off de la politique

> **Statut :** OUVERTE · **Nature :** produit
> **Née de :** step-192, relevée par step-340 · **Portée par :** — (orpheline ; step-340 a tranché le
> constat, pas la dette)

Sur le chemin différé, seul `max_attempts` traverse : le runner « ne pace que sur ses propres
constantes », et la politique n'est consultée que pour décider de l'épuisement.

**Ce qu'on a fait à la place (step-340, 2026-09-20).** On documente au lieu d'honorer. La description
de `retry_policy_json` dit désormais, dans les trois schémas du contrat Admin, que seul `max_attempts`
s'applique aux retries différés et que les champs de back-off ne pacent que l'envoi en bande.

**Pourquoi.** Honorer la politique telle quelle la casserait : ses défauts publiés
(`initial_backoff_ms = 1000`, `max_backoff_ms = 30000`, `multiplier = 2`) dimensionnaient la boucle
synchrone de quelques secondes, celle que `Sender.Send` n'emprunte plus depuis que le sink est câblé.
Appliqués au chemin différé, les 8 essais du budget brûleraient en une minute au lieu de quarante, et
un endpoint revenu après dix minutes serait de nouveau perdu — exactement ce que step-192 avait
corrigé en montant ce budget de 3 à 8. Re-dimensionner les défauts est un choix produit, et le faire
dans le schéma publié est un changement de contrat : ni l'un ni l'autre n'appartient à une step qui
livre un CRUD.

**Ce qu'il en coûte.** Le réglage reste partiellement inerte : un opérateur qui veut un rythme autre
que 30 s × 2 plafonné à 10 min n'a aucun levier, et croit en avoir un tant qu'il ne lit pas la
description. La honnêteté de la surface tient à une phrase, pas à un mécanisme.

**À quoi on reconnaîtra qu'il faut la payer.** Au premier client qui demande un rythme de retry
différent de celui du runner — typiquement un récepteur qui préfère être repris vite, ou au contraire
beaucoup plus tard. Le payer veut dire : faire lire la politique au sink au moment où il stampe
`NotBefore` (l'échéance reste absolue, l'invariant de step-192 n'est pas en cause), **et** re-décider
des défauts pour ce chemin.

Sources : `internal/webhook/retry.go:92-100` (seul `max_attempts` traverse),
`internal/modlrrouter/webhook_retry.go:64` et `internal/modlrrouter/webhook_retry_runner.go:18-21`
(le pace et ses constantes).
