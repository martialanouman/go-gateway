---
paths:
  - "debts/**"
---

# Fiches de dette

Une dette = ce qu'on a sciemment **choisi de ne pas faire**, et qui coûtera plus
tard. Pas un bug (ça se corrige), pas une idée (ça se propose) : un arbitrage
rendu, dont le prix est différé.

- **Un fichier par dette**, nommé par son sujet en kebab-case. `ls debts/` EST
  l'index — il n'y a pas d'`INDEX.md`, parce qu'un index dérivé rouille et qu'on
  ne s'en aperçoit qu'au go-live.
- L'en-tête porte le **statut**, la **nature** (technique / produit), d'**où**
  elle est née, et **qui la porte** — une step de `tasks-todo/`, ou `—` si elle
  est orpheline.
- **Une dette payée garde son fichier.** On passe son statut à `PAYÉE`, avec la
  date et la PR. Supprimer le fichier effacerait la seule trace de pourquoi on
  avait choisi autrement.
- **Le corps répond à quatre questions, dans cet ordre** : ce qu'on a fait à la
  place · pourquoi (la raison *écrite* à l'époque, pas une reconstruction) · ce
  qu'il en coûte si on ne la paie jamais · à quoi on reconnaîtra qu'il faut la
  payer.
- **Citer la source**, `fichier:ligne`. Une dette dont on ne retrouve plus l'aveu
  dans le code n'est plus vérifiable.
- Une dette **portée par une step** n'est pas orpheline : elle a une échéance.
  C'est la distinction qui décide de l'urgence, elle se met à jour dans les deux
  sens quand une step naît ou meurt.
- **Une dette dont le paiement est tout le sujet d'une step ouverte n'a pas de
  fiche ici** — step-303, step-330, step-395… : la fiche de step EST son suivi,
  et la recopier créerait un second backlog qui divergerait du premier. `debts/`
  porte ce qu'aucune step ne couvre entièrement : les orphelines, et les dettes
  logées *à l'intérieur* d'une step dont le sujet est autre chose.
