# La file de revue anti-spam n'existe pas : le compteur de signalements ne pointe nulle part

> **Statut :** OUVERTE · **Nature :** produit
> **Née de :** step-291 (décision humaine du 05/10/2026) · **Portée par :** —

**Ce qu'on a fait à la place.** step-291 compte les signalements par sender ID dans des tranches horaires
Redis (24 h glissantes) et publie une métrique sans label d'expéditeur. Aucun message signalé n'est conservé
avec son `message_id`. La spec du tableau de bord (§6.6, « file de revue d'activité signalée :
approuver/bloquer/liste blanche ») n'a ni opération au contrat ni source de données.

**Pourquoi.** Une file de revue exige de porter le verdict sur `mt.routed` puis `mt.outcome` jusqu'au CDR,
et que l'API Admin lise ClickHouse. L'écran des sender IDs n'avait besoin que d'un compteur, et ce prix
n'était justifié par aucun usage mesuré.

**Ce qu'il en coûte si on ne la paie jamais.** Un opérateur voit qu'un expéditeur est signalé, mais pas
quels messages l'ont été. Au-delà de 24 h, il ne voit même plus le compteur. Il ne lui reste que l'alerte,
qui ne nomme pas l'expéditeur.

**À quoi on reconnaîtra qu'il faut la payer.** Le premier litige client sur un blocage anti-spam, ou le
premier écran du tableau de bord qui doit lier le compteur à une liste.
