# La rétention du corps par client n'est pas appliquée

> **Statut :** OUVERTE · **Nature :** produit
> **Née de :** step-165 (aveu dans un commentaire SQL), fichée en step-370 · **Portée par :** —

**Ce qu'on a fait à la place.** Le corps d'un message expire par une TTL de colonne ClickHouse **unique
pour toute la plateforme** : 30 jours (`migrations/clickhouse/0003_cdr_content_ttl.up.sql:9`).
`customers.content_retention_days` est accepté par `update-customer` et
`update-customer-content-policy`, stocké, relu — et lu par aucun code qui purge. Les descriptions du
contrat Admin le disent depuis step-370.

**Pourquoi.** Une TTL ClickHouse ne lit que les colonnes de sa propre ligne : appliquer une durée par
client exige de matérialiser la rétention sur chaque ligne CDR à l'écriture (colonne neuve, projection
du routeur, migration ClickHouse sans réécriture de l'existant). step-165 l'a noté en commentaire et a
livré le défaut plateforme.

**Ce qu'il en coûte.** Dans les deux sens, en silence : un client à 7 jours garde ses corps 30 jours
(exposition PII plus longue que l'accord de traitement ne le dit) ; un client à 90 jours les perd au
30ᵉ (le tableau de bord montre un contenu qui a disparu).

**À quoi on reconnaîtra qu'il faut la payer.** Un accord de traitement de données qui fixe une durée
de conservation du corps différente de 30 jours, ou une question d'audit RGPD sur la durée effective.
