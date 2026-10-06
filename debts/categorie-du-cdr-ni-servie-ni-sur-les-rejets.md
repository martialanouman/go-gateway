# Catégorie du CDR : ni servie, ni sur les rejets

> **Statut :** OUVERTE · **Nature :** produit
> **Née de :** step-292 (PR1) · **Portée par :** —

**Ce qu'on a fait à la place.** `cdr.traffic_category` et `cdr.priority` sont écrits par chaque ligne
postérieure à l'autorisation du sender ID, et lus par l'agrégat (`internal/storage/clickhouse/cdr.go`,
`CDRRow.TrafficCategory`). Mais aucune réponse d'API ne les sert : ni `messageFromRow`
(`internal/restapi/messages.go:282`), ni `toMessageSummaryDTO` (`internal/adminapi/messages_search.go:256`),
et aucun filtre de la recherche Admin ne les accepte. La ligne `rejected` du routeur
(`internal/router/router.go:310`) reste à `''`/0, y compris pour un rejet postérieur à l'autorisation
(opt-out, anti-spam, `no_route`, crédit).

**Pourquoi.** ADR-0020 (action item 2) ne demande aucun champ de contrat CDR. Les servir est un bump
MINEUR, déclaré avant l'implémentation, que l'écran qui les lira doit décider. Porter la catégorie sur la
ligne de rejet obligerait `Pipeline.Process` à rendre un gabarit partiel avec son erreur, sur six chemins
de retour.

**Ce qu'il en coûte.** Le CDR Explorer ne peut ni filtrer ni ventiler par catégorie (ADR-0020, action
item 8). Les rejets ne se comptent pas par catégorie, alors qu'un `category_mismatch` en `block` est
justement un rejet par catégorie.

**À quoi on reconnaîtra qu'il faut la payer.** Le tableau de bord ouvre le filtre par catégorie du CDR
Explorer (§6.4), ou le trafic temps réel la ventilation par catégorie (§6.3).
