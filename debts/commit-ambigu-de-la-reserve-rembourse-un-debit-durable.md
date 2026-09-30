# Un commit ambigu de la réserve rembourse le cache d'un débit qui devient durable

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-284 (revue) · **Portée par :** —

**Ce qu'on a fait à la place.** Quand l'écriture durable d'une réserve échoue, `Reserve` cherche l'entrée dans
le grand livre (`internal/billing/billing.go:297`) et, faute de la trouver, rembourse le cache puis refuse.
Si l'échec est une échéance atteinte **pendant** le `COMMIT`, le serveur peut valider après coup : la
recherche, sur une autre connexion, ne voit pas encore la réserve, le cache est remboursé, puis le débit
durable atterrit.

**Pourquoi.** Le risque préexiste à step-284, qui ne l'amplifie pas : l'échéance de l'appelant
(`RESERVE_TIMEOUT`, 200 ms par défaut) est plus courte que la borne de 4 s ajoutée (`reserveDurableTimeout`).
La revue l'a relevé ; le corriger demande de rendre le commit non ambigu, hors du sujet de step-284.

**Ce qu'il en coûte.** Le cache surestime le solde de ces crédits jusqu'à la réhydratation suivante (≤ 10 min) :
un dépassement de la taille de la réserve. La réserve reste sans capture ni libération jusqu'au reaper, qui
la tranche contre le CDR (le message, refusé, n'est jamais parti : il la libère).

**À quoi on reconnaîtra qu'il faut la payer.** Des réserves orphelines libérées par le reaper sans CDR
(`reaper_reaped_total{action="release"}`) qui suivent les pics de latence Postgres.
