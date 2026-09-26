# `PATCH { "champ": null }` est un no-op silencieux

> **Statut :** OUVERTE · **Nature :** produit
> **Née de :** revue M1 (`docs/plan-execution-passerelle.md:253`) · **Portée par :** —

Les `UPDATE` partiels passent par `COALESCE(narg, col)` : un champ à `nil` veut dire « ne change
pas ». Le tri-state (absent / `null` explicite / valeur) a été reporté, sans autre raison écrite que
le report lui-même. Le plan disait « y revenir si aucun jalon ultérieur ne le résout » — aucun ne
l'a fait, et M12 est le dernier.

**Ce qu'il en coûte.** Écrit : « impossible de remettre une valeur à `NULL` via l'Admin API ». Donc
impossible de **retirer** un `overdraft_limit` ou un `throughput_limit_per_sec` une fois posé. Un
plafond de découvert qu'on ne peut plus effacer est une dette de facturation déguisée en dette de
plan de contrôle. Le contrat OpenAPI marque pourtant ces champs `nullable`.

**À quoi on reconnaîtra qu'il faut la payer.** Le premier exploitant qui doit annuler un plafond
qu'il vient de poser.

**Elle s'étend à chaque surface neuve.** step-330 : `update-customer-group` déclare `description`
nullable au contrat et ne peut pas la vider, pour la même raison.
step-350 : `update-sender-rewrite-rule` ne peut vider ni `rewrite_to`, ni `reason`, ni les motifs.
Pour un motif, `.*` vaut NULL (pas `""`, qui ne correspond qu'à une adresse vide) ; pour les champs
propres à un type, les laisser en place est sans effet (seuls ceux que lit le type courant sont
évalués), et c'est ce qui rend un changement de type possible.
step-370 : `update-customer-content-policy` ne peut vider `content_retention_days` (même chemin
qu'`update-customer`) ; un `null` rend 200 et la valeur précédente.

Sources : `internal/controlplane/doc.go:15` ·
`internal/storage/postgres/queries/customer_groups.sql` (`UpdateCustomerGroup`) ·
`internal/storage/postgres/queries/sender_rewrite_rules.sql` (`UpdateSenderRewriteRule`) ·
`internal/adminapi/content_policy.go` (`updateCustomer`)
