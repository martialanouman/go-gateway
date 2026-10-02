# Suspendre un client repasse ses comptes fermés en « suspendu »

> **Statut :** PARTIELLEMENT PAYÉE (02/10/2026) — la cascade ; reste la transition au PATCH · **Nature :** produit
> **Née de :** remontée de l'exploitant (02/10/2026), vérifiée dans le code · **Portée par :** —

**Ce qu'on a fait à la place.** La cascade de suspension d'un client écrase le statut de **tous** ses comptes,
fermés compris : `SuspendCustomerAccounts` fait `UPDATE … SET status = 'suspended' WHERE customer_id = …`
sans exclure `closed` (`internal/storage/postgres/queries/customers.sql:65`, appelée par
`CustomerRepo.Suspend`, `internal/storage/postgres/customers.go:131`). Un compte fermé devient suspendu,
donc indiscernable d'un compte qu'on peut réactiver.

**Pourquoi.** Aucune raison écrite : la spec ne dit que « suspending a customer suspends all its smpp_accounts »
(`docs/specification-technique-passerelle-sms.md:139`), et le test de la cascade
(`internal/storage/postgres/customers_integration_test.go:88`) n'amorce qu'un compte actif. Le schéma, lui,
définit le statut effectif comme `min(customer, this)` (`db/schema_passerelle_sms.sql:321`) : `closed` est le
plus bas, une suspension du client ne devrait pas le relever.

**Ce qu'il en coûte.** La fermeture d'un compte est effacée sans trace par une suspension du client : l'API
Admin et le tableau de bord le montrent « suspendu », et un opérateur le réactive en croyant lever une
suspension. À noter : le PATCH d'un compte accepte déjà n'importe quelle transition, `closed → active`
compris (`internal/storage/postgres/queries/accounts.sql:38`) — `closed` n'est terminal nulle part ; la
cascade en supprime surtout la mémoire.

**Payé le 02/10/2026.** La cascade exclut `closed` (`AND status <> 'closed'`), prouvé par
`TestSuspendCustomerLeavesClosedAccountsClosed`, rouge lu sur l'ancienne requête.

**Reste ouvert : `closed` est-il terminal ?** Le PATCH d'un compte accepte toujours `closed → active`
(`internal/storage/postgres/queries/accounts.sql:38`). C'est une décision produit, pas un oubli de code. À
trancher avant le go-live : si `closed` est définitif, le PATCH doit refuser d'en sortir (et le contrat
Admin le dire).
