# Suspendre un client repasse ses comptes fermés en « suspendu »

> **Statut :** PAYÉE (02/10/2026, PR #245) · **Nature :** produit
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

**Payé aussi le 02/10/2026 : `closed` est définitif**, pour un compte comme pour un client (décision de
l'exploitant). La fonction `control_plane.closed_is_final()` et ses triggers `smpp_accounts_closed_is_final`
et `customers_closed_is_final` (schéma et `migrations/0025_closed_is_final.*`) refusent toute sortie de
`closed`, quel que soit l'écrivain — PATCH Admin, suspension, script — en `check_violation`, donc 422 ; le
contrat Admin le dit sur `update-smpp-account`, `suspend-smpp-account`, `update-customer` et
`suspend-customer` (6.10.1). Prouvé par `TestClosedAccountIsFinal` et `TestClosedCustomerIsFinal`, rouges lus
avant les triggers.
