# step-401 — Le MO n'est compté nulle part : câbler `RecordMO` dans mo-dlr-router-svc

> **Jalon :** M9, défaut de câblage découvert par step-400 · **Statut :** À FAIRE
> **Dépend de :** step-143 · **Bloque :** l'alerting métier du tableau de bord, avec step-400 ; step-410

## Le constat
`RecordMO` (`internal/billing/billing.go`, gRPC `internal/billing/grpcserver.go:144`) n'a aucun appelant
en production : `mo-dlr-router-svc` n'a pas de client de facturation (`grep pb.NewBillingClient cmd/` :
router-svc et connector-pool-svc seulement). La spec l'exige :

> `mo-dlr-router-svc` : … → **remise immédiate** (jamais conditionnée à un solde) → **comptage MO** =
> `segment_count × credits_per_segment_mo` sur le solde MO (§6.9 ; aucun effet sur le MT) → CDR écrit.
> — `specification-technique-passerelle-sms.md` §544 (et §531, guide §94)

step-143 a livré le compteur sans son appelant. Conséquences : facturation activée, aucun MO n'est
compté, et ni le WS `mo_floor_reached` ni le topic `billing.events` (step-400) n'émettent jamais.

## Arbitrages à trancher (dans la fiche, avant tout code)
- **Où dans la voie MO** : après la remise, comme le dit la spec ; un STOP n'est **jamais** facturé (§544).
- **Panne de billing-svc** : le MO est toujours remis (§835) ; que devient le comptage — perdu, ou
  rejoué (le message reste-t-il non commité) ? La facturation est fail-open par défaut (§73).
- **`segment_count`** : un MO concaténé est réassemblé avant remise (§808) ; d'où vient le compte ?
- **Propriétaire** : résolution `balance_scope` (`customer` / `smpp_account`) à partir du compte résolu.
- **`credits_per_segment_mo_json`** : lu où, et par qui (config-sync du routeur MO, ou billing-svc) ?

## Hors périmètre
Le transport durable des transitions → step-400.
