# step-287d — La capture quitte le chemin chaud du pool

> **Jalon :** M12 · **Statut :** EN COURS
> **Dépend de :** step-287c · **Bloque :** step-287 (reprise de la campagne)
> Née de la campagne step-287 du 07/10/2026, demande humaine du même jour ; unité faute de multiple de dix
> libre.

## Pourquoi
Le chronomètre de step-287c a été relevé sur le VPS de test le 07/10/2026 (image `8a2cc4b`, écoulement d'un
backlog sans ingestion). Le pool passe **172,7 ms par message dans `capture`**, sur environ 200 ms, contre
7,7 ms pour le `submit_sm`. Environ 80 % des captures échouent : 217 156 échecs pour 270 342 envois. Elles
expirent à `BILLING_SETTLE_TIMEOUT` (200 ms), passent en fail-open, et le reaper les rattrape au rythme
d'environ une par seconde. Le pool envoie 195 `submit_sm`/s alors que son SMSC répond en 5 ms.

## Design arrêté (07/10/2026)
L'arbitrage a été rendu par Fable, qui s'appuie sur ADR-0012 §2 (« la seule chose fail-closed après le
`submit_sm` devient un produce ») et sur le patron d'ADR-0023 (un groupe dédié sur `mt.outcome`).

- **Le pool ne fait plus d'appel de facturation après le `submit_sm_resp`.** Un groupe
  `billing-svc-settle` dans billing-svc lit `mt.outcome` et décide par `decideFromStatus` :
  - `enroute` déclenche une capture ;
  - `failed` déclenche une libération ;
  - tout le reste est ignoré.

  Il passe par le même `ExternalBiller` que le RPC, donc par `Accountant.Capture`/`Release`, inchangés : le
  verrou terminal et l'idempotence par `(message_id, entry_type)` sont conservés, et l'invariant c tient.
  Il travaille en concurrence bornée sur le lot de poll. Un échec de facturation fait échouer le record, qui
  est rejoué sur les offsets de ce seul groupe, jamais sur ceux de `mt.routed`. Une panne de facturation ne
  peut donc plus renvoyer de SMS : c'est désormais une propriété de la structure.
- **`mt.outcome` gagne `billable` et `owner_type`.** C'est un ajout sur un topic interne. Un record sans
  `billable` est ignoré, et le reaper le rattrape.
- **Les champs `billed`/`credits_charged` du CDR ne sont plus remplis par le pool.** Ils ne faisaient déjà
  pas autorité : la ligne DLR (rang 40, `Billed: false`, `internal/modlrrouter/modlrrouter.go`) les
  écrase pour tout message livré, via `argMax(billed, version)`. Le grand livre est la seule autorité. Les
  colonnes restent, sans migration. Un CDR qui dirait vrai sur la facturation est une autre affaire :
  `debts/`.
- **Inchangés :**
  - les chemins sans envoi (`cancelBeforeDispatch`, chaîne de repli épuisée) gardent le `Release` gRPC
    fail-open : ils ne passent pas par `mt.outcome` ;
  - le reaper reste le filet ;
  - le proto ne change pas.
- **Pas de relèvement de `BILLING_SETTLE_TIMEOUT`.** Le rallonger ralentirait chaque message pour sauver un
  champ CDR qui est faux de toute façon.
- **Deux PR, déployées dans l'ordre :**
  - **PR1, billing-svc** : `mt.outcome` gagne ses deux champs (le pool les publie et capture toujours),
    plus le consommateur `billing-svc-settle`. Le double règlement est idempotent pendant la transition.
  - **PR2, pool et documents** : `settleOutcome` ne capture ni ne libère plus. ADR-0024, spec §4 et §6.9
    point 3, et la fiche de dette du CDR.

## Definition of Done
- [ ] PR1 : un `enroute` est capturé, un `failed` est libéré, un `billable=false` ne coûte aucun appel ; un
      échec de facturation fait échouer le record ; le double règlement avec le pool reste une seule
      entrée (intégration)
- [ ] PR2 : `settleOutcome` ne fait plus d'appel de facturation ; ADR-0024 ; spec amendée
- [ ] sur le VPS : le temps de `capture` disparaît du pool, et la traversée est relevée dans le journal de
      step-287
