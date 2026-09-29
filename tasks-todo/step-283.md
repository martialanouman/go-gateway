# step-283 — Le débit se refuse avant l'ACK, jamais après

> **Jalon :** M12 · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-287
> Décision humaine du 29/09/2026 ; unité faute de multiple de dix libre.

## Pourquoi

Un message acquitté peut être perdu pour une simple question de débit.
- Le contrôle de débit n'a lieu qu'au routeur, **après** l'ACK durable. `Enforcer.Check` vérifie tour à tour
  le compte, la route et le connecteur (`internal/pipeline/ratelimit/enforcer.go:96`), et rend
  `ErrRateLimited`.
- Le routeur traite toute erreur codée comme un rejet définitif (`internal/router/router.go:182`) : CDR
  `rejected`, offset commité. Le client avait reçu un 200 ou un `ESME_ROK`.
- Le seau du **connecteur** est partagé : un gros client qui le vide fait rejeter les messages des petits
  clients routés sur ce connecteur.
- La spec §6.4 promet l'inverse : « à l'approche du plafond, `connector-pool-svc` ralentit la consommation
  Kafka ; les messages restent durablement en file ».
- Aucun contrôle de débit n'existe sur la soumission : REST et SMPP acquittent tout ce qui arrive.

## Décision humaine

1. **Admission à l'ingestion, par compte.** Le seau `rate_limits` du compte s'applique **avant l'ACK** :
   `429` en REST, `ESME_RTHROTTLED` en SMPP. Un client ne fait plus entrer dans la file partagée plus que son
   contrat.
2. **Le routeur ne rejette plus pour débit.** Le plafond du connecteur devient de la backpressure dans le
   pool : on ralentit, on ne jette pas.

## À arbitrer (spec → Fable → humain)

- **Où vit le plafond du connecteur** une fois sorti du routeur. `Enforcer.AllowConnector` existe déjà
  pour le drainer (step-126) : le pool attend-il le jeton avant chaque `submit_sm` ? Et que devient la fenêtre
  SMPP pendant cette attente ?
- **Le seau `route`** (`entity_type = 'route'`) : il se paie au routeur ou au pool, mais ne peut plus rejeter.
  Sinon, le supprimer.
- **Le compte au routeur** : il est retiré, sinon le même segment serait décompté deux fois.
- **Le coût d'admission** : une soumission REST ou SMPP de N segments coûte N jetons. L'ingestion ne
  segmente pas aujourd'hui. Faut-il estimer le nombre de segments à l'entrée, ou compter un jeton par
  message ? Chiffrer l'écart sur un trafic UCS-2.
- **La politique de panne à l'ingestion** : le plafond local par pod (`localCeiling`) multiplié par le
  nombre de pods REST et SMPP. Écrire la borne réelle.
- **`MaxPerDay`**, chargé mais jamais appliqué : l'admission le rend-elle applicable, ou reste-t-il une
  dette ?
- **Le reroute et la file de parking** passent déjà par `AllowConnector` : vérifier qu'aucun chemin ne
  reste où `ErrRateLimited` devient un rejet.

## Definition of Done

- [ ] un test prouve qu'un message au-delà du débit du compte est aujourd'hui **rejeté après l'ACK** —
      rouge lu sur le code actuel
- [ ] un test prouve qu'un connecteur saturé par un compte ne produit **aucun** CDR `rejected` pour un
      autre compte — rouge lu sur le code actuel
- [ ] `429` REST et `ESME_RTHROTTLED` SMPP au-delà du débit du compte, sans écriture sur `mt.inbound`
- [ ] spec §6.4 et guide alignés ; le guide §4.1 corrigé au passage (`mt.routed` est clé par
      `message_id`, pas par `(connector_id, shard)`)
- [ ] l'ordre du pipeline de `CLAUDE.md` et de la spec §5.1 dit que le débit se contrôle avant l'ACK
- [ ] les quatre invariants verts
