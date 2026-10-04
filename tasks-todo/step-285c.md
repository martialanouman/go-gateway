# step-285c — Plusieurs réserves en vol par voie du routeur, publiées dans l'ordre

> **Jalon :** M12 · **Statut :** À FAIRE
> **Dépend de :** step-285b · **Bloque :** step-286, step-287
> Porte `debts/debit-par-client-borne-par-la-latence-de-la-reserve.md` ; décision humaine du 04/10/2026 ;
> lettre faute d'unité libre avant step-286.

## Pourquoi

step-285b a payé le coût Postgres d'une réserve (un commit pour ~8 mouvements, CPU de Postgres ÷ 2, attente
du WAL de 36 % à 11 %), pas le plafond d'un client : ~300 réserves/s mesurées, contre 273 avant. Une voie
du routeur (une partition de `mt.inbound`) réserve un message après l'autre, de façon synchrone, avant de
le publier : le plafond reste voies ÷ latence d'une réserve (12 ÷ 32 ms). Ces 12 réserves en vol ne
forment que des lots de 8, et chacune attend le lot en cours puis le sien.

La piste écartée le 04/10 devient la bonne : plus de réserves en vol par voie. Elle coûtait une
transaction Postgres par réserve ajoutée ; depuis step-285b, elle grossit les lots au lieu de multiplier
les commits.

## Périmètre

- `internal/router/router.go`, `handleBatch` : dans une voie, une fenêtre de N messages traités
  concurremment (pipeline jusqu'à la réserve comprise), **publiés dans l'ordre des offsets**, la voie
  s'arrêtant au premier échec.
- La règle de sûreté de `handleBatch` doit survivre telle quelle : **rien n'est publié au-dessus d'un
  échec** (un enregistrement publié mais non commité est republié au rejeu, un doublon sur un combiné,
  ADR-0012). Une réserve faite pour un message au-dessus d'un échec n'est pas publiée ; elle est rejouée
  (la réserve est idempotente par `message_id`) ou laissée au reaper si le message ne revient pas.
- Ce qu'il faut trancher au design : la taille de la fenêtre (constante ou réglage), l'interaction avec les
  étapes du pipeline qui ont un état (anti-spam, compteurs), l'ordre des CDR de rejet, et la mesure.

## Definition of Done

- [ ] design arrêté et commité, arbitré (spec → Fable → humain)
- [ ] rien n'est publié au-dessus du premier échec d'une voie, prouvé par un test qui tombe sous mutation
- [ ] la publication suit l'ordre des offsets d'une voie, même quand les réserves finissent dans le désordre
- [ ] VPS, même protocole que step-285b (backlog mono-client) : réserves/s, taille moyenne des lots,
      CPU de Postgres, comparés à 294/s, 8,4 et 622 m
