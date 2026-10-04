# step-285c — Plusieurs réserves en vol par voie du routeur, publiées dans l'ordre

> **Jalon :** M12 · **Statut :** EN COURS (code livré, mesure VPS à faire après merge)
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

- [x] design arrêté et commité, arbitré (spec → Fable → humain)
- [x] rien n'est publié au-dessus du premier échec d'une voie, prouvé par un test qui tombe sous mutation
- [x] la publication suit l'ordre des offsets d'une voie, même quand les réserves finissent dans le désordre
- [ ] VPS, même protocole que step-285b (backlog mono-client) : réserves/s, taille moyenne des lots,
      CPU de Postgres, comparés à 294/s, 8,4 et 622 m

## Design arrêté

Arbitré par Fable le 04/10/2026 (spec muette sur le parallélisme intra-voie et le rejeu de l'anti-spam).

1. **Fenêtre par voie.** Dans `handleBatch`, chaque voie lance `Pipeline.Process` (étapes 1-8, réserve
   comprise) pour au plus `laneWindow` messages devant le message courant, et consomme les résultats
   **dans l'ordre des offsets**. La suite de `handle` (CDR de rejet, ou produce de chaque segment) reste
   séquentielle dans cette phase ordonnée. Le CDR de rejet y reste aussi : `CDR.Insert` peut échouer, et
   c'est un échec de voie.
2. **Premier échec.** La voie cesse de lancer de nouveaux Process, marque `errLaneHalted` sur tout ce qui
   est au-dessus (même un Process réussi : rien n'est publié ni écrit au-dessus d'un échec), puis attend
   les Process en vol **sans annuler leur ctx**. Annuler une réserve en vol la ferait passer par le chemin
   ambigu de billing-svc ; laissée finir, elle est rejouée en « held » sans coût. `handleBatch` ne laisse
   aucune goroutine après son retour.
   - **L'arrêt du service, lui, annule comme avant** (revue, arbitré par Fable) : jusqu'à 8 réserves par
     voie au lieu d'une. L'idempotence par `message_id` couvre le rejeu au redémarrage.
     `context.WithoutCancel` est écarté : il se propagerait à la publication, et une voie finirait tout
     son lot à l'arrêt, avec un broker en panne qui peut dépasser `DRAIN_BUDGET`.
   - Le span `router.process` d'un record arrêté se termine en erreur (`errLaneHalted`). C'est vrai, et
     un parent non marqué serait jeté par l'échantillonnage biaisé erreur. Deux racines pour un message
     rejoué, c'est ce qu'un rejeu montre déjà.
3. **`laneWindow = 8`, constante.**
   - 12 partitions × 8 = 96 réserves en vol dans tout le cluster (une partition n'a qu'un consommateur),
     sous le plafond de lot de 256 de step-285b.
   - Plafond théorique ≈ 3 000 réserves/s.
   - `ponytail:` le nombre de réserves en vol suit `TOPIC_PARTITIONS × 8` sans borne. Passer à un réglage
     si la campagne de mesure veut balayer N, ou au-delà de 32 partitions.
   - Implémentation : un chan de résultat par indice ; le record `pos + 8` n'est lancé qu'après la
     publication de `pos`. Pas de sémaphore ni d'`errgroup`.
4. **L'anti-spam devient rejouable.** Aujourd'hui, un message rejoué retrouve sa propre empreinte de
   doublon : avec une règle `block`, il est rejeté comme doublon de lui-même, et la vélocité le compte
   deux fois. Le défaut préexiste (échec de produce après l'anti-spam), mais la fenêtre le multiplie
   (jusqu'à N-1 messages rejoués au-dessus d'un échec). Corrigé à la racine :
   - `Evaluate`, `Seen` et `Hit` reçoivent le `message_id` ;
   - `Seen` fait `SET NX GET` avec le `message_id` en valeur, et ne voit un doublon que si la valeur
     existante est un **autre** `message_id` ;
   - `Hit` prend le `message_id` comme membre du ZSET (idempotent) ;
   - `Record` (MO) ne change pas.
5. **États non touchés.**
   - Le round-robin de `routing/snapshot.go` (atomique) : un rejeu peut choisir un autre connecteur, sans
     conséquence, puisque rien du premier passage n'a été publié.
   - Sender ID, opt-out, E.164, encodage et segmentation sont purs ou en lecture seule.
   - La réserve est idempotente par `message_id`.
6. **Prochain plafond.** Après la fenêtre, c'est le produce synchrone de la phase ordonnée qui bornera la
   voie. Sa latence se mesure dans le même run, car `pipeline_duration_seconds` l'exclut.
   - Le span `router.process` englobe désormais pipeline, attente de tour et publication. L'attente
     apparaît comme un trou entre `pipeline.credit` et le produce.
   - `pipeline_duration_seconds` inclut la contention de 8 Process concurrents (du vrai temps de
     pipeline), et observe deux fois un record arrêté puis rejoué : après une faute d'infra, et à chaque arrêt du
     service (jusqu'à 8 records par voie).
7. **Mesure contre step-408.** Si step-408 (vidage de `DEFAULT`) est déployée avant la mesure, on remesure
   la référence sans step-285c juste avant, sans aucun vidage entre les deux runs. Sinon, 294/s reste la
   référence.

**Tests.**
- Un producteur factice échoue à l'offset k : aucun produce au-dessus de k, et `errLaneHalted` au-dessus.
- Un crédit factice finit dans l'ordre inverse : l'ordre de produce reste celui des offsets.
- Avec une règle de doublon `block`, le même `message_id` évalué deux fois n'est pas un doublon ; un autre
  `message_id` avec le même contenu l'est.
- La vélocité ne compte pas deux fois un rejeu.
