# step-285b — billing-svc groupe ses écritures durables : un commit pour N mouvements

> **Jalon :** M12 · **Statut :** À FAIRE
> **Dépend de :** step-285 · **Bloque :** step-286, step-287
> Paie `debts/debit-par-client-borne-par-la-latence-de-la-reserve.md` ; décision humaine du 04/10/2026 ;
> lettre faute d'unité libre avant step-286, qui touche la même porte.

## Pourquoi

Après step-285, un client plafonne à ~225 messages/s : une voie réserve un message après l'autre, et une
réserve coûte 41 ms, dont 33 ms d'écriture durable. Les sondes du 04/10/2026 sur le VPS disent où ça passe.

**`pgbench`, transaction de réserve seule (réclamation + delta + grand livre), sur une copie des tables :**

| clients | avec FK | sans FK |
|---|---|---|
| 1 | 4,3 ms · 234 tps | 3,0 ms · 334 tps |
| 12 | 8,0 ms · 1 500 tps | 8,4 ms · 1 434 tps |
| 48 | 25 ms · 1 909 tps | 24 ms · 1 979 tps |

Les clés étrangères ne coûtent rien sous concurrence. Postgres plafonne vers 2 000 transactions/s sur
cet hôte ; 8 000 messages/s en demandent 16 000 (une réserve, une capture). **Une transaction par message
n'atteint pas la cible, quelle que soit la concurrence en amont.**

**A/B du pool de billing-svc, même backlog mono-client :** 10 connexions → 273 réserves/s, durable 26,9 ms ;
32 connexions → 305 réserves/s, durable 20,6 ms. L'attente du pool n'explique qu'un quart de l'écart
avec `pgbench`. Le reste : un hôte saturé (file d'exécution 40–60 pour 8 vCPU, 88 % de CPU dont 39 % en
noyau, 92 k changements de contexte/s, iowait 1 %) et, côté Postgres, 54 % des échantillons sur le CPU et
36 % en `WALWrite`/`WalSync` — le prix du commit. Plus de réserves en vol par voie allongerait la file ;
le seul levier est le **coût par message** : moins de commits, d'allers-retours et d'appels système.

## Design arrêté

Validé par l'humain le 04/10/2026, après les sondes.

1. **Un `Batcher` dans `internal/storage/postgres`**, qui enveloppe `BillingRepo` et satisfait
   `billing.LedgerStore`. Ni `Accountant`, ni le gRPC, ni le routeur, ni connector-pool ne changent :
   billing-svc l'insère dans son câblage, et le ferme comme ses autres composants.
2. **Ce qui passe par le lot :** une entrée avec `MessageID` **et** `BalanceAfter` — la réserve, la capture
   et la libération d'un cache chaud, soit le chemin chaud. Le reste (recharges et ajustements sans
   `message_id`, libération à froid, réparation d'une réserve rejouée) garde le chemin unitaire : son
   `balance_after` se lit dans la base, et dans un lot il dépendrait des autres entrées.
3. **Regroupement sans minuterie.** Une goroutine d'écriture prend tout ce qui attend (jusqu'à 256
   entrées), l'écrit en une transaction, recommence. À vide, un lot vaut un et n'ajoute aucune latence ;
   sous charge, il grossit de lui-même pendant que le précédent s'écrit. Aucun délai, aucun réglage hors
   la borne. Une seule goroutine (`ponytail:` — plusieurs si la mesure la montre limitante).
4. **Une transaction par lot, une requête par table**, en tableaux `unnest` :
   - réclamation `INSERT INTO billing_idempotency … ON CONFLICT DO NOTHING RETURNING message_id,
     entry_type` : appliquée ou non, entrée par entrée ;
   - `balance_deltas` et `billing_ledger` en INSERT multi-lignes, pour les seules entrées réclamées ;
   - le solde rendu est `BalanceAfter` pour une entrée appliquée ; pour une entrée non réclamée (un
     rejeu), le solde courant de son propriétaire, lu dans la même transaction, comme aujourd'hui.
   Deux entrées du même lot sur le même `(message_id, entry_type)` : la première s'applique, la seconde
   rend `applied=false` — ce que rendent aujourd'hui deux appels concurrents (invariant c).
5. **Pannes.** Une transaction de lot qui échoue rejoue chacune de ses entrées seule par le chemin
   unitaire : une entrée empoisonnée (clé étrangère violée) ne fait pas échouer les autres, et l'erreur
   rendue à chacun est la sienne. Un appelant dont le contexte expire rend une erreur pendant que son
   entrée peut encore être commitée : c'est l'ambiguïté d'un accusé de commit perdu, déjà traitée
   (la réserve relit `ReserveEntry`, la capture et la libération sont idempotentes).
   `reserveDurableTimeout` (4 s) borne toujours l'attente, file comprise.
6. **Hors périmètre, fiché :** les trois lectures préalables de la capture et son verrou terminal restent
   unitaires — les regrouper touche à l'exclusion mutuelle capture/libération, un autre design. Le commit
   et le WAL dominent ; on les attaque d'abord.
7. **Instrument :** histogramme `billing_durable_batch_size` (taille des lots écrits), qui prouve le
   regroupement en test et sert à l'exploitation.

**Amendements pendant le TDD (04/10/2026) :**
- **Un COMMIT qui échoue ne se rejoue pas entrée par entrée** (point 5) : il a pu aboutir, et un rejeu
  unitaire rendrait `applied=false` à un mouvement appliqué. La réserve rembourserait alors le cache, et
  ce crédit fantôme pourrait être revendu. Chaque appelant reçoit l'erreur, comme un accusé de commit perdu
  aujourd'hui : la réserve relit `ReserveEntry`. Seul un échec *avant* le COMMIT déclenche le repli.
- **Pas de déduplication dans le lot** (point 4) : la mutation qui la retirait a survécu. Deux copies du
  même mouvement se lisent toutes deux réclamées, l'index unique du grand livre refuse la seconde (une
  transaction, un seul `now()`), et le lot passe par le repli unitaire. Deux filtres qui se recouvrent :
  on garde celui que la base impose.
- **`COPY` plutôt qu'INSERT multi-lignes** (point 4) : les colonnes nullables du grand livre
  (`account_id`, `reference`) ne passent pas par des tableaux `unnest` typés, et `COPY` est le chemin
  d'insertion en masse de Postgres. La réclamation reste un INSERT, puisqu'il lui faut `ON CONFLICT … RETURNING`.

**Amendement de revue (04/10/2026), arbitré par Fable — remplace la fin du point 5.** La revue a montré
qu'un mouvement pouvait survivre à son appelant : rendu `ctx.Err()` par `RecordDurable`, il commitait
jusqu'à ~8 s plus tard, verrou terminal déjà relâché. Une libération qui expire et une capture qui passe
pouvaient alors commiter toutes les deux : un message livré gratuit. Règle :
le contexte d'un lot est borné par `batchWriteTimeout` **et par la plus proche échéance de ses membres** ;
une entrée déjà finie à la collecte est rendue en erreur sans être écrite ; une fois remise, une entrée
attend la réponse de son lot, que son échéance borne ; un lot qui échoue avant le COMMIT rejoue chaque
entrée seule **sous son propre contexte**. Au retour de `RecordDurable`, le sort du mouvement est scellé :
seul un COMMIT en vol reste ambigu, comme sur le chemin unitaire. Contrat : `RecordDurable` rend la main à
l'échéance de l'appelant, pas à son annulation. Plafond connu : une entrée empoisonnée dans un lot de 256
coûte 256 transactions unitaires, bornées par l'échéance de chacune (cas rare : client supprimé).

## Definition of Done

- [ ] N écritures concurrentes : N lignes au grand livre, sa somme égale le solde
- [ ] un doublon dans un même lot ne s'applique qu'une fois (invariant c)
- [ ] une entrée empoisonnée dans un lot n'empêche pas les autres
- [ ] sous concurrence, des lots de plus d'une entrée sont écrits (l'histogramme le montre)
- [ ] chaque test ci-dessus tombe sous mutation
- [ ] VPS, même protocole que l'A/B (backlog mono-client, pool à 10) : réserves/s et durée de l'étape
      durable comparées à 273/s et 26,9 ms ; part `WALWrite`/`WalSync` relevée
- [ ] fiche de dette des lectures préalables de la capture
