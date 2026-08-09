# step-201b — Campagne NFR pleine échelle sur environnement représentatif

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-201, **step-201c**, **step-201d**, **step-201e**, **step-201f**, step-207 · **Bloque :** step-208

## But
Rendre le **verdict NFR** que step-201 ne pouvait pas rendre : débit soutenu **8 000 SMS/s**, pic
**15 000**, ingestion p99 < 250 ms, bout-en-bout p99 < 2 s, disjoncteur fermé (spec §1.2, plan §16).

## Pourquoi cette fiche existe
Le débit soutenu de la spec est **traversant**, pas un débit d'acceptation (step-201 `D1`). Le tenir
suppose ~10 400 `submit_sm/s` absorbés en sortie — soit **≥ 52 binds** sur un simulateur qui sérialise
le service par bind — pendant que tournent 9 services, 4 magasins et l'injecteur. La spec §2.5
dimensionne la cible à 8–16 vCPU de workers *dédiés* plus un Kafka répliqué 3. Aucune machine de
développement ne porte ça : un « 8 000/s tenu » mesuré là ne validerait rien, et un échec ne
condamnerait rien.

step-201 a donc livré les **instruments** et prouvé l'état stationnaire à la borne basse du modèle
par-worker (§2.5). Ici, seule **l'échelle** change.

> **Correction (08/08/2026, après step-201d).** « Les instruments sont réutilisés tels quels » n'est plus
> vrai. step-201d les a poussés jusqu'à leur limite : à 4 800 msg/s le harnais ne sait plus **attribuer**
> un plafond — l'injecteur décroche de 17,3 %, les étages se disputent l'hôte, et la comptabilité CPU ne
> voit que le processus Go quand le suspect probable est un conteneur. Ce qui manque est listé et
> planifié en **step-201e**, qui bloque cette fiche.

> **Mise à jour (09/08/2026, step-201e livrée).** Le harnais sait désormais attribuer, et il l'a fait :
> le plafond de 4 800 msg/s était la **co-résidence**, ni le routeur (×4,2 une fois isolé) ni le broker
> (131 µs de latence de service pour 0,56 cœur). Reste **un seul étage jamais mesuré seul** — le pool de
> connecteurs, qui borne aujourd'hui le bout-en-bout à 2 400/s contre les 10 400 `submit_sm/s` de la
> cible. C'est **step-201f**, et elle bloque cette fiche pour la même raison que step-201e la bloquait :
> une campagne pleine échelle qui démarre sans savoir à qui appartient le plafond mesurera l'hôte.

## Prérequis logiciel : step-201c
Le run de référence de step-201 a mesuré un plafond de sortie de **192–330 `submit_sm/s`** dû à quatre
allers-retours ClickHouse par message dans le `connector-pool-svc`. Mesurer à pleine échelle avant de
l'avoir levé mesurerait ce goulot, pas la passerelle.

## Prérequis matériel (à provisionner — ce n'est pas du code)
- Environnement représentatif : workers dédiés, Kafka **répliqué 3**, ClickHouse et Postgres séparés
  des workers, simulateur SMSC sur sa propre machine (sinon il concourt pour le CPU qu'il mesure).
- Dimensionné d'après la spec §2.5 et les valeurs de leviers retenues en step-201.
- La dépendance à **step-207** est structurelle : les manifests Kubernetes sont ce qui rend cet
  environnement instanciable de façon reproductible.

## Périmètre
- Provisionner l'environnement et y appliquer les leviers de step-201 (`D5`) + le provisionneur de
  topics (`D7`).
- Établir le **plafond du pair à cette échelle** avec l'injecteur de step-201 (`D3`) — il ne se déduit
  pas de la mesure locale : le simulateur y tourne sur une autre machine, avec un autre nombre de binds.
- Runs `sustained` (8 000/s) et `peak` (15 000/s) du harnais, en **état stationnaire** : sortie =
  acceptation, lag consumer plat.
- Mesurer la latence bout-en-bout par corrélation `message_id` (step-201 `D4`).
- Consigner les valeurs de leviers retenues, la courbe débit-vs-ressources et le goulot identifié.

## Points d'implémentation clés
- **Le plafond du pair d'abord, le tuning ensuite.** Un run de référence au niveau du plafond du
  simulateur ne prouve rien de la passerelle. Si le plafond reste sous 10 400 `submit_sm/s` malgré le
  balayage de binds, c'est le simulateur qu'il faut traiter (sharding des goroutines, cf. sa propre
  spec §250) — pas la passerelle qu'il faut régler contre une contrainte artificielle.
- **Mesurer aussi le chemin `Idempotency-Key`** (`IDEMPOTENCY=on`, step-201 `D10`) : les NFR déclarés
  tenus sur le seul cas favorable ne valent pas.
- Dimensionner le `maxmemory` du Redis cible : un run `peak` ajoute ~900 000 clés d'idempotence à 24 h
  de TTL (~150 Mo), cumulables sur la fenêtre (step-201 `D12`).
- Aucun réglage ne doit affaiblir un invariant (idempotence, ordre, non-fuite) ni toucher aux six
  frontières de contrat listées en step-201 `D6`.

## Tests
- Plafond du pair mesuré et consigné **à cette échelle**, et les runs de référence se situent en dessous.
- `sustained` : 8 000 SMS/s traversants soutenus ≥ 10 min, lag plat, p99 ingestion < 250 ms,
  bout-en-bout p99 < 2 s, disjoncteur fermé.
- `peak` : 15 000 SMS/s tenus sur la durée de pic, dégradation conforme aux politiques documentées.
- Les 4 invariants (a/b/c/d) restent verts sous charge.

## Definition of Done
- [ ] plafond du pair à l'échelle mesuré et consigné · runs de référence en dessous
- [ ] débit soutenu **8 000 SMS/s traversants** tenu, budgets de latence respectés (disjoncteur fermé)
- [ ] pic **15 000 SMS/s** tenu ou dégradation conforme aux politiques documentées
- [ ] valeurs de leviers retenues consignées et reportées dans les manifests de step-207
- [ ] chemin `Idempotency-Key` mesuré, pas seulement le chemin nominal
- [ ] si un NFR n'est pas tenu : consigné **nommément** comme non tenu, avec le goulot identifié —
      jamais coché par approximation

## Hors périmètre
Chaos → step-202/203. Sécurité → step-204+. Manifests → step-207 (prérequis, pas livrable d'ici).
