# step-280 — Campagne NFR sur le VPS de test : outillage répétable et chiffres non représentatifs

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** FAIT (sans verdict, voir le journal)
> **Dépend de :** step-201c, step-201f, step-270b, step-270c, step-270d, step-275 · **Bloque :** step-409

> **Recadrage (28/09/2026), décision humaine.** L'ancienne step-280 (verdict NFR sur environnement
> représentatif) devient **step-409**, qui en garde tout l'historique. Ici : la même campagne, sur le
> VPS de test (8 vCPU, un nœud k3s, tout co-résident). Elle ne rend **aucun verdict** — un « 8 000/s
> tenu » mesuré là ne validerait rien, un échec ne condamnerait rien (step-409 § « Pourquoi »). Elle
> livre l'outillage que step-409 réutilisera et les chiffres de cette échelle, consignés comme tels.

## But
Monter la campagne de bout en bout sur l'environnement qui existe : semer un compte de charge, mesurer
le plafond du pair **dans le cluster**, lancer `sustained` et `peak` (avec et sans `Idempotency-Key`),
nommer le goulot. Chaque NFR sort **mesuré, verdict non rendu (→ step-409)**.

## Design arrêté

Arbitré par Fable le 28/09/2026 (six points), sans contradiction avec la spec.

1. **Injecteur dans le cluster.** Job k6 (`grafana/k6` épinglé par digest, `test/load/k6/messages.js`
   en ConfigMap) visant `http://rest-api-svc:8080` en direct. Pas Cloudflare : le budget est
   l'ingestion, pas l'Internet, et 8 000 req/s d'une IP déclenche ses protections. k6 porte une
   `limits.cpu` fixe pour que sa part de l'hôte soit connue.
2. **Plafond du pair dans le cluster.** Job `smsc-ceiling` sur `smsc-simulator:2775` et `:9000` (port
   ajouté au Service). Un `port-forward` mesurerait le tunnel. Image : même Dockerfile, `BINARY=smsc-ceiling`
   dans `deploy-test.yml` — zéro code Go.
3. **`e2e-budget` depuis le poste**, `port-forward` sur le port ops du pod pool, `-connector` du compte
   de charge. Deux scrapes, pas de débit : le tunnel n'y coûte rien.
4. **`test-env seed-load`**, sous-commande lancée par la campagne seule (pas à chaque déploiement : la
   clé API n'est lisible qu'à la création). Client `billing_enabled=true`, `postpaid`, `credit_limit_is_hard`
   faux (chemin de facturation de production, jamais bloquant) ; sender ID actif ; comptes REST + clé
   `api_key` tournée à chaque run (réactivée si révoquée) imprimée `API_KEY=…` ; connecteur au
   `bind_pool_size` retenu par le plafond ; route catch-all ; routes exactes à la part portée retenue.
   **Refuse de démarrer** si une règle anti-spam `duplicate`/`velocity` couvre le client : k6 répète un
   corps sur 10 000 destinations, le run serait faux en silence.
5. **Leviers : `run.sh apply` les patche**, valeurs versionnées dans le script, jamais un `kubectl scale` à la main :
   `rest-api-svc`, `router-svc`, `connector-pool-svc` à 2 réplicas par HPA min = max (avec des requests à
   50 m l'HPA sature dès la première seconde et ne mesure rien). `bind_pool_size` passe par l'Admin
   (`seed-load`), que le pool relit avant chaque connexion ; `CONNECTOR_BIND_POOL_SIZE` reste à 2.
   Redpanda (`--smp=1`) et Redis (aucun `maxmemory`) restent tels quels et sont consignés.
6. **`messages.js` gagne un override `DURATION`** : les profils sont figés à 60 s, la fenêtre mesurée
   doit faire ≥ 10 min.

**Révisé le 28/09/2026 après le premier run (le profil mono-client ne mesurait que deux plafonds par client).**
`mt.inbound` est partitionné par compte (guide §4.1) : un compte = une partition = une voie du routeur,
et le premier run a tout mis sur la partition 8 des 12. Chaque capture incrémente la ligne
`control_plane.balances` du client : un client = une ligne verrouillée, 6-7 transactions en attente,
captures au-delà de leurs 200 ms, pool à 369 `submit_sm/s`. Ce sont des plafonds **par client**, voulus
par l'ordre par compte et consignés comme tels. Pour mesurer la passerelle, `seed-load` sème
`LOAD_CUSTOMERS` clients (`load-00`…), chacun avec son sender ID, son compte `load`, sa facturation postpayée
et sa clé ; k6 reçoit `API_KEYS` et donne à chaque VU la clé `__VU % n`. 24 clients : deux par partition
en moyenne.

**Pièges consignés d'avance.** (a) Le simulateur sert ~200 `submit_sm/s` par bind (latence fixe 5 ms,
service sérialisé) et `seed` crée le connecteur à `bind_pool_size` 1 : sans levier, la campagne mesure
200 SMS/s. Ne pas baisser la latence du simulateur : un plafond sous un autre profil ne borne rien.
(b) Redpanda mono-cœur, RF 1. (c) k6 + simulateur + 14 pods sur 8 vCPU : le chiffre sera celui de
l'hôte (cf. le plafond 4 800 de step-201d). (d) Un seul disque pour Postgres, ClickHouse, Redpanda,
l'AOF Redis et RustFS : `iostat` pendant chaque run. (e) Un 202 ne prouve rien en aval
(`rest-api-svc` n'applique ni crédit ni anti-spam) : chaque run porte quatre chiffres côte à côte.
(f) `e2e-budget` sort « non résolu » quand 2 s tombe dans un bucket : ni succès ni échec.
(g) 52 binds × 200/s = 10 400 est un plafond **théorique** : le pool envoie en série par bind (fenêtre
effective 1, plus claim Redis, DLR, CDR et settle par message), là où `smsc-ceiling` pousse une fenêtre
de 32 — les deux courbes ne se comparent que si le simulateur sérialise vraiment chaque session.
(h) billing-svc relit la config client toutes les 30 s : `run.sh seed` attend 35 s avant de rendre la main.
(i) L'authentification REST fait une requête Postgres par appel, sans cache : elle est dans la mesure.

## Plafond du pool après step-350 PR2 (prérequis local)

`make load-reference RUN=TestPoolSubmitCeiling`, 29/09/2026, hôte au repos, balayage complet :
4 742 · 7 971 · 12 815 · 19 578 · 29 598 `submit_sm/s` à 1 · 2 · 4 · 8 · 16 binds (w64), et 19 474 à
19 566 à 8 binds de w1 à w256. Chaque palier est dans la bande de step-201f ou juste au-dessus : la
réécriture de sender ID ne se voit pas au bruit de l'hôte. Le banc n'a **pas** d'étage de facturation. C'est
exactement ce qui le sépare des 369 `submit_sm/s` du VPS (journal, goulot 2).

## Journal de la campagne (28-29/09/2026, VPS de test, 8 vCPU, image `v0.0.1-sha-59d7eec`)

**Aucun verdict n'est rendu ici.** Tout ce qui suit est mesuré sur un seul nœud où k6, le simulateur,
les 4 magasins et les 11 services se partagent 8 vCPU.

**Plafond du pair, dans le cluster** (`smsc-ceiling`, `healthy`, latence fixe 5 ms, fenêtre 32, pool
arrêté pendant la mesure) : 51 066 · 67 917 · 72 144 · **73 077** `submit_sm/s` à 10 · 20 · 40 · **52**
binds, soit ×7 la cible de 10 400. La courbe plie entre 40 et 52 binds. Le palier à 80 binds est disqualifié :
le pair a lâché 8 sessions en cours de fenêtre. Le simulateur ne sert jamais de contrainte.

**Ingestion `sustained` (8 000 req/s visés, `IDEMPOTENCY=off`, 10 min)** :

| Profil | req/s tenues | 202 | p99 | Itérations abandonnées |
|---|---:|---:|---:|---:|
| 1 client | 5 820 | 100 % | 981 ms | 2 173/s (4 000 VUs, le maximum) |
| 24 clients | 5 540 | 100 % | 1,1 s | 2 453/s |

L'hôte était saturé : `vmstat` moyen à 52 % us, 29 % sy, **18 % idle** et 0 % wa. CPU moyen par pod : k6 1,8 cœur,
ClickHouse 1,4, Postgres 0,87 (l'auth REST fait une requête par appel, sans cache), `rest-api-svc` ~0,65
par pod. **Goulot de l'ingestion : l'hôte (co-résidence)**, dans la lignée du plafond 4 800 de step-201d.
NFR ingestion p99 < 250 ms : **mesuré 981 ms-1,1 s, non tenu ici, verdict non rendu (→ step-409).**

**Traversée : trois goulots nommés, aucun chiffre NFR.**

1. **Une partition par compte** (voulu, guide §4.1) : avec 1 client, tout `mt.inbound` est allé sur la
   partition 8, soit une seule voie du routeur.
2. **Une ligne de solde verrouillée par client.** Chaque capture fait `AdjustBalance … ON CONFLICT` sur la
   même ligne `control_plane.balances`. On a observé 6-7 transactions en attente de verrou, des captures
   au-delà de leurs 200 ms (fail-open, ~1 000/min) et **369 `submit_sm/s`** en traversée, pour un pool à
   0,24 cœur. Ce plafond est propre à un client unique : un gros client A2P le heurterait en production.
3. **Le routeur ne sort pas d'un backlog quand la facturation est active.** Avec 24 clients, les
   réservations en rafale dépassent `RESERVE_TIMEOUT` et le superviseur abat le processus. Résultat :
   CrashLoopBackOff, **0 message consommé en 60 s** sans aucune ingestion, 3,3 M messages en attente.
   → **step-285**, prérequis de step-409.

NFR débit soutenu 8 000/s, pic 15 000/s et bout-en-bout p99 < 2 s : **non mesurables** sur cette
campagne tant que step-285 n'est pas livrée. Les runs `IDEMPOTENCY=on` et `peak` n'ont pas été faits :
ils auraient mesuré le même CrashLoop.

**Défauts de l'outillage trouvés en le faisant tourner**, tous corrigés :
- le pool s'est garé 85 min au premier reset du simulateur, parce que l'auto-reconnexion est opt-in et que
  le plan de contrôle écrase l'env (dette `connector-auto-reconnect-du-manifest-sans-effet`) ;
- les binds du pool disqualifiaient le plafond du pair ;
- un sshd public sous brute-force refusait les poignées de main au-delà de `MaxStartups` (d'où une seule
  connexion multiplexée) ;
- le premier `ssh -L` du script de relevé (hors dépôt) devenait le maître du multiplexage et figeait
  tout : ouvrir un maître dédié (`ssh -MNf`) avant tout tunnel.

**Hors de la passerelle.** Le simulateur v0.8.1 a coupé deux sessions à 21:16:59 et 21:18:45 UTC sans
aucune trace (ni log ni métrique de fermeture). Un prompt pour lui ajouter une télémétrie des fermetures a
été remis à l'exploitant ; son déploiement est un prérequis de lecture pour step-409.

**Non fait, renvoyé à step-409 :**
- ratios L0 à l'échelle ;
- `e2e-budget` (le tunnel a été coupé, et la traversée était nulle) ;
- `iostat`, absent de Rocky, remplacé par `vmstat`.

## Definition of Done
- [x] `TestPoolSubmitCeiling` rejoué en entier, hôte au repos, consigné ci-dessus
- [x] `test-env seed-load` livré (24 clients, auto-reconnexion, clé tournée), testé contre le faux admin,
      refus anti-spam compris
- [x] image `smsc-ceiling` publiée ; Jobs et `deploy/test-load/run.sh` versionnés ; `make test-env` vert
- [x] plafond du pair mesuré dans le cluster, nombre de binds et profil de latence cités
- [ ] `sustained`/`peak` × `IDEMPOTENCY` — **fait :** `sustained` off (1 client puis 24) ; **non fait,
      nommément :** les trois autres, bloqués par step-285
- [x] les goulots **nommés** : hôte (ingestion), partition par compte, ligne de solde par client, routeur
      en CrashLoop sur backlog (step-285)
- [ ] ratios L0 à l'échelle — **non fait**, renvoyé à step-409
- [x] tableau NFR : aucun NFR coché, chacun « mesuré ou non mesurable ici — verdict non rendu (→ step-409) »
- [x] `deploy/test/README.md` §10 : la charge n'y est plus absente, elle y est non représentative

## Hors périmètre
Le verdict NFR, le matériel représentatif, les valeurs de dimensionnement → step-409.
