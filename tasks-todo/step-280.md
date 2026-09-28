# step-280 — Campagne NFR sur le VPS de test : outillage répétable et chiffres non représentatifs

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** EN COURS
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
5. **Leviers : `run.sh apply` les patche**, valeurs versionnées dans le script, jamais `kubectl scale` :
   `rest-api-svc`, `router-svc`, `connector-pool-svc` à 2 réplicas par HPA min = max (avec des requests à
   50 m l'HPA sature dès la première seconde et ne mesure rien). `bind_pool_size` passe par l'Admin
   (`seed-load`), que le pool relit avant chaque connexion ; `CONNECTOR_BIND_POOL_SIZE` reste à 2.
   Redpanda (`--smp=1`) et Redis (aucun `maxmemory`) restent tels quels et sont consignés.
6. **`messages.js` gagne un override `DURATION`** : les profils sont figés à 60 s, la fenêtre mesurée
   doit faire ≥ 10 min.

**Révisé le 28/09/2026 après le premier run (le profil mono-client ne mesurait que deux plafonds par client).**
`mt.inbound` est partitionné par compte (guide §4.1) : un compte = une partition = une voie du routeur,
et le premier run a tout mis sur la partition 8 des 12. Chaque capture incrémente la ligne
`control_plane.balance` du client : un client = une ligne verrouillée, 6-7 transactions en attente,
captures au-delà de leurs 200 ms, pool à 369 `submit_sm/s`. Ce sont des plafonds **par client**, voulus
par l'ordre par compte et consignés comme tels. Pour mesurer la passerelle, `seed-load` sème
`LOAD_CUSTOMERS` clients (`load-00`…), chacun avec son sender ID, son compte `load`, sa facturation postpayée
et sa clé ; k6 reçoit `API_KEYS` et donne à chaque VU la clé `__VU % n`. 24 clients : deux par partition
en moyenne. Le second profil « facturation coupée » que j'avais retiré n'est plus nécessaire : la
contention est attribuée par `pg_stat_activity`, pas par soustraction.

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

`make load-reference RUN=TestPoolSubmitCeiling`, 28/09/2026, interrompu après 4 binds :
4 516 · 7 584 · 12 624 `submit_sm/s` à 1 · 2 · 4 binds (pair 134 646–167 990/s). Dans la bande de
step-201f (3 294–4 351 · 4 814–7 724 · 8 125–12 947) : la réécriture de sender ID ne se voit pas au
bruit de ±30 % de l'hôte. Balayage complet à relancer, hôte au repos.

## Definition of Done
- [ ] `TestPoolSubmitCeiling` rejoué en entier, hôte au repos, consigné ci-dessus
- [ ] `test-env seed-load` livré, testé contre le faux admin, refus anti-spam compris
- [ ] image `smsc-ceiling` publiée ; Jobs et `deploy/test-load/run.sh` versionnés ;
      `make test-env` vert
- [ ] plafond du pair mesuré dans le cluster, nombre de binds et profil de latence cités
- [ ] `sustained` et `peak`, `IDEMPOTENCY=off` et `on`, ≥ 10 min chacun ; par run : 202 k6,
      `submit_sm` servis par le simulateur, CDR, lag consumer, `e2e-budget`, `kubectl top` par pod,
      `iostat`
- [ ] le goulot **nommé**
- [ ] ratios L0 (lookups/msg, `outcome`, octets/clé, attentes pgx) consignés ; valeurs → step-409
- [ ] tableau NFR : chaque ligne « mesuré : X sur 8 vCPU co-résidents — verdict : non rendu (→ step-409) »,
      aucune case NFR cochée
- [ ] `deploy/test/README.md` §10 : la charge n'y est plus absente, elle y est non représentative

## Hors périmètre
Le verdict NFR, le matériel représentatif, les valeurs de dimensionnement → step-409.
