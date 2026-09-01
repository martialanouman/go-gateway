# Stratégie de test — Passerelle SMS

**Composant :** Passerelle SMS principale (Go)
**Statut :** Stratégie de test v1.0
**À lire avec :** `plan-execution-passerelle.md` (jalons), `guide-codage-go.md` §13 (règles de test).

Ce document dit **quoi tester, à quel niveau, et comment**. Les deux pairs SMPP coexistent : le **faux SMSC minimal in-repo** (`internal/testutil/fakesmsc`) porte les tests ordinaires, et le **vrai simulateur**, livré à `M8` (`internal/testutil/smscsim`), porte l'injection de pannes des tests de résilience.

---

## 1. Principes

Trois objectifs : **rapide** (la boucle de dev reste courte), **fiable** (pas de test flaky), **porteur de sens** (on teste les chemins critiques et les invariants, pas les getters). On applique la pyramide : beaucoup d'unitaires, quelques tests d'intégration, très peu de bout-en-bout.

```
        /   E2E    \      Peu — le squelette vertical via le faux SMSC
       / Intégration \    Quelques-uns — repos & flux (testcontainers)
      /  Unitaires     \  Beaucoup — logique de domaine, codec, pipeline
```

Ce qu'on **couvre en priorité** : chemins métier critiques (le pipeline MT/MO/DLR), gestion d'erreurs et codes de sortie, cas limites (segmentation, encodage), frontières de sécurité (auth, secrets, corps jamais loggé), intégrité des données (idempotence de facturation). Ce qu'on **ne teste pas** : getters/setters triviaux, code de framework, scripts one-off.

Règle CI : `go test -race ./...` vert obligatoire ; aucune fusion sans CI vert (guide de codage §13).

---

## 2. Le faux SMSC in-repo (débloque M2→M7)

Le vrai **simulateur SMSC** (`specification-technique-simulateur-smsc.md`) est un projet compagnon plus riche, non disponible pour l'instant. Pour ne pas bloquer le squelette vertical, on écrit un **double de test minimal** dans le dépôt.

**Emplacement :** `internal/testutil/fakesmsc`. Lançable en test (embarqué) ou en process (`make fake-smsc`).

**Périmètre minimal (juste ce qu'il faut pour M2→M7) :**

- Accepte un bind SMPP (TX/RX/TRX), répond `bind_*_resp`.
- Reçoit `submit_sm`, répond `submit_sm_resp` avec un `message_id` SMSC ; **comportement scriptable** : succès, erreur (`ESME_RTHROTTLED`, `ESME_RSYSERR`…), délai injecté.
- Émet `deliver_sm` sur demande (test) pour simuler un **MO** ou un **DLR** corrélé à un `message_id`.
- Répond `enquire_link` ; gère `unbind`.

**Ce qu'il ne fait PAS** (réservé au vrai simulateur, `M8`) : injection de pannes réaliste (flapping de lien, latences distribuées, fenêtres saturées, dégradation progressive), profils vendeur, volumétrie.

**Contrat de test :** le faux SMSC expose une API Go pour piloter ses réponses depuis un test :

```go
smsc := fakesmsc.Start(t, fakesmsc.Config{
    OnSubmit: func(pdu smpp.SubmitSM) fakesmsc.Resp {
        return fakesmsc.OK() // ou fakesmsc.Throttled(), fakesmsc.Delay(200*time.Millisecond)
    },
})
defer smsc.Close()
// ... plus tard, simuler un DLR :
smsc.SendDLR(messageID, smpp.StatusDelivered)
```

> **Règle :** un test choisit son pair par ce qu'il exerce, pas par le jalon. Le faux SMSC suffit dès qu'il s'agit de réponses applicatives (`OK`, `Throttled`, `SysErr`, `Delay`) ; le vrai simulateur est requis pour l'injection de pannes réaliste (disjoncteur, reroute, reconnexion). Ces tests s'auto-sautent quand l'image `smsc-simulator:dev` est absente — **`make smsc-sim` la construit** — mais **seulement hors CI** : `internal/testutil/ciguard` transforme un saut en échec dès que `CI` est posée. C'est la règle générale de la suite d'intégration, pas une exception du simulateur : **en CI, un test qui saute se lit exactement comme un test qui passe**, et dix tests de résilience ont traversé un jalon entier sans jamais s'exécuter avant qu'on le remarque (step-250b/250c).

---

## 3. Les 4 invariants — tests bloquants, verts à vie

Priorité absolue. Ils sont posés tôt (voir la colonne « jalon ») et ne doivent **jamais** repasser au rouge.

| # | Invariant | Comment le tester | Jalon |
|---|---|---|---|
| **a** | Le corps ne fuit dans aucune sérialisation (log, span, label) | Sérialiser un `*Message` (JSON `slog`, attributs de span via exporteur de test OTel) et **échouer si** le clair y apparaît. Property test sur des corps aléatoires. | M0 |
| **b** | Un message routé par numéro exact traverse toutes les étapes de conformité | Router un MSISDN présent dans `exact_routes` et **asserter** le passage par E.164, sender ID, opt-out, anti-spam, segmentation, débit (spies sur chaque étape). | M7 |
| **c** | Facturation idempotente sous double livraison d'un même `message_id` | Rejouer une réserve+capture avec le même `message_id` et **asserter** un seul débit (grand livre + solde). | M9 |
| **d** | `max_sessions` refuse le bind au-delà du quota | Ouvrir `max_sessions+1` binds concurrents et **asserter** `ESME_RBINDFAIL` sur le dernier ; libérer un jeton et vérifier qu'un nouveau bind passe. | M3 |

---

## 4. Stratégie par composant

### 4.1 Logique de domaine (unitaire, table-driven) — le gros du volume

Cible : `internal/pipeline` (chaque étape), `internal/routing` (résolution 3 niveaux, stratégies), `internal/…/encoding` (segmentation, encodage), `internal/billing` (formule de crédit, réserve/capture). Tests **purs**, sans I/O, avec dépendances injectées (fakes écrits à la main). Table-driven idiomatique.

Exemples de cas : normalisation E.164 (formats variés → forme canonique) ; autorisation sender ID par politique (`strict`/`allow_unregistered_numeric`/`disabled`) ; opt-out sur union de portées ; `segment_count` pour GSM-7/UCS-2 aux frontières (160/153, 70/67 caractères) ; `credits = segment_count × credits_per_segment` ; chaque stratégie de distribution (`weighted`/`hash_based` déterministes).

### 4.2 Codec SMPP (unitaire + fuzzing)

Cible : `internal/smpp`. Round-trip encode/decode de chaque PDU ; TLV ; UDH ; payload > 254 o. **Fuzzing obligatoire** (`go test -fuzz`) sur le décodeur PDU et sur le parseur/segmenteur d'encodage — ce sont des surfaces d'entrée non fiables. Un décodeur qui panique sur une entrée malformée est un bug de sécurité.

### 4.3 Repositories & état (intégration, testcontainers)

Cible : `internal/storage/{postgres,redis,kafka,clickhouse}`. On monte les vraies dépendances avec `testcontainers-go` — pas de mock de base. Tests : CRUD + contraintes (cardinalité credentials, `NULLS NOT DISTINCT` des suppressions platform, refus de scope-change soldes ≠ 0) ; **atomicité Lua** du token-bucket et de la réserve de crédit sous concurrence (`-race`, N goroutines) ; sink et lecture CDR ClickHouse ; production/consommation Kafka avec commit après traitement.

### 4.4 API (unitaire + contrat + intégration HTTP)

Cible : `admin-api-svc`, `rest-api-svc` (chi + huma). Trois niveaux :

- **Unitaire** : la logique des handlers, avec les repos en fake.
- **Contrat** : l'OpenAPI généré par Huma doit rester cohérent avec la **source de vérité** `openapi-*.yaml`. Test dédié qui compare (opérations, schémas, `Error`) et **échoue en cas de dérive** — le code ne doit jamais s'écarter du contrat en silence.
- **Intégration HTTP** : requêtes réelles contre le service câblé sur des repos testcontainers ; asserte les codes de sortie et la forme d'erreur plate `{ code, message, errors[] }`.

### 4.5 Concurrence

Détection de course activée partout (`-race`). Cibles sensibles : le modèle une-goroutine-par-connexion SMPP, le token-bucket, l'instantané immuable + pointeur atomique du routeur (lecture concurrente pendant un hot reload), l'agrégation multi-pod du disjoncteur. Un test qui passe sans `-race` mais échoue avec a trouvé un bug.

### 4.6 Bout-en-bout (peu nombreux, via le faux SMSC)

Le squelette vertical (`M2`) : `POST /messages` → CDR `enroute` → `GET /messages/{id}`, avec le faux SMSC comme pair. Puis parité protocole (`M3`) : même message en REST et en SMPP → traitement identique. Puis MO/DLR (`M4`) : le faux SMSC émet un MO/DLR → remise/corrélation correcte. On garde ces tests **peu nombreux et robustes** — la couverture fine est aux niveaux inférieurs.

### 4.7 Charge & NFR (M12)

Outils : `k6` ou `vegeta` côté REST, un générateur de binds SMPP côté ingress. Cibles (§1.2 de la spec) : soutenu 8 000 SMS/s, pic 15 000 ; ingestion p99 < 250 ms ; bout-en-bout p99 < 2 s (disjoncteur fermé). Mesurer la profondeur de file Kafka, le lag consumer, la latence par étape. Tuning : partitions Kafka, batch ClickHouse, pool `pgx`.

### 4.8 Chaos & injection de pannes

Perte Redis (vérifier **chaque** politique de panne — la matrice de référence est `guide-codage-go.md` §16, pas cette liste) ; flapping de connecteur (disjoncteur ouvre → `fallback_chain` → `mt.reroute-park`) ; redémarrage de pods (drain gracieux, `PodDisruptionBudget`, binds préservés) ; failover Postgres (réhydratation du cache de solde).

La perte Redis n'a jamais eu besoin du simulateur SMSC : elle se coupe avec un `tcpproxy` devant le Redis de `redistest` (`redistest.Cuttable`), et chaque politique est prouvée dans le paquet qui la porte plutôt que dans une suite de chaos unique — l'assertion y est nette et le test n'a besoin que de Redis. Livré en step-250. Seuls les scénarios de **connecteur** (flapping, reroute) demandent un pair SMPP.

**Le drain gracieux est livré (step-260)**, et il n'a demandé ni conteneur tué ni cluster : le `cancel()` d'un `context.WithCancel` **est** le SIGTERM. C'est ce qui rend ces tests ordinaires — `internal/smppserver/drain_integration_test.go` bind un ESME sur le vrai listener, draine, et exige l'`unbind` puis un rebind immédiat sur un pod de **nouvelle identité**, à `max_sessions=1`. Le PDB lui-même n'est pas testable ici (aucun manifeste avant step-270) ; ce que step-260 fige, c'est le comportement que le PDB devra respecter.

**Le failover Postgres est livré (step-260b)** par la même mécanique : `pgtest.Cuttable` /
`CuttableConfig`, écrits par symétrie exacte avec leurs jumeaux `redistest`. La coupure passe par un
`tcpproxy` devant le conteneur partagé — jamais par l'arrêt du conteneur, que les tests frères du paquet
utilisent aussi. Deux règles s'en dégagent, payées cher : le pool sous test se construit **avant** la
coupure (son constructeur de production pingue à chaud), et les assertions se lisent par un **second pool
non coupé**, faute de quoi la vérification meurt avec la dépendance et le test passe en n'observant rien.

**Les quatre politiques Redis de step-250 sont prouvées (step-250d)**, chacune dans le paquet qui la
porte. Trois règles s'en dégagent. D'abord, **le contrôle doit rendre la coupure observable** : un test
qui passe aussi bien Redis debout ne teste rien — d'où un sujet réellement bloqué avant de couper le
throttle, un MSISDN réellement dans le Bloom pour le L0, un quota réellement plein pour distinguer
`ESME_RBINDFAIL` d'`ESME_RSYSERR`. Ensuite, **une politique asymétrique demande un test par côté** : la
même mutation doit faire tomber le versant fail-closed et laisser le versant fail-open vert, faute de
quoi on a testé deux fois le même. Enfin, **compter les sites, pas les politiques** : une seule ligne de
§16 peut nommer deux points de repli — le jeton d'annulation en a deux côté pool, la réclamation avant
envoi et la branche d'expiration — et n'en couvrir qu'un laisse l'autre sur un double, ce que la ligne
§16 ne dit pas.

Restent à couvrir : les **trois politiques PostgreSQL hors facturation** — auth de bind SMPP, clés API
REST, snapshots du routeur (step-260c). Aucune n'a de ligne §16, et le `[MUST]` de §16 exige documentée
**et** testée : on n'écrit pas la ligne avant d'avoir le test, sous peine de refaire la dette que
step-250d vient de solder.

---

## 5. Cibles de couverture (indicatives)

| Package | Cible | Priorité |
|---|---|---|
| `internal/pipeline`, `internal/routing`, `internal/billing`, `internal/…/encoding` | ≥ 85 % | Critique — logique métier |
| `internal/smpp` (codec) | ≥ 80 % + fuzz | Critique — surface non fiable |
| `internal/storage/*` | chemins couverts par intégration | Élevée |
| `admin-api-svc`, `rest-api-svc` (handlers) | ≥ 75 % + contrat | Élevée |
| `cmd/*` (wiring) | fumée | Faible — pas de logique |

La couverture est un indicateur, pas une fin : 100 % de lignes couvertes sans les 4 invariants ni les cas d'erreur mappés ne vaut rien. On vise la **couverture des comportements**, pas des lignes.

---

## 6. Rattachement aux jalons

| Jalon | Tests introduits |
|---|---|
| M0 | Invariant (a) ; squelette CI (`-race`, lint, `govulncheck`) ; migration applique le DDL |
| M1 | Contrat OpenAPI (admin) ; contraintes de schéma (cardinalité) |
| M2 | **Codec SMPP unit + fuzz** (voie sortante) ; E2E squelette via **faux SMSC** ; ACK durable Kafka ; producteur/consommateur |
| M3 | Invariant (d) ; parité REST/SMPP ; rotation avec grâce ; session/registre |
| M4 | MO/DLR via faux SMSC ; corrélation DLR ; webhook signé + retry |
| M5 | Sender ID ; opt-out union de portées ; **Bloom sans faux négatif** ; anti-spam |
| M6 | Segmentation aux frontières ; **atomicité token-bucket** sous concurrence |
| M7 | Invariant (b) ; scénario MNP ; script (timeout/instructions → repli) ; hot reload sans downtime |
| M8 | **Bascule au vrai simulateur** ; chaos (disjoncteur, reroute, reconnexion) — dé-`Skip` |
| M9 | Invariant (c) ; réserve/capture ; facturation désactivée = zéro I/O |
| M10 | Invariant (a) sous chaque politique ; crypto-shred ; effacement MSISDN |
| M11 | Aucun corps dans un trace ; labels bornés ; export masqué |
| M12 | Charge/NFR ; chaos complet ; revue sécurité |

---

## 7. Écriture des tests avec Claude Code

Pour chaque tâche, demande à Claude Code d'**écrire les tests en même temps que le code**, en recopiant les critères d'acceptation du plan comme cas de test. Rappels à mettre dans le prompt : table-driven idiomatique ; fakes écrits à la main plutôt que mocks lourds ; `-race` systématique ; pour toute nouvelle étape de pipeline, un test qui vérifie qu'elle **ne logge pas le corps**. Les scénarios qui exigent le simulateur SMSC s'écrivent contre `internal/testutil/smscsim` : sans son image ils sautent en local (`make smsc-sim` la construit) et **échouent en CI**, par `internal/testutil/ciguard`.
