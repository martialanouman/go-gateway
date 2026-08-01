# Guide de codage Go — Passerelle SMS

**Composant :** Passerelle SMS principale (Go)
**Spécification de référence :** `specification-technique-passerelle-sms.md`
**Statut :** Guide de codage v1.0
**Public :** tout ingénieur contribuant au code Go de la passerelle.

> *Convention d'équipe : la prose est en français ; le code, les noms d'identifiants, de packages, de types et les commentaires de code sont en anglais. Ce guide suit cette règle — les commentaires dans les blocs Go sont volontairement en anglais.*

Ce guide fixe les conventions de code Go du projet. Il complète — et ne remplace pas — [Effective Go](https://go.dev/doc/effective_go), les [Go Code Review Comments](https://go.dev/wiki/CodeReviewComments) et le [Google Go Style Guide](https://google.github.io/styleguide/go/). Là où ces références sont muettes ou où le projet a une raison spécifique de diverger, ce document tranche. Les règles marquées **[MUST]** sont vérifiées en revue et/ou en CI ; les règles **[SHOULD]** sont des recommandations fortes.

---

## 1. Pourquoi Go, et ce que ça implique

Le choix de Go est motivé par le modèle **une goroutine par connexion** qui convient aux sessions SMPP persistantes (§1.3 de la spec). Deux conséquences guident tout le code :

Le système est **massivement concurrent** (5 000–20 000 binds simultanés, 8 000–15 000 msg/s). La correction sous concurrence prime : pas de course de données, pas de goroutine qui fuit, propagation systématique de `context.Context`.

Le socle de traitement est **sans état et horizontalement scalable**. Le code ne doit jamais supposer qu'une requête reste sur le même pod : l'état partagé vit dans Redis ou est porté par le message (en-têtes Kafka), jamais dans une variable de package.

---

## 2. Structure du dépôt

Layout mono-repo, un binaire par service, code partagé sous `internal/`.

```
sms-gateway/
├── cmd/                          # one main package per deployable (§3 du guide d'ingénierie)
│   ├── smpp-server-svc/main.go
│   ├── rest-api-svc/main.go
│   ├── router-svc/main.go
│   ├── connector-pool-svc/main.go
│   ├── mo-dlr-router-svc/main.go
│   ├── session-manager-svc/main.go
│   ├── billing-svc/main.go
│   └── admin-api-svc/main.go
├── internal/                     # NOT importable outside this module — the default home for code
│   ├── smpp/                     # SMPP v3.4/5.0 PDU codec, session state machine, window mgmt
│   ├── pipeline/                 # the shared MT stages: normalize, senderid, optout, antispam...
│   ├── routing/                  # 3-level resolver, distribution strategies, script runtime
│   ├── billing/                  # reserve/capture/release, MO meter, ledger, idempotency
│   ├── connector/               # SMSC pool, circuit breaker, reconnect supervisor
│   ├── storage/
│   │   ├── postgres/             # control-plane repositories (sqlc-generated + hand-written)
│   │   ├── redis/                # keyspaces, Lua scripts, bloom filters
│   │   ├── kafka/                # producers, consumers, topic constants
│   │   └── clickhouse/           # CDR sink
│   ├── config/                   # snapshot loading, hot-reload, config-sync client
│   ├── observability/            # otel, prometheus, structured logging setup
│   └── platform/                 # cross-cutting: e164, encoding/UDH, uuidv7, errors
├── api/                          # protobuf (gRPC) + OpenAPI specs — generated code committed
├── migrations/                   # golang-migrate SQL files (mirror schema_passerelle_sms.sql)
├── deploy/                       # k8s manifests, helm charts
└── go.mod
```

**[MUST]** Tout code métier vit sous `internal/`. Un package ne devient `pkg/` (public) que si un dépôt tiers doit l'importer — cas rare, à justifier en revue.

**[MUST]** Pas de dépendance cyclique. `pipeline` importe `routing`, `billing`, `storage` ; jamais l'inverse. Les interfaces sont déclarées côté consommateur (voir §6).

**[SHOULD]** Un package est nommé par ce qu'il fournit, au singulier, sans redondance : `routing`, pas `routingutils` ni `routing_helpers`. Éviter `util`, `common`, `helpers`, `base`.

---

## 3. Formatage et style

**[MUST]** `gofmt` (via `goimports`) sur tout fichier. Non négociable, vérifié en CI.

**[MUST]** Le linter est `golangci-lint` avec au minimum : `govet`, `staticcheck`, `errcheck`, `ineffassign`, `unused`, `gosec`, `bodyclose`, `contextcheck`, `rowserrcheck`, `sqlclosecheck`. La configuration vit dans `.golangci.yml` et le CI échoue sur toute alerte.

**[SHOULD]** Longueur de ligne indicative 100–120 colonnes ; `gofmt` ne coupe pas, donc c'est au jugement — préférer extraire une variable à une ligne illisible.

Nommage : `MixedCaps`, jamais `snake_case`, pour les identifiants Go (les colonnes SQL restent en `snake_case`, c'est le mapping qui fait le pont). Les acronymes gardent leur casse : `msgID`, `smppPDU`, `httpClient`, `mccMNC`. Les getters n'ont pas de préfixe `Get` (`c.Name()`, pas `c.GetName()`) — sauf le code gRPC généré, qu'on ne touche pas.

**[SHOULD]** Commentaires de doc sur tout symbole exporté, commençant par le nom du symbole. Le commentaire explique le *pourquoi* et les invariants, pas la paraphrase du code.

---

## 4. Gestion des erreurs

**[MUST]** Les erreurs sont des valeurs, propagées explicitement. Enrober avec `%w` pour préserver la chaîne :

```go
if err := repo.Save(ctx, acct); err != nil {
    return fmt.Errorf("save smpp account %s: %w", acct.ID, err)
}
```

**[MUST]** Le message enrobant décrit l'opération, en minuscule, sans ponctuation finale, sans le mot « error » ni « failed » (le lecteur sait que c'est une erreur). On lit une chaîne : `save smpp account: query row: connection refused`.

**[MUST]** Les erreurs sentinelles et typées vivent dans `internal/platform/errors`. On les teste avec `errors.Is`/`errors.As`, jamais par comparaison de chaîne.

```go
var ErrSenderIDNotAuthorized = errors.New("sender id not authorized")
var ErrRecipientOptedOut     = errors.New("recipient opted out")
var ErrInsufficientCredit    = errors.New("insufficient MT credit")
```

**[MUST]** À la frontière protocole, chaque erreur de domaine est mappée vers son code de sortie, une seule fois, dans un traducteur central — jamais dispersé dans le pipeline :

| Erreur de domaine | REST | SMPP |
|---|---|---|
| `ErrSenderIDNotAuthorized` | `403` | `ESME_RINVSRCADR` |
| `ErrRecipientOptedOut` | `403 recipient_opted_out` | `ESME_RSUBMITFAIL` |
| `ErrInsufficientCredit` | `402` | code d'extension |
| op SMPP désactivée (§6.22) | — | `ESME_RINVCMDID` |
| bind au-delà de `max_sessions` | — | `ESME_RBINDFAIL` |

**[MUST]** `panic` est réservé aux invariants de programmation irrécupérables (bug), jamais au contrôle de flux ni à une erreur d'entrée. Toute goroutine longue durée (bind, consumer) a un `recover` de dernier ressort qui journalise (sans le corps) et redémarre proprement l'unité, sans faire tomber le pod.

**[SHOULD]** Ne jamais ignorer une erreur silencieusement. Si c'est volontaire, l'annoter : `_ = conn.Close() // best-effort on teardown`.

---

## 5. Contexte, concurrence et cycle de vie des goroutines

**[MUST]** `context.Context` est le premier paramètre de toute fonction faisant de l'I/O ou pouvant bloquer : `func (s *Service) Route(ctx context.Context, m *Message) (...)`. Ne jamais le stocker dans une struct. Ne jamais passer `nil` — utiliser `context.TODO()` seulement en transition.

**[MUST]** Toute goroutine lancée doit avoir un chemin de sortie clair, lié à un `context` ou à la fermeture d'un canal. **Une goroutine sans condition d'arrêt est un bug.** Pas de `go f()` orphelin dans du code de service.

**[MUST]** La concurrence structurée passe par `errgroup.Group` (avec `WithContext`) pour les fan-out à durée bornée, et par un superviseur explicite pour les boucles longue durée (binds, consumers). Le `main` de chaque service câble l'arrêt gracieux sur `SIGTERM` :

```go
func main() {
    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
    defer stop()

    svc, err := newService(ctx, loadConfig())
    if err != nil {
        log.Fatal(err) // only place a Fatal is acceptable: startup
    }
    if err := svc.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
        log.Error("service exited", "err", err)
        os.Exit(1)
    }
}
```

**[MUST]** L'arrêt gracieux draine : `smpp-server-svc` fait un unbind gracieux des binds (§6.3), les consumers Kafka valident leurs offsets en cours puis s'arrêtent, `connector-pool-svc` termine les `submit_sm` en vol dans la fenêtre. Respecter le `terminationGracePeriodSeconds` du pod.

**[MUST]** Modèle SMPP : **une goroutine par connexion** pour la lecture, une pour l'écriture, communiquant par canaux ; l'état de session (fenêtre, `enquire_link`) est possédé par une seule goroutine pour éviter les verrous. Le registre inter-pods (`session-manager-svc`) est la seule source de vérité partagée.

**[MUST]** Détection de course activée en CI (`go test -race ./...`) et sur les images de staging. Un test qui passe sans `-race` mais échoue avec est un test qui a trouvé un bug.

**[SHOULD]** Préférer les canaux pour transférer la propriété d'une donnée ; préférer un `sync.Mutex` (petit, à portée locale) pour protéger un champ. Ne pas mélanger les deux sur la même donnée. Documenter, en commentaire, quel verrou protège quel champ.

**[SHOULD]** Borner le parallélisme des fan-out (pool de workers ou `semaphore.Weighted`) — ne jamais lancer une goroutine par message sans borne, sous peine d'épuiser la mémoire au pic.

### 5.1 Instantané immuable + pointeur atomique (§6.1)

La configuration de routage se recharge à chaud sans verrou sur le chemin critique. Le motif normatif :

```go
// RoutingSnapshot is immutable once published. Never mutate a field after Store.
type RoutingSnapshot struct {
    routes      []*Route
    prefixTrie  *trie.Trie
    exactBloom  *bloom.Filter
    // ... precompiled regexes, script handles
}

type Router struct {
    snap atomic.Pointer[RoutingSnapshot] // swapped by config-sync; read lock-free per message
}

func (r *Router) Resolve(ctx context.Context, m *Message) (*Route, error) {
    s := r.snap.Load() // no lock, no allocation on the hot path
    return s.resolve(ctx, m)
}

func (r *Router) onConfigChange(next *RoutingSnapshot) {
    r.snap.Store(next) // atomic publish; old snapshot GC'd when last reader drops it
}
```

L'état volatil (disjoncteur agrégé, `connectorload`) vit dans une **surcouche mutable séparée** (également `atomic.Pointer` ou champs atomiques), pour ne pas reconstruire l'instantané immuable à chaque transition de disjoncteur.

---

## 6. Interfaces, dépendances et testabilité

**[MUST]** Les interfaces sont définies **côté consommateur**, petites, centrées sur l'usage. `pipeline` déclare ce dont il a besoin ; `storage/postgres` fournit une implémentation concrète.

```go
// in internal/pipeline — the consumer owns the interface
type SuppressionChecker interface {
    IsSuppressed(ctx context.Context, msisdn string, scopes ScopeSet) (bool, error)
}
```

**[SHOULD]** Accepter des interfaces, retourner des structs concrètes. Un constructeur retourne `*Service`, pas `Servicer`.

**[MUST]** Injection de dépendances par constructeur explicite, pas de singleton global ni de `init()` qui ouvre une connexion. Le `main` câble le graphe.

**[SHOULD]** Pas de framework de mock lourd : préférer des fakes écrits à la main (une struct implémentant l'interface) ou `moq`/`mockery` généré, selon ce qui existe déjà dans le package. Rester cohérent par package.

---

## 7. Accès aux données

### 7.1 PostgreSQL (plan de contrôle)

**[MUST]** Driver `pgx/v5` (pool `pgxpool`). Pas de `database/sql` sur les chemins chauds. `sqlc` génère le code type-safe depuis les requêtes ; les requêtes dynamiques rares sont écrites à la main avec des paramètres liés.

**[MUST]** **Toujours** des requêtes paramétrées. Jamais de concaténation de chaîne pour construire du SQL — c'est une faille d'injection, détectée par `gosec` et refusée en revue.

**[MUST]** Les IDs sont des `uuid.UUID` (google/uuid) générés en base (`uuidv7()`) pour le plan de contrôle. `message_id`/`trace_id` sont générés **côté application à l'ingestion** (avant Kafka), en UUIDv7 également — voir §7.4.

**[MUST]** Toute ligne ouverte est fermée ; toute transaction a un `defer tx.Rollback(ctx)` (le rollback après commit est un no-op). `rowserrcheck`/`sqlclosecheck` gardent la CI honnête.

### 7.2 Redis / Dragonfly (état opérationnel)

**[MUST]** Les opérations atomiques (token-bucket, réserve/capture/libère de crédit) sont des **scripts Lua** chargés une fois (`EVALSHA`), jamais une séquence read-modify-write côté Go. C'est la seule façon de garantir l'atomicité sous concurrence multi-pod.

**[MUST]** Chaque famille de clés a une politique de panne codée explicitement (§16). Une erreur Redis n'est jamais avalée : elle déclenche le mode dégradé prévu (fail-closed pour le débit, fail-open avec flag pour l'anti-spam à état, fail-closed pour le crédit strict).

**[SHOULD]** Les filtres de Bloom (numéros exacts, suppressions) sont en mémoire, rafraîchis par `config-sync`. Propriété invariante : **jamais de faux négatif**. Un « peut-être » lit Redis ; un « absent » court-circuite sans réseau. Documenter cette propriété là où le filtre est construit.

### 7.3 Kafka (plan de données)

**[MUST]** Producteur : `acks=all`, idempotence activée, la clé de partition suit strictement §4.1 du guide d'ingénierie — `mt.routed` est clé sur l'**ID de message logique** pour que tous les segments UDH aillent au même bind, dans l'ordre. Ne jamais publier sans clé sur un topic ordonné.

**[MUST]** Consommateur : commit d'offset **après** traitement réussi (at-least-once). Le code doit être idempotent en aval (la facturation l'est par `message_id`, §7.4). Ne jamais commit avant le travail.

**[MUST]** L'accusé au client (REST 202 / `submit_sm_resp` OK) n'a lieu qu'**après** confirmation d'écriture durable dans `mt.inbound` — c'est la frontière de durabilité (§6.7). Ne jamais acquitter avant.

**[SHOULD]** Les en-têtes Kafka portent `trace_id`, `message_id`, `account_id`, `customer_id`, `fallback_chain`. Le corps du message est dans le payload, jamais dans un en-tête loggable.

### 7.4 Idempotence de la facturation (§6.9)

Réserve (router) et capture (connector) encadrent un hop Kafka au moins une fois. L'idempotence repose sur `message_id` :

```go
// Reserve is idempotent: a duplicate message_id returns the existing hold, never a second debit.
// Enforced by a unique reservation key in Redis AND UNIQUE(message_id, entry_type) in the ledger.
func (e *Engine) Reserve(ctx context.Context, m *Message, credits int) (Reservation, error) {
    if credits == 0 || !e.enabled {
        return Reservation{}, nil // billing disabled: booleen check, no network call
    }
    // Lua: floor at 0 or -overdraft_limit; return existing hold if message_id already reserved.
    return e.reserveAtomic(ctx, m.LogicalID, m.OwnerKey(), Direction MT, credits)
}
```

**[MUST]** Ne jamais débiter deux fois sous retry. Tout chemin d'écriture de solde passe par la clé d'idempotence.

---

## 8. Le pipeline MT — règles de mise en œuvre

**[MUST]** L'ordre des étapes est celui de la spec (§6.1) et n'est pas réordonnable : E.164 → sender ID → opt-out → anti-spam → route → encodage/segmentation → débit → réserve crédit. La segmentation **précède** débit et crédit (le coût dépend du nombre de segments). L'opt-out est **bloquant** et précède le routage et la facturation.

**[MUST]** Le court-circuit L0 (numéro exact) ne saute **que** la résolution de route. Un test d'intégration vérifie qu'un message routé par numéro exact traverse quand même sender ID, opt-out, anti-spam, segmentation, débit et crédit. **Un raccourci de routage n'est jamais un contournement de conformité.**

**[SHOULD]** Chaque étape est une fonction pure sur un `*Message` enrichi + dépendances injectées, testable en isolation. Le pipeline est une composition de ces étapes, identique pour SMPP et REST (une seule implémentation).

**[MUST]** Émettre un span par étape (§12). Aucun span, log ou en-tête ne contient le corps — voir §11.

### 8.1 Scripts de routage embarqués (§6.2)

**[MUST]** Runtime `goja` (JS) principal / `gopher-lua` (Lua), en processus, **pool de runtimes réutilisés** (pas d'allocation par message). Garde primaire = **plafond d'instructions/bytecode** (déterministe), timeout mur en filet, plafond mémoire. Aucun accès réseau ni fichier exposé au script. Toute violation → repli déclaratif + log + métrique. L'état du runtime est réinitialisé entre invocations (isolement inter-comptes).

---

## 9. SMPP — codec et machine à états

**[MUST]** Le codec PDU vit dans `internal/smpp`, sans dépendance vers `pipeline` ni `storage`. Il encode/décode SMPP v3.4 (v5.0 optionnel), support TLV pour UDH et payload > 254 o. Pas de `replace_sm`/`data_sm` (non supportés, §5.1).

**[MUST]** La fenêtre (`window_size`) borne les `submit_sm` en vol non acquittés par bind ; elle est distincte du token-bucket métier. La machine à états de session (bind → bound → unbinding → closed) est possédée par la goroutine de la connexion.

**[MUST]** `enquire_link` : timer par bind, N réponses manquées → unbind forcé, jeton de session libéré immédiatement dans le registre.

**[SHOULD]** Les profils vendeur (`vendor_profile`) pré-remplissent les champs du connecteur ; les valeurs explicites priment. Isoler les adaptations par vendeur derrière l'abstraction de connecteur — ne pas parsemer le pipeline de `if vendor == ...`.

---

## 10. Configuration et démarrage

**[MUST]** Configuration lue au démarrage depuis l'environnement (12-factor) ; les secrets (mots de passe SMSC, clés) viennent d'un gestionnaire de secrets, jamais du dépôt. Validation stricte au boot : un service refuse de démarrer avec une config invalide (`log.Fatal` — le seul endroit toléré).

**[MUST]** Aucun secret, mot de passe, clé API ou corps de message dans les logs, y compris au niveau debug. `gosec` et une revue humaine gardent cette ligne.

**[SHOULD]** Les valeurs par défaut du domaine (intervalles `enquire_link`, tailles de fenêtre, seuils de disjoncteur) reflètent le DDL ; ne pas les redéfinir en dur dans le code — les lire de la configuration du connecteur.

---

## 11. Sécurité et confidentialité — invariants testables

**[MUST]** **Le corps du message n'apparaît jamais dans un log ni un span**, sous aucune politique de stockage ni aucun environnement (§6.11/§6.23). C'est un invariant, pas un réglage. Un type dédié empêche les fuites accidentelles :

```go
// Body wraps the message content. Its String()/MarshalJSON are redacted so it can never leak
// through %v, structured logs, or a span attribute. Access the plaintext only via Reveal().
type Body struct{ b []byte }

func (Body) String() string           { return "[REDACTED]" }
func (Body) MarshalJSON() ([]byte, error) { return []byte(`"[REDACTED]"`), nil }
func (b Body) Reveal() []byte          { return b.b } // explicit, greppable, audited call sites
```

**[MUST]** Un test d'invariant vérifie que sérialiser un `*Message` (log JSON, attribut de span) ne contient jamais le clair. Ce test est bloquant en CI.

**[MUST]** Les identifiants ne sont stockés qu'en hash (bcrypt/argon2 pour les mots de passe de bind et clés API). Le secret n'est retourné qu'à la création/rotation. Comparaisons en temps constant (`subtle.ConstantTimeCompare`) pour les vérifications d'auth.

**[MUST]** Anti-brute-force au bind/auth : compteur par `system_id` et IP (Redis TTL), backoff, événement de sécurité auditable (§6.3).

**[MUST]** Le chiffrement de contenu (§6.23) utilise l'enveloppe KMS + clé par client ; le clair n'existe qu'en mémoire transitoire. Le crypto-shred (destruction de clé) est l'un des chemins d'effacement RGPD.

---

## 12. Observabilité dans le code

**[MUST]** Logs structurés via `log/slog`, en JSON, avec `trace_id`/`message_id`/`account_id` en attributs — **jamais** le corps. Niveaux : `error` (action requise), `warn` (dégradation), `info` (événement métier notable), `debug` (désactivé en prod). Pas de `fmt.Println`.

**[MUST]** OpenTelemetry : un span par étape du pipeline, nommé de façon stable (`pipeline.senderid_auth`, `pipeline.route_resolve`, `connector.submit_sm`). Attributs = identifiants et décisions, jamais le corps ni un secret. Échantillonnage 100 % sur erreur/rejet/timeout.

**[MUST]** Métriques Prometheus : compteurs et histogrammes avec des labels **bornés** (compte, connecteur, route, statut). **Jamais** de label à cardinalité non bornée (MSISDN, `message_id`, corps). Le groupe client n'est pas un label — il se dérive du compte (§6.17).

Cette règle est **appliquée, pas seulement écrite** : `OpsServer.Registry()` renvoie un registre gardé (`internal/observability/metrics`) qui contrôle deux fois — il **refuse l'enregistrement** d'un collecteur dont un label déclaré sort du vocabulaire borné, et il **écarte à la collecte** une famille dont un label réellement exposé en sort (un collecteur écrit à la main peut déclarer un label et en émettre un autre : Prometheus identifie un `Desc` par son nom et ses labels constants, il ne le voit pas). Les services enregistrent au démarrage avec `MustRegister` : un mauvais label fait donc échouer le boot, dans n'importe quel service, au lieu de gonfler un TSDB de production. Élargir la liste blanche ne suffit pas à contourner la garde — la liste noire (MSISDN, `message_id`, corps, adresses…) est consultée **avant**. Question à se poser avant d'ajouter un label : *qui choisit cette valeur ?* Si la réponse est « l'expéditeur », ce n'est pas un label.

**[SHOULD]** Déclarer une métrique transverse dans le **catalogue** (`internal/observability/metrics.Catalog`) plutôt que dans un `main.go` : un nom choisi deux fois avec des labels différents est inrequêtable, et `customer_id` sur un histogramme se paie en `buckets × clients` (règle : `customer_id` sur compteurs et jauges, jamais sur un histogramme).

**[SHOULD]** Exposer les métriques métier clés : latence d'ingestion, latence bout-en-bout, taux de rejet par cause, profondeur de file par topic, état de disjoncteur par connecteur, taux de timeout de script, fraîcheur du cache de solde.

---

## 13. Tests

**[MUST]** Table-driven tests idiomatiques Go pour la logique de domaine (étapes du pipeline, résolution de route, calcul de segments, formule de crédit). Couverture significative sur `pipeline`, `routing`, `billing`, `smpp` (le codec surtout).

**[MUST]** `go test -race ./...` en CI. Aucune fusion sans CI vert.

**[MUST]** Tests d'invariants bloquants : (a) le corps ne fuit dans aucune sérialisation ; (b) un message routé par numéro exact traverse toutes les étapes de conformité ; (c) la facturation est idempotente sous double livraison d'un même `message_id` ; (d) `max_sessions` refuse le bind au-delà du quota.

**[SHOULD]** Tests d'intégration avec `testcontainers-go` (Postgres, Redis, Kafka, ClickHouse) pour les repositories et les flux bout-en-bout. Un SMSC simulé (voir la spec du simulateur SMSC) sert de pair de connecteur.

**[SHOULD]** Fuzzing (`go test -fuzz`) sur le décodeur PDU SMPP et le parseur/segmenteur d'encodage — ce sont des surfaces d'entrée non fiables.

**[SHOULD]** Benchmarks (`testing.B`) sur le chemin chaud (résolution de snapshot, token-bucket, réserve de crédit) pour garder les budgets de latence (§1.2) sous contrôle. Comparer avec `benchstat`.

---

## 14. Performance — règles pragmatiques

**[SHOULD]** Optimiser sur mesure, pas au jugé : profiler (`pprof`) avant d'optimiser. Le chemin chaud est le pipeline MT ; le reste tolère la simplicité.

**[MUST]** Zéro allocation évitable sur le chemin par message : lecture d'instantané par pointeur atomique (pas de copie), runtimes de script poolés, filtres de Bloom en mémoire, réutilisation de buffers (`sync.Pool`) pour l'encodage PDU quand le profil le justifie.

**[SHOULD]** Précompiler les regex (matching de contenu, sender pattern) dans l'instantané, jamais `regexp.MustCompile` par message. Le préfixe-trie de destination est O(longueur du préfixe).

**[SHOULD]** Borner les pools et fenêtres depuis la configuration (fenêtre SMPP, `bind_pool_size`, parallélisme consumer) ; ne jamais laisser un fan-out non borné consommer la mémoire au pic.

---

## 15. Dépendances

**[SHOULD]** Bibliothèque standard d'abord. Une dépendance externe se justifie par un gain net et une maintenance saine. Piliers acceptés : `pgx`, `go-redis`, un client Kafka (`franz-go` recommandé pour les perfs et le contrôle des offsets), `goja`, `gopher-lua`, `google/uuid`, `prometheus/client_golang`, `go.opentelemetry.io/otel`, `golang.org/x/sync`.

**[MUST]** `go.mod`/`go.sum` à jour, `go mod tidy` propre. Scan de vulnérabilités (`govulncheck`) en CI. Pas de dépendance non maintenue ni de fork ad hoc sans décision d'équipe.

---

## 16. Politiques de panne (matrice de référence)

Chaque dépendance dégradée a un comportement **codé explicitement**, jamais implicite. Le tableau ci-dessous est la source de vérité côté code (spec §6.4/§6.5/§6.9/§6.12) :

| Dépendance en panne | Sous-système | Comportement | Raison |
|---|---|---|---|
| Redis (rate-limit) | débit | **fail-closed** : plafond technique statique local du connecteur | ne jamais envoyer sans borne |
| Redis (anti-spam à état) | dédup/vélocité/réputation | **fail-open avec flag** ; les règles de contenu statiques continuent | disponibilité > précision, traçable |
| Redis (cache de solde) | crédit MT strict | **fail-closed** pendant la réhydratation depuis Postgres | garantie de zéro dépassement |
| Facturation (globale) | crédit | **fail-open** par défaut ; fail-closed opt-in par plateforme/client | l'envoi ne dépend pas de la facturation |
| SMSC (connecteur dégradé) | envoi | disjoncteur ouvre → `fallback_chain` → dead-letter borné | isoler un opérateur défaillant |
| Kafka | pipeline | pas d'accusé tant que non durable ; aucune perte après ACK | frontière de durabilité |

**[MUST]** Toute nouvelle dépendance sur un chemin de requête arrive avec sa politique de panne documentée et testée. Une dépendance sans politique de panne explicite ne passe pas la revue.

---

## 17. Revue de code — checklist

Avant d'approuver une PR, le relecteur vérifie : `gofmt`/lint verts et `-race` propre ; `context` propagé, aucune goroutine sans condition d'arrêt ; erreurs enrobées avec `%w` et mappées une seule fois à la frontière ; aucune requête SQL concaténée ; opérations Redis critiques en Lua atomique ; politique de panne explicite pour toute dépendance touchée ; **aucun corps/secret dans un log, span ou label** (invariant) ; ordre des étapes du pipeline préservé ; idempotence de facturation intacte ; tests couvrant le cas nominal, les cas d'erreur mappés et les invariants ; pas de label Prometheus à cardinalité non bornée ; commentaires de doc à jour sur les symboles exportés.

---

## 18. Ce qu'on ne fait pas

Pour éviter les débats récurrents : pas de framework HTTP lourd là où `net/http` + un routeur léger suffisent ; pas d'ORM masquant le SQL (on utilise `sqlc`) ; pas de state global mutable ; pas de `init()` à effet de bord réseau ; pas de goroutine non supervisée ; pas de rotation automatique d'identifiant (elle est manuelle, §6.3) ; pas de logique de campagne/programmation d'envoi (hors périmètre, §1.2bis) ; pas de corps de message hors de la mémoire transitoire et du CDR chiffré.
