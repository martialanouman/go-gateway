# Convention de style Go — Passerelle SMS

**Composant :** Passerelle SMS principale (Go)
**Statut :** Convention de style v1.0
**Portée :** ce guide fixe **l'apparence du code** — nommage, formatage, imports, godoc, ordre des déclarations, idiomes. Les décisions d'**architecture et de patterns** (concurrence, gestion d'erreurs enrobées, accès données, tests, politiques de panne) sont dans `guide-codage-go.md` ; ce document ne les répète pas, il y renvoie.

> *La prose est en français ; le code, les identifiants et les commentaires de code sont en anglais.*

Base de référence, dans cet ordre de priorité : ce document, puis [Google Go Style Guide](https://google.github.io/styleguide/go/), [Go Code Review Comments](https://go.dev/wiki/CodeReviewComments) et [Effective Go](https://go.dev/doc/effective_go). Règles **[MUST]** vérifiées en CI/revue ; **[SHOULD]** fortement recommandées.

---

## 1. Formatage

**[MUST]** Tout fichier est passé à `gofmt` via `goimports`. Le CI rejette tout diff non formaté (`gofmt -l` doit être vide). Le formatage n'est jamais un sujet de revue : l'outil tranche.

**[MUST]** Indentation par tabulations (imposé par `gofmt`). Ne jamais réaligner à la main.

**[SHOULD]** Ligne cible 99 colonnes, plafond souple 120. `gofmt` ne coupe pas les lignes longues : si une ligne dépasse, extraire une variable ou un paramètre nommé plutôt que de la laisser filer.

**[SHOULD]** Une ligne vide sépare les groupes logiques dans une fonction ; pas de doubles lignes vides. Pas de ligne vide juste après une accolade ouvrante ni juste avant une fermante.

---

## 2. Nommage

### 2.1 Règles générales

**[MUST]** `MixedCaps` / `mixedCaps`, jamais de `snake_case` ni de `SCREAMING_CASE` pour les identifiants Go. Les `snake_case` n'existent que dans les colonnes SQL, les clés JSON et les tags — le mapping fait le pont.

**[MUST]** La visibilité vient de la casse. On n'exporte (`PascalCase`) que ce qui doit l'être ; tout le reste est `camelCase`. Pas de préfixe/suffixe de visibilité (`internalFoo`, `FooPublic`).

**[MUST]** Les acronymes gardent une casse homogène : `ID`, `URL`, `API`, `SMPP`, `PDU`, `MSISDN`, `TON`, `NPI`, `MCC`, `MNC`, `TLV`, `UDH`, `HTTP`, `TLS`, `DLR`, `MO`, `MT`. Donc `messageID`, `smppPDU`, `parseURL`, `httpClient`, `destMSISDN`, `mccMNC`, `senderID`. Jamais `messageId`, `SmppPdu`, `HttpClient`.

**[SHOULD]** La longueur d'un nom est proportionnelle à sa portée. Un index de boucle est `i` ; un receveur est court (§2.4) ; une variable de package ou un champ exporté est explicite. Éviter les noms redondants avec le type (`var users []User`, pas `var userList []User`).

**[MUST]** Pas de « bruit » : pas de `util`, `common`, `helpers`, `base`, `manager`, `data`, `info`, `object` comme nom de package ou de type structurant. Nommer par le rôle (`routing`, `throttle`, `Reservation`).

### 2.2 Packages

**[MUST]** Nom de package court, en minuscules, un seul mot, sans underscore ni majuscule : `routing`, `smpp`, `billing`, `clickhouse`. Le nom est un préfixe d'appel — viser la lisibilité au point d'usage : `routing.Resolve`, pas `routing.RoutingResolve` (pas de bégaiement package/symbole).

**[SHOULD]** Éviter que le nom du package se répète dans ses symboles : dans `billing`, préférer `billing.Engine` et `billing.Reserve`, pas `billing.BillingEngine`.

**[MUST]** Le fichier `doc.go` porte le commentaire de package quand celui-ci dépasse quelques lignes.

### 2.3 Constantes et énumérations

**[MUST]** Une énumération est un **type nommé** sur `string` (lisible dans les logs/CDR) ou, si la performance l'exige, un entier avec `iota`. Valeurs préfixées par le type :

```go
// BindType is the SMPP bind kind negotiated on a session.
type BindType string

const (
    BindTX  BindType = "tx"
    BindRX  BindType = "rx"
    BindTRX BindType = "trx"
)
```

**[MUST]** Les valeurs string des énumérations correspondent **exactement** aux valeurs du DDL et des specs OpenAPI (`tx`/`rx`/`trx`, `mt`/`mo`, `stored_encrypted`…). Une divergence est un bug de contrat.

**[SHOULD]** Fournir une méthode `Valid()` ou un `Parse<Type>` pour les énumérations venant d'une entrée externe, plutôt que de disperser des `switch` de validation.

### 2.4 Receveurs

**[MUST]** Nom de receveur court (1–2 lettres), dérivé du type, **cohérent** sur toutes les méthodes d'un type : `func (e *Engine) …`, `func (s *Session) …`, `func (r *Router) …`. Jamais `this`, `self`, ni `me`.

**[MUST]** Cohérence pointeur/valeur : si une méthode a un receveur pointeur, toutes en ont un. Les types avec état mutable, un mutex, ou passés en I/O utilisent un receveur pointeur.

### 2.5 Interfaces

**[SHOULD]** Interface d'une méthode nommée par l'agent : `-er` (`Router`, `Reserver`, `SuppressionChecker`). Les interfaces sont petites et définies **côté consommateur** (détaillé dans le guide de codage, §6) — ici on ne fixe que le nommage.

### 2.6 Erreurs

**[MUST]** Variables sentinelles préfixées `Err` : `ErrRecipientOptedOut`, `ErrInsufficientCredit`. Types d'erreur suffixés `Error` : `type RouteError struct{…}`.

**[MUST]** Chaîne d'erreur en minuscule, sans ponctuation finale, sans « error »/« failed » (détaillé dans le guide de codage, §4). Style : `"reserve credit: insufficient balance"`.

---

## 3. Imports

**[MUST]** `goimports` gère les imports en **trois groupes** séparés par une ligne vide, dans cet ordre : (1) bibliothèque standard, (2) dépendances tierces, (3) packages internes du module. Configuré via `-local github.com/org/sms-gateway`.

```go
import (
    "context"
    "fmt"

    "github.com/jackc/pgx/v5"
    "go.opentelemetry.io/otel"

    "github.com/org/sms-gateway/internal/platform/e164"
    "github.com/org/sms-gateway/internal/routing"
)
```

**[MUST]** Pas d'import à effet de bord non justifié. Un import `_` (blank) est autorisé uniquement pour un driver/enregistrement, avec un commentaire expliquant pourquoi.

**[SHOULD]** Pas d'alias d'import sauf collision de nom ou nom de package non idiomatique. Quand un alias est nécessaire, il est court et clair.

**[MUST]** Interdiction du dot-import (`import . "…"`) hors fichiers de test très spécifiques (et même là, à éviter).

---

## 4. Commentaires et godoc

**[MUST]** Tout symbole **exporté** a un commentaire godoc, commençant par le nom du symbole et formant une phrase complète :

```go
// Reserve holds `credits` on the MT balance of the message owner and returns the reservation.
// It is idempotent by message ID and a no-op when billing is disabled. See §6.9.
func (e *Engine) Reserve(ctx context.Context, m *Message, credits int) (Reservation, error) {
```

**[MUST]** Le commentaire explique le **pourquoi**, les invariants et les effets de bord — pas la paraphrase de la signature. Un commentaire qui répète le code est du bruit.

**[SHOULD]** Les invariants critiques du projet sont annotés en clair là où ils sont maintenus (ex. « never logs the body », « no false negatives », « one active script per scope »), avec la référence de spec (`§6.11`). Ces annotations sont la mémoire de conception.

**[MUST]** Les marqueurs `TODO`/`FIXME` sont suivis d'un identifiant traçable : `// TODO(martial): borne le fan-out — TICKET-123`. Pas de TODO anonyme.

**[SHOULD]** Pas de code commenté laissé dans l'arbre : on supprime, l'historique Git garde la trace.

---

## 5. Déclarations et structure d'un fichier

**[SHOULD]** Ordre de lecture décroissant : dans un fichier, le type ou la fonction exportée principale vient en premier, ses aides privées ensuite. Le lecteur voit l'API avant les détails.

**[SHOULD]** Regrouper les déclarations liées dans un bloc `const (…)` / `var (…)` unique par thème plutôt que des lignes éparses.

**[MUST]** Constructeur nommé `New<Type>` (ou `New` si le package n'expose qu'un type), retournant `*Type` concret (accepter des interfaces, retourner des structs — guide de codage §6). Un type doit être utilisable proche de son zéro-value quand c'est raisonnable ; sinon le constructeur est le seul point d'initialisation.

**[MUST]** Pas de variable de package mutable exportée (pas de singleton global). Les valeurs de package sont des constantes ou des valeurs immuables.

---

## 6. Structs, champs et tags

**[SHOULD]** Champs regroupés par cohésion, pas par type. Un commentaire de champ tient sur la même ligne s'il est court.

**[MUST]** Tags cohérents et en `snake_case`, alignés sur le contrat externe. JSON en `snake_case` (comme les specs OpenAPI), colonnes DB en `snake_case` (comme le DDL) :

```go
type SmppAccount struct {
    ID              uuid.UUID `json:"id"              db:"id"`
    CustomerID      uuid.UUID `json:"customer_id"     db:"customer_id"`
    MaxSessions     int       `json:"max_sessions"    db:"max_sessions"`
    SenderIDPolicy  string    `json:"sender_id_policy" db:"sender_id_policy"`
}
```

**[MUST]** Un secret (mot de passe, clé API, corps de message, clé de contenu) n'est jamais exposé par un tag `json` sérialisable en clair. On utilise le type `Body` masquant et l'omission explicite (guide de codage §11). Champ sensible → `json:"-"` ou type dédié.

**[SHOULD]** Les booléens sont nommés positivement et sans double négation : `smppEnabled`, `autoReconnectEnabled` — pas `disabledSMPP`.

---

## 7. Idiomes imposés

**[MUST]** Retour anticipé (early return) plutôt qu'imbrication : gérer l'erreur et sortir, garder le chemin nominal non indenté.

```go
acct, err := repo.Account(ctx, id)
if err != nil {
    return fmt.Errorf("load account %s: %w", id, err)
}
if !acct.SMPPEnabled {
    return ErrChannelDisabled
}
// happy path continues, un-indented
```

**[MUST]** `context.Context` est toujours le premier paramètre, nommé `ctx` ; jamais stocké dans une struct (détaillé dans le guide de codage, §5).

**[SHOULD]** Préférer `any` à `interface{}` (Go ≥ 1.18). Éviter `any` sur le chemin chaud du pipeline — préférer des types concrets.

**[MUST]** Vérifier les erreurs immédiatement ; ne jamais les ignorer silencieusement. Un ignore volontaire est annoté : `_ = conn.Close() // best-effort on teardown`.

**[SHOULD]** Utiliser le zéro-value utilement : un `sync.Mutex`, un `bytes.Buffer` sont utilisables sans initialisation. Ne pas écrire de constructeur qui ne fait que mettre des zéros.

**[SHOULD]** Slices : préférer `var s []T` (nil slice) à `s := []T{}` quand la sémantique « vide » suffit. Préallouer avec `make([]T, 0, n)` quand la taille est connue (chemin chaud).

**[MUST]** Formatage de log/erreur : jamais `fmt.Println`/`log.Printf` — uniquement `log/slog` structuré (guide de codage §12).

---

## 8. Anti-idiomes (refusés en revue)

Sont systématiquement rejetés : le bégaiement package/symbole (`http.HTTPClient`) ; les noms `util`/`helper`/`manager`/`base` ; `this`/`self` en receveur ; les getters préfixés `Get` ; `panic` pour le contrôle de flux ; une variable globale mutable ; un `interface{}` là où `any` convient ; une abstraction à une seule implémentation créée « au cas où » ; un commentaire qui paraphrase la signature ; du code mort commenté ; un `else` après un `return` (early-return à la place) ; l'imbrication profonde évitable ; un `time.Sleep` en guise de synchronisation dans du code non-test.

---

## 9. Configuration du linter (source de vérité)

**[MUST]** Le style est **exécutable**, pas seulement documenté : `golangci-lint` avec `.golangci.yml` versionné. Le CI échoue sur toute alerte. Ensemble minimal :

```yaml
run:
  timeout: 5m
linters:
  enable:
    - gofmt          # formatting is law
    - goimports      # import grouping (local-prefix set below)
    - govet
    - staticcheck
    - revive         # naming / style (replaces golint)
    - errcheck       # every error handled
    - ineffassign
    - unused
    - unconvert
    - misspell
    - gocritic
    - gosec          # no injected SQL, no leaked secrets
    - bodyclose
    - contextcheck
    - noctx
    - rowserrcheck
    - sqlclosecheck
    - nakedret       # no naked returns in long funcs
    - prealloc
linters-settings:
  goimports:
    local-prefixes: github.com/org/sms-gateway
  revive:
    rules:
      - name: exported          # godoc on exported symbols
      - name: var-naming        # MixedCaps, acronym casing
      - name: receiver-naming   # consistent short receivers
      - name: context-as-argument
      - name: error-strings     # lowercase, no punctuation
issues:
  max-same-issues: 0
```

**[SHOULD]** `revive` porte l'essentiel des règles de nommage de ce document ; quand une règle de style est ajoutée ici, on cherche d'abord à la faire porter par le linter avant de compter sur la revue humaine.

---

## 10. Résumé — la revue de style en dix points

Un relecteur vérifie, pour le style seul : `gofmt`/`goimports` verts ; nommage `MixedCaps` avec casse d'acronymes correcte (`messageID`, `smppPDU`) ; pas de bégaiement ni de `util`/`manager` ; receveurs courts et cohérents ; godoc présent et utile sur l'exporté (le *pourquoi*) ; imports en trois groupes ; énumérations typées alignées sur DDL/OpenAPI ; tags `snake_case` corrects et aucun secret sérialisable ; early-return sans `else` superflu ; aucun anti-idiome du §8. Le fond (concurrence, erreurs, tests, politiques de panne) relève du guide de codage.
