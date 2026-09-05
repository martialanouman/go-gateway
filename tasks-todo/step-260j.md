# step-260j — Un seul codec de curseur keyset

> **Jalon :** Audit du 2026-09-03 (correctifs) · **Statut :** EN COURS (2026-09-05)
> **Dépend de :** — · **Bloque :** —

## Pourquoi cette fiche existe

L'audit du 2026-09-03 a compté quatre codecs de curseur : le grand livre
(`internal/adminapi/billing_admin.go:248-273`, µs), les MO non routés
(`internal/adminapi/unrouted_mo.go:138-169`, µs), le CDR (`internal/storage/clickhouse/cursor.go`, ms,
verrouillé par `toUnixTimestamp64Milli` dans `cdr.go:532,640`) et le plan de contrôle
(`internal/controlplane/cursor.go`, UUID seul, versionné `cp1`). Les trois premiers sont la même
fonction `base64url(ts|uuid)` copiée trois fois, et leurs erreurs de décodage deviennent un 422 par
**quatre** chemins différents (`FailValidation` avec champ au grand livre et à la recherche,
`huma.Error422UnprocessableEntity` aux MO, `FromError(errs.ErrValidation)` à la liste publique).

## Ce que l'exploration a établi

- Quatre sites de décodage, pas trois : `billing_admin.go:227`, `unrouted_mo.go:105`,
  `messages_search.go:165`, `restapi/messages_list.go:66` (recherche et liste partagent le codec CDR).
- Les trois codecs sont identiques au séparateur `|` et à base64url sans padding près ; seule la
  précision diffère (µs pour les deux colonnes `timestamptz` Postgres, ms pour `DateTime64(3)`).
- Le codec du plan de contrôle est différent (UUID seul, versionné, cas « première page ») et type
  déjà ses erreurs `errs.ErrValidation` : il reste tel quel. `exact_routes.go` pagine sur le MSISDN en
  clair (`cursorString`) : cinquième schéma, hors périmètre, noté.
- `TestUnroutedCursorRoundTripPreservesMicroseconds` teste les helpers internes qui vont disparaître ;
  `TestGetLedgerReturnsPageWithNextCursor` et `TestListUnroutedMOPaginates` décodent `next_cursor` à
  travers le handler mais avec des horodatages **alignés à la seconde** : ils ne verraient pas une
  précision ms glissée au site d'appel. C'est le trou que la suppression des helpers rend visible.

## Design arrêté

Nouveau paquet `internal/platform/keyset` (voisin de `uuidx`) :

```go
type Precision int // Milli, Micro
type Key struct{ At time.Time; ID uuid.UUID }
func Encode(k Key, p Precision) string
func Decode(s string, p Precision) (Key, error) // toute erreur enveloppe errs.ErrValidation
```

- La précision est un **paramètre** : le CDR reste en ms (couplé au SQL), les deux Postgres en µs.
  Séparateur `|` et base64url sans padding conservés : **les curseurs déjà émis restent valides**, pas
  de rupture de contrat, pas de bump.
- Les appelants deviennent des adaptateurs d'une ligne vers leur clé (`cp.LedgerKey`,
  `cp.UnroutedMOKey`, `clickhouse.CDRKey`) ; `EncodeCDRCursor`/`DecodeCDRCursor` restent exportés (4
  appelants) et délèguent.
- **Un seul chemin d'erreur** : les quatre sites répondent `humaerr.FromError(err)`. Le message devient
  « keyset: malformed cursor: validation_error », la forme des listings du plan de contrôle
  (`controlplane.DecodeCursor`) ; le grand livre et la recherche perdent leur détail `errors[cursor]`.
  Écart avec le plan : aucun (c'est le mécanisme prévu) ; consigné parce que c'est visible du client.
- Les tests handler gagnent des horodatages **non alignés** (µs `…789`) pour que la précision du site
  d'appel soit gardée là où elle se décide, à la place du test des helpers supprimés.

## Chaîne de preuves

1. `internal/platform/keyset/keyset_test.go`, rouge par « paquet inexistant » : aller-retour ms (le
   sous-ms est tronqué) et µs (conservé) ; **vecteurs connus** calculés à la main pour
   `2026-09-03T10:00:00.123456Z|018f6a2e-1c3b-7c4d-8e5f-0a1b2c3d4e5f` en µs et en ms, encodés ET
   décodés (pas de test circulaire) ; malformés (base64, séparateur, horodatage, UUID) →
   `errors.Is(err, errs.ErrValidation)`.
2. Migration appelant par appelant, suite verte à chaque commit : `cursor_test.go`, les tests de
   pagination du grand livre, des MO, de la recherche et de la liste, et les trois tests 422.
3. Mutations : `Decode` en µs pour `Milli` → `TestCDRCursorRoundTripsAtMillisecondPrecision` et le
   vecteur ms tombent ; enveloppe `ErrValidation` retirée → les tests 422 des quatre endpoints tombent
   (ils passent tous par `FromError`) ; `Micro`→`Milli` au site du grand livre ou des MO → le test de
   pagination correspondant tombe (horodatage non aligné).

## Commits

1. Cette fiche.
2. `keyset` : paquet + tests.
3. `clickhouse` : le codec CDR délègue.
4. `adminapi` : grand livre.
5. `adminapi` : MO non routés.
6. `adminapi`, `restapi` : recherche et liste sur `FromError`.
7. Fiche → `tasks-done/`.

## Definition of Done

- [ ] `make check` vert
- [ ] une seule implémentation de `base64url(ts|uuid)` dans `internal/`
- [ ] un curseur émis avant la PR se décode après (vecteurs connus)
- [ ] un curseur malformé donne le même 422 plat sur les quatre endpoints

## Hors périmètre

`controlplane/cursor.go` (format différent, versionné). `exact_routes.go` (MSISDN en clair) : à
traiter avec step-390 ou seul.
