# step-260j — Un seul codec de curseur keyset

> **Jalon :** Audit du 2026-09-03 (correctifs) · **Statut :** LIVRÉE (2026-09-05)
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
- `TestUnroutedCursorRoundTripPreservesMicroseconds` teste les helpers internes qui vont disparaître.
  Les tests handler ne gardaient pas la précision du site d'appel : le grand livre avec un horodatage
  aligné à la seconde, les MO avec une fixture qui positionne par `ID` seul (`fakes_test.go:802-806`)
  et ignore l'horodatage reçu. Une précision ms glissée au site d'appel passait inaperçue.

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
- **Un seul chemin d'erreur** : les quatre sites répondent `humaerr.FailValidation("invalid cursor",
  FieldError{cursor})`. Le plan prévoyait `FromError(err)` ; la revue a rappelé que le guide §11.1 et la
  réponse `ValidationError` du contrat (« see errors[] ») promettent `errors[]` sur une erreur de
  validation — deux des quatre sites le faisaient déjà, les deux autres le gagnent. Le message reste
  fixe (« malformed page cursor ») : le détail du codec (base64, séparateur…) n'est pas exposé à un
  client qui sonde. L'enveloppe `errs.ErrValidation` de `keyset.Decode` reste : un futur appelant qui
  passerait par `FromError` obtient un 422 par défaut.
- `list-unrouted-mo` répondait 422 sans le déclarer au contrat : déclaré (`4.1.0 → 4.2.0`, mineur) et
  côté huma (`Errors`). Trouvé en vérifiant le critère « même 422 sur les quatre endpoints ».
- `keyset.Precision` : le zéro n'est pas une précision (`Milli = iota + 1`) — une valeur oubliée dans
  une struct panique dans `Encode` au lieu de tronquer les µs en silence.
- Les tests handler gagnent des horodatages **non alignés** (µs `…789`) et la fixture des MO capture la
  position reçue, pour que la précision du site d'appel soit gardée là où elle se décide.

**Écarts avec le plan** : quatre sites de décodage, pas trois (recherche Admin et liste publique
partagent le codec CDR) ; neuf commits au lieu de cinq ; `unrouted_mo_cursor_test.go` supprimé
(remplacé par la garde handler) ; un bump de contrat que le plan n'avait pas (le 422 non déclaré).

## Chaîne de preuves

1. `internal/platform/keyset/keyset_test.go`, rouge par « paquet inexistant » : aller-retour ms (le
   sous-ms est tronqué) et µs (conservé) ; **vecteurs connus** calculés à la main pour
   `2026-09-03T10:00:00.123456Z|018f6a2e-1c3b-7c4d-8e5f-0a1b2c3d4e5f` en µs et en ms, encodés ET
   décodés (pas de test circulaire) ; malformés (base64, séparateur, horodatage, UUID) →
   `errors.Is(err, errs.ErrValidation)`.
2. Migration appelant par appelant, suite verte à chaque commit : `cursor_test.go`, les tests de
   pagination du grand livre, des MO, de la recherche et de la liste, et les trois tests 422.
3. Mutations vues tomber : `Decode` en µs pour `Milli` → les vecteurs connus et l'aller-retour ms ;
   enveloppe `ErrValidation` retirée → `TestDecodeRejectsMalformedTokensAsValidationErrors` (et, tant
   que les sites passaient par `FromError`, les quatre tests 422 ; avec `FailValidation` la garde est
   le test du paquet seul) ; `Micro`→`Milli` au site du grand livre ou des MO → le test de pagination
   correspondant tombe sur l'horodatage à la microseconde.

## Commits

1. Cette fiche.
2. `keyset` : paquet + tests.
3. `clickhouse` : le codec CDR délègue.
4. `adminapi` : grand livre.
5. `adminapi` : MO non routés.
6. `adminapi`, `restapi` : recherche et liste (convergence).
7. `api` : `list-unrouted-mo` déclare son 422, `4.2.0`.
8. Revue : `FailValidation` aux quatre sites, `Errors` de l'opération, doc et zéro de `keyset`.
9. Fiche → `tasks-done/`.

## Definition of Done

- [x] `make check` vert (86 paquets, deux passes : avant et après les correctifs de revue)
- [x] une seule implémentation de `base64url(ts|uuid)` dans `internal/` (`grep RawURLEncoding` :
      keyset, le codec `cp1` du plan de contrôle, les clés API — rien d'autre)
- [x] un curseur émis avant la PR se décode après (vecteurs connus, formats comparés octet par octet
      par la revue)
- [x] un curseur malformé donne le même 422 avec `errors[cursor]` sur les quatre endpoints, tous
      déclarés au contrat

## Revue

Un sous-agent en lecture seule, aucun bloquant. Required corrigés : la convergence vers `FromError`
contredisait le guide §11.1 sur `errors[]` (→ `FailValidation`) ; `list-unrouted-mo` ne déclarait pas
son 422 (→ contrat 4.2.0 + `Errors`) ; le doc de `keyset` décrivait une répétition de lignes impossible
sur un keyset DESC (mécanisme inventé, corrigé) ; la fiche cachait ses écarts avec le plan et décrivait
le mauvais trou du test des MO. Nits retenus : zéro de `Precision` invalide, commentaire narratif retiré.

## Hors périmètre

`controlplane/cursor.go` (format différent, versionné). `exact_routes.go` (MSISDN en clair) : à
traiter avec step-390 ou seul.
