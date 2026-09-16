# step-290 — Sécurité : gosec, govulncheck, secrets, piste d'audit

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** —

## But
Durcir la chaîne : scan d'injection `gosec`, gate `govulncheck` en CI, gestion des secrets, et une
piste d'audit consolidée des actions sensibles.

## Périmètre (ce que fait CETTE PR)
- `make lint`/CI : intégrer **gosec** (scan injection/mauvais usages) en plus de golangci-lint.
- `govulncheck` déjà présent (`make`) → en faire une **gate** bloquante documentée.
- Gestion des secrets (les défauts de développement sont déjà refusés en production par `config`,
  step-260f) : vérifier qu'aucun secret n'est en clair (hash pour mots de passe bind & clés API,
  §1.9), comparaison temps constant partout.
- Piste d'audit consolidée des actions opérateur/sensibles (réutilise les audits M10 : content, GDPR).

## Points d'implémentation clés
- **gosec** est un outil binaire (§16) — **`ctx7`** pour son intégration/exclusions justifiées ; ajouté à
  `make tools`.
- Vérifier les invariants de sécurité déjà posés : **SQL paramétré** (pas d'injection), secrets **hashés**
  (argon2id pour bind, SHA-256 pour clé API, §1.9), révélés une seule fois à la création/rotation.
- Aucune suppression d'alerte gosec sans justification en commentaire.
- La piste d'audit ne contient jamais de corps ni de secret en clair (invariant a).

## Tests (écrits dans la même PR)
- gosec passe (ou n'a que des exclusions justifiées) ; govulncheck vert.
- Un secret est bien hashé/comparé en temps constant (tests existants renforcés).
- Une action sensible produit une entrée d'audit.

## Hérité de la revue de step-250e (2026-09-02) — trois constats à trancher

Aucun n'est une régression de step-250e ; les deux premiers sont préexistants, le troisième est un
risque que cette step a rendu atteignable.

1. **Une cible `connector` lue depuis Redis n'est confrontée à rien.** `internal/routing/snapshot.go`
   vérifie l'appartenance d'une cible `route` au snapshot, mais renvoie une cible `connector` telle
   quelle. Qui écrit dans ce Redis peut donc détourner le trafic d'un MSISDN vers n'importe quel
   connecteur. Le modèle de menace le relativise — le même Redis porte les soldes de facturation et
   les token-buckets, il est dans la frontière de confiance — mais la posture mérite d'être écrite
   explicitement plutôt que subie.
2. **Aucun vecteur de hash `argon2` n'est épinglé.** Les tests de `internal/credential` hachent et
   vérifient avec la même version : un aller-retour circulaire. Si un futur bump de
   `golang.org/x/crypto` changeait la dérivation, ils resteraient **verts** pendant que tous les
   secrets déjà en base (mots de passe de bind SMPP, clés API) deviendraient invérifiables — panne
   totale d'authentification. Vérifié au moment du bump v0.53→v0.56 : le code d'`argon2` est identique
   entre les deux, donc le risque ne s'est pas matérialisé. Il n'est pas gardé pour autant.
3. **Une clé `exactroute:` de mauvais type boucle sans borne.** Un `WRONGTYPE` n'est ni `redis.Nil` ni
   une erreur de décodage : il est classé faute Redis, donc remonté, donc redélivré — sur la même clé,
   indéfiniment, et le TTL ne le borne pas puisque la clé n'expire pas d'elle-même. Toute la partition
   est figée jusqu'à un `DEL` manuel. Le traitement de la valeur illisible (guérie depuis la table)
   s'applique mot pour mot ; la distinction demande de reconnaître l'erreur `WRONGTYPE`, ce qui repose
   sur son texte — d'où le renvoi ici plutôt qu'un correctif fragile dans step-250e.

## Design arrêté

Arrêté le 2026-09-16, avant la première ligne de code. Q1 à Q3 sont tranchés par le modèle Fable, sans
conflit avec la spec ; les deux points restés ouverts (PII dans `target`, découpage) sont validés par
l'utilisateur.

### Ce que la lecture a changé à la fiche

- **gosec tourne déjà** : il est activé dans golangci-lint (`.golangci.yml`), comme l'exige
  `guide-codage-go.md` §71. **govulncheck est déjà une gate** : le job « Vulnerabilities » fait partie
  des vérifications exigées par le ruleset de `main`. Un binaire gosec séparé ferait double emploi avec
  les mêmes règles : on ne l'ajoute pas. Ce qui manque, c'est la preuve qu'aucune suppression n'est
  injustifiée, et l'épinglage de govulncheck (`@latest` des deux côtés).
- **Le jeton admin est un secret stocké en clair.** `StaticVerifier` pose le jeton brut dans
  `Principal.Subject`. `operatorSubject()` l'écrit ensuite dans la colonne `operator` de
  `content_access_audit`, `gdpr_erase_jobs` et `message_export_jobs`, et dans le log
  « content crypto-shred ».
- **Aucune mutation admin n'est tracée.** Pourtant, la spec (l.624) annonce `DELETE /suppressions/{id}`
  comme « audit-logged ».
- **`mutating()` (`configchange.go`) classe comme mutants deux POST de lecture**,
  `/billing-providers/{id}/test-connection` et `/suppressions/check`. Chacun publie donc un changement de
  config pour rien.

### Découpage : quatre PR, dans l'ordre a → b → c → d

**290a — Suppressions justifiées, govulncheck épinglé.**
- Activer `nolintlint` (`require-explanation`, `require-specific`), ainsi que `nosec-require-rules` et
  `nosec-require-justification` dans la config gosec.
- Supprimer les 8 `//nolint:gosec` que gosec ne justifie plus : G115 raisonne désormais sur les bornes.
  Réécrire le `// nolint:contextcheck` mal formé (`observability/ops.go`).
- `GOVULNCHECK_VERSION` est épinglé dans le `Makefile`, et la CI lit la même version.
- La CI documente que « Vulnerabilities » est une vérification exigée.
- Preuve par mutation, non commitée : une requête SQL construite par concaténation fait tomber
  `make lint`, et un `//nolint:gosec` sans explication le fait tomber aussi.

**290b — Une identité d'opérateur qui n'est pas un secret.**
- `auth.Fingerprint(token)` renvoie `"tok_"` suivi des 16 premiers caractères hexadécimaux de
  SHA-256(jeton). `StaticVerifier` pose cette empreinte dans `Subject`. Le format de
  `HTTP_ADMIN_TOKENS` ne change pas : step-310 le retire.
- Migration `0014` : les colonnes `operator` des trois tables sont réécrites en empreinte, sauf
  `'unknown'`. Le calcul se fait en SQL (`sha256(convert_to(…))`) et doit donner exactement la même
  empreinte que `auth.Fingerprint` : un test d'intégration compare les deux. Le down est un no-op
  documenté, puisqu'on ne restaure pas un secret.
- En production, `validateAdminConfig` (`cmd/admin-api-svc/wiring.go`) refuse un jeton de moins de
  32 caractères. La garde ne peut vérifier que la longueur, pas l'aléa, et son message le dit.
- Les logs déjà émis ne se nettoient pas. La PR le signale et recommande une rotation des jetons.

**290c — Piste d'audit consolidée.**
- Nouvelle table `control_plane.audit_log`, dans le schéma et dans une migration :
  - colonnes `id`, `operator`, `operation_id`, `method`, `target`, `request_id`, `status`, `at`,
    `finished_at` ;
  - `target` est le chemin résolu, **jamais** la query string. Les numéros qui figurent dans le chemin
    y sont stockés en clair : un audit qui ne dit pas quel numéro a été détourné ne sert à rien ;
  - jamais de corps de requête ou de réponse (invariant a, §1.9) ;
  - un `status` NULL signifie « issue non enregistrée », pas « succès » ;
  - index sur `(at)` et sur `(operator, at)` ;
  - pas de partition mensuelle : le volume est de l'ordre de dizaines d'actions par jour. C'est un écart
    assumé à la spec l.890.
- **Un seul middleware huma**, enregistré juste après `auth.Middleware` (huma enchaîne dans l'ordre
  d'enregistrement). Il s'applique à :
  - toute requête qui n'est pas une lecture ;
  - `search-messages` et `get-message-trace` quand le principal détient `msisdn:reveal`.

  `get-message-content` reste dans `content_access_audit`, sans double écriture.
- **L'intention est écrite avant le handler**, sous 5 s. Si l'écriture échoue, la requête reçoit un 503
  et le handler n'est pas appelé. C'est la généralisation de `recordGranted` : aucune action sensible
  sans trace. Le statut est écrit ensuite, en best-effort, dans un `defer` sous
  `context.WithoutCancel`.
- **Les 401/403 ne sont pas écrits en base.** Une écriture bloquante par requête non authentifiée serait
  un levier de DoS sur Postgres. `auth.Middleware` les logge en `Warn`, avec l'opération et
  `RemoteAddr`, jamais le jeton.
- Une seule fonction `readOnlyRequest(method, path)` sert à la fois à la publication de config
  (`!readOnly && !selfAnnouncing`) et à l'audit (`!readOnly`). Ses suffixes de lecture sont
  `/validate`, `/test`, `/test-connection` et `/check`, ce qui corrige les deux POST mal classés.
  `/exact-routes/import` est audité.
- `audit_log` rejoint les exclusions de `attestationScope` (`gdpr.go`) : le journal est immuable
  (spec l.909), et l'effacement RGPD ne le touche pas.
- **Aucun endpoint de lecture dans cette step.** `GET /audit-log` et le scope `audit:read` appartiennent
  au BFF du tableau de bord et à l'auth réelle (step-310). Aucun contrat ne bouge. Une fiche de suite est
  ouverte, et le runbook donne la requête SQL de lecture.

**290d — Constats hérités et secrets restants.**
1. **Cible `connector` lue depuis Redis.** On écrit la posture en addendum à ADR-0015 : Redis est dans la
   frontière de confiance, puisqu'il porte les soldes et les token-buckets. Écrire dans ce Redis revient
   déjà à pouvoir tout faire, donc on n'ajoute pas de contrôle d'appartenance, que le snapshot ne
   pourrait d'ailleurs pas porter (il n'a pas de registre de connecteurs).
2. **Vecteur argon2id épinglé.** Il est produit par **l'implémentation C de référence**
   (`phc-winner-argon2`), jamais par le code testé. Le test l'exerce par `VerifyBindPassword`, qui est
   le chemin des hashes déjà en base.
3. **Clé `exactroute:` de mauvais type.** `goredis.HasErrorPrefix(err, "WRONGTYPE")` est une API publique
   de go-redis, et `WRONGTYPE` est un code d'erreur du protocole RESP, pas du texte libre. Une telle clé
   suit donc le chemin de la valeur illisible : compteur de corruption, relecture depuis la table, puis
   `SET` qui écrase la clé quel que soit son type.
4. **`CONNECTOR_PASSWORD`.** La valeur par défaut `gateway` est refusée en production, sur le modèle de
   la garde ClickHouse.
5. **Clé API.** L'égalité SQL sur le hash n'est pas en temps constant, mais le temps de réponse ne
   renseigne que sur un préfixe du SHA-256 d'une clé de 256 bits, qu'aucun attaquant ne sait choisir.
   La justification est écrite au §1.9 du plan. `VerifyAPIKey`, qu'aucun chemin de production n'appelle,
   est supprimé.
6. **Fiches et renvois**, pour que ces dettes ne se perdent pas au go-live :
   - `smsc_connectors.password_hash` est inutilisable pour un bind sortant, qui doit envoyer le mot de
     passe en clair ;
   - `external_billing_providers.auth_config_json` est stocké en clair ;
   - la CLI `mt-replay` n'a ni authentification ni audit ;
   - un MSISDN apparaît dans le log d'échec d'attestation RGPD ;
   - `GET /audit-log`, et l'immuabilité de `audit_log` au niveau de la base ;
   - dans step-300, ajouter le gRPC `content-key-svc`, où la DEK circule en clair sans authentification.

   La fiche est déplacée en `tasks-done/` au dernier commit de 290d.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] gosec intégré ; govulncheck en gate ; audit sans secret/corps

## Hors périmètre
TLS/mTLS transport → step-300. Auth opérateur OIDC → step-310.
