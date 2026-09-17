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
- Activer `nolintlint` (`require-explanation`, `require-specific`).
- **Une seule syntaxe de suppression : `//nolint:gosec // raison`.** Une première version de ce design
  prévoyait aussi `nosec-require-rules` et `nosec-require-justification`. Une sonde a montré que le gosec
  embarqué dans golangci-lint v2.12.2 **ignore** ces deux clés : `// #nosec` sans règle ni raison
  passe. Arbitrage Fable : les 3 `#nosec` existants sont convertis au même endroit, et une garde de
  source (`internal/config/nosec_guard_test.go`, à côté de `sections_guard_test.go`) refuse tout
  `#nosec` dans les commentaires des `.go` non générés, `_test.go` compris. La revue a trouvé une
  seconde forme native, `//gosec:disable`, que gosec v2.26.1 accepte aussi : la garde refuse les deux.
  nolintlint juge alors toutes les suppressions ; il ne signale seulement pas comme inutile une
  directive qui vise un linter désactivé.
- Supprimer les directives qui ne suppriment plus rien. La sonde de lecture en comptait 8 (gosec seul) ;
  l'arbre réel en avait 27 : 10 contextcheck, 10 gosec, 3 noctx, 2 errcheck, et 2 errchkjson qui visaient
  un linter désactivé (trouvées en revue). Une raison qui dit ce que le code ne dit pas reste en
  commentaire. Réécrire le `// nolint:contextcheck` mal formé (`observability/ops.go`).
- `GOVULNCHECK_VERSION` est épinglé dans le `Makefile`, et la CI lit la même version.
- La CI documente que « Vulnerabilities » est une vérification exigée.
- Preuve par mutation, non commitée : une requête SQL construite par concaténation fait tomber
  `make lint`, et un `//nolint:gosec` sans explication le fait tomber aussi. Un `#nosec` ajouté fait
  tomber la garde.

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
- **Détail validé le 2026-09-16, avant le code :**
  - **Migration :** l'`UPDATE` laisse intacte toute valeur déjà au format `^tok_[0-9a-f]{16}$`. Si le
    nouveau binaire écrit des empreintes avant que la migration tourne, elles ne sont donc pas hachées
    une seconde fois. Le schéma ne change pas de structure : seuls les commentaires des trois
    colonnes `operator` disent « empreinte, jamais le jeton ».
  - **Test de migration :** il crée une base neuve dans le conteneur partagé, puisque `pgtest` en
    fournit une déjà migrée jusqu'au bout. Il applique `Steps(13)`, insère un jeton brut, `unknown` et
    une valeur déjà au format empreinte, puis applique `Steps(1)`. Il vérifie que le jeton brut vaut
    exactement `auth.Fingerprint`, et que les deux autres valeurs restent intactes.
  - **Vecteur de `Fingerprint` :** il est calculé hors Go (`shasum -a 256`), pour que le test ne soit
    pas circulaire.
  - **Message de la garde de longueur :** il nomme le numéro de l'entrée, jamais le jeton.
  - **Corrigé en revue :** l'ordre de déploiement n'est **pas** indifférent. Le job de migration tourne
    avant le rollout (`deploy/k8s/jobs/migrate-postgres.yaml`), si bien que les anciens pods peuvent
    encore écrire des jetons après la migration. L'`UPDATE` étant idempotent, la PR demande de le rejouer
    une fois le rollout terminé. Le filtre sur le format empreinte garantit seulement qu'aucune
    empreinte n'est hachée deux fois. En contrepartie, un jeton brut qui vaut exactement `unknown` ou a
    exactement la forme `tok_` + 16 caractères hexadécimaux reste tel quel : la rotation des jetons le
    couvre.
  - **Format de `HTTP_ADMIN_TOKENS` :** inchangé. Le dépôt n'a pas de runbook : la rotation est
    recommandée dans la PR.

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
  un levier de DoS sur Postgres. Ils ne sont pas non plus journalisés : retiré du plan le 2026-09-17 à la
  demande de l'utilisateur, faute d'alerte qui consommerait ces logs. À ajouter quand une telle alerte
  existera.
- Une seule fonction `readOnlyRequest(method, path)` sert à la fois à la publication de config
  (`!readOnly && !selfAnnouncing`) et à l'audit (`!readOnly`). Ses suffixes de lecture sont
  `/validate`, `/test`, `/test-connection` et `/check`, ce qui corrige les deux POST mal classés.
  `/exact-routes/import` est audité.
- `audit_log` rejoint les exclusions de `attestationScope` (`gdpr.go`) : le journal est immuable
  (spec l.909), et l'effacement RGPD ne le touche pas.
- **Aucun endpoint de lecture dans cette step.** `GET /audit-log` et le scope `audit:read` appartiennent
  au BFF du tableau de bord et à l'auth réelle (step-310). Aucun contrat ne bouge. Une fiche de suite est
  ouverte, et le runbook donne la requête SQL de lecture.
- **Détail validé le 2026-09-17, avant le code :**
  - **Refus d'audit : 503 non déclaré (arbitrage Fable).** Le contrat ne déclare aucune panne
    d'infrastructure opération par opération, pas même le 500 d'une panne Postgres. `humaspec.Prune`
    retire le 500/default de huma, et `recordGranted` renvoie déjà un 503 non déclaré. **Ne pas** ajouter
    503 aux `Errors` des opérations : `contract_test.go` compare l'ensemble exact des codes et
    échouerait. Le message est fixe ; le détail va dans un log `Error`.
  - **Migration :** `0015_audit_log`. `FinishAudit` n'écrit que si `status IS NULL` : une issue ne
    s'écrase pas. Dépôt `postgres.AuditLogRepo` avec `Begin(ctx, cp.AuditIntent) (uuid.UUID, error)` et
    `Finish(ctx, id, status)`. La requête de lecture est donnée dans le commentaire de schéma, puisque le
    dépôt n'a pas de runbook.
  - **Lectures sensibles :** `search-messages` et `get-message-trace` forment un ensemble local
    d'identifiants d'opération, et un test vérifie qu'ils existent dans la spec générée. Leur filtre
    vit dans la query string, qui n'est jamais stockée : la ligne dit qui a révélé et quand, pas quel
    numéro.
  - **Issue et panique :** l'issue est écrite dans un `defer`, sous `context.WithoutCancel` et 5 s. Une
    panique du handler est enregistrée comme 500, puis relancée.
  - **Suffixes de lecture :** `readOnlyRequest` ajoute `/test-connection` et `/check` ;
    `/exact-routes/import` reste exclu de la publication, mais il est audité.
  - **Test de câblage :** une requête à travers `newAdminApp` doit produire une ligne dans `audit_log`.
  - **Fiche de suite (`GET /audit-log`, immuabilité en base) :** elle est ouverte en 290d.

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
   - `GET /audit-log`, et l'immuabilité de `audit_log` au niveau de la base. Attention : l'écriture se
     fait en deux temps, donc « aucun UPDATE » ne suffit pas — il faut un trigger qui n'autorise que la
     transition `status IS NULL → NOT NULL`, plus un `REVOKE DELETE` ;
   - la **rétention** de `audit_log` : la spec veut 1 à 7 ans et une purge par partition, or la table
     n'est pas partitionnée (volume négligeable) et `target` contient des numéros en clair, exclus de
     l'effacement RGPD. Aucune échéance n'existe aujourd'hui ;
   - la collision de nom avec `dashboard.audit_log` (spec du tableau de bord) : deux tables du même nom,
     de formes différentes ; dire laquelle fait foi, au plus tard à step-310 ;
   - l'audit de `test-billing-provider` le jour où sa sonde HTTP sera réelle : appel sortant vers un
     tiers avec des identifiants stockés, sous scope `admin:write`, aujourd'hui sans trace ;
   - la base légale de la conservation d'un MSISDN dans `audit_log` malgré un effacement attesté : elle
     n'est écrite que dans un commentaire Go et dans cette fiche, pas dans `docs/` ;
   - dans step-300, ajouter le gRPC `content-key-svc`, où la DEK circule en clair sans authentification.

   La fiche est déplacée en `tasks-done/` au dernier commit de 290d.

- **Détail validé le 2026-09-17, avant le code :**
  - **ADR-0015 :** un `## Addendum (2026-09-17)` après *Consequences* écrit la posture, et le commentaire
    de `routeForTarget` (`internal/routing/snapshot.go:369`) y renvoie. Le garde-fou « valeur illisible
    traitée comme un miss » gagne une phrase pour le `WRONGTYPE` du point 3.
  - **Vecteur argon2id :** deux vecteurs, pas un. Celui de `src/test.c` de `phc-winner-argon2` est en
    `p=1`, or la production hache en `p=4` (`argonThreads`) et le chemin parallèle d'`argon2.IDKey` est
    un code distinct. **Corrigé à l'écriture :** le vecteur `p=4` du `README` amont est un `argon2i`,
    que `parsePHC` refuse — le second vecteur est donc celui de `src/test.c` en `m=256,t=2,p=2`, seul
    Argon2id amont avec `p > 1`. Le premier porte les paramètres de production (`t=1`, `m=64 Mio`) ;
    celui à 256 Mio est écarté, il ferait allouer un quart de gigaoctet à chaque run. Les deux sont
    cités par commit dans le test et passés à `VerifyBindPassword`, donc le décodage PHC est épinglé
    avec la dérivation. Mutation : `argon2.IDKey` remplacé par `argon2.Key` **aux deux sites** — l'aller-
    retour reste vert, seuls les vecteurs tombent, ce qui est exactement ce que ce test ajoute.
  - **`WRONGTYPE` :** `goredis.HasErrorPrefix` existe en v9.21.0 (`error.go:37`). Le test est un test
    d'intégration sur un vrai Redis (`redistest.Client`) : le texte `WRONGTYPE` vient du serveur, et un
    faux qui le fabrique testerait notre propre littéral.
  - **`CONNECTOR_PASSWORD` :** la garde vit dans `cmd/connector-pool-svc`, au point d'usage, comme
    `validateAdminConfig` — `connectorEnv` est un bloc local à ce service, pas une section de
    `internal/config`. **`CONNECTOR_ADDR` n'est pas gardé**, contrairement au modèle ClickHouse : un
    défaut de mot de passe a deux issues, dont une où le SMSC l'accepte et où la jambe sortante est
    protégée par un secret public ; un `localhost:2775` n'a pas de second cas de ce genre, et step-300
    rend ce pair local légitime (terminaison TLS en sidecar). Dans `deploy/k8s`, ClickHouse est adressé
    par nom de service, jamais en sidecar. `CONNECTOR_SYSTEM_ID` non plus : c'est une identité, que le
    SMSC distant refuse de lui-même.
  - **Clé API :** la dernière phrase du §1.9 (« Comparaisons en temps constant dans les deux cas ») est
    **fausse** pour la clé API depuis que `internal/restapi/auth.go:38` cherche par hash en SQL. Le §1.9
    et le godoc du paquet disent désormais ce qui est vrai.
  - **Le MSISDN du log d'échec d'attestation (`gdpr.go:176`) part en fiche, il n'est pas corrigé ici.**
    Cette branche est la copie de dernier recours de l'attestation, écrite quand la base l'a refusée ;
    décider ce qu'elle a le droit de contenir, c'est répondre à la question que porte `audit_log` —
    combien de temps le numéro d'une personne effacée survit dans un artefact que l'effacement ne touche
    pas. Une seule décision, appliquée aux deux endroits (step-297).
  - **Quatre fiches**, pas neuf : l'INDEX pose qu'un `step-NNN.md` est une PR. Neuf fiches feraient neuf
    PR dont plusieurs d'un paragraphe ; une seule ferait une fiche inexécutable — step-290 est ce
    cas-là. Le critère est donc « ce qui se merge ensemble, ou ce qui se décide une seule fois » :
    - **step-295** — `smsc_connectors.password_hash` et `external_billing_providers.auth_config_json` :
      même mécanisme, un secret rejouable donc réversible ;
    - **step-296** — `mt-replay` et `test-billing-provider` : deux actions d'opérateur sans ligne d'audit ;
    - **step-297** — une seule politique de conservation (rétention d'`audit_log`, base légale écrite
      dans `docs/`, MSISDN du log d'attestation) ;
    - **step-315** ⛓ step-310 — `GET /audit-log`, scope `audit:read`, immuabilité en base, collision avec
      `dashboard.audit_log`. Le seul lot bloqué par l'auth réelle, et le seul qui soit une fonctionnalité.

    Couplage nommé dans 297 **et** dans 315 : le `REVOKE DELETE` de 315 et la purge de 297 se contredisent
    si personne ne l'écrit — la purge doit être le seul titulaire du `DELETE`. `step-300` gagne une section
    pour la DEK en clair sur gRPC non authentifié ; `step-410` une ligne renvoyant aux quatre fiches.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] gosec intégré ; govulncheck en gate ; audit sans secret/corps

## Hors périmètre
TLS/mTLS transport → step-300. Auth opérateur OIDC → step-310.
