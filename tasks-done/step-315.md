# step-315 — Le journal d'audit se lit, et la base le rend immuable

> **Jalon :** Dette ouverte par step-290d · **Statut :** LIVRÉE (2026-09-26)
> **Dépend de :** step-310 (levée, voir Design arrêté) · **Bloque :** —

## Pourquoi cette fiche existe

step-290c a posé `control_plane.audit_log` et l'écrit : chaque mutation de l'Admin API, et chaque lecture
qui démasque un numéro d'abonné, y laisse une ligne avant que le handler ne s'exécute. Elle s'est arrêtée
là, et l'a dit : **aucun endpoint de lecture**, et une immuabilité qui n'existe que par convention.

## Constat 1 — le journal ne se lit qu'en SQL

Le commentaire de schéma donne la requête de lecture, faute de runbook dans ce dépôt. Cela suffit à un
incident, pas à l'usage : le tableau de bord attend un journal consultable, et l'opération n'est
**déclarée dans aucun contrat** aujourd'hui — `api/openapi-admin.yaml` n'a pas d'`audit-log`. L'ajouter
est donc un changement de contrat, déclaré **avant** l'implémentation (`.claude/rules/contracts-api.md`),
et un bump de `api/package.json`.

Le scope `audit:read` ne peut pas être posé avant step-310 : le vérificateur statique de M1 attribue les
scopes par jeton, sans rôles ni annuaire. D'où la dépendance, et la place de cette fiche après step-310.

## Constat 2 — l'immuabilité est une convention, pas une contrainte

La spec classe le journal d'audit **immuable** (l.909), et `attestationScope` s'appuie sur cette
propriété pour exclure `audit_log` de l'effacement RGPD. Or le rôle applicatif peut aujourd'hui
`UPDATE` et `DELETE` la table à volonté.

**Attention au piège :** l'écriture se fait en **deux temps** — `InsertAuditIntent` avant le handler,
`FinishAudit` après. « Aucun `UPDATE` » ne suffit donc pas. Il faut :

- un **trigger** qui n'autorise que la transition `status IS NULL → NOT NULL` (et refuse toute autre
  modification de colonne) ;
- un **`REVOKE DELETE`** sur le rôle applicatif.

**Couplage avec step-297 :** cette step-là ajoute une purge de rétention, qui a besoin du `DELETE`. La
purge doit en être le **seul** titulaire. Celle des deux qui merge en second respecte ce que la première
a posé.
*(step-297, arrivée en second : la purge est la seule porte du trigger — réglage transactionnel
`audit_log.purge`, jamais sous 365 jours. `DELETE` est rendu au propriétaire, le `REVOKE` ne porte plus que
`TRUNCATE` ; ADR-0018.)*

**Hérité de step-296 : toutes les lignes ne viennent pas de HTTP.** `mt-replay` écrit `method = REPLAY`,
`target = mt.dead-letter`, `operation_id = mt-replay` (absent du contrat) et `operator = declared:<nom>`.
Le schéma de réponse doit les accepter : une enum de verbes HTTP ou un motif `tok_…` ferait échouer la
réponse sur une ligne de rejeu, et durcir le champ après publication serait un bump MAJEUR.

## Constat 3 — deux tables s'appellent `audit_log`

`docs/specification-technique-tableau-de-bord.md` déclare un `audit_log` dans le schéma `dashboard`, de
forme différente, partitionné mensuellement. Le nôtre vit dans `control_plane`. Les deux peuvent
coexister — les schémas les séparent — mais rien ne dit **laquelle fait foi** pour l'audit des actions
opérateur, ni si le BFF doit lire la nôtre ou projeter la sienne.

step-290 annonçait cette décision « au plus tard à step-310 ». Elle glisse d'un cran, assumé : la
question ne se tranche utilement qu'avec le lecteur, et le lecteur attend `audit:read`, donc step-310.
Si le BFF avance avant, elle se tranche là-bas, pas ici.

## Design arrêté

Arbitrages du 26/09/2026. La dépendance ⛓ step-310 est **levée** : `msisdn:reveal` et `cdr:export_bulk`
ont été posés sur le vérificateur statique, et step-310 consomme les scopes existants sans en créer (Fable ;
confirmé par l'utilisateur après lecture du dépôt `go-gateway-bo`). Périmètre entier.

**Constat 3 — tranché (ADR-0017).** `control_plane.audit_log` fait foi pour tout ce qui atteint la
passerelle : l'Admin API, quel que soit l'appelant, et `mt-replay`. `dashboard.audit_log` fait foi pour les
actions propres au BFF (sessions, MFA, opérateurs, rôles). Le BFF ne projette pas la nôtre : il la lit par
`GET /admin/audit-log`, et son écran (step-184 côté BFF) montre les deux sources.

**Contrat (6.8.0, mineur).** `GET /admin/audit-log`, `list-audit-log`, sous le scope neuf `audit:read`.
Filtres optionnels : `operator` (égalité exacte), `from_date` (inclus), `to_date` (exclu). Tri `at DESC, id DESC`,
keyset `platform/keyset` (micro). `AuditEntry` : `operator`, `operation_id`, `method`, `target` sont des
chaînes libres, **sans enum ni motif**, parce qu'une ligne `REPLAY` / `declared:<nom>` doit passer et que
step-310 ajoutera le format `sub`. `status`, `request_id` et `finished_at` sont nullables, et un `status`
null veut dire « issue non enregistrée ».

**MSISDN dans `target`.** `update-exact-route` / `delete-exact-route` écrivent `/…/exact-routes/{msisdn}`.
Sans `msisdn:reveal`, ce segment est masqué (`maskMSISDN`). Avec ce scope, la lecture démasque un numéro :
`list-audit-log` entre donc dans `revealReads` et se journalise lui-même.

**Immuabilité (migration 0020).**
- Trigger `BEFORE UPDATE OR DELETE` par ligne. Il n'autorise qu'une modification : `status` et `finished_at`
  passent de NULL à non NULL, toutes les autres colonnes restent identiques. Tout le reste lève une
  exception, DELETE compris. Un trigger par instruction refuse aussi le vidage de table.
- `REVOKE DELETE` et le privilège de vidage `FROM CURRENT_USER`, c'est-à-dire le propriétaire, qui est
  aujourd'hui aussi le rôle applicatif (un seul `POSTGRES_URL`).
- Pourquoi les deux : les tests et compose tournent en **superuser**, qui ignore les privilèges, donc le
  trigger seul y porte la preuve. Le REVOKE tient un propriétaire non superuser en production et se prouve
  par l'ACL. Aucun des deux ne résiste à un propriétaire qui les défait : c'est une dette
  (`debts/`), qui porte la séparation migrate/app. step-297 (la purge) sera l'unique porte ouverte dans le
  trigger.
- Index `audit_log_operator_at_idx (operator, at)`.

**Tests.** Intégration, contre la base : UPDATE d'une autre colonne, second finish brut, DELETE et vidage
refusés ; ACL du propriétaire sans ces deux privilèges ; Begin puis Finish toujours verts ; List filtre et
pagine. Handler : forme du contrat, filtres transmis, curseur invalide renvoyant 422, `target` masqué avec
et sans reveal, 403 sans `audit:read`.

## Tests

- Un `UPDATE` d'une colonne autre que `status`, un second `FinishAudit` sur une ligne déjà close, et un
  `DELETE` par le rôle applicatif : les trois sont refusés **par la base**, pas par le code Go.
- `GET /audit-log` respecte le contrat déclaré. **Rien à retirer de `deferred`** : cette liste
  (`internal/adminapi/contract_test.go`) ne recense que des opérations **déjà publiées** que personne ne
  sert, et l'audit n'en fait pas partie puisqu'il n'est déclaré nulle part. Déclarer et servir dans la
  même step garde la garde verte sans y toucher — et `deferredSteps` est une liste close step-330→390,
  à laquelle step-315 n'appartient pas.

## Definition of Done

- [x] Contrat déclaré avant l'implémentation, `api/package.json` bumpé, collection Admin regénérée.
- [x] `GET /audit-log` servi sous `audit:read`, avec pagination keyset (`platform/keyset`).
- [x] L'index `(operator, at)` que step-290c avait annoncé est créé : la migration 0015 n'a livré que
      `audit_log_at_idx (at)`, et un filtre par opérateur sans lui balaie la table.
- [x] Trigger + `REVOKE DELETE` en migration, prouvés par des tests d'intégration.
- [x] La question de la collision de nom est tranchée et écrite.

## Hors périmètre

La rétention et la base légale → step-297. Les actions d'opérateur encore hors piste → step-296.
