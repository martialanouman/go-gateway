# step-315 — Le journal d'audit se lit, et la base le rend immuable

> **Jalon :** Dette ouverte par step-290d · **Statut :** À FAIRE
> **Dépend de :** step-310 · **Bloque :** —

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

## Constat 3 — deux tables s'appellent `audit_log`

`docs/specification-technique-tableau-de-bord.md` déclare un `audit_log` dans le schéma `dashboard`, de
forme différente, partitionné mensuellement. Le nôtre vit dans `control_plane`. Les deux peuvent
coexister — les schémas les séparent — mais rien ne dit **laquelle fait foi** pour l'audit des actions
opérateur, ni si le BFF doit lire la nôtre ou projeter la sienne.

step-290 annonçait cette décision « au plus tard à step-310 ». Elle glisse d'un cran, assumé : la
question ne se tranche utilement qu'avec le lecteur, et le lecteur attend `audit:read`, donc step-310.
Si le BFF avance avant, elle se tranche là-bas, pas ici.

## Tests

- Un `UPDATE` d'une colonne autre que `status`, un second `FinishAudit` sur une ligne déjà close, et un
  `DELETE` par le rôle applicatif : les trois sont refusés **par la base**, pas par le code Go.
- `GET /audit-log` respecte le contrat déclaré. **Rien à retirer de `deferred`** : cette liste
  (`internal/adminapi/contract_test.go`) ne recense que des opérations **déjà publiées** que personne ne
  sert, et l'audit n'en fait pas partie puisqu'il n'est déclaré nulle part. Déclarer et servir dans la
  même step garde la garde verte sans y toucher — et `deferredSteps` est une liste close step-330→390,
  à laquelle step-315 n'appartient pas.

## Definition of Done

- [ ] Contrat déclaré avant l'implémentation, `api/package.json` bumpé, collection Admin regénérée.
- [ ] `GET /audit-log` servi sous `audit:read`, avec pagination keyset (`platform/keyset`).
- [ ] L'index `(operator, at)` que step-290c avait annoncé est créé : la migration 0015 n'a livré que
      `audit_log_at_idx (at)`, et un filtre par opérateur sans lui balaie la table.
- [ ] Trigger + `REVOKE DELETE` en migration, prouvés par des tests d'intégration.
- [ ] La question de la collision de nom est tranchée et écrite.

## Hors périmètre

La rétention et la base légale → step-297. Les actions d'opérateur encore hors piste → step-296.
