# step-297 — Ce qui survit à un effacement attesté : rétention, base légale, journaux

> **Jalon :** Dette ouverte par step-290d · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** —

## Pourquoi cette fiche existe

`internal/adminapi/gdpr.go` délivre une **attestation** : une pièce qui dit ce qui a été effacé pour une
personne. Elle nomme déjà ses exclusions (`attestationScope` : archives froides, log Kafka, `audit_log`).
Trois d'entre elles n'ont aujourd'hui **aucune échéance ni aucune base écrite ailleurs que dans un
commentaire Go**. Une exclusion sans durée n'est pas une exclusion, c'est une conservation indéfinie.

Les trois posent la même question — *combien de temps le numéro d'une personne effacée survit-il dans un
artefact que l'effacement ne touche pas, et à quel titre ?* — d'où une seule fiche : une décision,
appliquée aux trois endroits.

## Constat 1 — `control_plane.audit_log` n'a aucune rétention

La table est née en step-290c. Sa colonne `target` est le chemin de la requête, et **les numéros qui
figurent dans un chemin y sont en clair** — c'était le point validé : un audit qui ne dit pas quel numéro
a été touché ne sert à rien.

La spec veut, pour le journal d'audit, **1 à 7 ans selon conformité** (l.909) et un **partitionnement
mensuel** pour rendre la purge réalisable (l.890). La table n'est ni partitionnée (volume négligeable :
quelques dizaines d'actions par jour, écart assumé en step-290c) ni purgée. Il faut donc :

- une durée choisie dans la fourchette, et écrite ;
- une purge qui n'a pas besoin de partitions à ce volume (`DELETE ... WHERE at < now() - interval`), avec
  son planificateur ;
- **le couplage avec step-315** : cette step-là pose l'immuabilité en base (`REVOKE DELETE` + trigger).
  Les deux se contredisent si personne ne l'écrit — **la purge doit être le seul titulaire du `DELETE`**.
  Quel que soit l'ordre de merge, celle qui arrive en second respecte ce que la première a posé.
  **step-315 a mergé en premier** (migration 0020) : le trigger `audit_log_append_only` refuse tout
  `DELETE`, même en superuser, et le propriétaire n'a plus le privilège. La purge doit ouvrir une porte
  dans le trigger, réservée à un rôle distinct, ce qui paie
  `debts/audit-log-immuable-contre-tout-sauf-son-proprietaire.md`.

## Constat 2 — la base légale n'est écrite nulle part dans `docs/`

Conserver un MSISDN dans `audit_log` **malgré** un effacement attesté est défendable — la piste d'audit
est immuable par conception, et l'imputabilité des accès est elle-même une obligation. Mais cette
justification ne vit que dans un commentaire Go (`attestationScope`) et dans les fiches step-290/297.
Ce n'est pas là qu'un auditeur la cherche, ni là qu'elle engage.

## Constat 3 — le MSISDN part dans le log quand l'attestation ne peut pas s'écrire

`gdpr.go` journalise l'attestation **entière** quand `Finish` échoue : c'est la copie de dernier recours,
et l'attestation contient `subject=msisdn:<numéro>`. Le numéro d'une personne effacée atterrit donc dans
le journal applicatif, dont la rétention est celle de la collecte de logs — plus longue, moins contrôlée,
et hors du périmètre de l'effacement.

Le correctif n'est pas d'une ligne : cette branche existe pour que l'attestation ne soit **pas perdue**.
Trois voies : ne journaliser que `job_id` (la ligne de job porte le sujet, et elle existe — c'est `Finish`
qui a échoué, pas la création) ; journaliser une attestation au sujet masqué ; ou l'assumer sous la
décision du constat 2. À trancher **avec** les deux autres, pas séparément.

## Tests

- La purge supprime ce qui a dépassé l'échéance et **rien** d'autre, sur une base réelle.
- Une attestation dont l'enregistrement échoue ne fait pas apparaître le numéro dans le log (selon la
  voie retenue) : le test lit le log capté, il ne relit pas le code.

## Definition of Done

- [ ] Une durée de rétention d'`audit_log` est choisie, écrite dans `docs/` et appliquée par une purge.
- [ ] La purge et l'immuabilité de step-315 sont compatibles, et chacune des deux fiches le dit.
- [ ] La base légale de ce qui survit à un effacement attesté est écrite dans `docs/`, pas seulement
      dans un commentaire.
- [ ] Le log d'échec d'attestation ne conserve un MSISDN que si la décision ci-dessus l'autorise.

## Hors périmètre

La rétention du CDR et le tiering (step-16x). `GET /audit-log` et l'immuabilité en base → step-315.
