# Le journal d'audit est immuable contre tout, sauf contre son propriétaire

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-315 · **Portée par :** —

**Ce qu'on a fait à la place.** `control_plane.audit_log` refuse toute modification autre que sa clôture,
et tout effacement, par un trigger et par un `REVOKE DELETE` pris au **propriétaire** de la table. Le
propriétaire est aussi le rôle applicatif : le job de migration et chaque service partagent le même
`POSTGRES_URL`. Ce rôle peut donc se re-`GRANT` le droit, ou supprimer le trigger.

**Pourquoi.** Le design arrêté de step-315 : séparer un rôle `migrate` propriétaire d'un rôle applicatif
touche chaque secret et chaque manifest de `deploy/k8s`, hors du périmètre du journal.

**Ce qu'il en coûte si on ne la paie jamais.** Un service compromis, ou une requête d'exploitation lancée
avec ses identifiants, peut effacer ou réécrire la trace en deux instructions. L'immuabilité protège contre
l'accident, pas contre l'intention. C'est pourtant sur l'intention que `attestationScope` compte pour
exclure `audit_log` de l'effacement RGPD.

**À quoi on reconnaîtra qu'il faut la payer.** Dès qu'un audit de conformité demande la preuve que la trace
est inaltérable par l'application. step-297 a posé la purge **sans** la payer (ADR-0018) : une porte
`audit_log.purge` dans le trigger, avec un plancher de 8760 heures, et `DELETE` rendu au propriétaire. Seul un
propriétaire distinct du rôle applicatif rend ce plancher inaltérable.

Source : `migrations/0020_audit_log_append_only.up.sql:1`
