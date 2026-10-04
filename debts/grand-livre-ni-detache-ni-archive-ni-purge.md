# Le grand livre crée ses partitions mais ne les détache, n'archive ni ne purge jamais

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-408 (`db/schema_passerelle_sms.sql:785`) · **Portée par :** —

**Ce qu'on a fait à la place.** step-408 crée les partitions journalières de `control_plane.billing_ledger` d'avance, et rien d'autre : aucune
partition n'est détachée, archivée vers le stockage objet, ni supprimée. Le grand livre grandit d'une
partition par jour, sans fin.

**Pourquoi.** La spec veut le tiède en stockage objet et la purge par drop de partition au-delà de 13 mois
(§6.14.2, guide §13.2). step-408 avait pour but de rendre cette purge *possible* avant la campagne de
step-409 ; l'archivage est un second mécanisme, sur le modèle de step-407 pour les CDR (écrire l'objet, le
relire, l'inscrire au catalogue, et seulement alors supprimer), et ne bloque pas la campagne.

**Ce qu'il en coûte.** Au débit visé (jusqu'à ~250 M mouvements par jour à pleine adoption, spec §2), le disque
de Postgres. Et deux chemins restent jamais exercés : la branche « partition détachée » de
`ListOrphanedReservations` (`internal/storage/postgres/queries/billing.sql:198-200`), et la lecture d'un
mouvement archivé par la capture.

**À payer** avant le treizième mois de production, ou dès que le volume du grand livre pèse sur les
sauvegardes de Postgres. Les lignes que `DEFAULT` aurait reçues (jauge `billing_ledger_default_rows`) ne
s'archivent pas par partition : elles demandent une décision à part.
