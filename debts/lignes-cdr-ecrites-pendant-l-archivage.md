# Une ligne de CDR écrite pendant l'archivage de son jour est perdue

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-165, élargie par step-407 (`internal/storage/clickhouse/retention.go:373`) · **Portée par :** —

L'archiveur exporte la partition, relit l'objet, l'inscrit au catalogue, puis le Retainer la supprime
(`retention.go:373`). Une ligne écrite dans ce jour entre l'export et le DROP n'est dans aucun objet, et
le DROP l'emporte. L'inscription au catalogue (une requête Postgres) a élargi la fenêtre.

**Pourquoi.** Un jour n'est supprimé que 90 jours après sa fin : seule une nouvelle version d'un très vieux
message peut y écrire, et DROP PARTITION ne se conditionne pas au contenu. Relevé en revue de step-407,
non traité pour ne pas ouvrir un verrou entre les écrivains de CDR et la rétention.

**Ce qu'il en coûte.** Une version de CDR perdue sans trace, rare mais silencieuse.

**À payer quand** un écrivain de CDR peut toucher un message de plus de 90 jours (rejeu de dead-letter
sans âge maximal, import historique).
