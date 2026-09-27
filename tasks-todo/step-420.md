# step-420 — Exporter les CDR archivés

> **Jalon :** après le go-live (besoin produit du 2026-09-27) · **Statut :** À FAIRE
> **Dépend de :** step-407, step-410 · **Bloque :** les extractions planifiées du tableau de bord sur des données de plus de `CDR_RETENTION`

## Pourquoi cette fiche existe

Le tableau de bord va planifier des extractions, dont certaines portent sur de **vieilles données**. La
planification est à la charge du BFF ; les jobs y tournent au nom de leur créateur (ADR-0019). La passerelle
n'exporte aujourd'hui que ce que ClickHouse détient encore :

- `create-message-export` (step-187) lit par `SearchStore`, donc la table CDR chaude, avec une fenêtre de
  31 jours et un plafond de 100 000 lignes.
- Au-delà de `CDR_RETENTION` (90 jours par défaut), le Retainer **supprime la partition**
  (`internal/storage/clickhouse/retention.go:172`). Il ne l'archive en Parquet que si `ARCHIVE_PREFIX` est
  posé (`internal/config/config.go:403-407`).
- step-407 fait exister l'archive en production (destination objet, catalogue de l'objet qui fait foi par
  jour). Cette step la **lit**.

## Périmètre (ce que fait CETTE PR)

- **Export depuis l'archive** : un job d'export dont la fenêtre dépasse la rétention chaude lit les jours
  archivés, en prenant pour chaque jour l'objet désigné par le catalogue de step-407. La lecture passe par la
  même table function, `SELECT … FROM s3(...)` : c'est ClickHouse qui lit le Parquet, pas de décodeur
  Parquet en Go. Il applique les mêmes filtres, le même masquage MSISDN (`msisdn:reveal`), le même format et
  le même plafond, et n'exporte jamais de corps (les archives n'en ont pas).

## Points d'implémentation clés

- **À arbitrer avant tout code (échelle : spec → Fable → humain)** :
  - **RGPD, le point dur.** L'archive froide échappe à l'effacement (ADR-0018 : « responsabilité de
    l'exploitant »). L'exporter **refait sortir** le numéro d'une personne effacée, cette fois vers un fichier
    remis à un opérateur. Trois options : exclure à la lecture les MSISDN effacés (ce qui exige de garder une
    trace de l'effacement, donc un numéro, au moins sous forme de hash) ; refuser l'export de l'archive tant
    que l'exploitant ne purge pas ses archives ; ou l'accepter par écrit en amendant ADR-0018. **À trancher
    en premier** : les deux autres points en dépendent.
  - Fenêtre et plafond : garder 31 jours et 100 000 lignes pour l'archive, ou un profil propre. Un export
    froid est lent, et c'est le BFF qui découpe.
  - Les jours sans entrée au catalogue (partitions supprimées avant step-407) : les signaler dans le job,
    jamais les taire — un export incomplet ne doit pas passer pour exhaustif.
- **Contrat** : si la requête d'export gagne un paramètre, ou si la description change (31 jours, source
  chaude seulement), le contrat est déclaré **avant** l'implémentation, avec un bump de `api/package.json`.
  Durcir un champ existant serait une rupture (`deferred-operation-schemas-already-ship`).
- **Pièges connus** (step-165) : un seul propriétaire de la rétention ; ne jamais supprimer ce qu'on n'a pas
  relu ; la projection vient de `system.columns` (UUID et Enum castés), donc une archive ancienne peut avoir
  un **schéma plus ancien** que la table : la lecture doit tolérer une colonne absente.
- Dettes voisines, à solder ici ou à relier : `debts/export-cdr-eteint-par-defaut.md` (`EXPORT_DIR` absent
  des manifests : sans lui, l'export chaud comme froid répond 503) et `debts/artefacts-d-export-jamais-purges.md`.
  Une extraction planifiée **multiplie** les fichiers : sans purge, le disque se remplit.

## Tests (écrits dans la même PR)

- Deux objets pour un même jour (une tentative échouée mais complète, puis celle du catalogue) : les lignes
  sont exportées **une seule fois**.
- Un jour absent du catalogue est signalé dans le job.
- Export : une fenêtre à cheval entre le chaud et l'archive rend chaque ligne une seule fois, masquée selon le
  scope, sans corps.
- La règle RGPD retenue, prouvée par un MSISDN effacé présent dans l'archive.

## Definition of Done

- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] un export couvre une fenêtre plus ancienne que `CDR_RETENTION`
- [ ] l'arbitrage RGPD est écrit (ADR-0018 amendé, ou nouvel ADR)
- [ ] `debts/export-cdr-eteint-par-defaut.md` et `debts/artefacts-d-export-jamais-purges.md` soldées ou
      rattachées à une échéance

## Hors périmètre

La planification des extractions, leur identité et leur notification : dépôt du BFF (ADR-0019). La
rétention des archives elles-mêmes (13 mois, §6.14) : politique de cycle de vie du bucket, côté exploitant.
