# Conventions du socle du monorepo

2026-10-17 · Décision 9 du monorepo (`docs/audit-architecture-2026-10-14.md` §4, §6).
Statut : validé.

## 1. Problème

Les deux dépôts suivent des règles contraires, et chacune de ces collisions doit être tranchée avant le
premier commit (audit §4) : lettres d'invariants qui désignent des choses différentes, deux registres de
dettes aux règles opposées, 35 numéros de steps en collision, deux `go.mod`, deux workflows nommés
`CI`, un tag de release commun, outillage divergent, deux mémoires Claude liées aux anciens chemins.

## 2. Décisions

| # | Sujet | Règle |
|---|---|---|
| C1 | Module Go | **Un seul `go.mod`.** Les types sont générés une fois et partagés (décision 5). `contract_copy_test.go` est supprimé : il n'y a plus de copie. Les outils passent par la directive `tool` |
| C2 | Version et release | **Une version pour tout le dépôt** ; toutes les images partent ensemble (13 avec le dashboard). Déploiement par SHA. Ni tags préfixés, ni filtres `paths:` |
| C3 | CI | **Un seul workflow** qui lance tout : agrégateur, actions épinglées par SHA, `ciguard` (un test sauté échoue), `check-generated` partout (sqlc, buf, oapi-codegen, catalogue de topics, scopes, ACL, GRANT). Des filtres `paths:` viendront sur une durée de CI mesurée |
| C4 | Invariants | **Numérotation unique `I1…In`**, citée par le code ; chacun a un test bloquant. La liste reprend les invariants de la passerelle et du dashboard, plus ceux des décisions 1 à 8 |
| C5 | Dettes | **GitHub Issues** (§3) |
| C6 | Steps | **Nouvelle série depuis `001`** ; le numéro est l'ordre d'exécution. Aucune correspondance avec les anciens numéros |
| C7 | ADR | **La série de la passerelle continue.** Les 9 décisions restent des specs de design, sans ADR en double. Un ADR cite une section, jamais un numéro de ligne |
| C8 | Spec unique | Construite **depuis le code, les ADR et les 9 designs**, pas depuis le guide d'ingénierie, qui diverge du code. Le guide est réécrit ensuite, depuis la spec |
| C9 | Outillage | `gofmt` (pas `gofumpt`) ; `revive exported` partout ; **goose** pour toutes les migrations, Postgres et ClickHouse ; une version par dépendance |
| C10 | Claude | `.claude/rules/` commun ; les enseignements de l'annexe de l'audit y deviennent des règles. Mémoire neuve pour le monorepo, où ne passent que les mémoires de méthode ; l'historique des steps reste dans les anciens dépôts |

## 3. Les dettes dans GitHub Issues

- **Une dette est une issue** étiquetée `debt`, ouverte depuis un gabarit aux champs obligatoires :
  ce qu'on a fait à la place, pourquoi, ce qu'il en coûte, le déclencheur, le porteur.
- **Différer quelque chose** (« hors périmètre », « pour l'instant », « deferred ») exige l'issue
  **dans la même PR**, liée dans sa description.
- **Un endroit du code qui porte le report** le marque `debt: #N`.
- **Garde CI, fermée sur le remède** : chaque `debt: #N` du dépôt doit désigner une issue **ouverte**
  étiquetée `debt`. Fermer l'issue sans retirer les marques fait échouer la CI ; payer la dette, c'est
  donc retirer le report du code et fermer l'issue dans la même PR.
- Le dossier `debts/` n'existe pas dans le monorepo.

**Reprise de l'existant** : à l'ouverture, chaque fiche ouverte des deux registres est triée. Ou elle
est soldée par l'une des décisions 1 à 8, et elle n'est pas reprise ; ou elle devient une issue.

## 4. À supprimer à l'ouverture

- Le clone `go-gateway-adr0019` (branche mergée, ADR identique).
- Le schéma `dashboard` vide que crée la base de la passerelle.
- La seconde copie de la spec du dashboard, après report de ses deux ajouts (audit §4).

## 5. Effets

| Où | Effet |
|---|---|
| `CLAUDE.md` du monorepo | Le couplage « différer quelque chose » renvoie à une issue `debt`, plus à `debts/` |
| `.claude/rules/debts.md` | Réécrite pour les issues et la marque `debt: #N` |
| GoReleaser | Une version, 13 images ; la garde « exactement N images » passe à 13 |
