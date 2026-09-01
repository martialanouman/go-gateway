---
paths:
  - "tasks-todo/**"
  - "tasks-done/**"
---

# Fiches de travail

Le tableau de bord est `tasks-todo/INDEX.md` (dérivé de
`docs/plan-execution-passerelle.md`) : jalons, cases cochées, règle de
numérotation. Ce qu'il ne dit pas :

- Une step porte son design sous `## Design arrêté`.
- Elle passe en `tasks-done/` par un `git mv`, **dernier commit de sa PR**.
- **Le numéro est l'ordre d'exécution**, pas un identifiant : une fiche neuve
  prend un multiple de dix libre *à sa place dans l'ordre*, **jamais le suivant
  disponible**. Une step ne doit dépendre que de numéros plus petits.
- **Rien ne le vérifie** — les en-têtes sont de la prose, et une garde qui les
  regexerait passerait au vert le jour où l'une d'elles se reformule.
