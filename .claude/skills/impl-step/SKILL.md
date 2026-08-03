---
name: impl-step
description: Procédure obligatoire pour implémenter une step de tasks-todo/. À invoquer AVANT toute lecture de code et toute écriture, dès qu'une step est engagée — « attaque step-NNN », « implémente step-NNN », « continue sur step-NNN », « enchaîne sur la suivante ». Passe par using-agent-skills à chaque phase, implémente en sub-agents parallèles, fait relire par des sub-agents en lecture seule. Quatre portes bloquantes : design commité avant le code, rouge lu avant l'implémentation, mutation vue tomber avant « vert », revue sans bloquant avant la DoD.
---

# Implémenter une step

Une step = une PR petite et verte (plan §0.1). Neuf phases ordonnées, **une todo par phase** — c'est ce
qui rend une phase sautée visible. Quatre sont des **PORTES** : tant qu'elle n'est pas franchie, la
suite est interdite.

**Les portes valent pour tout code, y compris un correctif de revue.** Un correctif n'est pas une
réparation mécanique : il tranche entre des options, donc il repasse par l'arbitrage (ph. 2) et par le
design commité (ph. 3) avant son TDD. Sauter ces deux-là est le raccourci le plus coûteux du dépôt —
détail et mesure en phase 7.

## Skills à invoquer

Obligatoires, ils portent les portes : `doubt-driven-development` (ph. 2) · `test-driven-development`
(ph. 5-6) · `code-review-and-quality` (ph. 7).

Selon la step : `context-engineering`, `ctx7`/`find-docs`, `documentation-and-adrs`,
`planning-and-task-breakdown`, `incremental-implementation`, `security-and-hardening` (surface
exposée), `ci-cd-and-automation`, `git-workflow-and-versioning`. `golang-how-to` s'active seul sur le code.

Quand un skill générique propose une convention concurrente à celle du dépôt (`tasks/plan.md` vs
`tasks-todo/`, `docs/decisions/` vs `docs/adr/`), **le dépôt gagne**.

---

## Phase 1 — Contexte

Rassembler : la **fiche** en entier · les **contrats** (`api/openapi-*.yaml`, `db/schema_*.sql`) · la
section du plan citée · le **précédent le plus proche** dans le code.

Bibliothèque en jeu → `ctx7`, puis vérifier **dans la source du module** ce qui engage la correction :
une API devinée de mémoire compile parfois.

Outil externe → relever la version **installée** (`<outil> version`). Si elle est postérieure à ma
connaissance, la sonder avec un cas jetable : une majeure change codes de sortie et format de sortie
sans rien signaler.

Paralléliser avec des `Explore` sur axes disjoints (lecture seule, aucun risque).

## Phase 2 — Arbitrages · PORTE 1

Lister **tous** les points ouverts. Aucun ne se tranche en silence.

Vaut aussi pour les **correctifs de revue** (ph. 7) : un correctif tranche entre des options, donc il
repasse par ici. Écarter une option parce qu'« elle casse des tests » est un arbitrage, pas une
évidence.

1. **La spec** (specs, plan, guides, ADR, contrats) — chercher avant de délibérer.
2. **Fable** si la spec ne tranche pas :
   `Agent(subagent_type: "general-purpose", model: "fable", prompt: "<décision> · <options et conséquences> · <ce que dit la spec> · <contraintes> Tranche et justifie. Si tu ne peux pas, dis-le et dis pourquoi.")`
   S'il tranche clairement sans contredire la spec → appliquer. Remonter à l'humain seulement s'il
   refuse, se contredit, ou heurte la spec.
3. **L'utilisateur** en dernier recours : options + recommandation motivée, jamais une question nue.

## Phase 3 — Design commité · PORTE 1 (suite)

Écrire les décisions dans la fiche sous `## Design arrêté (AAAA-MM-JJ)`, une par `### DN — …`, chacune
avec **sa raison**.

```bash
git checkout main && git pull       # brancher depuis main
git checkout -b <type>/step-NNN-<slug>
git commit -m "docs(tasks): arrêter le design de step-NNN (…)"
```

Dépendance à une branche non mergée → la consigner en `DN` et l'annoncer en tête du corps de PR ; sinon
elle ne se découvre qu'en phase 9, quand la PR porte plusieurs sujets.

**Aucune ligne de code avant ce commit.** Sans lui, la justification s'écrit après le code, pour lui.
**Y compris pour un correctif de revue** — c'est là que la règle se perd le plus souvent.

## Phase 4 — Plan et todos

Découper en unités livrables (rouge → vert → commit). Une unité = ce qu'un relecteur accepte ou refuse
seul. Marquer **les fichiers touchés** par unité : c'est le critère de parallélisation. Une todo par
unité. Le plan vit dans la fiche et la todo list, pas dans un fichier concurrent.

## Phase 5 — TDD · PORTE 2

Par unité : écrire le test → **le lancer et lire son échec** → implémenter le minimum → relancer.

Le rouge doit échouer *pour la bonne raison* : « symbole inexistant » prouve ; « connexion refusée » ne
prouve rien.

**Aucune implémentation avant un rouge lu.**

### Sub-agents parallèles

Unités sans **aucun fichier commun** → simultanées ; sinon séquentielles. Mandat autonome (le sub-agent
ne voit pas la conversation) :

```
Agent(subagent_type: "general-purpose",
      prompt: "Unité <N> de step-NNN : <objectif>.
               Design arrêté applicable : <DN recopiées>.
               Fichiers dont tu es le SEUL propriétaire : <liste>. N'édite rien d'autre.
               Ne fais AUCUN git add / commit / checkout.
               Procédure : écris le test, LANCE-LE, cite son message d'échec, implémente le
               minimum, relance.
               Rends : le message du rouge, les fichiers touchés, le résultat final.")
```

- **Exiger le rouge cité verbatim** : seule preuve qu'il n'a pas écrit le code puis un test complaisant.
  Rapport sans rouge = unité à refaire.
- **Leur interdire de committer** : deux agents se disputent l'index git.
- Unité en doute (invariant sensible, opération irréversible) → `doubt-driven-development`, pas plus de
  parallélisme.

## Phase 6 — Mutation · PORTE 3

Avant tout « vert » : casser le comportement testé et **voir le test tomber**. Une assertion jamais vue
échouer n'en est pas une.

```bash
cp fichier.go /tmp/f.bak    # muter, lancer, constater l'échec
cp /tmp/f.bak fichier.go
git diff --stat fichier.go  # VÉRIFIER la restauration, commande séparée
```

Vérifier la restauration **hors de la commande de mutation** : un test muté peut pendre, la commande est
interrompue, et le `cp` qui suivait n'est jamais exécuté.

```bash
go test -race -count=10 ./<paquets touchés>/...   # 25 si concurrent
```

Un passage unique ne dit rien d'un défaut d'ordonnancement. Un test flaky sous répétition est une
information : ne pas le « stabiliser » en relâchant son seuil.

Se méfier d'un test vert du premier coup. Mode d'échec récurrent : la **fixture creuse**, dont le montage
n'atteint jamais la condition qu'elle prétend vérifier.

Tenir le **tableau des mutations** (mutation → test tombé) pour le corps de PR.

## Phase 7 — Revue lecture seule · PORTE 4

Relecteurs `subagent_type: "Plan"` (sans `Edit`/`Write` par construction), en parallèle, sur **axes
disjoints** : invariants projet · correction & concurrence · contrats & schéma · les tests peuvent-ils
réellement échouer.

```
Agent(subagent_type: "Plan",
      prompt: "Relis le diff de step-NNN (`git diff main...HEAD`) sur l'axe <axe>.
               Design arrêté : <DN>. Ne modifie aucun fichier.
               Pour chaque constat : fichier:ligne, le défaut, le scénario concret qui casse,
               et une classification bloquant | à corriger | note.")
```

Ils constatent, **je corrige** : un relecteur qui répare son constat ne le rapporte plus.

### Un correctif est du code — il repasse par TOUTES les phases

Pas seulement par le TDD. Pour chaque bloquant, dans cet ordre :

1. **Phase 2 — arbitrer.** Lister les options, chercher ce que la spec tranche, escalader à Fable puis
   à l'utilisateur si elle est muette. **Écarter une option parce qu'« elle casse des tests » est un
   arbitrage, pas une évidence** : c'est un coût, opposable à un mode de défaillance.
2. **Phase 3 — commiter le design.** La décision va dans la fiche en `### DN — …` **avec sa raison**,
   commitée **avant** la première ligne du correctif.
3. **Phases 5-6 — TDD.** Écrire le test qui reproduit le défaut et **le voir échouer** → corriger →
   **muter et voir tomber**.

**Le TDD seul ne voit pas un mauvais choix.** Un correctif mal conçu passe son test, tombe sous
mutation, et reste faux : le défaut est dans la décision, pas dans l'exécution. Cas réel (step-201c) —
un défaut no-op rendu « bruyant » pour qu'un câblage manquant échoue au premier envoi ; sauf que
l'envoi précédait la publication, donc le correctif faisait re-soumettre le même SMS en boucle. Tout
était vert. C'est la phase 2 qui l'a renversé, pas un test.

**Pourquoi cette section existe.** Mesuré sur une step à quatre tours : **8 constats sur 9 au tour 2,
puis 11 sur 12 au tour 3, venaient des correctifs des tours précédents** — jamais du code d'origine.
Les correctifs ne sont pas plus difficiles que le code initial : ils échappaient aux portes que le code
initial franchit. Le volume n'excuse pas le raccourci — c'est quand il y a beaucoup de correctifs que
chacun échappe au regard.

Un constat qu'on ne parvient pas à reproduire se discute avant d'être corrigé : soit il n'est pas réel,
soit le test qui manque compte plus que la correction.

### Boucler et s'arrêter

Relancer une revue sur le nouveau diff tant qu'il reste un bloquant. Lire **ce que** le tour trouve :

- bloquants dans le **code d'origine** → continuer ;
- bloquants dans les **correctifs** → je corrige trop vite : appliquer la règle ci-dessus, pas un
  relecteur de plus.

Deux sorties :

- **même** bloquant à 3 tours → désaccord de conception, remonter à l'utilisateur, trancher en phase 2 ;
- bloquants **nouveaux** sans convergence → la question devient « le coût d'un tour de plus dépasse-t-il
  le risque résiduel ? ». **Arbitrage de l'utilisateur** : lui donner le compte par tour, ce que chacun a
  trouvé, ce qui reste non relu, et une recommandation. Ne jamais s'arrêter en silence ni enchaîner seul.

Consigner nommément dans la fiche ce qui est gelé sans avoir été relu.

## Phase 8 — Definition of Done

```bash
gofmt -l cmd internal            # vide
make lint                        # 0 issue
DOCKER_HOST=unix://$HOME/.orbstack/run/docker.sock go test -race ./...
govulncheck ./...
```

`DOCKER_HOST` est obligatoire : sans lui les tests conteneurisés skippent en silence. Vérifier le
**nombre de skips**, pas seulement le code de sortie.

API modifiée → contrat d'abord, **bump `api/package.json`** (majeur si `oasdiff` classe `ERR`),
`api/collections/admin-api.yaml` synchronisée, entrée dans `m1Operations`, `make contracts` vert.

Cocher la DoD en **nommant les tests** qui couvrent chaque critère.

## Phase 9 — Livraison

```bash
git mv tasks-todo/step-NNN.md tasks-done/    # dernier commit de la PR
```

Cocher `tasks-todo/INDEX.md`, ouvrir la PR, attendre la CI, merger.

Corps de PR : les **DN** avec leur raison · les ruptures assumées · le **tableau des mutations** · les
bloquants de revue et leur résolution · ce qui reste non relu.

---

## Pièges

- **`cmd | tail` masque le code de sortie** — rediriger vers un fichier et tester `$?` (ou `PIPESTATUS`).
- **Une commande interrompue n'exécute pas sa fin** : ce qui doit avoir lieu (restauration, arrêt d'un
  service) se fait dans une commande séparée, et se vérifie.
- **Un motif écrit contre une sortie non observée est une garde morte** : produire la sortie réelle, la
  lire, puis écrire le `grep` — et le tester sur le cas qu'il doit attraper.
- **Un correctif qui s'arrête à la structure ne change rien** : ajouter un champ ou un compteur sans
  câbler ce qui l'expose (affichage, code de sortie) laisse le problème entier.
- **Un test d'intégration qui skippe est vert** — vérifier qu'il a tourné.
- **Édition par script sur du code déjà transformé** : au-delà d'une substitution triviale, éditer à la main.
- **Conteneurs partagés entre tests** : générer des valeurs uniques par appel, jamais une valeur « jolie ».
- **Le contrat sous-déclare parfois le réel** — vérifier contre le schéma, pas contre la mémoire.
- **Un sub-agent ne voit pas la conversation** : recopier design, fichiers autorisés et procédure.
- **Deux sub-agents sur un même fichier le cassent** : le partage de fichier est le critère de
  séquentialité, pas la proximité thématique.
