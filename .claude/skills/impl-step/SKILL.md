---
name: impl-step
description: Procédure obligatoire pour implémenter une step de tasks-todo/. À invoquer AVANT toute lecture de code et toute écriture, dès qu'une step est engagée — « attaque step-NNN », « implémente step-NNN », « continue sur step-NNN », « enchaîne sur la suivante ». Passe par using-agent-skills à chaque phase, implémente en sub-agents parallèles, fait relire par des sub-agents en lecture seule. Quatre portes bloquantes : design commité avant le code, rouge lu avant l'implémentation, mutation vue tomber avant « vert », revue sans bloquant avant la DoD.
---

# Implémenter une step

Une step = une session ciblée = **une PR petite et verte** (plan d'exécution §0.1).

Les neuf phases ci-dessous sont **ordonnées et non réordonnables**. Avant de commencer, crée **une todo
par phase** : c'est ce qui rend une phase sautée visible au lieu de la laisser passer inaperçue.

Quatre phases sont des **PORTES** : tant qu'une porte n'est pas franchie, l'étape suivante est
interdite, même si le travail semble évident.

---

## Règle transverse — passer par `using-agent-skills`

`using-agent-skills` est le routeur qui sait quel skill encode le savoir-faire d'une phase. Trois de
ces skills portent les portes elles-mêmes : **leur invocation est obligatoire**, parce qu'ils contiennent
les vérifications qu'on ne refait jamais correctement de tête.

| Phase | Skill | |
|---|---|---|
| 2 · Arbitrages | `doubt-driven-development` | **obligatoire** — porte 1 |
| 5 · TDD · 6 · Mutation | `test-driven-development` | **obligatoire** — portes 2 et 3 |
| 7 · Revue | `code-review-and-quality` | **obligatoire** — porte 4 |
| 1 · Contexte | `context-engineering` · `find-docs`/`ctx7` si bibliothèque | selon la step |
| 3 · Design | `documentation-and-adrs` | selon la step |
| 4 · Plan | `planning-and-task-breakdown` | selon la step |
| 5 · TDD | `incremental-implementation` | selon la step |
| 7 · Revue | `security-and-hardening` | si la step expose une surface |
| 8 · DoD | `ci-cd-and-automation` | selon la step |
| 9 · Livraison | `git-workflow-and-versioning` | selon la step |

Les optionnels s'invoquent quand la step le justifie, pas par principe : ils coûtent du contexte et
proposent parfois des conventions **concurrentes** à celles du dépôt (un `tasks/plan.md` à côté de
`tasks-todo/`, un `docs/decisions/` à côté de `docs/adr/`). Quand c'est le cas, **le dépôt gagne** — et
le dire tout de suite évite de laisser deux conventions coexister.

Le projet est en Go : `golang-how-to` s'active en plus dès qu'on touche au code et charge les skills Go
pertinents (concurrence, erreurs, tests, base de données…). Laisse-le faire, il voit mieux que toi
quelles familles sont en jeu.

---

## Phase 1 — Contexte

`using-agent-skills` → `context-engineering`.

Rassembler avant d'écrire une ligne :

- la **fiche** `tasks-todo/step-NNN.md` en entier (but, périmètre, points clés, tests, DoD, hors périmètre) ;
- les **contrats** concernés : `api/openapi-*.yaml`, `db/schema_passerelle_sms.sql` — ils sont la source
  de vérité, le code s'y conforme et jamais l'inverse ;
- la section du **plan d'exécution** citée par la fiche (`docs/plan-execution-passerelle.md`) ;
- le **précédent le plus proche** dans le code : la step qui a résolu un problème de même forme. Le
  suivre coûte moins cher que d'inventer un second patron (job asynchrone → step-166 ; endpoint admin
  paginé → step-186 ; couture de destination → step-165).

Si une bibliothèque est en jeu — ajout, mise à jour, ou simple usage d'une API — c'est ici qu'on appelle
`ctx7`, et on **vérifie dans la source du module** ce qui engage la correction (un ordre de `case`, une
signature), pas seulement la doc. Une signature devinée de mémoire est la panne la plus chère du lot :
elle compile parfois.

**Pour un outil externe, relever la version réellement installée** (`<outil> version`) et la comparer à
ce que la doc décrit. Une majeure sortie après ma date de connaissance change les codes de sortie, le
format de sortie et parfois le nom des options — et rien ne le signale, le script tourne simplement en
disant autre chose que ce qu'on croit. Si la version est postérieure à ce que je connais, la sonder avec
un cas jetable coûte deux minutes et évite d'écrire une garde morte.

Cette phase se parallélise bien : lancer plusieurs `Explore` (lecture seule) sur des axes disjoints
— la fiche et son plan, le précédent dans le code, les contrats concernés — coûte moins cher qu'une
lecture séquentielle et ne risque rien puisque personne n'écrit.

## Phase 2 — Arbitrages · **PORTE 1**

`using-agent-skills` → `doubt-driven-development`.

Lister **tous** les points que la fiche laisse ouverts. Aucun ne se tranche en silence : un choix fait
sans trace est un choix que personne ne pourra contester en revue.

Échelle d'arbitrage, dans cet ordre :

1. **La spec.** `docs/specification-technique-*.md`, le plan d'exécution, les guides, les `docs/adr/`,
   les contrats. La réponse y est plus souvent qu'on ne croit — cherche avant de délibérer.
2. **Le modèle Fable**, si la spec ne tranche pas. Lui soumettre la décision, les options, les extraits
   de spec pertinents et les contraintes :

   ```
   Agent(subagent_type: "general-purpose", model: "fable",
         prompt: "<décision> · <options et leurs conséquences> · <ce que dit la spec> · <contraintes>
                  Tranche et justifie. Si tu ne peux pas trancher, dis-le explicitement et dis pourquoi.")
   ```

   **Si Fable tranche clairement et sans contredire la spec, on applique** et on consigne la décision
   avec sa raison. On remonte à l'humain seulement si Fable refuse de trancher, se contredit, ou
   propose quelque chose que la spec interdit.
3. **L'arbitrage de l'utilisateur**, en dernier recours. Toujours options + recommandation motivée,
   jamais une question nue : une question nue transfère le travail de réflexion au lieu de la décision.

## Phase 3 — Design écrit et commité · **PORTE 1 (suite)**

`using-agent-skills` → `documentation-and-adrs`.

Écrire les décisions dans la fiche, sous `## Design arrêté (AAAA-MM-JJ)`, une par titre `### DN — …`,
chacune avec **la raison**, pas seulement le choix. Puis :

```bash
git checkout main && git pull            # depuis main, sauf dépendance déclarée
git checkout -b <type>/step-NNN-<slug>
git commit -m "docs(tasks): arrêter le design de step-NNN (…)"
```

**Brancher depuis `main`.** Partir de la branche courante paraît sans conséquence et ne se voit qu'en
phase 9, quand la PR se révèle porter plusieurs sujets et qu'il est trop tard pour les séparer sans
réécrire l'historique. Si la step dépend réellement d'une branche non mergée, c'est une décision : la
consigner en `DN` et l'annoncer en tête du corps de PR, pour que le relecteur sache avant d'ouvrir le
diff pourquoi il y trouve autre chose.

**Aucune ligne de code avant que ce commit existe.** C'est la porte : elle force à savoir ce qu'on
construit et pourquoi, elle laisse une trace lisible en revue, et elle empêche l'inversion la plus
fréquente — écrire le code puis fabriquer la justification qui lui va.

## Phase 4 — Plan et todos

`using-agent-skills` → `planning-and-task-breakdown`.

**Toujours un plan avant la moindre implémentation.** Découper en unités livrables, chacune avec son
cycle rouge → vert → commit. Une unité = ce qu'un relecteur peut accepter ou refuser seul.

Le plan vit dans la **fiche de la step et dans la todo list**. Les skills génériques proposent souvent
d'écrire un `tasks/plan.md` : ne pas le faire ici. Ce dépôt a déjà `tasks-todo/step-NNN.md`, et deux
conventions concurrentes pour la même chose finissent toujours par diverger — la convention du dépôt
prime sur celle d'un skill générique, ici comme ailleurs.

Le plan sert aussi à décider ce qui se parallélise. Marque pour chaque unité **les fichiers qu'elle
touche** : deux unités qui partagent un fichier ne partent jamais en parallèle (voir phase 5).

Crée la todo list ici, une entrée par unité, en plus des todos de phase.

## Phase 5 — TDD · **PORTE 2**

`using-agent-skills` → `test-driven-development` + `incremental-implementation`.

Pour chaque unité, dans cet ordre :

1. écrire le test ;
2. **le lancer et lire son message d'échec.** Il doit échouer *pour la bonne raison* — un test qui
   échoue parce que le symbole n'existe pas encore est correct ; un test qui échoue parce que la
   connexion est refusée ne prouve rien de ce qu'il affirme ;
3. implémenter le minimum ;
4. relancer.

**Aucune ligne d'implémentation avant un rouge lu.**

### Paralléliser les unités indépendantes

Les unités qui ne partagent **aucun fichier** partent en sub-agents simultanés (un seul message, un
`Agent` par unité). Celles qui partagent un fichier restent séquentielles : deux agents qui éditent le
même fichier produisent un demi-fichier, pas un conflit propre.

Chaque sub-agent reçoit un mandat complet et autonome — il ne voit pas ta conversation :

```
Agent(subagent_type: "general-purpose",
      prompt: "Unité <N> de step-NNN : <objectif>.
               Design arrêté applicable : <DN concernées, recopiées>.
               Fichiers dont tu es le SEUL propriétaire : <liste>. N'édite rien d'autre.
               Ne fais AUCUN git add / git commit / git checkout.
               Procédure imposée : écris le test, LANCE-LE, cite son message d'échec dans ton
               rapport, puis implémente le minimum, relance.
               Rends : le message du rouge, les fichiers touchés, le résultat final du test.")
```

Exige le message du rouge dans le rapport : c'est la seule preuve que le sub-agent a bien fait du TDD
et non écrit le code puis un test complaisant par-dessus. Un rapport sans rouge cité = unité à refaire.

**Interdis-leur de committer**, explicitement, dans chaque mandat. Deux agents qui committent en même
temps sur la même branche se disputent l'index git, et l'un des deux embarque le travail à moitié écrit
de l'autre. C'est toi qui commites, après relecture, une unité par commit.

Si une unité est en doute — code inconnu, invariant sensible, opération irréversible — c'est
`doubt-driven-development` qu'il faut, pas plus de parallélisme.

## Phase 6 — Mutation · **PORTE 3**

Avant de déclarer une unité verte : casser volontairement le comportement testé et **voir le test
tomber**. Une assertion jamais vue échouer n'est pas une assertion.

```bash
cp fichier.go /tmp/f.bak    # muter, lancer, constater l'échec, restaurer
cp /tmp/f.bak fichier.go
git diff --stat fichier.go  # VÉRIFIER que la restauration a eu lieu
```

**Vérifie la restauration séparément, jamais dans la même commande que la mutation.** Un test muté peut
partir en deadlock ou dépasser le délai d'exécution ; la commande est alors interrompue et le `cp` de
restauration qui la suivait n'est jamais exécuté. Le fichier reste muté sans que rien ne l'annonce, et
la suite de la session travaille sur du code saboté. Un `git diff` explicite après chaque mutation coûte
une seconde.

Se méfier en particulier d'un test qui passe du premier coup : il passe peut-être pour une raison qui
n'est pas celle qu'il annonce. Le mode d'échec récurrent sur ce dépôt est la fixture creuse — un test
dont le montage n'atteint jamais la condition qu'il prétend vérifier, et qui reste donc vert quoi qu'on
casse.

Tenir le **tableau des mutations** au fil de l'eau (mutation appliquée → test qui tombe) : c'est ce qui
part dans le corps de PR en phase 9.

### Répéter, avant de conclure au vert

```bash
go test -race -count=10 ./<paquets touchés>/...    # 25 si le code est concurrent
```

Un passage unique ne dit rien d'un défaut qui dépend de l'ordonnancement. Une course qui bloque un run
sur dix passe en `-count=1`, traverse la revue, et n'apparaît qu'en production ou chez le prochain qui
lance la suite deux fois. Sur ce dépôt, une course a survécu à **deux tours de revue complets** et à
toutes les vérifications manuelles avant d'être vue au premier `-count=10`.

La répétition porte sur les paquets touchés, pas sur tout le module : c'est ce qui la rend assez rapide
pour être systématique. Et un test qui devient flaky sous répétition est une information, pas un
inconvénient — ne jamais le « stabiliser » en relâchant son seuil sans avoir compris ce qui varie.

## Phase 7 — Revue par sub-agents en lecture seule · **PORTE 4**

`using-agent-skills` → `code-review-and-quality`.

Lancer plusieurs relecteurs **en lecture seule** en parallèle, chacun sur un axe distinct. La lecture
seule est structurelle : `subagent_type: "Plan"` n'a ni `Edit` ni `Write`. Les relecteurs constatent,
**c'est toi qui corriges** — un relecteur qui répare son propre constat ne le rapporte plus, et le
constat disparaît sans que personne l'ait jugé.

Des axes distincts trouvent plus que des relecteurs redondants. Pour ce dépôt :

- **Invariants projet** — corps de message jamais logué/spanné/labellisé, ordre du pipeline MT, SQL
  paramétré, atomicité Lua côté Redis, idempotence de facturation, `max_sessions` ;
- **Correction & concurrence** — goroutines sans condition d'arrêt, `context.Context` en 1er paramètre,
  chemins d'erreur, cas limites, races ;
- **Contrats & schéma** — conformité à `api/openapi-*.yaml` et `db/schema_passerelle_sms.sql`, le code
  se conforme au contrat et jamais l'inverse ;
- **Tests** — est-ce que chaque test peut réellement échouer ? fixtures creuses, intégrations qui
  skippent en silence, assertions qui n'assertent rien.

Demande à chaque relecteur de classer ses constats : **bloquant** (défaut de correction, invariant
violé, contrat trahi) · **à corriger** (dette lisible qu'on ne laisse pas passer) · **note** (avis).

```
Agent(subagent_type: "Plan",
      prompt: "Relis le diff de step-NNN (`git diff main...HEAD`) sur l'axe <axe>.
               Design arrêté : <DN>. Ne modifie aucun fichier.
               Pour chaque constat : fichier:ligne, le défaut, le scénario concret qui casse,
               et une classification bloquant | à corriger | note.")
```

### Un correctif est du code : il repasse par les portes 2 et 3

C'est **la** règle de cette phase, et celle qu'on saute le plus volontiers, parce qu'un constat de revue
donne l'impression que la réponse est évidente et urgente. Pour chaque constat bloquant :

1. **écrire le test qui le reproduit**, et le voir échouer — le constat du relecteur n'est pas une preuve,
   c'est une hypothèse tant qu'un rouge ne l'a pas confirmée chez toi ;
2. corriger ;
3. **muter la correction** et voir le nouveau test tomber.

Sur ce dépôt, l'écart entre les deux façons de faire est mesuré : les correctifs passés par un rouge ont
tenu ; ceux écrits directement — une garde qui ne pouvait pas matcher, un plafond qui accusait un pair
sain, une vérification qui avait perdu ce qu'elle vérifiait — ont tous été retrouvés cassés au tour
suivant. **La moitié des bloquants d'un run venaient des correctifs des tours précédents.**

Un constat qu'on ne parvient pas à reproduire par un test mérite d'être discuté avant d'être corrigé :
soit il n'est pas réel, soit le test qui manque est plus important que la correction.

### Boucler, et savoir s'arrêter

**Tant qu'il reste un bloquant**, tu corriges et tu relances une revue sur le nouveau diff. Mais lis ce
que le tour trouve, pas seulement combien :

- **des bloquants dans le code d'origine** → la revue fait son travail, continue ;
- **des bloquants dans les correctifs du tour précédent** → le signal ne dit pas « ajoute un tour », il
  dit *tu corriges trop vite*. La réponse est la règle ci-dessus, pas un relecteur de plus.

Deux sorties, et une seule est automatique :

- le **même** bloquant survit à trois tours → ce n'est plus un défaut mais un désaccord de conception :
  il remonte à l'utilisateur et se tranche en phase 2 ;
- des bloquants **nouveaux** à chaque tour, sans convergence → la question n'est plus « en reste-t-il ? »
  (la réponse sera toujours peut-être) mais « le coût d'un tour de plus dépasse-t-il le risque
  résiduel ? ». **Cet arbitrage est celui de l'utilisateur, pas le tien** : présente-lui le compte par
  tour, ce que chacun a trouvé, ce qui reste non relu, et une recommandation. Ne t'arrête jamais en
  silence, et n'enchaîne pas non plus indéfiniment de ta propre initiative.

Ce qui est gelé sans avoir été relu se consigne dans la fiche, nommément, avant de passer en DoD.

Deux formes que prennent presque toujours ces bloquants de second tour :

- **La garde qui échoue ouverte.** Une vérification écrite contre un format de sortie qu'on n'a pas
  observé (un `grep` sur un rapport d'outil, un code de sortie supposé) ne se plaint pas quand elle ne
  correspond à rien : elle laisse passer en silence exactement ce qu'elle surveille. Avant d'écrire un
  motif, **produire la sortie réelle et la lire** ; puis vérifier la garde sur le cas qu'elle doit
  attraper, pas seulement sur le cas nominal.
- **Le correctif qui s'arrête à la structure.** Ajouter le champ, la mesure ou le compteur sans câbler
  ce qui l'expose — affichage, code de sortie, message d'erreur — ne change rien pour qui utilise
  l'outil. Se demander à chaque correctif : *qu'est-ce qui aurait changé, à l'écran, pour la personne
  qui a signalé le problème ?*

Si le même bloquant survit à trois tours, arrête la boucle et remonte-le à l'utilisateur avec les
positions en présence : à ce stade ce n'est plus un défaut, c'est un désaccord de conception, et il se
tranche par la phase 2, pas par un tour de revue de plus.

## Phase 8 — Definition of Done (§0.4)

`using-agent-skills` → `ci-cd-and-automation`.

```bash
gofmt -l cmd internal            # vide
make lint                        # 0 issue
DOCKER_HOST=unix://$HOME/.orbstack/run/docker.sock go test -race ./...   # sinon les tests conteneurisés skippent en silence
govulncheck ./...
```

Si l'API a bougé : contrat d'abord, **bump de `api/package.json`** (majeur si `oasdiff` classe `ERR`),
`api/collections/admin-api.yaml` synchronisée, entrée dans `m1Operations` de `contract_test.go`,
`make contracts` vert.

Puis cocher la DoD dans la fiche en **nommant les tests** qui couvrent chaque critère. Un critère coché
sans nom de test est une case cochée, pas un critère couvert.

## Phase 9 — Livraison

`using-agent-skills` → `git-workflow-and-versioning`.

```bash
git mv tasks-todo/step-NNN.md tasks-done/    # dernier commit de la PR
```

Cocher la ligne dans `tasks-todo/INDEX.md`, ouvrir la PR, attendre la CI, merger.

Corps de PR : les décisions **DN** avec leur raison, les ruptures assumées, le **tableau des mutations**
(phase 6), et les bloquants remontés en revue avec leur résolution.

---

## Pièges constatés sur le terrain

- **`cmd1 | tail` masque le code de sortie.** `make test | tail -3 && git commit` commite même sur
  échec. Rediriger vers un fichier et tester `$?`.
- **Édition par script sur du code déjà transformé** : le remplacement se mord la queue et laisse le
  fichier à moitié édité. Au-delà d'une substitution triviale, éditer à la main.
- **Conteneurs partagés entre tests d'un même paquet** : semer une valeur « jolie » (`22507000001`)
  entre en collision avec un test qui compte globalement. Générer des valeurs uniques par appel.
- **Un test d'intégration qui skippe est vert.** Vérifier qu'il a réellement tourné (`-v`) avant de
  s'appuyer dessus.
- **Le contrat sous-déclare parfois le réel** (un enum à 6 valeurs pour une colonne qui en écrit 8).
  Le vérifier contre le schéma, pas contre la mémoire.
- **Un sub-agent ne voit pas ta conversation.** Le design arrêté, les fichiers autorisés et la
  procédure attendue se recopient dans son prompt, sinon il réinvente — et il réinvente autrement que
  ses voisins lancés en même temps.
- **Deux sub-agents sur un même fichier le cassent.** Le partage de fichier, pas la proximité
  thématique, est le critère de séquentialité. Et deux sub-agents qui committent en même temps se
  disputent l'index git : leur interdire de committer est plus simple que de démêler après coup.
- **Une commande interrompue n'exécute pas sa fin.** Tout ce qui suit un `&&` derrière une commande
  qui peut pendre — restauration d'un fichier muté, arrêt d'un service, nettoyage — n'est pas garanti.
  Ce qui doit absolument avoir lieu se fait dans une commande séparée, et se vérifie.
- **Un motif écrit contre une sortie non observée est une garde morte.** Produire la sortie réelle,
  la lire, puis écrire le `grep` — et le tester sur le cas qu'il doit attraper.
