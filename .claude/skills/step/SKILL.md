---
name: step
description: Procédure obligatoire pour implémenter une step de tasks-todo/. À invoquer AVANT toute lecture de code et toute écriture, dès qu'une step est engagée — « attaque step-NNN », « continue sur step-NNN », « enchaîne sur la suivante ». Porte trois barrières bloquantes : design commité avant le code, test rouge avant l'implémentation, mutation avant de déclarer vert.
---

# Implémenter une step

Une step = une session ciblée = **une PR petite et verte** (plan d'exécution §0.1).

Les huit phases ci-dessous sont **ordonnées et non réordonnables**. Avant de commencer, crée **une todo
par phase** : c'est ce qui rend une phase sautée visible au lieu de la laisser passer inaperçue.

Trois phases sont des **PORTES** : tant qu'une porte n'est pas franchie, l'étape suivante est
interdite, même si le travail semble évident.

---

## Phase 1 — Contexte

Rassembler avant d'écrire une ligne :

- la **fiche** `tasks-todo/step-NNN.md` en entier (but, périmètre, points clés, tests, DoD, hors périmètre) ;
- les **contrats** concernés : `api/openapi-*.yaml`, `db/schema_passerelle_sms.sql` — ils sont la source
  de vérité, le code s'y conforme et jamais l'inverse ;
- la section du **plan d'exécution** citée par la fiche (`docs/plan-execution-passerelle.md`) ;
- le **précédent le plus proche** dans le code : la step qui a résolu un problème de même forme. Le
  suivre coûte moins cher que d'inventer un second patron (job asynchrone → step-166 ; endpoint admin
  paginé → step-186 ; couture de destination → step-165).

Si la fiche impose `ctx7` pour une bibliothèque, c'est ici qu'on l'appelle — et on **vérifie dans la
source du module** ce qui engage la correction (un ordre de `case`, une signature), pas seulement la
doc.

## Phase 2 — Arbitrages · **PORTE 1**

Lister **tous** les points que la fiche laisse ouverts. Aucun ne se tranche en silence.

Échelle d'arbitrage, dans cet ordre :

1. **La spec.** `docs/specification-technique-*.md`, le plan d'exécution, les guides, les `docs/adr/`,
   les contrats. La réponse y est plus souvent qu'on ne croit.
2. **Le modèle Fable**, si la spec ne trancherait pas. Lui soumettre la décision, les options, les
   extraits de spec pertinents et les contraintes, et lui demander un arbitrage motivé :

   ```
   Agent(subagent_type: "general-purpose", model: "fable",
         prompt: "<décision> · <options et leurs conséquences> · <ce que dit la spec> · <contraintes>
                  Tranche et justifie.")
   ```

   Son avis est **consultatif** : la décision reste la mienne, et je la consigne avec sa raison.
3. **L'arbitrage de l'utilisateur**, si le doute persiste. Présenter options + recommandation motivée,
   jamais une question nue.

## Phase 3 — Design écrit et commité · **PORTE 1 (suite)**

Écrire les décisions dans la fiche, sous `## Design arrêté (AAAA-MM-JJ)`, une par titre `### DN — …`,
chacune avec **la raison**, pas seulement le choix. Puis :

```bash
git checkout -b <type>/step-NNN-<slug>
git commit -m "docs(tasks): arrêter le design de step-NNN (…)"
```

**Aucune ligne de code avant que ce commit existe.** C'est la porte : elle force à savoir ce qu'on
construit et pourquoi, et elle laisse une trace lisible en revue.

## Phase 4 — Plan et todos

Découper en unités livrables, chacune avec son cycle rouge → vert → commit. Une unité = ce qu'un
relecteur peut accepter ou refuser seul.

## Phase 5 — TDD · **PORTE 2**

Pour chaque unité, dans cet ordre :

1. écrire le test ;
2. **le lancer et lire son message d'échec.** Il doit échouer *pour la bonne raison* — un test qui
   échoue parce que le symbole n'existe pas encore est correct ; un test qui échoue parce que la
   connexion est refusée ne prouve rien de ce qu'il affirme ;
3. implémenter le minimum ;
4. relancer.

**Aucune ligne d'implémentation avant un rouge lu.**

## Phase 6 — Mutation · **PORTE 3**

Avant de déclarer une unité verte : casser volontairement le comportement testé et **voir le test
tomber**. Une assertion jamais vue échouer n'est pas une assertion.

```bash
cp fichier.go /tmp/f.bak    # muter, lancer, constater l'échec, restaurer
cp /tmp/f.bak fichier.go
```

Se méfier en particulier d'un test qui passe du premier coup : il passe peut-être pour une raison
qui n'est pas celle qu'il annonce.

## Phase 7 — Definition of Done (§0.4)

```bash
gofmt -l cmd internal            # vide
make lint                        # 0 issue
DOCKER_HOST=unix://$HOME/.orbstack/run/docker.sock go test -race ./...   # sinon les tests conteneurisés skippent en silence
govulncheck ./...
```

Si l'API a bougé : contrat d'abord, **bump de `api/package.json`** (majeur si `oasdiff` classe `ERR`),
`api/collections/admin-api.yaml` synchronisée, entrée dans `m1Operations` de `contract_test.go`,
`make contracts` vert.

Puis cocher la DoD dans la fiche en **nommant les tests** qui couvrent chaque critère.

## Phase 8 — Livraison

```bash
git mv tasks-todo/step-NNN.md tasks-done/    # dernier commit de la PR
```
Cocher la ligne dans `tasks-todo/INDEX.md`, ouvrir la PR (corps : les décisions DN, les ruptures
assumées, le tableau des mutations), attendre la CI, merger.

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
