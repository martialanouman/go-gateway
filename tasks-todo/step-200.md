# step-200 — Harnais de charge k6/vegeta + générateur de binds SMPP (NFR)

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-201

## But
Livrer la campagne de charge : scripts k6/vegeta (REST) + générateur de binds SMPP, ciblant les NFR —
8 000 SMS/s soutenu, 15 000 en pic — avec les budgets de latence (ingestion p99 < 250 ms, bout-en-bout
p99 < 2 s, disjoncteur fermé).

## Périmètre (ce que fait CETTE PR)
- `deploy/load/` (ou `test/load/`) : scripts **k6** ou **vegeta** pour `POST /messages` + générateur de
  binds SMPP concurrents (réutilise `internal/smpp`).
- Profils : soutenu 8000/s, pic 15000/s ; seuils de latence encodés dans les scripts.
- Documentation courte de lancement (make cible `make load`).

## Points d'implémentation clés
- **k6/vegeta sont des binaires hors `go.mod`** (§1.3/§16) — installés à part, pas une dépendance Go.
  **`ctx7`** pour la syntaxe des scripts k6 (thresholds, scenarios) / l'usage de vegeta.
- Le générateur de binds SMPP est du Go (client `internal/smpp`), pas un binaire externe.
- Les seuils encodent les NFR : le run échoue si p99 dépasse le budget (§16 critère).
- Ne pas polluer le chemin de prod : le harnais vit sous `deploy/`/`test/`, pas `internal/`.

## Tests (écrits dans la même PR)
- Un run local court (débit réduit) passe les seuils encodés — preuve que le harnais mesure bien.
- Le générateur de binds établit N binds concurrents (test unitaire du générateur).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] NFR encodés en seuils ; k6/vegeta hors `go.mod`

## Hors périmètre
Tuning (partitions/batch/pool) → step-201. Chaos → step-202/203.

## Design arrêté (2026-08-01)

Arbitrage : la spec a tranché D0 ; D1–D5 sont revenus à Fable (avis appliqué) ; D6 diverge de son avis
et l'écart est motivé ci-dessous. Pas de contre-avis externe : `gemini` et `codex` sont absents de la
machine, le cross-model n'a donc pas pu tourner.

### D0 — Débit soutenu encodé : 8 000/s, pic 15 000/s

La spec paraissait se contredire — §1.2 (tableau NFR) annonce une plage « soutenu 5 000–10 000 SMS/s »
là où §2.1, le livrable M12 et la stratégie de test §4.7 disent tous « 8 000 ». Il n'y a pas de conflit :
8 000 est un point de la plage, et c'est celui que retiennent les trois sources qui parlent de M12.
**Raison :** encoder la plage produirait trois profils au lieu d'un sans rien prouver de plus ; la valeur
de référence d'un jalon doit être un point, pas un intervalle, sinon « tenu » n'a pas de définition.

### D1 — k6 seul, vegeta écarté

Le critère d'acceptation est « le run échoue si p99 dépasse le budget ». k6 le fait nativement
(`thresholds` + `abortOnFail` → code de sortie non nul) ; vegeta produit un rapport qu'il faut
post-traiter, c'est-à-dire du shell non testé qui porterait l'assertion la plus critique de la step.
**Raison :** la contrainte « hors `go.mod` » (§16) interdit d'importer vegeta comme bibliothèque Go —
son seul avantage décisif sur k6. Réduit à son binaire, il est dominé. Livrer les deux, ce serait deux
chaînes d'installation et deux syntaxes de profil à maintenir pour une seule capacité.
**Coût assumé :** du JavaScript dans un dépôt Go, hors `golangci-lint` et hors `go vet`. Compensé par D5,
qui l'exécute à chaque PR.

### D2 — Les fichiers vivent sous `test/load/`, pas `deploy/load/`

**Raison :** `deploy/` est réservé par le plan (§16) aux manifests Kubernetes livrés plus tard dans le
même jalon ; un harnais de charge n'est pas un artefact de déploiement. Sous `test/`, tout package Go est
compilé, testé `-race` et linté par les cibles existantes — ce n'est pas un inconvénient, c'est la
garantie anti-pourrissement recherchée en D5.

### D3 — Cette PR prouve que le harnais mesure et sait échouer. Elle ne prouve pas le NFR.

Un p99 mesuré à débit réduit sur une machine de dev ne dit rien du NFR à 8 000/s : la latence se dégrade
non linéairement à la saturation. Et un run « qui passe les seuils » contre un stub local passe
**trivialement** — c'est un vert sans contenu.

Donc la preuve est le **run négatif** : le même script, relancé contre un stub à délai artificiel
au-dessus du budget, doit sortir en code non nul. C'est la mutation appliquée au harnais lui-même. Le
run positif seul ne serait pas une assertion.

Les profils `sustained` (8 000/s) et `peak` (15 000/s) sont écrits et sélectionnables
(`PROFILE=smoke|sustained|peak`) mais **ne tournent ni dans cette PR ni en CI**. Le verdict NFR appartient
à step-201, sur matériel réel.

### D4 — `bindgen` : logique sous `test/load/bindgen`, entrée sous `cmd/smpp-bindgen`

Un `package main` n'est pas testable de l'extérieur, or la fiche exige un test unitaire du générateur —
la logique doit donc être un package importable. Elle va sous `test/load/bindgen` (D2), le point d'entrée
suit le patron établi par `cmd/fake-smsc`, `cmd/migrate` et `cmd/mt-replay` (doc-comment « tool, not a
deployable service »). Le test tourne contre `fakesmsc.Start` : port éphémère, cleanup automatique,
aucune dépendance externe, donc aucun skip.

`internal/connectorpool.dialAndBind` **n'est pas exporté** pour l'occasion : élargir la surface publique
d'un package de prod au bénéfice d'un outil de test est exactement la pollution que la fiche interdit.
**Coût assumé :** ~30 lignes de dial-and-bind dupliquées, prix du découplage.
**Écart à la lettre de la fiche :** elle dit « le harnais vit sous `deploy/`/`test/` », et le `main` vit
sous `cmd/`. Le contraste qu'elle trace est avec `internal/` ; `cmd/` est déjà le foyer des outils non
déployables du dépôt.

### D5 — Un job CI `load-smoke`, jamais les profils pleins

Le job installe k6 (version épinglée + checksum), lance le smoke positif (exit 0 exigé) puis le run
négatif de D3 (exit ≠ 0 exigé). **Raison :** trois couches empêchent le harnais de pourrir en silence —
`bindgen` et le stub sont des packages Go ordinaires soumis aux gates existants ; le script k6 est
*exécuté* à chaque PR, donc toute dérive de syntaxe ou du contrat de requête casse la CI bruyamment ;
et `make load` échoue en dur si k6 est absent au lieu de se skipper.
Jamais 8 000/15 000 en CI : un runner GitHub ne les tient pas, et un échec là-bas serait du bruit.

### D6 — Le budget bout-en-bout p99 < 2 s n'est pas encodé dans cette PR

k6 mesure soumission → réponse HTTP, ce qui couvre exactement l'**ingestion** p99 < 250 ms (le 202 suit
l'ACK durable Kafka, l'ordre du pipeline le garantit). Le bout-en-bout — « soumission → tentative de
remise SMSC » — exige une corrélation côté sortie (horodatages fake SMSC ou CDR ClickHouse) qu'un script
k6 ne voit pas. La fiche demande pourtant « les seuils » encodés dans les scripts : c'est infaisable pour
celui-là.

Fable recommandait un vérificateur post-run dans cette PR. **Je ne le retiens pas** : un tel vérificateur
exige un pipeline complet en marche (Kafka + SMSC + CDR), c'est-à-dire précisément l'environnement que
monte step-201. L'y construire ici gonflerait la PR sans pouvoir l'exercer.
**Décision :** cette PR encode le seuil d'ingestion et documente le bout-en-bout comme non mesurable
côté client ; le vérificateur de corrélation est un prérequis explicite de step-201.

### D7 — Aucun skip : `make load` échoue en dur si k6 manque

Un test d'intégration qui skippe est vert (piège documenté du dépôt). k6 n'étant installé nulle part
aujourd'hui, un `go test` qui l'invoquerait se skipperait **partout** et n'assurerait rien.
**Raison :** le run k6 n'est donc pas un `go test` mais une cible Make + un job CI qui installe son propre
binaire ; l'absence de k6 est une erreur bruyante avec le message d'installation, jamais un silence.
