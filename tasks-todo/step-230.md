# step-230 — Deux gardes du banc `loadref` ne refusent pas ce qu'elles nomment

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-201f (livrée) · **Bloque :** step-280 — la campagne publie des chiffres, et une
> garde qui avertit au lieu de refuser est précisément la façon dont un mauvais chiffre se publie

## But

Fermer deux trous trouvés à la revue de step-201f PR2, et laissés ouverts délibérément parce que les
combler proprement demandait de toucher le banc du routeur et de le revalider par un run.

Aucun chiffre publié n'est en cause : les six paliers du run à 30 s passaient les deux gardes. Ce qui
est en cause, c'est que **rien ne les y obligeait**.

## Le constat

### a) `crossCheck` avertit là où ses trois sœurs refusent

`internal/e2e/refceiling_test.go` porte quatre gardes de palier. Trois retournent une `error` et
l'appelant fait `t.Fatalf` : `backlogHeld`, `shardBalance`, `breakerHeld`. La quatrième,
`crossCheck`, retourne une **chaîne** — et quand les deux sources divergent de plus de 5 %, cette chaîne
contient « the two sources disagree, this palier is not quotable ».

Le palier est donc déclaré non citable **et cité quand même** : il entre dans la courbe du balayage
(`sweepsAgree`) et dans le verdict de fidélité (`fidelityDelta`). Ce n'est pas théorique — le run de
fumée de step-201f (fenêtres de 5 s) a marqué deux paliers « avec le store » à −5,1 % et −6,7 %, et les
deux ont compté dans un verdict de 34 %.

L'asymétrie est un accident d'histoire : `crossCheck` a été écrite en step-201e comme un **rendu** pour
une courbe lue par un humain. Elle alimente désormais un **nombre unique** qui part en dimensionnement.

### b) `SubmitsByConn` / `subtractSubmits` soustraient par position

`fakesmsc.SubmitsByConn()` ne rend que les connexions **vivantes**, triées par identifiant. Le banc
appelle `subtractSubmits(avant, après)`, qui soustrait **index par index**.

Si un bind tombe et rappelle en cours de fenêtre, l'ancienne connexion quitte la table et la nouvelle
entre avec un identifiant plus grand : la longueur ne change pas, les positions glissent d'un cran, et
le compteur de la nouvelle repart de zéro. La soustraction compare alors la fin du bind *i* au début du
bind *i+1*. `shardBalance` **peut** l'attraper — un delta négatif ou minuscule ressemble à un bind
affamé — mais rien ne le garantit : si les binds portent des volumes voisins, les deltas glissés restent
plausibles et le palier passe.

## Design arrêté

### a) Scinder le contrôle numérique du rendu

`crossCheck` garde son rôle de rendu. Le calcul de l'écart en sort dans un `sourcesAgree(...) error` sur
le patron exact de `sweepsAgree`, et `measurePoolCeiling` comme `measureRouterCeiling` le mettent en
`t.Fatalf` à côté des trois autres gardes. Les deux fonctions partagent le débit dérivé du backlog
(`lagRate`), sinon l'arithmétique diverge entre l'avertissement et le refus — la faute exacte que
`TestProduceBucketsAgreeWithTheirReading` garde ailleurs.

**Le prix, et il est réel :** un palier qui avertissait fera désormais tomber tout le balayage. C'est
correct — un balayage dont un palier n'est pas citable n'est pas citable — mais ça change le
comportement du banc du routeur, qui n'appartient pas à step-201f. D'où cette fiche plutôt qu'un
correctif de revue. **La validation exige un run complet des deux bancs**, pas seulement `make check`.

### b) Nommer les binds au lieu de les compter

`SubmitsByConn` rend une `map[int]int64` clé par identifiant de connexion, et `subtractSubmits` refuse
quand l'ensemble des identifiants a changé entre les deux lectures : c'est exactement la preuve qu'un
bind a été remplacé, et le palier a mesuré deux pools différents. Un refus, pas une correction — on ne
sait pas ce que le bind disparu avait porté.

`shardBalance` prend la même carte. Son message actuel nomme l'index du bind (« bind 3 carried no
submit ») ; il nommera l'identifiant, ce qui est ce que le journal du pair permet de retrouver.

Les deux tests de `fakesmsc` qui épinglent la lecture — `TestSubmitsByConnCountsEachBindSeparately` et
`TestSubmitsByConnCountsARejectedSubmit` — comparent aujourd'hui une slice par `slices.Equal`. Ils
suivent le changement de forme : ce sont les gardes de la lecture, pas des dommages collatéraux, et ils
doivent continuer à dire *séparément par bind* ce qu'ils disent déjà.

## Périmètre

Des instruments seuls : `internal/e2e`, `internal/testutil/fakesmsc`. **Aucun changement du chemin chaud
de production**, même contrainte qu'en step-201e et step-201f.

## Tests

- `sourcesAgree` : rouge d'abord, hors build tag, sur le patron de `TestSweepsAgreeOnTheirCrossingPoint`
  — un écart hors bande échoue, la bande est symétrique, une lecture nulle échoue plutôt que de diviser.
- **Le rouge qui compte pour (a)** : `crossCheck` et `sourcesAgree` doivent s'accorder sur le MÊME jeu
  de lectures. Un test qui n'exercerait que l'un des deux laisserait le rendu et le refus dériver, et
  c'est la forme du défaut que ce dépôt a déjà payée deux fois.
- `subtractSubmits` : une carte dont les identifiants ont changé doit produire une erreur nommant la
  reconnexion, pas une soustraction. Une carte identique doit produire les mêmes deltas qu'aujourd'hui.
- Mutation obligatoire, aucune ne cassant la compilation : rendre `sourcesAgree` tolérant à 500 % ;
  faire glisser les identifiants d'un cran dans `subtractSubmits` et vérifier que `shardBalance` crie.
- **Validation par run**, et elle n'est pas optionnelle : `TestRouterConsumeCeiling`,
  `TestPoolSubmitCeiling` et `TestPoolDLRMapFidelity` doivent passer avec les gardes durcies. Si un
  palier tombe, c'est une découverte, pas une régression — mais il faut alors décider entre allonger la
  fenêtre et élargir la bande, et le consigner.

## Definition of Done

- [ ] `make check` vert (lint · `test -race` · govulncheck · contrats)
- [ ] aucune garde de palier ne rend un avertissement là où ses sœurs refusent
- [ ] une reconnexion de bind en cours de fenêtre fait tomber le palier au lieu de le fausser
- [ ] les trois bancs `loadref` ont été relancés, et tout palier nouvellement refusé est consigné dans
      `test/load/README.md` en lignes ajoutées
- [ ] aucun changement du chemin chaud de production

## Hors périmètre

Le verdict NFR et l'environnement représentatif → step-280. Le dimensionnement → step-270. La couture
autour de `bind.Submit` que step-201f a refusée — chronométrer le `submit_sm` in situ exige d'ajouter
une interface à `connectorpool.Deps`, donc un arbitrage sur le chemin chaud : si step-280 en a besoin,
elle porte cet arbitrage, pas cette fiche.
