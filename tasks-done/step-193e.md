# step-193e — L'ordre de fermeture est une propriété du câblage, et rien ne le garde

> **Jalon :** Audit pré-production, suite de step-193/193b/193c · **Statut :** À FAIRE
> **Dépend de :** step-193c (livrée) · **Bloque :** —

## But

Faire porter au test de câblage la propriété qu'il prétend déjà couvrir : **l'ordre dans lequel
`newXxxApp` enregistre ses closers**, et non le seul fait qu'une pile s'inverse.

## Le constat

Chaque `wiring_test.go` porte un `TestAppCloseReleasesInReverseOrderOfOpening` qui empile deux ou trois
closures à la main sur un `xxxApp{}` nu et vérifie que `close()` les rejoue à l'envers. Il prouve le
**mécanisme**. Il ne touche jamais `newXxxApp`, donc il ne prouve rien du **câblage**.

L'écart est concret chez `admin-api-svc`, dont le commentaire de `newRunners` décrit une propriété qui
n'est pas celle que son test vérifie :

> « the background runners' jobs use the Postgres pool, so the drain must complete BEFORE the pool is
> closed: this closer is registered after the stores', and closers run in reverse, so it does. »

C'est une affirmation sur l'**ordre d'enregistrement** dans `newAdminApp`. Échanger les deux `onClose`
fermerait le pool sous des jobs en vol — à chaque déploiement, sur le chemin du drain — et **toute la
suite de tests resterait verte**. Le commentaire est aujourd'hui la seule garde.

Les autres services s'en tirent par chance plus que par preuve : chez `billing-svc` les trois closers
sont indépendants (le producer Kafka ne touche pas le pool Postgres), donc un mauvais ordre n'y casse
rien *aujourd'hui*. C'est vrai jusqu'à ce qu'un closer se mette à dépendre d'un autre.

Trouvé à la revue de step-193c (PR #153).

## Design arrêté

Nommer les closers, et remplacer le test synthétique par une assertion sur le graphe réel.

```go
type closer struct {
	name string
	fn   func()
}

func (a *adminApp) onClose(name string, f func()) {
	a.closers = append(a.closers, closer{name, f})
}

func (a *adminApp) close() {
	for i := len(a.closers) - 1; i >= 0; i-- {
		a.closers[i].fn()
	}
}
```

Un test frère construit le graphe complet et asserte l'ordre — il est dans le même `package main`, donc
**aucune méthode de production n'est ajoutée pour les besoins du test**.

`TestAppCloseReleasesInReverseOrderOfOpening` **disparaît** : la nouvelle assertion couvre à la fois
l'inversion et l'ordre d'enregistrement. Le bilan est un échange — un test synthétique en moins par
service, une assertion réelle en plus, zéro API nouvelle.

### L'extrait que cette fiche portait ne tenait pas sa propre exigence

Cette fiche demandait, plus bas : « Inverser la boucle de `close()` doit le faire tomber aussi. » Elle
proposait pour cela de lire la pile à l'envers dans le test :

```go
for i := len(app.closers) - 1; i >= 0; i-- {   // ← rejoue la boucle de close() côté test
	got = append(got, app.closers[i].name)
}
```

`close()` n'y est jamais appelé : le test réimplémente sa boucle, donc l'inverser en production le laisse
vert. Les deux côtés bougent ensemble — le défaut aller-retour.

L'ordre s'observe donc en **exécutant** `close()`. `releaseOrder` enveloppe chaque `fn` enregistrée pour
qu'elle s'annonce avant de libérer, puis appelle la vraie méthode :

```go
func releaseOrder(a *adminApp) []string {
	var released []string
	for i := range a.closers {
		c := a.closers[i] // une copie : c.fn est l'originale, pas la doublure
		a.closers[i].fn = func() { released = append(released, c.name); c.fn() }
	}
	a.close()
	return released
}
```

Conséquence de structure : l'assertion ne peut pas vivre dans `TestNewXxxAppBuildsTheWholeGraph`, qui
tient déjà un `defer app.close()` — libérer deux fois un consumer Kafka n'est pas anodin. Elle prend la
place laissée par le test synthétique, en test frère.

Vérifié : les deux mutations tombent, aucune ne casse la compilation. Sur `admin-api-svc`,
`onClose("stores", …)` déplacé après `onClose("runners", …)` donne `[… stores runners]`, et `close()`
itéré à l'endroit donne `[stores runners redis clients feed]`.

## Périmètre (ce que fait CETTE PR)

Les **dix** services : les cinq de step-193/193b (`router-svc`, `connector-pool-svc`,
`mo-dlr-router-svc`, `admin-api-svc`, `smpp-server-svc`) et les cinq de step-193c (`billing-svc`,
`content-key-svc`, `rest-api-svc`, `session-manager-svc`, `config-sync`).

Pour chacun : `onClose` nommé, `close()` adapté, assertion d'ordre dans le test du graphe complet,
suppression du test synthétique.

## Points d'implémentation clés

- **Aucun changement de comportement.** L'ordre réel doit rester exactement celui d'aujourd'hui : la
  PR le rend vérifiable, elle ne le corrige pas. Si un service s'avère avoir le mauvais ordre, c'est
  une découverte à traiter dans son propre commit, annoncée comme telle.
- Les noms sont ceux des sous-graphes déjà en place (`stores`, `runners`, `clients`, `alerts`…), pas des
  noms neufs. Deux corrigent ce que le test synthétique racontait de faux : `router-svc` empilait trois
  marqueurs (`postgres`, `redis`, `billing`) pour un graphe qui en enregistre **six**, et `billing` n'y
  est même pas un closer ; `billing-svc` nommait `clickhouse` ce qui est le sous-graphe `reaper`, dont
  la connexion n'est qu'un champ.
- Les quatre services à un seul closer (`content-key-svc`, `session-manager-svc`, `config-sync`,
  `rest-api-svc`) gagnent une assertion à un élément. Elle vaut quand même : elle cassera le jour où
  quelqu'un ajoutera un second closer du mauvais côté — précisément ce que step-205 (TLS) va faire.

## Tests

- Par service : l'assertion d'ordre sur le graphe réellement construit. **Vérifié** : les dix passent, et
  aucune ne saute — elles vivent derrière `pgtest`/`redistest`, donc elles sont muettes sous `-short` et
  sans Docker. C'est déjà le régime de tous les `TestNewXxxAppBuildsTheWholeGraph`, et la CI
  (`go test -race ./...`, sans `-short`) les exécute.
- Muter à la couche où le défaut vivrait : **échanger deux `onClose` dans `newXxxApp`** doit faire
  tomber le test. Inverser la boucle de `close()` doit le faire tomber aussi. **Vérifié** sur
  `admin-api-svc` (stores/runners) et `router-svc` (accepted/outcome), pour les deux mutations.
- Vérifier qu'un service à un seul closer voit bien son assertion échouer si on retire l'`onClose`.
  **Vérifié** sur `config-sync` : `release order is [], want [stores]` — le client Redis fuyait, et plus
  rien ne le disait.

## Definition of Done

- [x] `make check` vert
- [x] les dix services assertent leur ordre de fermeture sur le graphe réel
- [x] `TestAppCloseReleasesInReverseOrderOfOpening` n'existe plus nulle part
- [x] aucun ordre de fermeture n'a changé — le diff des `onClose` est une pure addition d'argument

## Hors périmètre

Le patron de câblage lui-même → step-193c (livrée). TLS → step-205, probes → step-207.
