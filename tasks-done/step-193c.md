# step-193c — Les cinq mains restées hors du patron de câblage

> **Jalon :** Audit pré-production, suite de step-193/193b · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-205, step-207

## But

Finir ce que step-193/193b ont commencé : donner un `wiring.go` / `wiring_test.go` aux services qui
n'en ont pas, **avant** que step-205 (TLS/mTLS) et step-207 (probes k8s) n'ajoutent du câblage à des
mains non testables.

## Le constat

Cinq services sur dix n'ont pas le patron :

| Service | `main.go` | Ce que step-193b en disait |
|---|---:|---|
| `billing-svc` | **351 l.** | « main déjà courte, à aligner au fil de l'eau si elle grossit » |
| `content-key-svc` | 184 l. | idem |
| `rest-api-svc` | 168 l. | *non mentionné* |
| `session-manager-svc` | 141 l. | « main déjà courte » |
| `config-sync` | 94 l. | « main déjà courte » |

Le pari de step-193b — « à aligner au fil de l'eau si elles grossissent » — a été perdu sur
`billing-svc`, devenu la **plus longue main du dépôt**. Sa `run()` porte des décisions qui méritent un
test et n'en ont pas : la config billing initiale dont l'échec est volontairement fatal, le provider
externe, et surtout la readiness — « ClickHouse est en lecture seule ici et **PAS** une dépendance de
readiness », propriété que step-207 va traduire en probe alors que rien ne la vérifie.

`rest-api-svc` n'a jamais été classé nulle part : ni dans le périmètre de 193b, ni dans son hors
périmètre. C'est l'angle mort qui a fait écrire « les deux seules mains » dans une version antérieure
de step-205.

## Périmètre (ce que fait CETTE PR)

Le patron de step-193/193b, appliqué tel quel — ne pas en inventer une variante :
`newXxxApp(ctx, cfg, logger) (*xxxApp, error)` qui assemble le graphe **sans** démarrer de goroutine ni
lier de port, une erreur retournée en **valeur** plutôt qu'un `log.Fatal`, et une pile de fermeture LIFO
qui remplace les `defer Close` de `run()`.

Priorité si la PR doit être découpée : `billing-svc` d'abord (la plus longue, et la seule dont une
propriété de readiness est déjà invoquée par une autre fiche), puis `rest-api-svc` (touchée par
step-205), puis les trois courtes.

## Points d'implémentation clés

- **Aucun changement de comportement.** C'est un déplacement de code : même ordre d'initialisation,
  même sémantique de fermeture. Toute correction tentante croisée en chemin appartient à une autre PR.
- **La readiness de `billing-svc` est la propriété à épingler** : ClickHouse ouvert en lecture seule,
  hors des dépendances de readiness, parce que le reaper est un job périodique et qu'une panne du store
  analytique ne doit pas sortir le service du load balancer.
- Le test de câblage type est déjà écrit **cinq fois** (`cmd/router-svc/wiring_test.go` et ses pairs) :
  une dépendance injoignable remonte une erreur **attribuée** (« connect postgres »), la chaîne de
  connexion et son mot de passe ne fuient **pas** dans l'erreur, et aucun port n'écoute tant que `Run`
  n'a pas été appelé.

## Tests

- Par service : une URL Postgres/Redis inparsable ou injoignable fait échouer `newXxxApp` en **valeur**,
  avec une erreur qui nomme la dépendance et ne contient aucun secret.
- Aucun port n'est lié par la construction seule — vérifié en tentant de se lier au port du service
  après `newXxxApp`.
- Muter le patron pour vérifier que le test peut échouer : remettre un `log.Fatal` (ou un `os.Exit`)
  dans le chemin d'erreur doit faire tomber le test de câblage, pas le faire passer.

## Definition of Done

- [ ] `make check` vert (lint · `test -race` · govulncheck · contrats)
- [ ] les cinq services ont `wiring.go` + `wiring_test.go`, ou la PR déclare lesquels elle laisse et
      pourquoi
- [ ] **aucun changement de comportement** — même ordre d'initialisation, même fermeture
- [ ] la readiness de `billing-svc` (ClickHouse hors readiness) est vérifiée par un test, plus par un
      commentaire

## Hors périmètre

TLS/mTLS → step-205. Probes et manifests → step-207. Toute correction de comportement croisée pendant
l'extraction : elle a sa propre fiche.
