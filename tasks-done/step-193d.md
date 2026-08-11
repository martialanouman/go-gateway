# step-193d — `billing-svc` lit une section de config qu'il ne déclare pas

> **Jalon :** Audit pré-production, suite de step-193c · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** —

## But

Rendre vraie la promesse que `config.Load` porte pour tout le dépôt : une section lue est une section
validée. `billing-svc` en lit une qu'il ne déclare pas, donc personne ne la valide pour ce binaire.

## Le constat

`cmd/billing-svc/main.go` déclare six sections :

```go
cfg, err := config.Load(serviceName, config.SectionOTel, config.SectionRedis, config.SectionPostgres,
    config.SectionGRPC, config.SectionKafka, config.SectionClickHouse)
```

`config.SectionBilling` n'y est pas. Or le service lit `cfg.Billing.ReaperMinAge` (le seuil au-delà
duquel une réservation est réputée orpheline) et `cfg.Billing.ReaperInterval` (la cadence des passes).
Les valeurs par défaut de ces champs s'appliquent donc, mais **aucune validation de section ne tourne** :
ce que l'exploitant met dans `BILLING_REAPER_MIN_AGE` n'est vérifié par rien pour ce binaire.

Le risque n'est pas théorique. `ReaperMinAge` trop court est la direction dangereuse, et sa propre
documentation le dit : le reaper courserait `connector-pool` et réglerait des messages encore en vol —
il rembourserait des SMS réellement envoyés. C'est précisément le genre de valeur qu'une validation
existe pour attraper.

Trouvé en écrivant le câblage de step-193c ; laissé dehors parce que le correctif est un changement de
comportement (voir ci-dessous), pas un déplacement de code.

## Design arrêté

**Le correctif que cette fiche proposait est un placebo, et l'arbitrage qu'elle réservait est le bon.**
`billingProblems()` ne valide qu'`Addr`, `ReserveTimeout` et `SettleTimeout` — trois champs de *client*.
Déclarer `SectionBilling` n'aurait validé **aucun** des deux champs Reaper, et aurait exigé de `billing-svc`
un `BILLING_ADDR` non-loopback en production alors qu'il ne dial jamais billing-svc : il *est* billing-svc.

Les deux champs partent donc dans leur propre section serveur, `BillingReaper` / `SectionBillingReaper`,
déclarée par `billing-svc` seul. Le préfixe imbriqué `BILLING_REAPER_` laisse les variables **inchangées
au caractère près** (`knownVars` n'a pas bougé) : rien à faire pour un exploitant. Le godoc de `Billing` —
« configures a service's CLIENT connection » — redevient vrai.

Ce que la validation refuse, et le défaut réel derrière chaque refus :

| Refus | Le défaut qu'il attrape |
|---|---|
| `BILLING_REAPER_INTERVAL <= 0` | `runReap` le passe à `time.NewTicker`, qui **panique** : le pod mourait au boot |
| `BILLING_REAPER_MIN_AGE <= 0` | `billing.WithMinAge` l'ignore et garde son défaut : le knob rapportait un réglage qu'il n'avait pas |
| `BILLING_REAPER_MIN_AGE < 1 min` | la direction dangereuse : le reaper courserait `connector-pool` et rembourserait des SMS que le SMSC a pris |

Le plancher d'une minute est un garde-fou, pas une politique : le settle nominal suit la réponse du SMSC de
quelques millisecondes, donc une minute est deux ordres de grandeur au-dessus de tout vol nominal et quinze
fois sous le défaut. Le godoc invite ops à **élargir** la fenêtre une fois mesurée, jamais à la rétrécir.

### Le balayage a trouvé deux autres choses

- **`smpp-server-svc` lisait `cfg.GRPC.Port` sans déclarer `SectionGRPC`** — même asymétrie, depuis
  step-048. Deux surfaces gRPC s'y rejoignent, et c'est ce qui l'a masquée : le dial *client* vers
  session-manager voyage dans `SectionSMPP`, et le commentaire du `Load` ne parlait que de lui.
- **`rest-api-svc` et `smpp-server-svc` déclaraient `SectionBilling` sans jamais lire `cfg.Billing`.**
  `git log -S` les date de `e4ce5f8` (step-162b) : ils scellaient alors le corps et la DEK venait de
  billing-svc, seul détenteur du KMS. step-167 (ADR-0011) et step-101/201c ont emporté ce code ; la
  déclaration est restée. Ce n'est donc pas la sur-validation que le godoc de `Load` assume, c'est le cas
  que le godoc de `Section` interdit — un boot refusé pour une dépendance qu'on n'ouvre pas. Retiré.

### La garde plutôt qu'une relecture

Un test parse `cmd/` et échoue si un binaire lit une section sans nommer sa `config.Section<Nom>`.
Elle a nommé les deux trous avant tout correctif, et c'est son argument : trois steps de câblage avaient lu
ces fichiers sans les voir. Elle est **à sens unique** — garder l'inverse demanderait des exemptions
(`config-sync` déclare `SectionOTel` sans jamais nommer `cfg.OTel`, il passe `cfg` entier à
`InitTracing`), et une garde qui s'ouvre sur une liste d'exceptions est une garde qu'on désapprend à lire.
Deux planchers de sanité la protègent du faux vert, vérifiés en mutant l'identifiant racine et le chemin.

Elle ne suppose pas que la variable de config s'appelle `cfg` : elle **dérive** les identifiants qui
portent un `config.Config` (paramètre, `var`, résultat de `config.Load`). La convention tient partout
aujourd'hui, mais rien ne l'impose, et un service qui renommerait sa variable serait sorti de la garde sans
qu'aucun plancher ne le voie — le minimum est global, dix-sept services suffisent à le franchir. De même,
l'exhaustivité de `SectionAll` se vérifie en comptant ses bits, pas via une table de noms : une table peut
contenir une entrée fausse et passer quand même.

Ajouter une section ne touche donc que deux endroits — la struct et la constante — le test dérive le reste.

## Périmètre (ce que fait CETTE PR)

Sortir `ReaperMinAge` / `ReaperInterval` de `Billing` vers `SectionBillingReaper`, les valider, poser la
garde, et fermer les deux écarts que le balayage a trouvés.

## Points d'implémentation clés

- **C'est un changement de comportement, et c'en est tout l'intérêt.** Une section validée peut refuser
  au boot une configuration que le binaire acceptait hier. Il faut donc lire ce que
  `internal/config` valide pour `SectionBilling` (notamment `Addr`, dont le défaut `localhost:7001`
  tombe peut-être sous la règle « pas de défaut loopback en production ») et confirmer que le déploiement
  actuel passe. Si la validation porte sur des champs de **client** que billing-svc n'utilise pas — il
  est le serveur, pas l'appelant — la bonne réponse peut être de déplacer `ReaperMinAge` /
  `ReaperInterval` hors de `Billing`, pas d'y ajouter la section.
- Trancher explicitement entre les deux options et écrire pourquoi dans `## Design arrêté`.
- Balayer les neuf autres services au passage : la même asymétrie (champ lu, section non déclarée) peut
  exister ailleurs, et une garde vaudrait mieux qu'une relecture.

## Tests

- Une valeur invalide de la section fait échouer `config.Load` pour `billing-svc`, en valeur —
  `TestRequiredSectionsValidatesTheReaperKnobs`, qui charge la déclaration que le processus charge.
- Muter : remettre la section hors du `Load` doit faire tomber ce test, pas le faire passer. **Vérifié**,
  avec trois autres mutations : supprimer le refus d'intervalle, ramener le plancher à zéro, retirer la
  section de `SectionAll`. La garde, elle, a été mutée sur son identifiant racine et son chemin relatif —
  ses deux planchers de sanité crient.

### Ce que la revue a ajouté

La validation garde la valeur, elle ne garde pas le fil qui la porte. Écrire
`billing.WithMinAge(cfg.BillingReaper.Interval)` au câblage compilait et laissait toute la suite verte :
deux durées sur la même struct, interchangeables en silence. Le reaper aurait balayé à une minute au lieu
d'un quart d'heure — le défaut même que le plancher interdit, réintroduit une couche plus bas. Le défaut
est **antérieur** à cette step (`cfg.Billing.ReaperMinAge` avait la même exposition), il est fermé ici
parce que c'est cette step qui déplace ces valeurs et prétend les rendre sûres.

`TestReaperIsWiredWithTheConfiguredMinAge` construit le reaper via `reaperOptions`, l'appel que le
processus fait — répéter `WithMinAge(cfg…)` dans le test aurait gardé une copie, pas le code. Aucune API
n'est ajoutée à `internal/billing` : `ReapOnce` sur une source vide n'appelle ni l'`OutcomeReader` ni le
settler, donc la fenêtre s'observe sans base.

## Definition of Done

- [x] `make check` vert
- [x] les champs concernés ont changé de section — et sont validés là où ils ont atterri
- [x] l'arbitrage est écrit dans `## Design arrêté`, avec la raison qui l'a tranché
- [x] les dix services sont vérifiés, et la vérification est devenue une garde plutôt qu'une relecture

## Hors périmètre

Le patron de câblage → step-193c (livrée). Toute évolution du reaper lui-même.
