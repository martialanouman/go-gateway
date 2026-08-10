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

## Périmètre (ce que fait CETTE PR)

Ajouter `config.SectionBilling` au `config.Load` de `cmd/billing-svc/main.go` — **après** avoir vérifié
ce que la section valide et ce qu'un déploiement existant y met.

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

- Une valeur invalide de la section fait échouer `config.Load` pour `billing-svc`, en valeur.
- Muter : remettre la section hors du `Load` doit faire tomber ce test, pas le faire passer.

## Definition of Done

- [ ] `make check` vert
- [ ] la section lue par `billing-svc` est validée, ou les champs concernés ont changé de section
- [ ] l'arbitrage entre les deux est écrit, pas implicite
- [ ] les autres services sont vérifiés pour la même asymétrie

## Hors périmètre

Le patron de câblage → step-193c (livrée). Toute évolution du reaper lui-même.
