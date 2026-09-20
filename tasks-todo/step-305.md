# step-305 — Le TLS client vers les quatre magasins de données

> **Jalon :** M12 · **Statut :** À FAIRE
> **Dépend de :** step-300 (livrée) · **Bloque :** step-410 (go-live)

## Pourquoi cette fiche existe

step-300 a chiffré **nos écoutes et nos appels** : les quatre serveurs gRPC, les deux APIs HTTP, le
port SMPP entrant et le bind SMPP sortant. Elle a délibérément laissé dehors le lien vers les
**magasins de données**, en annonçant cette step pour le porter — et en constatant que l'axe n'est pas
uniforme :

| | Atteignable aujourd'hui |
|---|---|
| PostgreSQL | **oui**, sans code — `sslmode=require` dans `POSTGRES_URL`, lu par pgx |
| Redis | **oui**, sans code — le schéma `rediss://` pose le `TLSConfig` dans `ParseURL` |
| Kafka | **non** — `dialOpts` ne pose que `DialTimeout`, il manque `kgo.DialTLS` |
| ClickHouse | **non** — les `Options` sont construites à la main, sans champ `TLS` |

**Les deux lignes vertes sont une ligne de checklist ; les deux rouges sont du code.** C'est cet écart
qui fait la step : sans elle, les corps de message chiffrés au repos (step-162) transitent en clair
vers ClickHouse et Kafka, sur le réseau du cluster — le chiffrement au repos protège d'un vol de base,
de rien d'autre.

La fiche `debts/tls-client-vers-kafka-et-clickhouse-absent.md` disait que le risque n'était pas
l'oubli du besoin mais l'absence de ce fichier. Le voici.

## Périmètre

- Kafka : `kgo.DialTLS` (ou `DialTLSConfig`) dans les options du client, producteurs **et**
  consommateurs, sur les huit services qui en ouvrent un.
- ClickHouse : le champ `TLS` des `Options`, sur les services qui lisent ou écrivent le CDR.
- PostgreSQL et Redis : **aucun code**. Une ligne de la checklist de go-live (step-410) qui vérifie
  que `POSTGRES_URL` porte `sslmode=require` et que `REDIS_URL` est en `rediss://` en production.
- La documentation de `deploy/` : quelle CA ces liens vérifient.

## Le point à trancher avant le code

**Quelle ancre de confiance**, et c'est la même question qu'a tranchée step-300d pour le SMSC. Les
quatre magasins ne sont pas des pods de ce dépôt : ni Postgres, ni Kafka, ni ClickHouse, ni Redis ne
sont déployés par `deploy/`, qui pose cette frontière explicitement. Leur certificat vient donc de
l'exploitant, pas de notre CA — `tlsconf.ClientConfig()` n'est probablement **pas** le bon
constructeur ici, contrairement au bind sortant.

Conséquence probable : une ancre configurable par magasin, ce que step-300d a écarté pour le SMSC
faute de pair réel. Voir `debts/ancre-de-confiance-par-connecteur.md` : les deux décisions gagneraient
à être prises ensemble, et la seconde pourrait payer la première.

## Pièges hérités de step-300

- **Le mode de panne d'une CA absente est muet** : un chemin illisible doit être une **erreur de
  boot rendue en valeur**, jamais un handshake qui commence à échouer à trois heures du matin.
- **Côté client, la CA ne tourne pas** — `RootCAs` est figé dans la `*tls.Config` remise une fois au
  transport, et `crypto/tls` n'offre aucun équivalent client de `GetConfigForClient`. Un changement de
  CA exige un redémarrage. Voir `debts/rotation-de-ca-cote-client-exige-un-redemarrage.md`.
- **Ne pas casser les suites d'intégration** : `testcontainers` démarre ces quatre magasins en clair.
  Le TLS doit rester activable par configuration, éteint par défaut, comme partout ailleurs.

## Definition of Done

- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] Kafka et ClickHouse joignables en TLS par configuration, éteint par défaut
- [ ] Un chemin de certificat illisible est une erreur de boot **rendue**, prouvée par un test
- [ ] Les deux lignes Postgres/Redis inscrites dans la checklist de go-live de step-410
- [ ] `debts/tls-client-vers-kafka-et-clickhouse-absent.md` passe à `PAYÉE`, avec la date et la PR

## Hors périmètre

Le TLS de nos écoutes et de nos appels : livré par step-300. L'ingress. L'ancre de confiance du bind
SMSC, qui a sa propre fiche de dette.
