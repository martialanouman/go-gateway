# ADR-0001 : Kafka comme couche d'ingestion durable

**Status:** Accepted
**Date:** 2026-07-14
**Deciders:** Équipe plateforme
**Réf spec:** §3.3, §4.2, §6.7, §6.12, §7

## Context

La passerelle doit absorber 8 000–15 000 SMS/s en découplant la **réception** de l'**envoi** : un soumetteur doit être acquitté en quelques dizaines de millisecondes (ingestion p99 < 250 ms) sans attendre que le SMSC accepte le message. Il faut une garantie de durabilité (aucune perte après accusé), du rejeu, des files de dead-letter et un parallélisme par partition, le tout à l'échelle d'un agrégateur national.

## Decision

Utiliser **Apache Kafka** comme socle de durabilité du plan de données. L'écriture dans le topic `mt.inbound` est la **frontière d'accusé de réception** : le client n'est acquitté qu'après validation durable. Le pipeline s'articule en topics (`mt.inbound` → `mt.routed` → `mo.inbound`/`dlr.events`, plus dead-letter et `mt.reroute-park`).

## Options Considered

### Option A : Kafka
| Dimension | Évaluation |
|---|---|
| Complexité | Élevée (opérationnelle) |
| Coût | Moyen/élevé |
| Scalabilité | Excellente (partitions) |
| Familiarité équipe | Bonne |

**Pros :** durabilité et rejeu éprouvés à cette échelle ; sémantique consumer-group ; excellent antécédent d'intégration ClickHouse ; partitionnement = parallélisme naturel.
**Cons :** surface opérationnelle (ZooKeeper/KRaft, tuning, réplication) ; latence non nulle par hop.

### Option B : NATS JetStream
| Dimension | Évaluation |
|---|---|
| Complexité | Moyenne |
| Coût | Faible/moyen |
| Scalabilité | Bonne |
| Familiarité équipe | Moyenne |

**Pros :** plus léger à exploiter ; latence faible.
**Cons :** écosystème analytique (ClickHouse) moins mûr ; antécédents plus rares à cette échelle ; sémantique de rétention/rejeu moins riche.

### Option C : écriture directe en base (pas de broker)
**Pros :** architecture plus simple.
**Cons :** couple les pics de charge au traitement ; pas de rejeu/dead-letter natif ; la base devient le goulot d'étranglement au débit cible. **Écarté d'emblée.**

## Trade-off Analysis

Le vrai arbitrage est A vs B. Kafka est retenu pour la **maturité de l'intégration ClickHouse** (le CDR est un consommateur central), la richesse de la sémantique consumer-group/rétention, et l'antécédent avéré à 15 k msg/s. On accepte en contrepartie une surface opérationnelle plus lourde — jugée maîtrisable et justifiée par la criticité de la durabilité.

## Consequences

- **Plus facile :** absorber les pics, rejouer, isoler les échecs (dead-letter), scaler par partition.
- **Plus difficile :** l'exploitation (dimensionnement partitions, réplication 3, monitoring du lag).
- **À revisiter :** partitionnement plus fin au-delà d'un certain nombre de binds (§7).

## Action Items

1. [ ] Provisionner Kafka (réplication 3) en dev via docker-compose, en prod via l'infra managée.
2. [ ] Fixer les conventions de topics/partitions (`internal/storage/kafka`) : clé = hash compte pour `mt.inbound`, `(connector_id, shard_index)` pour `mt.routed`.
3. [ ] Producteur `acks=all` + idempotent ; commit d'offset après traitement (at-least-once).
