# step-287n — Lire un message dans un gros compte sans parcourir le compte : index bloom sur `message_id`

> **Jalon :** M12 · **Statut :** EN COURS
> **Dépend de :** step-287m (le reaper lit par compte)
> Demande humaine du 11/10/2026, née du run 13 de step-287.

## Pourquoi
Au run 13, le reaper lit bien par `Current` (step-287m), mais chaque lecture parcourt encore ~1,5 M lignes :
844 lectures, 1,23 milliard de lignes, 54,7 Go, 172 s de CPU sur 483 pour tout ClickHouse (36 %). La clé de
tri est `(customer_id, account_id, submitted_at, message_id)` ; `Current` ne filtre pas `submitted_at`, donc le
préfixe ne retient que le compte, et un compte, c'est toute son histoire. Le banc n'a qu'un compte, un gros
client en production pareil : à quelques milliers de SMS/s, des centaines de millions de lignes par jour, lues
à chaque vérification du reaper et à chaque `GET /messages/{id}`. Le coût croît avec le compte ; le gain sur le
débit du banc, lui, est attendu sous la variance (~0,3 cœur de contabo75).

## Design arrêté
- Migration ClickHouse `0008` : `ALTER TABLE cdr ADD INDEX message_id_bloom message_id TYPE bloom_filter(0.01)
  GRANULARITY 1`. Un filtre par granule de 8 192 lignes, ~1,2 octet par ligne ; `Current` et `ByMessageID`
  (`message_id = ?` sur la table) sautent toute granule qui ne contient pas le message. Aucun code Go ne change.
- Pas de `MATERIALIZE INDEX` dans la migration, comme `0003` pour le TTL : construire l'index des parts
  existantes relit toute la CDR retenue en arrière-plan pendant que le déploiement se dit réussi. Les parts
  neuves et fusionnées le portent d'elles-mêmes ; les parts anciennes s'indexent par l'opérateur, partition
  par partition, hors pointe : `ALTER TABLE cdr MATERIALIZE INDEX message_id_bloom IN PARTITION 'AAAA-MM-JJ'`.
- `cdr_events` n'en a pas besoin : sa clé de tri commence par `message_id`.
- `db/schema_passerelle_sms.sql` (annexe A) porte l'index.
- Test d'intégration : un compte de 4 granules écrit en un lot ; lire un message par `Current` lit au plus
  2 granules (`read_rows` du `query_log`), contre les 4 sans l'index.

## Definition of Done
- [x] migration `0008` up/down, annexe A du schéma à jour
- [x] `Current` dans un compte de 4 granules n'en lit pas plus de 2 (intégration ; rouge sans la migration : 32 768
      lignes lues ; mutation `GRANULARITY 4` tuée)
- [ ] mesuré à un run : `read_rows` par lecture du reaper, CPU des SELECT de ClickHouse
