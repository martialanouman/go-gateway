# step-287b — ClickHouse sobre : CDR des DLR par lot de poll, journaux système bornés

> **Jalon :** M12 · **Statut :** EN COURS
> **Dépend de :** step-201c · **Bloque :** step-287 (reprise de la campagne)
> Née de la campagne step-287 du 06/10/2026, demande humaine du même jour ; unité faute de multiple de dix
> libre.

## Pourquoi
Pendant le run interrompu de step-287 (VPS de test, image `84b18e7`), l'hôte est resté à 96 % de CPU sans
aucune ingestion. ClickHouse y prenait 1,9 cœur en écoulement, puis 4,6 cœurs à vide. Deux causes ont été
mesurées le 06/10/2026 :

- **Les CDR des DLR s'écrivent un accusé à la fois.** `modlrrouter` consomme `dlr.events` enregistrement
  par enregistrement (`Consumer.Run`) et appelle `CDR.Insert` pour chacun : un insert d'**une ligne** dans
  `cdr` puis dans `cdr_events`, en série. Sur 2 minutes, `cdr` a reçu 4 004 parts d'une ligne, soit environ
  39 inserts/s, à environ 11 ms + 8 ms par accusé. Chaque insert crée une part, et les fusions ne
  s'arrêtent jamais (5 279 en 5 minutes).
- **ClickHouse se journalise en `Trace`.** C'est le niveau par défaut de l'image Docker : environ 59 000
  lignes de `text_log` par minute, et 33 Go de tables système sur le VPS (`text_log` 14,4 Go,
  `processors_profile_log` 7,8 Go, `query_log` 4,7 Go, `part_log` 4,2 Go, `trace_log` 2,1 Go), sur un PVC
  de 20 Gi.

Séparer les magasins sur un autre serveur ne ferait que déplacer ce gaspillage.

## Design arrêté (06/10/2026)
- **CDR des DLR par lot de poll**, sur le patron du projecteur `outcome` (step-201c). C'est la règle `D8`
  de step-201 : `poll Kafka = PrepareBatch/Send = commit`, jamais de buffer client. `modlrrouter` passe à
  `RunBatch` :
  - chaque accusé est décodé et corrélé ;
  - les lignes du lot partent en **un** `InsertBatch` (un insert `cdr`, un insert `cdr_events`) ;
  - un échec d'écriture fait échouer tous les enregistrements qui ont fourni une ligne. Ils ne sont pas
    commités et seront relus. Les lignes CDR sont idempotentes (ReplacingMergeTree, même clé, même rang).
  - une erreur Redis de corrélation ne fait échouer que son accusé. Le consommateur ne commite jamais
    au-delà du premier échec d'une partition et rejoue la suite : les accusés suivants réécrivent leur
    ligne à l'identique. Seule `cdr_events`, table append-only, garde un doublon de jalon par rejeu.
  - l'insert a son propre span `dlr.cdr_write`, qui marque une panne ClickHouse.

  Ce qui ne change pas : un enregistrement illisible, un état non terminal ou une correspondance absente
  sont écartés et commités, comme aujourd'hui.
- **Pas d'`async_insert`.** `D8` le réservait au cas « too many parts », mais avec
  `wait_for_async_insert=1`, chaque insert attend le flush serveur (50 à 200 ms). Sur les écrivains encore
  appelés message par message (ligne `rejected` du routeur, lignes `rerouted`/`failed` du pool), cela
  effondrerait le débit au lieu de l'aider. Le lot de poll règle la cause.
- **Journaux système de ClickHouse bornés**, dans un fichier `config.d` versionné, monté par le VPS
  (`deploy/test/deps/clickhouse.yaml`) et par le compose local :
  - `logger` en `warning` ;
  - `text_log` au niveau `warning` ;
  - `processors_profile_log` retiré ;
  - TTL de 3 jours sur `query_log`, `part_log`, `trace_log`, `text_log`, `metric_log` et
    `asynchronous_metric_log`.

  La production n'est pas déployée par ce dépôt (`deploy/k8s` n'a pas de ClickHouse). Le guide §13.2 dit ce
  que l'exploitant doit poser.
- **Purge des 33 Go existants**, lancée par l'utilisateur **après** le déploiement (le hook interdit ces
  commandes à Claude). Comme le TTL change leur définition, ClickHouse renomme les anciennes tables en
  `<table>_0` et en crée des neuves : vider `system.text_log` viderait la neuve et laisserait les 14 Go. Il
  faut donc supprimer les `system.*_0`, ainsi que `system.processors_profile_log` : `remove="1"` cesse d'y
  écrire, mais ne supprime pas la table existante.
- **Un changement futur du XML** ne prend effet qu'au redémarrage de `clickhouse-0`. La ConfigMap est
  montée en `subPath`, sans suffixe de hachage, donc rien ne redémarre le pod de lui-même.

## Definition of Done
- [x] `modlrrouter` écrit un lot de poll en un `InsertBatch` ; un échec d'écriture fait échouer et
      rejouer tout le lot (tests, mutation)
- [x] fichier de configuration ClickHouse versionné, monté sur le VPS et en local
- [ ] sur le VPS, après déploiement : CPU de ClickHouse à vide et taille moyenne des inserts `cdr`
      sous un flux de DLR, mesurées avant et après
