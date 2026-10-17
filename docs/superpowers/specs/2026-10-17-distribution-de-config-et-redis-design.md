# Distribution de la configuration et découpage de Redis

2026-10-17 · Décision 7 du monorepo (`docs/audit-architecture-2026-10-14.md` §3 T2, T3, §6).
Statut : design validé.

## 1. Problème

- **T2 — distribution au cas par cas.** Cinq mécanismes coexistent : environnement, snapshot au
  démarrage, watcher, read-through, resync. S'y ajoutent le pub/sub unique `breaker:events`, où chaque
  transition reconstruit tout (Bloom d'opt-out compris), le compteur `cfggen` et `config-sync`, qui
  coalesce deux fois et que l'on contourne.
- **T3 — un Redis unique porte tout** : sessions, débits, anti-spam, réservations, cache de solde,
  annulation, pub/sub, `dlrmap` (12,5 M clés). Un cache évictable (`exactroute`) y cohabite avec de
  l'état qui ne doit pas l'être (ADR-0015).

## 2. Décisions

| # | Décision |
|---|---|
| D1 | **Un seul mécanisme de configuration : Postgres `LISTEN/NOTIFY` plus une resync bornée.** La notification part du trigger de la décision 5, ne porte qu'une clé, et n'est délivrée qu'au commit. |
| D2 | **Chaque service tient un snapshot immuable** des tables qu'il lit, et relit la ligne notifiée. Une notification ne porte jamais de valeur. |
| D3 | **Les lecteurs d'une table sont ceux que déclare le fichier des GRANT** de la décision 6 : une source pour « qui lit » et « qui recharge ». |
| D4 | **Deux instances Redis, séparées par leur politique d'éviction** : `redis-state` et `redis-cache`. |
| D5 | **Aucun état durable dans Redis** : chaque clé a un TTL de 24 h au plus. |

**Écarté** : un topic Kafka compacté alimenté par une outbox transactionnelle. Même sémantique, plus
une outbox, son relais et un topic ; ses seuls gains, le rejeu et un nombre d'écouteurs sans borne, ne
servent pas la configuration.

## 3. Configuration

### 3.1 Notification

- Le trigger de la décision 5 sur chaque table auditée appelle aussi `pg_notify('config',
  '<table>:<pk>')`. Postgres ne délivre qu'au commit : aucune annonce d'un changement qui n'a pas eu
  lieu, aucune outbox.
- **Import de masse** : un trigger *par instruction* émet une seule notification `<table>:*`.
- **Opt-outs** : canal `optout`, qui porte la clé de la ligne ; le routeur l'ajoute à son Bloom au fil
  de l'eau, sans reconstruction. Les opt-outs sortent du rechargement de configuration.

### 3.2 Snapshot

- Au démarrage, le service charge les tables qu'il déclare lire, puis ouvre une connexion **directe et
  dédiée** à l'écoute (`LISTEN` ne traverse pas un PgBouncer en mode transaction).
- Sur `<table>:<pk>`, il relit la ligne et échange un nouveau snapshot de façon atomique ; sur
  `<table>:*`, il recharge la table.
- **Resync** complète à chaque reconnexion de l'écoute, puis toutes les 5 min (step-399) : une
  notification perdue pendant une déconnexion est rattrapée au plus à la reconnexion.
- **Fraîcheur** : propagation p99 < 5 s, mesurée et alertée.

### 3.3 Ce qui reste hors du snapshot

- `exact_routes` : cache read-through (ADR-0015), sur `redis-cache`. Le Bloom se recharge sur
  `exact_routes:*`.
- Les secrets scellés : ouverts à l'usage par `ConfigSecrets` (ADR-0016), jamais gardés dans un
  snapshot partagé.

### 3.4 Gardes

1. Chaque table que lit un service figure dans sa déclaration (décision 6) ; une lecture non déclarée
   échoue faute de GRANT.
2. Chaque table auditée notifie ; un test écrit une ligne et attend la notification.
3. Aucun service ne lit de configuration métier dans l'environnement (prolonge la garde AST de la
   décision 3 à tous les services).

## 4. Redis

| Instance | Politique | Persistance | Contenu |
|---|---|---|---|
| **`redis-state`** | `noeviction` | AOF, toutes les secondes | Sessions SMPP (`max_sessions`, `sess:live`), seaux d'admission, compteurs anti-spam, jeton d'annulation (ADR-0013), clés d'idempotence REST (24 h), tampon de réassemblage des MO concaténés, anti-brute-force du bind, diagnostic des binds refusés |
| **`redis-cache`** | `allkeys-lru` | aucune | `exactroute:{msisdn}` (ADR-0015) |

- **Chaque clé a un TTL** de 24 h au plus ; une garde le vérifie sur les écritures du code.
- **Pas une instance par usage** : l'ACL par service (décision 6) isole déjà les espaces de clés. Une
  instance de plus ne se justifie que par une politique d'éviction ou de persistance différente.
- La perte de `redis-cache` dégrade la latence du L0 ; elle ne casse rien (ADR-0015).

## 5. Pannes

| Panne | Effet |
|---|---|
| Connexion d'écoute perdue | Reconnexion avec backoff, puis resync complète ; le snapshot courant sert en attendant |
| Postgres injoignable | Le snapshot courant sert ; la resync reprend au retour. Les lanes qui dépendent de Postgres suivent leur mode (décision 4) |
| `redis-state` injoignable | Chaque usage garde sa politique de panne actuelle (admission : plafond local fail-closed ; annulation : fail-open) |
| `redis-cache` injoignable | Lecture directe de `exact_routes` par clé primaire, fail-closed en rejeu (ADR-0015) |

## 6. Supprimé, neuf

| | |
|---|---|
| **Supprimé** | `cmd/config-sync` ; le compteur `cfggen` ; le pub/sub `breaker:events` ; le topic `config.changes` (décision 4) ; la configuration métier par variables d'environnement ; le Redis unique |
| **Neuf** | `pg_notify` dans le trigger de la décision 5 ; le snapshot avec écoute et resync, une bibliothèque partagée ; `redis-cache` ; la garde des TTL |

## 7. Effets sur la spec et les contrats actuels

| Où | Effet |
|---|---|
| Spec §3.2, §6.1 (rechargement à chaud), guide §4 | `config-sync` et le pub/sub remplacés par `LISTEN/NOTIFY` + resync |
| Décision 4 §5.1 | `config.changes` retiré de la table des topics |
| ADR-0015 | Amendé : le Bloom se recharge sur `exact_routes:*`, plus par `config-sync` ; la clé vit sur `redis-cache` |
| Manifests | `config-sync` supprimé ; deux StatefulSets Redis ; connexion d'écoute directe à Postgres |
