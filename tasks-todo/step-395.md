# step-395 — Le watcher de config ne rejoue jamais un rebuild échoué

> **Jalon :** Dette ouverte par step-260c · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** —

## Pourquoi cette fiche existe

step-260c a prouvé la politique du dégradé masqué et l'a écrite en §16 : un rebuild qui n'atteint pas
Postgres est journalisé puis jeté, et le dernier snapshot bâti continue de servir. C'est la bonne
politique — un routeur avec des routes périmées vaut mieux qu'un routeur sans routes. Ce qui ne l'est
pas, c'est qu'**elle n'a pas de sortie** :

```go
// internal/config/watcher.go:128-133
case <-timerC:
    timer, timerC = nil, nil
    if rerr := w.rebuild(ctx); rerr != nil {
        w.logger.Error("config watcher: rebuild failed; keeping current state", "err", rerr)
    }
```

Aucun timer n'est réarmé. La réparation attend **la prochaine notification d'invalidation**, c'est-à-dire
une action d'exploitant ou d'admin. Si la panne dure plus longtemps que le silence du plan de contrôle
— le cas normal la nuit — le pod sert une config périmée **sans borne**, et rien ne le dit : `/readyz`
ne regarde que Kafka, aucune métrique de rebuild n'existe, et la seule trace est la ligne `Error`
ci-dessus, écrite une fois.

Le cas est aggravé, pas créé, par le fait que le rebuild n'est pas atomique entre composants
(`cmd/router-svc/wiring.go:709` swappe les routes avant quatre `return err` possibles) : un échec en
cours laisse une config **partiellement** appliquée, que seule une invalidation ultérieure réconcilie.

Ce que ça coûte concrètement : un opt-out retiré qui continue de s'appliquer, un disjoncteur non
rafraîchi, un reroute qui ne prend jamais — autant de conformité et de routage qui divergent en
silence de ce que la base dit.

## Périmètre

Réarmer la fenêtre sur `rerr`, avec un backoff borné, et le prouver.

- Le correctif tient en quelques lignes dans `Watcher.Run` : sur erreur, réarmer `timer` avec un délai
  croissant (plafonné) au lieu de retomber dans l'attente pure. Une notification arrivant entre-temps
  doit continuer de coalescer normalement — le rejeu ne doit pas se transformer en deux rebuilds
  concurrents.
- Le backoff est un **paramètre de `Run` ou une option**, pas une constante muette : le test doit
  pouvoir le raccourcir sans mesurer autre chose que la production (cf. le piège de
  `loadref-harness-fidelity-traps`).
- **Une métrique** de rebuild (succès / échec, et l'horodatage du dernier succès) : le rejeu rend la
  panne récupérable, la métrique la rend visible. Les jauges `bloom_last_reload_timestamp_seconds`
  existent déjà pour les deux Bloom et donnent le patron ; elles ne couvrent ni les routes, ni les
  scripts, ni le crédit, ni la politique de contenu.

## Chaîne de preuves

1. Rouge dans `internal/config` : un rebuild qui échoue **une** fois puis réussirait doit être rejoué
   **sans** nouvelle notification. Aujourd'hui le test pend jusqu'à sa deadline — c'est le rouge.
2. La mutation qui garde le backoff : le figer à zéro doit faire tomber une assertion (sinon le
   « bornage » n'est pas gardé), et le rendre infini aussi.
3. Le test de bout en bout de step-260c
   (`TestRouterConfigSnapshotsDegradeSilentlyWhenPostgresIsCut`) doit pouvoir **retirer sa boucle de
   republication** : la reprise doit venir du rejeu autonome. C'est l'assertion qui relie le correctif
   au défaut observé.
4. `make check` vert.

## Hors périmètre

Rendre le rebuild atomique entre composants (double-buffer sur cinq composants) : c'est un autre
chantier, et le rejeu autonome en réduit déjà la fenêtre à un intervalle borné.
