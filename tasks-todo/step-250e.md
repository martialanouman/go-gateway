# step-250e — La table de routage exact n'a aucun écrivain : la portabilité des numéros ne marche pas

> **Jalon :** M12 · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-280 (fidélité de la mesure), step-410

## Ce qui a été constaté

Trouvé en step-250d, en cherchant comment semer une clé pour un test de chaos : **rien n'écrit
`exactroute:{msisdn}` en Redis.**

- `config-sync` ne connaît pas les routes exactes — il ne fait que coalescer des invalidations.
- L'Admin API les persiste en **Postgres seulement** (`internal/adminapi/exact_routes.go` ne touche
  jamais Redis), alors que ses endpoints `create/update/delete-exact-route` et `import-exact-routes`
  sont livrés et servis.
- `exact.EncodeTarget` n'a **aucun appelant hors tests**.

Conséquence en production : le Bloom, lui, est bien construit depuis Postgres (`LoadBloom`) et répond
« peut-être » sur un numéro connu. La lecture Redis qui suit répond alors toujours « clé absente », et
`Resolve` renvoie `(_, false, nil)` — que l'appelant lit comme « pas d'override » et qui le fait
retomber sur le routage déclaratif par préfixe.

**Le court-circuit L0 ne résout donc jamais.** Or c'est exactement ce que le préfixe ne sait pas faire :
la spec (§6.1, §724) pose le numéro exact comme *la* réponse à la **portabilité des numéros**, parce
qu'un préfixe suppose à tort que le préfixe identifie l'opérateur. Un numéro porté part aujourd'hui chez
son ancien opérateur.

## Pourquoi personne ne l'a vu

Le repli est silencieux et parfaitement légitime en apparence : « clé absente » est un cas nominal
(faux positif du Bloom, ou synchronisation pas encore faite), pas une erreur. Rien ne distingue « pas
d'override » de « override que personne n'a jamais écrit ». Les tests du paquet sèment la clé eux-mêmes
et passent ; l'intégration (`tasks-done/step-115.md`) fait de même.

Trois commentaires du code affirmaient que `config-sync` écrivait cette clé (« the Redis value
config-sync writes (step-105) »). Ils ont été corrigés en step-250d, la PR qui découvre une affirmation
fausse la corrigeant — mais **corriger le commentaire ne répare pas la fonctionnalité**.

## Périmètre

- Décider **qui** écrit, et le construire : `config-sync` (qui a déjà la boucle d'invalidation et le
  Redis), ou l'Admin API en écriture directe, ou une projection dédiée. Le choix n'est pas neutre :
  l'`import-exact-routes` est un import de masse asynchrone (une base MNP fait des millions de lignes),
  donc la voie doit supporter un remplissage en masse **et** une mise à jour unitaire.
- La **cohérence Bloom ↔ table** : le Bloom ne doit jamais promettre un numéro que la table n'a pas
  encore, sinon chaque possible-hit paie un aller-retour Redis pour rien. L'ordre de publication compte.
- La **suppression** : `delete-exact-route` doit retirer la clé, sans quoi un numéro re-porté garde son
  ancienne cible jusqu'à… jamais (la clé est écrite sans TTL).
- Un test de bout en bout : créer une route exacte par l'Admin API, puis prouver qu'un message pour ce
  numéro emprunte **le court-circuit L0** et non le déclaratif.

## Definition of Done

- [ ] `make check` vert
- [ ] une route exacte créée par l'Admin API est résolue par L0 en bout de chaîne, prouvée par un test
- [ ] la suppression retire la clé ; la mise à jour la remplace
- [ ] l'import de masse remplit la table (et le Bloom la reflète) sans dépasser le budget mémoire annoncé
      (~1,2 Mo de filtre par million d'entrées, spec §724)
- [ ] la note `NOTE (step-250d)` de `internal/routing/exact/resolver.go` est retirée — c'est elle qui
      dit que cette step reste à faire

## Hors périmètre

La politique de panne Redis du L0 est déjà prouvée (step-250d,
`internal/routing/exact/chaos_integration_test.go`) et n'a pas à être refaite : elle porte sur la
lecture, qui s'exécute bel et bien à chaque faux positif du Bloom.
