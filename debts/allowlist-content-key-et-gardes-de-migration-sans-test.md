# L'allowlist de `content-key-svc` et les gardes des migrations 0017/0018 ne sont lues par aucun test

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-295b (laissée dans la fiche `content-key-svc…`, séparée par step-285) · **Portée par :** —

**Ce qu'on a fait à la place.** `TLS_ALLOWED_CLIENTS` dans `deploy/k8s/content-key-svc.yaml` n'est lu par
aucun test : `allowlist_test.go` compose sa propre liste, et `internal/deploy` ignore cette clé. La CI
exerce l'`up`/`down`/`up` de la migration 0018 sur une base vide. Les deux `RAISE EXCEPTION` qui refusent
une table peuplée ne sont donc jamais déclenchés, et la migration 0017 est dans le même cas.

**Pourquoi.** Ces trous sont apparus pendant la revue de step-295b. Les combler sortait de son sujet.

**Ce qu'il en coûte.** Une entrée oubliée dans le manifeste reste invisible jusqu'au premier webhook, qui
rejoue alors en boucle sur `PermissionDenied`. Une garde de migration cassée ne se révèle qu'au `down`
d'une base de production.

**À quoi on reconnaîtra qu'il faut la payer.** Au premier service ajouté à l'allowlist, ou au premier
`down` de 0017/0018 sur une base peuplée.

Sources : `deploy/k8s/content-key-svc.yaml` (`TLS_ALLOWED_CLIENTS`) · `migrations/0017_*.down.sql` ·
`migrations/0018_*.down.sql`
