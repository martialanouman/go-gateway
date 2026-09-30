# L'instantané de débit n'est chargé qu'au démarrage

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-283 (revue, arbitrage Fable D) · **Portée par :** —

**Ce qu'on a fait à la place.** Les limites `rate_limits` et les `throughput_limit_per_sec` sont lues une
seule fois, au démarrage (`internal/pipeline/ratelimit/enforcer.go:57`). C'était déjà le cas du routeur ;
step-283 étend ce chargement à `rest-api-svc`, `smpp-server-svc` et `connector-pool-svc`
(`cmd/connector-pool-svc/wiring.go:142`).

**Pourquoi.** step-283 déplaçait le contrôle du débit, sans changer la façon dont on le configure. Le
rechargement à chaud reste le « later milestone » que la doc de `Snapshot` annonçait déjà.

**Ce qu'il en coûte.** Tant que les pods ne redémarrent pas, un compte neuf ou une limite modifiée
n'est pas appliqué. Il faut désormais redémarrer trois services au lieu d'un.

**À quoi on reconnaîtra qu'il faut la payer.** La première limite modifiée en exploitation, ou un compte
créé sans limite appliquée. La voie est connue : accrocher le rechargement au `config.Watcher` du pool
(canal `ChannelSnapshotInvalidation`), et faire de même côté REST et SMPP.
