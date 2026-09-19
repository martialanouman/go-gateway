# Les erreurs de handshake TLS partent sur stderr, hors `slog`

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-300c (PR #200) · **Portée par :** —

`srv.ErrorLog` n'est posé nulle part (`grep ErrorLog cmd/ internal/` : zéro). `net/http` écrit donc
ses erreurs de handshake avec le logger de la bibliothèque standard.

**Ce qu'il en coûte.** Écrit : « les erreurs de handshake — "tls: client didn't provide a
certificate", **le diagnostic n° 1 du nouveau port mTLS** — partent sur stderr, hors `slog`, non
structurées ». Elles restent visibles dans les journaux du conteneur, mais hors de la collecte
structurée sur laquelle tout le reste du dépôt s'appuie.

**À quoi on reconnaîtra qu'il faut la payer.** Le premier refus de certificat en production qu'on
cherche dans les mauvais journaux. Une ligne par serveur, plus sa preuve.

Source : `cmd/rest-api-svc/wiring.go`, `cmd/admin-api-svc/wiring.go` (absence)
