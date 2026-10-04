# Le JWKS de l'IdP n'est joignable que sous une autorité des racines système

> **Statut :** PAYÉE le 2026-10-04 · **Nature :** technique
> **Née de :** step-310 · **Payée par :** step-405 (#PR)

**Payée.** `OIDC_JWKS_CA_FILE` désigne l'autorité du JWKS, par `tlsconf.StoreClientConfig` comme
`KAFKA_TLS_CA_FILE` : vide, les racines système ; renseignée, le seul pool ; illisible, le boot échoue. Le
manifest la lit dans le ConfigMap `gateway-oidc`. Sous la PKI interne, step-410 y écrit
`/etc/gateway/tls/ca.crt`, déjà monté. Ce qui suit est l'aveu d'origine.

**Ce qu'on a fait à la place.** Le client HTTP qui charge le JWKS (`internal/auth/oidc.go:68`) utilise le
transport par défaut, donc les racines système de l'image. Aucune variable ne désigne une autorité de
l'exploitant, contrairement à Kafka et ClickHouse (`*_TLS_CA_FILE`, step-305).

**Pourquoi.** Aucun IdP n'est encore choisi : on ne sait pas s'il sera managé (certificat public) ou
interne. Ajouter une ancre avant de le savoir, c'était une variable que personne ne renseignerait.

**Ce qu'il en coûte si on ne la paie jamais.** Un IdP interne, signé par la PKI de l'exploitant, échoue au
handshake. Chaque jeton reçoit 503, et le log `operator token not judged` porte l'erreur x509 : la panne
se diagnostique, mais ne se contourne qu'en reconstruisant l'image.

**À quoi on reconnaîtra qu'il faut la payer.** Dès que l'IdP retenu pour la production présente un
certificat hors des racines publiques. C'est une ligne à trancher dans la checklist de step-410.

Source : `internal/auth/oidc.go` (`NewOIDCVerifier`, `keySetClient`)
