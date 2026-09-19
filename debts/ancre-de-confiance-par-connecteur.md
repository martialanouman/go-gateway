# Le TLS sortant ne connaît qu'une CA — la nôtre — et `tls_config_json` n'a toujours aucun lecteur

> **Statut :** OUVERTE · **Nature :** technique et produit
> **Née de :** step-300d (`tasks-done/step-300.md`, « Sortant : notre CA — arbitrage Fable du 2026-09-19 ») · **Portée par :** —

`CONNECTOR_TLS_ENABLED=true` fait composer le SMSC par `tlsconf.ClientConfig()` : plancher **TLS 1.3**,
`RootCAs` = **notre** CA interne, et notre certificat de pod présenté. C'est du mTLS, et c'est exactement
ce que la spec appelle « mTLS optionnel pour les liens SMSC ».

**Ce qu'on a fait à la place** d'une ancre de confiance par connecteur : rien. La colonne
`smsc_connectors.tls_config_json` existe depuis la migration initiale (`migrations/0001_init.up.sql:278`),
l'Admin API la lit et l'écrit (`internal/adminapi/connectors.go:67`), un test d'intégration prouve
même qu'un `{"verify": true, "min_version": "1.2"}` fait l'aller-retour en base
(`internal/storage/postgres/connectors_integration_test.go:31`) — et **personne ne la lit au moment de
composer**. Le drapeau `tls_enabled` de la même table ne se lit pas davantage : le pool prend sa
configuration de bind dans l'environnement.

**La raison écrite à l'époque.** Arbitrage du 2026-09-19 : l'autre option — racines système, plancher
1.2, un `CONNECTOR_TLS_CA_FILE` — « joindrait un opérateur réel, qui n'existe pas, au prix d'une
variable pour une valeur qui ne varie pas, et sans qu'aucun test puisse exister ». Le dépôt n'a jamais
été déployé et la seule forme de production que le code nomme est un sidecar local
(`cmd/connector-pool-svc/main.go:81`), qui est dans notre PKI.

**Ce qu'il en coûte.** Un SMSC d'opérateur signé par une CA publique ou par la PKI de l'opérateur, ou
qui n'accepte pas TLS 1.3, **n'est pas joignable en TLS depuis le pod**. Le contournement est un
sidecar qui termine le TLS — une brique de plus à exploiter, que `deploy/` ne déploie pas. Et
l'écart est silencieux dans l'autre sens : un exploitant qui pose `tls_enabled=true` et un
`tls_config_json` complet **par l'Admin API** voit sa configuration acceptée, stockée, relue… et
jamais appliquée. C'est la pire forme : une surface qui répond 200 à un réglage sans effet.

**À quoi on reconnaîtra qu'il faut la payer.** Au premier connecteur qui n'est pas un simulateur. La
question se posera en même temps que celle du mot de passe de bind, qui vit déjà dans l'environnement
pour la même raison — voir `mot-de-passe-de-bind-en-argv.md` et `deux-tables-sans-repo-ni-surface.md`.
Le jour où le pool lira sa configuration de connecteur dans la base, les douze champs migreront
ensemble, `tls_config_json` compris.

Sources : `cmd/connector-pool-svc/wiring.go` (projection `ClientConfig`) ·
`cmd/connector-pool-svc/main.go:36` (`connectorEnv`) · `migrations/0001_init.up.sql:277-278`
