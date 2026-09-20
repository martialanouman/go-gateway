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

## Addendum step-295 (2026-09-20) — le mot de passe est le treizième champ

step-295 a retiré **une** des deux raisons pour lesquelles le mot de passe vivait dans l'environnement :
`smsc_connectors.password_sealed` est désormais **réversible** (scellé par `content-key-svc`, ADR-0016),
là où `password_hash` ne pouvait structurellement pas servir un bind sortant. La seconde raison — celle
que cette fiche porte — reste entière : **le pool ne lit aucune colonne de bind et n'a aucun client
Postgres.** Ne câbler que le mot de passe produirait un bind hybride, à deux sources de vérité pour une
même session. Il rejoint donc les douze autres.

Deux choses que le jour du câblage devra savoir, et qui n'étaient écrites nulle part :

- **Le symptôme de cette fiche vaut aussi pour le mot de passe, et il vaut toujours.** Un exploitant qui
  fait tourner le mot de passe par l'Admin API reçoit 200, la ligne change, et le bind n'en sait rien.
  C'est « une surface qui répond 200 à un réglage sans effet », cette fois sur un secret. step-295 a
  rendu la colonne utilisable ; elle n'a pas rendu la rotation effective.
- **`connector-pool-svc` devra pouvoir ouvrir son mot de passe sans pouvoir lire les clés de contenu.**
  Le filtrage par méthode que cette fiche annonçait comme restant à faire **existe** : step-295 l'a livré
  (`cmd/content-key-svc/authz.go`), parce qu'enregistrer `ConfigSecrets` sur le listener partagé l'avait
  rendu nécessaire tout de suite — `router-svc` y avait gagné de quoi ouvrir tous les mots de passe de
  bind. `config.TLS.AllowedClients` reste par binaire ; c'est un intercepteur qui distingue les méthodes.

  Le jour venu, le pool rejoint donc `configSecretsCallers` et **rien d'autre**. Deux tests l'encadrent et
  doivent évoluer sciemment, pas disparaître : `TestTheWiredServerAdmitsOnlyTheCallersItNames` le nomme
  comme appelant refusé du port, et `TestConfigSecretsIsRefusedToCallersThatOnlyNeedContentKeys` fixe la
  distinction. Noter que `configSecretsCallers` autorise un **service**, pas un couple service/méthode :
  y inscrire le pool lui donnera `Seal` autant qu'`Open`. Si cette distinction-là compte à ce moment, la
  structure devra porter les méthodes — elle ne le fait pas aujourd'hui, faute d'un second appelant.

Sources : `cmd/connector-pool-svc/wiring.go` (projection `ClientConfig`) ·
`cmd/connector-pool-svc/main.go:36` (`connectorEnv`) · `migrations/0001_init.up.sql:277-278`
