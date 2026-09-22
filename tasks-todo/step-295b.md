# step-295b — Le troisième secret rejoué : la clé de signature des webhooks

> **Jalon :** Dette ouverte par step-295, relevée par step-340 · **Statut :** À FAIRE
> **Dépend de :** step-295 · **Bloque :** step-410 (go-live)

## Pourquoi cette fiche existe

`control_plane.webhooks.secret` est un `text NOT NULL` en clair. C'est un secret **rejoué** :
`webhook.Sign` en a besoin en clair à chaque remise de MO ou de DLR, donc le hacher le rendrait
inutilisable. Mais `.claude/rules/go-code.md` et ADR-0016 donnent déjà la forme qui convient à un secret
rejoué, et ce n'est pas le clair : c'est le **scellement** par `ConfigSecrets`.

L'inventaire de step-295 est parti d'une chasse aux mots de passe et aux clés d'API. Le secret de webhook
n'est ni l'un ni l'autre, et personne ne l'écrivait encore par l'API — il n'y est pas entré. step-340 a
livré l'administration des webhooks en écrivant la colonne telle qu'elle est, et a ouvert
`debts/secret-de-webhook-en-clair-en-base.md` plutôt que d'élargir son périmètre.

Ce que ça coûte aujourd'hui : une lecture de la base, une sauvegarde ou une réplique rend les clés de
signature de **tous** les clients. Qui les tient forge des MO et des DLR signés valides vers les endpoints
de ces clients. La surface est plus large que celle du mot de passe de bind sortant, qui ne concerne qu'un
connecteur opérateur : ici il y a une clé par compte client et par type d'événement.

## Ce que cette fiche N'EST PAS

Elle **ne rouvre pas ADR-0016**. La voie est tranchée : scellé par la KMS de `content-key-svc`, colonnes
`*_sealed bytea` + `*_kms_key_ref`, tag de domaine, autorisation par méthode. Cette step applique cette
décision à une troisième entité. Elle ne rouvre pas non plus le cas **entrant** — `credentials.password_hash`
et la clé d'API sont hachés, et c'est correct : la passerelle les compare, elle ne les rejoue jamais.

## Le fait neuf que step-295 n'a pas eu à traiter

**Aucun code de production n'appelle `ConfigSecrets.Open` aujourd'hui.** Les deux secrets de step-295 sont
scellés et jamais descellés : `connector-pool-svc` lit encore `CONNECTOR_PASSWORD` dans l'environnement, et
l'Admin API masque `auth_config_json` par une constante qui n'a jamais lu la vraie valeur. `SecretSealer`
n'a délibérément pas de contrepartie `Open` (`internal/adminapi/deps.go`).

Le secret de webhook est le premier qu'il faut **réellement ouvrir**, et il faut l'ouvrir sur le chemin de
la **remise**, à chaque événement — pas au démarrage d'un pod. C'est là que se situent toutes les décisions
propres à cette step.

## Design arrêté

Arbitrage Fable du 2026-09-22, trois points, verdict sur chacun. La spec tranchait le stockage (ADR-0016)
et rien du reste.

### 1. Le stockage : la migration 0017 à l'identique sur une troisième table

`secret text NOT NULL` → `secret_sealed bytea NOT NULL` + `secret_kms_key_ref text NOT NULL`, dans
`db/schema_passerelle_sms.sql` **et** `migrations/0018_*.{up,down}.sql`. Le domaine porte
`Webhook.Secret cp.SealedSecret`, comme `Connector.Password` et `ExternalProvider.AuthConfig` ; la paire
s'écrit et se relit ensemble, et `sealedPair` existe déjà pour le PATCH partiel.

**Ce n'est pas une conversion**, et la migration le dit comme 0017 : un secret en clair *pourrait*
techniquement se sceller, mais pas par du SQL — sceller exige la KMS, que `golang-migrate` n'a pas. La
migration **refuse donc une table non vide** avec un message qui nomme la cause, au lieu de laisser
`ADD COLUMN ... NOT NULL` échouer sur un nom de contrainte. Le `down` refuse symétriquement : rendre la
colonne `text` remettrait les clés en clair. Le dépôt n'a jamais été déployé ; une base de développement
se recrée.

### 2. Le `Open` est à l'entrée de `Send` et de `Retry`, jamais ailleurs

`internal/webhook.Sender` reçoit une interface `SecretOpener` déclarée côté consommateur, et ouvre **une
fois par remise** : au début de `Send` (avant la bifurcation `s.retry != nil`, pour que la boucle en bande
et `deliverOnce` la partagent) et au début de `Retry`. Le clair descend en paramètre jusqu'à
`buildRequest`. `Park` n'ouvre pas : il ne signe pas.

Pourquoi le sender et pas les appelants : il est la **seule chose qui signe**, donc la seule qui a besoin
du clair. Une dépendance, un point de câblage, et ni `Deliverer.Deliver` ni `WebhookRetryRunner.Handle` ne
changent au-delà du type. Deux variantes écartées :

- **Décorer les deux interfaces `Get`** de `mo-dlr-router-svc` : il faudrait rendre le clair dans un champ
  de structure, donc soit deux champs de secret dans `cp.Webhook`, soit un second type de webhook à côté.
- **Ouvrir chez les appelants et passer le clair en argument** : deux sites d'ouverture et deux branches
  d'erreur pour rien.

Le secret reste hors de Kafka, inchangé : les deux puits (`Defer`, `Park`) continuent de recevoir `wh`, et
la règle écrite dans `internal/webhook/retry.go` — un enregistrement en file est visible de l'opérateur et
survit à la requête, donc il ne porte jamais la clé — reste vraie mot pour mot.

### 3. La classification de l'erreur d'ouverture — ce que la première formulation manquait

Faire simplement remonter l'erreur de `Open` était juste **pour une panne**, et faux pour tout le reste. Le
serveur répond `Internal` sur une altération, une troncature ou une mauvaise clé maîtresse,
`InvalidArgument` sur un mauvais tag de domaine, et l'intercepteur répond `PermissionDenied`. Ces trois cas
sont **déterministes pour cette ligne** : l'événement reviendrait, échouerait identiquement, et
bloquerait la partition — donc les MO et DLR de **tous** les comptes derrière une seule ligne inouvrable,
indéfiniment. C'est exactement le mode de panne que `tryBinds` et la doc de `Handle` refusent déjà pour
une PDU malformée.

Le sender classe donc l'échec comme il classe déjà un résultat HTTP :

- **transitoire** — service injoignable, échéance dépassée, contexte terminé : on rend l'erreur telle
  quelle. Aucun essai consommé, rien de différé, rien de garé. L'enregistrement Kafka est retraité, et
  `NotBefore` étant déjà passé, la redélivrance ne réattend pas. C'est mot pour mot le comportement déjà
  écrit pour une panne de Postgres ;
- **tout le reste** — garé au dead-letter avec la raison `secret_unopenable`, et retour `nil`. Le
  dead-letter est le chemin de récupération prévu : l'opérateur rescelle la ligne par l'Admin API et
  rejoue. Le journal ne porte que des identifiants et le code d'erreur, jamais les octets scellés.

L'adaptateur gRPC traduit : service injoignable / échéance / annulation → `errs.ErrServiceUnavailable`,
tout le reste → `errs.ErrInternal`. `internal/webhook` décide sur le code, sans connaître gRPC.
`errs.Retryable` ne pouvait pas servir ici : elle classe `ErrInternal` comme rejouable, ce qui est vrai
d'une requête cliente et faux d'un chiffré qui ne s'ouvrira jamais.

### 4. Pas de cache du clair

La branche fait déjà un aller-retour Postgres par événement, et le dépôt a délibérément gardé **aucun**
cache de ligne de webhook — le runner de retry pace même *avant* de résoudre, précisément pour qu'une rafale
de différés ne devienne pas une rafale de lectures du plan de contrôle. Le coût ajouté est un aller-retour
gRPC dans le cluster plus un déballage AES-GCM, sur une branche qui ne s'exécute que pour les comptes sans
bind vivant.

Un cache de clairs sans invalidation ferait par ailleurs de la rotation d'un secret par l'Admin API un
mensonge pendant toute la durée du TTL — la même surface qui « répond 200 à un réglage sans effet » que
`debts/ancre-de-confiance-par-connecteur.md` reproche déjà au mot de passe de connecteur — et un appelant
d'ADR-0016 est censé recevoir *un* secret, celui qu'il va de toute façon mettre en clair sur le fil, pas
tenir un porte-clés en mémoire.

**À quoi on reconnaîtra qu'il faut en ajouter un :** à une mesure, pas à une intuition. Et ce qu'il faudra
alors cacher est le résultat de `Get`+`Open` ensemble, clé `(compte, type d'événement)`, TTL court, avec sa
fiche dans `debts/` — pas un cache de clair greffé sur le sender.

### 5. `configSecretsCallers` passe par méthode, maintenant

`mo-dlr-router-svc` a besoin d'`Open` **seul**. La carte actuelle autorise un *service*, pas un couple
service/méthode : l'y ajouter tel quel lui donnerait `Seal`. Or ce pod détient `POSTGRES_URL` avec accès en
écriture au plan de contrôle : avec `Seal`, il scelle un mot de passe qu'il choisit, l'écrit dans
`smsc_connectors.password_sealed` et prend la main sur un bind opérateur sortant. Sans `Seal`, il ne peut
pas produire un chiffré que le tag de domaine accepte.

Trois choses dans le dépôt disent que c'est le moment : ADR-0016 décide littéralement qu'« un intercepteur
autorise **par méthode** » — la carte par service est l'intérimaire, pas la décision ; le commentaire de la
carte nomme le déclencheur, « le jour où un appelant a besoin d'`Open` seul », et ce jour est cette step, pas
celui de `connector-pool-svc` ; et l'addendum de `debts/ancre-de-confiance-par-connecteur.md` planifie
l'entrée du pool « et rien d'autre », ce qui ne veut dire quelque chose qu'une fois la carte porteuse de
méthodes. Elle reste du **code**, pas de la configuration.

Les deux couches restent distinctes et toutes deux nécessaires : `TLS_ALLOWED_CLIENTS` admet un **binaire**
au handshake, l'intercepteur restreint la **méthode**.

### 6. Le contrat Admin ne bouge pas, et rien n'est à bumper

`secret` est déjà write-only : `minLength: 16` en écriture, absent de `webhookDTO`, et trois tests le
prouvent. La description ne mentait pas non plus — contrairement au `password` de step-295, qui annonçait
« stored hashed » et avait coûté un bump correctif. Aucune ligne d'`api/openapi-admin.yaml` ne change,
donc **pas de bump** de `api/package.json`.

## Plan

1. Schéma + migration 0018 + `db/schema_passerelle_sms.sql`.
2. Domaine `cp.Webhook.Secret cp.SealedSecret`, requêtes sqlc et `WebhookRepo`.
3. L'Admin API scelle à l'écriture (création et rotation), par le `SecretSealer` déjà câblé.
4. `SecretOpener` + adaptateur gRPC, hors de `internal/adminapi` pour que `mo-dlr-router-svc` l'importe
   sans tirer le paquet de l'Admin API.
5. Le sender ouvre, classe l'échec, et gare l'inouvrable.
6. `configSecretsCallers` par méthode, ses deux tests-gardes, et l'allowlist TLS de `content-key-svc`.
7. Câblage de `mo-dlr-router-svc` : section de config, dial, closer.
8. `debts/secret-de-webhook-en-clair-en-base.md` passe à `PAYÉE`.

## Definition of Done

- [ ] `webhooks.secret` n'existe plus : `secret_sealed` + `secret_kms_key_ref`, dans le fichier de schéma
      **et** dans la migration 0018, `up`/`down`/`up` vérifié.
- [ ] Un test prouve l'aller-retour **de bout en bout** : écrit par l'API HTTP → relu de la colonne →
      ouvert → égal à l'entrée, et la signature produite avec le secret ouvert est celle qu'un récepteur
      vérifie.
- [ ] Un test prouve qu'aucune surface ne rend le secret — ni en clair, ni scellé, ni en base64, ni sa
      référence de clé — en création, en rotation, en lecture unitaire et en liste.
- [ ] Un test prouve les **deux** branches de l'échec d'ouverture : service injoignable → erreur rendue,
      aucun essai consommé, rien de garé ; chiffré inouvrable → dead-letter `secret_unopenable` et retour
      `nil`, pour que la partition ne se bloque pas.
- [ ] `mo-dlr-router-svc` obtient `Open` et **se voit refuser `Seal`**, prouvé par un test à côté de
      `TestConfigSecretsIsRefusedToCallersThatOnlyNeedContentKeys`.
- [ ] Les deux tests-gardes de l'allowlist et le manifeste de `content-key-svc` ont évolué sciemment, pas
      disparu ; le commentaire de `configSecretsCallers` et l'addendum de
      `debts/ancre-de-confiance-par-connecteur.md` disent que la forme par méthode existe désormais.
- [ ] `debts/secret-de-webhook-en-clair-en-base.md` est `PAYÉE`, avec la date et la PR.

## Hors périmètre

- Le descellement du mot de passe de bind sortant par `connector-pool-svc` : il ne lit aucune colonne de
  bind et n'a pas de client Postgres ; les treize champs migreront ensemble
  (`debts/ancre-de-confiance-par-connecteur.md`).
- L'outillage d'une rotation de clé maîtresse : `secret_kms_key_ref` la rend possible, rien ne l'automatise,
  et aucune rotation n'est prévue avant le go-live (ADR-0016).
- Le back-off de `retry_policy_json` sur le chemin différé
  (`debts/retry-differe-n-honore-pas-la-politique-de-back-off.md`).
