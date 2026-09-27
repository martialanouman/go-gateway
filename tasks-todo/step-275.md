# step-275 — Environnement de test : seed du plan de contrôle et preuve bout-en-bout

> **Jalon :** M12 · **Statut :** À FAIRE
> **Dépend de :** l'environnement de test k3s (docs/superpowers/specs/2026-09-27-environnement-de-test-k3s-design.md) · **Bloque :** —

## Pourquoi cette fiche existe

L'environnement de test déploie et prouve que chaque pod est prêt ; il ne prouve pas qu'un SMS
traverse. Deux manques, reportés hors du premier livrable :

1. **Aucun seed.** Sans client, compte, identifiant de bind, sender ID actif, connecteur et route
   statique, rien ne s'envoie. Le connector-pool ne traite que les enregistrements dont `ConnectorID`
   égale son `CONNECTOR_ID` (`internal/connectorpool/submit.go:176`), qui doit donc être l'id d'une
   ligne `smsc_connectors` visée par une route. Le modèle du seed : `internal/e2e/e2e_test.go:221`
   (`seedControlPlane`) ; par l'API Admin, pour que `config:changed` invalide les caches.
2. **Aucune preuve bout-en-bout.** Un Job `smoke` qui binde en TLS, soumet un `submit_sm` avec
   `registered_delivery` et attend son DLR — smsc-simulator v0.8.0 les émet : le bloc `dlr` de sa
   configuration (`deploy/test/bootstrap-secrets.sh`) reprend celui de son propre `deploy/configmap.yaml`.

## Definition of Done
- [ ] Seed idempotent par l'API Admin, rejouable après une remise à zéro du namespace.
- [ ] `CONNECTOR_ID` de l'overlay égal à l'id du connecteur seedé.
- [ ] Job `smoke` lancé par `gateway-deploy` en dernière phase ; le workflow échoue s'il échoue.

## Design arrêté

Trois faits lus dans le code fixent la forme :

1. `ConnectorCreate` n'accepte pas d'`id` — le serveur l'attribue — et `connector-pool-svc` lit
   `CONNECTOR_ID` au démarrage (`cmd/connector-pool-svc/main.go:48`) : l'overlay ne peut pas le
   connaître d'avance.
2. Le secret d'une credential `smpp_bind` est généré par le serveur et rendu une seule fois
   (`create-credential`, `rotate-credential`) : un seed rejoué ne le retrouve pas.
3. L'Admin API exige un certificat client de la CA `tlsgen` (`tlsconf.ServerConfig`), dont la clé
   n'est pas conservée par `bootstrap-secrets.sh`.

**`cmd/test-env`**, deux sous-commandes, hors GoReleaser : `internal/deploy/images_test.go` refuse
toute image publiée que `deploy/k8s` ne déploie pas. `deploy-test.yml` compile le binaire et bâtit
l'image `ghcr.io/martialanouman/go-gateway/test-env:$VERSION` avec le `Dockerfile` commun
(distroless, uid 65532).

- **`seed`** — Job en phase `seed`, après le rollout de l'application. Idempotent par nom, par l'Admin
  API seule (pour que `config:changed` invalide les caches) : client `test`, sender ID actif, compte
  SMPP, connecteur `smsc-simulator`, route statique catch-all vers lui, credential `smpp_bind` de
  system_id `smoke`. Un second passage n'émet aucun POST. Sortie : une ligne `connector_id=<uuid>`,
  aucun secret.
- **`gateway-deploy`** lit cette ligne dans les logs du Job, l'écrit dans la ConfigMap `test-seed` ;
  l'overlay prend `CONNECTOR_ID` de `connector-pool-svc` en `configMapKeyRef` (`optional: true`). Si
  la valeur a changé (premier déploiement, namespace remis à zéro), il relance le rollout de
  `connector-pool-svc`.
- **`smoke`** — Job en dernière phase : `rotate-credential` sur `smoke` (mot de passe frais en
  mémoire, rien de persisté), bind TRX en TLS sur `smpp-server-svc:2775` avec réessais le temps que
  la config se propage, `submit_sm` avec `registered_delivery=1`, attente du `deliver_sm` de receipt
  (60 s max). Tout état final est accepté — le simulateur tire DELIVRD/UNDELIV/EXPIRED — : c'est
  l'arrivée du DLR qui prouve la traversée. Son échec fait échouer `gateway-deploy`, donc le workflow.
- **Identité des Jobs** : `bootstrap-secrets.sh` pose le Secret `operator-tls` (certificat
  `operator`) ; le jeton admin est la partie avant le premier `:` de `HTTP_ADMIN_TOKENS`
  (`gateway-secrets`). Environnement déjà en place : une commande du runbook crée `operator-tls` depuis
  `~/.config/go-gateway-test/`.
- **Runbook** : `bind-credentials` sont les identifiants passerelle → simulateur, pas un compte client ;
  un bind client passe par `rotate-credential` sur `smoke`. Le paquet GHCR `test-env` naît privé.

**Tests** : `seed` et `smoke` contre un faux Admin API `httptest` et un pair SMPP en processus (rejeu
sans POST ; DLR reçu → 0 ; aucun DLR → échec) ; `gateway-deploy_test.sh` : seed avant smoke, rollout
relancé seulement si l'id change, smoke en échec → déploiement en échec ; `check.sh` :
`CONNECTOR_ID` vient de `test-seed`.

**Écarté** : seed SQL à UUID fixés (contourne `config:changed` et le scellement du mot de passe
connecteur) ; `id` ajouté au contrat (la prod changée pour le test) ; écriture k8s depuis le Job (RBAC
en plus, `gateway-deploy` a déjà les droits) ; image `test-env` dans la release (garde
`images_test.go`).
