# step-287j — Le principal d'une clé d'API se garde en mémoire, vidé à chaque annonce de config

> **Jalon :** M12 · **Statut :** EN COURS
> **Dépend de :** step-260c (politique fail-closed de l'auth REST), step-395 (Watcher et resync)
> Demande humaine du 10/10/2026, née du run 8 de step-287 : `GetAPIKeyPrincipal` est la requête qui coûte
> le plus de CPU à Postgres (une lecture par requête REST authentifiée).

## Pourquoi
Chaque requête REST relit sa clé en base (`internal/restapi/auth.go`). Au run 8, c'est la première
consommatrice de CPU de Postgres. Le résultat ne change que sur une écriture d'admin-api-svc, et toutes
celles qui peuvent l'invalider publient déjà `config:changed` (révocation, suspension, rotation de la
clé ; statut, `rest_enabled`, suppression du compte ou du client).

## Design arrêté
Arbitré avec l'humain le 10/10/2026 : cache **en mémoire de chaque pod**, invalidé par Redis. Un cache
Redis aurait remplacé l'aller-retour Postgres par un aller-retour Redis et demandé un compteur de
génération côté admin.

- **`restapi.PrincipalCache`** enveloppe le `PrincipalStore` existant, clé = le hash SHA-256 de la clé.
  `Flush()` vide tout. Câblé dans rest-api-svc ; le `rebuild` du Watcher existant
  (`breaker:events`) vide le cache puis recharge les limites de débit.
- **Course lecture/vidage** : une génération incrémentée par `Flush`. Une lecture Postgres commencée
  avant un vidage n'est pas insérée après lui, sinon la valeur d'avant le commit admin survivrait à
  l'annonce qui devait l'effacer.
- **Expiration** : 30 s par entrée, ce qui borne le retard si une annonce se perd (Redis coupé,
  abonnement tombé ; la resync de 5 min ne suffit pas pour une révocation).
- **Délai de grâce d'une rotation** : `GetAPIKeyPrincipal` rend `grace_expires_at` quand la clé
  présentée est l'ancienne ; l'entrée expire au plus tôt de 30 s et de cette échéance.
- **Clé inconnue** : jamais en cache. Une clé tout juste créée marche tout de suite ; une rafale de
  fausses clés touche Postgres comme aujourd'hui.
- **Taille** : seules des clés valides entrent ; le cache est borné par le nombre de clés actives.
- **Politique de panne (écart à step-260c)** : Postgres coupé, une clé déjà en cache reste acceptée au
  plus 30 s au lieu d'un 500 immédiat. Une clé absente du cache reçoit toujours 500.
- **`breaker:events`** sert aussi au disjoncteur (step-123) : un disjoncteur qui bascule vide le cache.
  Au pire on retombe sur le coût d'aujourd'hui.

## Definition of Done
- [x] entrée servie depuis le cache sans relire le store (test, muté)
- [x] `Flush` vide ; une lecture commencée avant un `Flush` n'est pas insérée (test, muté)
- [x] expiration à 30 s et à `grace_expires_at` (test, borne comprise, muté)
- [x] clé inconnue et erreur du store jamais en cache (test, muté)
- [x] `grace_expires_at` rendu par `GetAPIKeyPrincipal` sur l'ancienne clé seulement (intégration, 3 mutations du SQL généré et du repo)
- [x] câblage : une clé révoquée passe encore depuis le cache, puis 401 après l'annonce (test du service, 2 mutations)
- [ ] mesuré à un run de step-287 : `GetAPIKeyPrincipal` sort des requêtes actives de Postgres
