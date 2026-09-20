# step-340 — Webhooks : le repo est livré depuis M4, l'admin n'a jamais été écrite

> **Jalon :** Surfaces Admin déclarées au contrat, jamais construites (§6.18 `docs/specification-technique-passerelle-sms.md`) · **Statut :** FAITE
> **Dépend de :** step-320 (triage) · **Bloque :** —

## But

Servir les 4 opérations de webhooks déclarées au contrat. Aujourd'hui un client ne peut **pas**
configurer où sont poussés ses MO et ses DLR : la remise fonctionne, sa configuration n'existe pas.

| Opération | Méthode et chemin |
|---|---|
| `list-webhooks` | `GET /admin/smpp-accounts/{id}/webhooks` |
| `create-webhook` | `POST /admin/smpp-accounts/{id}/webhooks` |
| `update-webhook` | `PATCH /admin/smpp-accounts/{id}/webhooks/{webhookId}` |
| `delete-webhook` | `DELETE /admin/smpp-accounts/{id}/webhooks/{webhookId}` |

## Le constat

Tout l'aval existe depuis M4 : `control_plane.webhooks`, `internal/storage/postgres/webhooks.go`,
l'envoi signé HMAC-SHA256 avec retries (step-047), la remise MO arbitrée entre bind actif et webhook
(step-048), et depuis step-192 le topic `webhook.retry` sur son propre groupe de consommation. Ce qui
manque est la seule chose qu'un opérateur touche.

## Points d'implémentation clés

- **`secret` est un secret d'un genre différent de ceux du dépôt, et la règle générale ne s'y applique
  pas.** Les mots de passe de bind et les clés API sont *vérifiés* par la passerelle, donc hachés. Ce
  secret-ci est *utilisé* : `webhook.Sign(wh.Secret, …)` en a besoin **en clair** à chaque remise. Le
  hacher rendrait toutes les signatures invérifiables côté client. Le contrat a déjà tranché la bonne
  forme — `secret` est **write-only, jamais retourné** (`Webhook` n'a pas ce champ ; `WebhookCreate` et
  `WebhookUpdate` l'acceptent en entrée) : c'est l'opérateur qui **fournit** la valeur, rien n'est
  « révélé ». Donc pas de hash, pas de sentinelle masquée (celle de step-149 sert à `auth_config_json`,
  qui, lui, est retourné masqué). La règle à tenir est plus étroite et déjà écrite dans le code : le
  secret ne doit **jamais** être persisté hors du plan de contrôle — la doc de `webhook.RetrySink`
  l'interdit explicitement pour les records de retry, un opérateur pouvant les lire.
- **`webhooks_uq UNIQUE (account_id, event_type)`** : un compte a au plus un webhook `mo` et un `dlr`.
  Une création en double est un 409 déterministe, pas une erreur de base remontée telle quelle. Le
  résumé du contrat le dit déjà (« one per event_type ») — le handler doit le dire aussi.
- **`status = disabled` ≠ suppression.** Désactiver doit couper la remise sans perdre l'URL ni le
  secret. Vérifier ce que le consommateur de remise lit réellement : s'il ignore `status`, la bascule
  est décorative, et c'est un défaut de cette step, pas une amélioration future.
- **`retry_policy_json` est déjà câblée et déjà au contrat** — la sortir serait une rupture.
  `webhook.parseRetryPolicy` lit `max_attempts`, `initial_backoff_ms`, `max_backoff_ms` et `multiplier`,
  sous des plafonds durs (20 essais, 5 min). Le vrai constat est ailleurs : **le runner différé
  (step-192) ne pace que sur ses propres constantes** — seul `max_attempts` traverse
  (`retriesExhausted`), le back-off de la politique n'est pas honoré sur ce chemin. Une surface qui
  laisse configurer `initial_backoff_ms` sans que le retry différé s'en serve promet un réglage inerte :
  soit le runner l'honore, soit la fiche le documente comme non honoré sur le chemin différé.

## Design arrêté

### Le contrat change — les 4 opérations sont publiques

Comme les 7 de step-330, les 4 opérations webhooks ne portent **aucun bloc `security:`**
(`api/openapi-admin.yaml:421-454`). Le document en a un global (`:31-32`, `OperatorBearer: []`), mais
`auth.Middleware` ne lit que `ctx.Operation().Security` et huma ne fusionne jamais le global dans
l'opération : les servir sans `scopeSecurity(...)` les rendrait ouvertes — créer, réécrire et
supprimer l'URL de remise d'un client sans token, c'est-à-dire **détourner son trafic retour**.
`TestEveryGeneratedOperationRequiresAScope` (posée par step-330 pour cette série) le refuserait ; le
contrat doit donc dire la même chose que le code, précédent step-149.

| Opération | `security` | Codes ajoutés | Pourquoi ce code |
|---|---|---|---|
| `list-webhooks` | `admin:read` | 401, 403, 404, 422 | compte inconnu ; `Id` est `format: uuid` → 422 avant le handler |
| `create-webhook` | `admin:write` | 401, 403, 404, 422 | compte inconnu ; uuid malformé, corps invalide |
| `update-webhook` | `admin:write` | 401, 403, 422 | uuid malformé (le 404 est déjà déclaré) |
| `delete-webhook` | `admin:write` | 401, 403, 422 | uuid malformé (le 404 est déjà déclaré) |

**Le 404 de `list-webhooks` et `create-webhook`** vient d'une garde `accounts.Get`, exactement comme
`list-credentials` (`internal/adminapi/credentials.go:113-127`) — même forme de chemin, même règle :
sans elle, un compte inconnu répondrait 200 avec un tableau vide, et une création pointerait sur une
violation de clé étrangère traduite en 422. Ce n'est pas contradictoire avec `set-customer-group`, qui
refuse le pré-contrôle : là le groupe est une **valeur de corps** (422), ici le compte est un
**segment de chemin** — un chemin vers une ressource qui n'existe pas est un 404.

Pas de 409 sur `update-webhook` : `WebhookUpdate` ne porte pas `event_type`, donc aucun chemin de
mise à jour ne peut heurter `webhooks_uq`.

Ces ajouts-là sont additifs (`api-security-added`, `response-non-success-status-added` : INFO). Ce qui
coûte, c'est la validation.

**`secret` et `url` n'avaient aucune contrainte** : `POST {"secret":""}` créait un webhook dont les
signatures HMAC sont calculables par n'importe qui, et `url: "acme.test/mo"` — sans schéma — un webhook
que `webhook.Sender` ne peut pas composer, qui brûle son budget d'essais et met en dead-letter **chaque**
MO et DLR du compte sans que rien n'ait signalé la faute de frappe. `minLength: 16` sur `secret` et
`pattern: ^https?://[^/]` sur `url`, dans `WebhookCreate` **et** `WebhookUpdate` — `format: uri` ne
suffit pas, la validation de huma est un `url.Parse` qui accepte les deux, et le `[^/]` final exige un
hôte, `https://` seul produisant exactement la même panne. Le motif est sensible à la casse et le
reste : `(?i)` n'existe pas en ECMA-262, et le tableau de bord génère ses validateurs depuis ce
contrat. Les deux schémas existent déjà sur
`main` (les opérations y sont `deferred`), donc `oasdiff` classe les restrictions en **ERR** : bump
**majeur** `api/package.json` 5.0.0 → **6.0.0**. La rupture est formelle — `deferred` veut dire 404,
aucun consommateur ne pouvait appeler ces opérations — et c'est le seul moment où le prix se négocie :
durcir après coup coûterait un second majeur. Même constat qu'en step-330, pour la même raison.

### `status = disabled` ne coupait que la moitié de la remise

Constat, à la demande de la fiche. Deux chemins résolvent le webhook par `(account_id, event_type)` via
le **même** `WebhookRepo.Get`, et un seul filtre :

- première remise MO/DLR — `internal/modlrrouter/deliverer.go:138` filtre en Go
  (`found && wh.Status == cp.WebhookActive`) ;
- retry différé — `internal/modlrrouter/webhook_retry_runner.go:141` ne teste que `!found`, alors que
  son propre commentaire affirme « The webhook was deleted **or disabled** while the event waited ».

Conséquence : désactiver un webhook n'arrête pas les événements déjà déférés ; ils continuent d'être
poussés vers une URL que l'opérateur croit coupée, jusqu'au budget d'essais (8) ou aux 6 h d'âge.

**Premier arbitrage, abandonné.** Faire porter la règle par la requête partagée (`GetActiveWebhook`,
`AND status = 'active'`) et supprimer le test de statut du `Deliverer` : un seul endroit, le nom force
tout appelant futur. C'est ce qui a été écrit d'abord, et la revue l'a renversé. La prémisse était
fausse — **les deux appelants ne posent pas la même question.** Ils traitent l'absence différemment :
le `Deliverer` met en dead-letter (l'événement survit), le runner **jette** (`Handled("dropped")`, un
log `Info`, l'offset commité). Filtrer dans la requête faisait donc de « désactiver » un ordre de
**destruction** du backlog déjà déféré — jusqu'à six heures de MO et de DLR d'un compte — alors qu'on
voulait exactement l'inverse. Un opérateur qui coupe son endpoint le temps d'une maintenance n'a pas
demandé ça.

**Le correctif retenu** laisse la requête rendre les lignes `disabled` et met la décision là où elle se
prend :

- `Deliverer` : inchangé. `found && Status == active` → envoi, sinon dead-letter. Il avait raison.
- `WebhookRetryRunner` : `!found` (supprimé) → drop, comme le voulait step-192 — le compte a demandé
  que ces événements cessent. `found && !active` (désactivé) → **`Sender.Park`**, le dead-letter que le
  `Deliverer` utilise déjà pour la même situation. Métrique `parked`.

`webhook.Sender` expose `Park` pour cela : le runner n'a ni producteur ni sink, et le dead-letter a
besoin de la ligne — l'URL comprise — que le filtre SQL jetait.

### `retry_policy_json` : ce qui est honoré, et ce qui ne l'est pas

Seul `max_attempts` traverse le chemin différé (`webhook.retriesExhausted`). Le rythme vient des
constantes du runner (`retryPaceBase = 30s`, doublement, `retryPaceMax = 10 min`).

**On ne l'honore pas, on l'écrit.** Honorer la politique aujourd'hui casserait le budget d'essais :
avec ses défauts publiés (`initial_backoff_ms = 1000`, `max_backoff_ms = 30000`), les 8 essais
brûleraient en ~1 minute au lieu de ~40 — ces défauts dimensionnaient la boucle en bande, qui n'est
plus prise en production depuis step-192. Les re-dimensionner est un choix produit, pas un effet de
bord d'une step CRUD. La description de `retry_policy_json` dit donc, dans les trois schémas, que les
champs de back-off ne valent que pour l'envoi en bande et que seul `max_attempts` borne les retries
différés. `debts/retry-differe-n-honore-pas-la-politique-de-back-off.md` reste **OUVERTE**, enrichie du
choix rendu ici et de son déclencheur.

### Ce qui s'écrit

| Fichier | Rôle |
|---|---|
| `internal/controlplane/webhook.go` | `CreatedAt`/`UpdatedAt` sur `Webhook`, `NewWebhook`, `WebhookPatch` |
| `internal/storage/postgres/queries/webhooks.sql` | list par compte · create · update (COALESCE partiel) · delete (`:execrows`) ; `GetWebhook` inchangée |
| `internal/storage/postgres/webhooks.go` | le CRUD, **sans pool** (aucune transaction) |
| `internal/adminapi/webhooks.go` | DTO + les 4 opérations |
| `internal/webhook/retry.go` · `internal/modlrrouter/webhook_retry_runner.go` | `Sender.Park` exporté ; la branche « désactivé » du runner |
| `deps.go` · `api.go` · `wiring.go` | une ligne chacun |

**Décisions.** Le chemin est `/admin/smpp-accounts/{id}/webhooks/{webhookId}` alors que la clé
naturelle est `(account_id, event_type)` : get, update et delete portent donc **les deux** identifiants
en clause `WHERE`. Un `webhookId` appartenant à un autre compte est un 404, jamais une écriture
inter-comptes — c'est la seule protection, le `webhookId` étant devinable par énumération d'UUID.
`secret` n'entre dans aucun DTO de sortie : il est requis à la création, optionnel à la mise à jour (il
la rotate), et jamais relu — pas de hash (`webhook.Sign` en a besoin en clair), pas de sentinelle
masquée, le contrat a déjà tranché. `created_at`/`updated_at` manquent à `cp.Webhook` alors que le
schéma `Webhook` les déclare **non requis** : les champs du DTO portent `omitempty`, ce qui les laisse
hors du `required` généré sans toucher au contrat, tout en étant toujours sérialisés. `event_type` est
immuable (`WebhookUpdate` ne le porte pas). Le doublon `(account_id, event_type)` remonte en 409 par
`pgerr.translate` — **aucun pré-contrôle du doublon**, donc aucune fenêtre de course sur le 409. (La
garde d'existence du compte, elle, en laisse une : un compte supprimé entre le `Get` et l'`INSERT`
donne un 422 de clé étrangère là où le contrat veut un 404. Fenêtre de quelques millisecondes, coût du
correctif supérieur au défaut.) La mise à jour partielle est
un `COALESCE` sur une valeur NULL, et le distinguo « absent / fourni » passe par une `json.RawMessage`
nil ou non : `{"retry_policy_json":{}}` **remet donc bien** la politique à l'objet vide, et
`retry_policy_json: null` est refusé en 422 par huma avant d'atteindre le handler. La dette
`debts/patch-null-ne-peut-pas-effacer-un-champ.md` ne s'applique pas à ce champ — vérifié, contre la
première rédaction de cette fiche qui affirmait le contraire.

**Ce qui ne s'écrit pas.** Aucune migration : la table existe depuis `0001_init`. Aucun code d'audit :
`audited()` couvre déjà toute requête non lecture-seule.

**Ce qui est différé, et pourquoi.** `control_plane.webhooks.secret` est un `text` en clair. C'est la
forme qu'impose son usage — `webhook.Sign` le rejoue à chaque remise, un hash ne se dé-hache pas — mais
c'est précisément la classe de secret que step-295 a **scellée** (`ConfigSecrets`, `content-key-svc`)
pour le mot de passe de bind sortant et la configuration d'auth d'un fournisseur. L'inventaire de
step-295 n'en avait recensé que deux ; celui-ci est le troisième, et cette step est la première à
l'écrire par l'API. Le sceller demande une migration, la KMS câblée dans `admin-api-svc` **et** dans
`mo-dlr-router-svc` qui doit l'ouvrir à chaque remise : c'est une step, pas une ligne. Une fiche de
dette est ouverte dans la même PR.

## Tests

- CRUD sur repo réel ; le secret **n'apparaît dans aucune réponse** après création (assertion sur le
  corps sérialisé, pas sur la struct — c'est la sérialisation qui fuit).
- Le doublon `(account_id, event_type)` produit le code d'erreur du contrat, pas un 500.
- Un webhook `disabled` n'est pas remis : test au niveau du consommateur de remise, seul endroit où la
  propriété est vraie ou fausse. La muter au niveau du handler ne prouverait rien. **Les deux chemins**
  sont à couvrir — première remise *et* retry différé, puisque le second ne coupait pas — et le second
  doit prouver en plus que l'événement est **parqué**, pas perdu.
- Un `webhookId` d'un autre compte répond 404 et n'écrit rien.

## Definition of Done

- [x] `make check` vert (lint · `test -race` · govulncheck · contrats)
- [x] les 4 opérations servies ; secret jamais relu ; unicité et `disabled` vérifiés côté remise
- [x] contrat corrigé : `security` et les codes d'échec d'auth ajoutés aux 4, `minLength` sur `secret`
      et `pattern` sur `url`, description de `retry_policy_json` rendue honnête — bump **majeur**
      `api/package.json` 5.0.0 → 6.0.0
- [x] désactiver un webhook coupe la remise **sans perdre** les événements déjà déférés
- [x] `api/collections/admin-api.yaml` synchronisée
- [x] les 4 lignes retirées de la liste `deferred` posée par step-320 (elle vit dans le test de
      contrat, pas dans la fiche)

## Hors périmètre

Le mécanisme de remise, ses retries et son dead-letter (livrés en M4 et step-192). La rotation
programmée du secret.
