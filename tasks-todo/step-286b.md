# step-286b — Échecs de bind récents d'un compte SMPP, lisibles à l'Admin API

> **Jalon :** M3 (diagnostic) · **Statut :** À FAIRE
> **Dépend de :** step-026 · **Bloque :** go-gateway-bo step-069
> Demande humaine du 05/10/2026 (spec BO §6.14 : « diagnostic d'échec de bind, échecs d'auth récents ») ;
> unité faute de multiple de dix libre avant step-287.

## Pourquoi
`internal/bindthrottle` ne tient que deux compteurs (`bindfail:{system_id}`, `bindfail:ip:{ip}`) : ni
horodatage, ni adresse, ni motif. Le tableau de bord ne peut pas dire à un opérateur pourquoi un ESME
ne binde pas — et le client SMPP ne le peut pas non plus, par construction (§11.3 : `system_id` inconnu
et mauvais mot de passe rendent tous deux `ESME_RINVPASWD`).

## Design arrêté

**Stockage — Redis seul, aucune écriture Postgres.** Une liste par compte `bindfail:log:{account_id}`,
écrite en `MULTI` mono-clé (`LPUSH` + `LTRIM 0 199` + `EXPIRE 86400`). Bornes : **24 h,
200 entrées par compte**, constantes déclarées au contrat. Une rafale fait tourner la liste, jamais
grossir ; elle peut évincer les entrées plus anciennes (dit au contrat). Entrée JSON
`{at, remote_ip, bind_type, command_status, reason}` : le type n'a aucun champ capable de porter le
secret (invariant a), un test fige l'ensemble des clés. Fail-open : une erreur Redis se journalise.
L'enregistrement (et, ralenti, sa lecture d'attribution) part **après** la réponse, sur un contexte
détaché borné à 2 s : l'attendre rendrait au chronomètre la différence révoqué/inconnu (revue).

**Motifs (énumération fermée) → statut rendu :** `password_mismatch` (hash stocké illisible inclus),
`credential_revoked`, `throttled` → `ESME_RINVPASWD` ; `credential_disabled`, `account_inactive`
(compte ou client suspendu/fermé), `smpp_channel_disabled`, `bind_type_not_allowed`,
`max_sessions_exceeded` → `ESME_RBINDFAIL` ; `registry_unavailable` → `ESME_RSYSERR`.

**Attribution.** Seul un `system_id` qui résout un identifiant écrit : un inconnu n'appartient à aucun
compte et ne crée aucune clé. Une panne du lookup Postgres ne résout rien, donc n'écrit rien.
- *Révoqué* : `GetBindPrincipal` lit aussi les lignes révoquées, la vivante d'abord
  (`ORDER BY status = 'revoked', created_at DESC LIMIT 1`), sur un index partiel neuf
  `(system_id) WHERE type = 'smpp_bind'` (schéma + migration). `authorize` répond `ESME_RINVPASWD`
  au révoqué **avant** argon2id — fil inchangé, statut et temps compris.
- *Ralenti* : après le backoff, une **lecture** de l'identifiant (garde UTF-8 comprise) attribue le
  refus. Elle coûte au plus ce que la tentative coûtait sans throttle (qui épargne argon2id, pas la
  lecture). Écartée : une clé Redis « propriétaire », qui ratait le compte légitime ralenti par une IP
  partagée.

**Code.** `internal/bindfailure` (`Record`, `List`) ; `authorize` rend aussi le motif ; `onBind`
enregistre à ses trois sorties (authorize, throttle, registre) ; câblage `cmd/smpp-server-svc`. Admin :
`BindFailureLog` dans `deps.go`, handler sur le modèle de `sessions.go`, câblage `cmd/admin-api-svc`.

**Contrat (mineur, 6.15.0).** `GET /admin/smpp-accounts/{id}/bind-failures?since=`, `admin:read`.
`since` par défaut `now − 24h` ; hors `[now − 24h, now]` → 422. Du plus récent au plus ancien, sans
pagination (le plafond suffit). 404 si le compte n'existe pas. La description dit que les `system_id`
inconnus n'y figurent pas et que la liste est plafonnée.

**Écarté (YAGNI) :** pagination, rétention configurable. Attribution des inconnus : différée, fiche
`debts/echecs-de-bind-sous-system-id-inconnu-invisibles.md`.

## Definition of Done
- [ ] contrat déclaré avant le handler, `api/package.json` bumpé, `make contracts` vert
- [ ] schéma + migration de l'index, `GetBindPrincipal` révoqué-compris, vivant prioritaire (intégration)
- [ ] `internal/bindfailure` : plafond, TTL, ordre, `since`, clés fermées (intégration Redis)
- [ ] chaque sortie de `onBind` enregistre son motif, l'inconnu n'écrit rien, fil inchangé
- [ ] handler : 200 ordonné, 404, 422, scope `admin:read` ; collection Admin régénérée
- [ ] une mutation tombée par point d'enregistrement
- [ ] revue (dont un axe « code en trop »), coupe, `make check`
