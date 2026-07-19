# step-026 — Anti-brute-force sur le bind SMPP

> **Jalon :** M3 (§7 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-024 · **Bloque :** —

## But
Freiner les tentatives de bind répétées : compteur d'échecs par `system_id`/IP (Redis TTL), backoff progressif et événement de sécurité auditable.

## Périmètre (ce que fait CETTE PR)
- Dans `smpp-server-svc` (ou `internal/smpp/session` côté serveur) : sur échec d'auth, incrémenter un compteur Redis `bindfail:{system_id}` et `bindfail:ip:{ip}` (TTL glissant).
- Au-delà d'un seuil configurable → backoff (délai avant réponse) puis refus `ESME_RBINDFAIL` sans même vérifier le hash (économie CPU argon2).
- Émettre un **événement de sécurité auditable** (log structuré `slog`, jamais de secret ni de corps) et un compteur Prometheus.
- Réinitialisation du compteur sur bind réussi.

## Points d'implémentation clés
- **`ctx7`** avant d'utiliser `go-redis` (INCR + EXPIRE atomiques, idéalement un petit script Lua — pas de read-modify-write côté Go, règle d'or).
- Ne jamais logger le mot de passe ; l'IP et le `system_id` sont des identifiants, autorisés.
- Seuils/fenêtres via `caarlos0/env/v11`, valeurs par défaut sûres.
- Le backoff ne doit pas bloquer une goroutine sans `ctx` (timer respectant l'annulation).

## Tests (écrits dans la même PR)
- Intégration Redis : N échecs → refus rapide + backoff appliqué ; bind réussi remet à zéro.
- L'événement de sécurité est émis sans fuite de secret.

## Definition of Done
- [x] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [x] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [x] compteur/backoff atomiques (Lua) et bornés dans le temps

## Décisions de mise en œuvre
- Refus throttlé → **`ESME_RINVPASWD`** (et non `ESME_RBINDFAIL`) : indistinguable d'un mauvais
  mot de passe, l'attaquant ne détecte pas le verrou (cohérent avec l'anti-énumération d'`authorize`).
  Écart assumé vs le périmètre ci-dessus, arbitré avec le porteur.
- **Fail-open** si Redis injoignable : le throttle est de la défense en profondeur (argon2 reste la
  vraie auth) ; Redis n'est donc pas une dépendance de readiness — une panne ne retire pas le pod du LB.
- **Reset sur succès** limité au compteur `system_id` ; le compteur IP décroît via son TTL glissant
  (évite qu'un succès légitime derrière un NAT partagé n'efface les échecs d'un attaquant co-IP).
- Compteur d'échecs : `INCR`+`EXPIRE` atomiques par clé (`internal/bindthrottle/lua/incr_expire.lua`),
  une clé par sujet (cluster-safe) ; `Check` = `GET` (lecture pure). Backoff `base·2^(n-seuil)` borné.

## Suites de la revue de code
- **Anti-amplification mémoire Redis** : une tentative déjà bloquée ne ré-incrémente PAS les compteurs
  (le `system_id` est attaquant-contrôlé → sinon gravure de clés Redis illimitée sur le magasin
  opérationnel partagé). Le blocage retombe via la fenêtre glissante. Corollaire : le verrou IP d'un
  co-NAT n'est plus prolongé indéfiniment.
- **Plafond de connexions** (`SMPP_MAX_CONNS`, défaut 16384) : `max_sessions` ne s'applique qu'après
  auth ; sans plafond, le tarpit (`sleep` du backoff) immobiliserait goroutines/FD sans borne. Sémaphore
  en amont de `serve`, backpressure via backlog noyau.
- **Bornes hautes de config** : fenêtre ≤ 1h, backoff max ≤ 5m (limitent la durée de vie des clés et
  l'immobilisation d'un slot). Métrique `smpp_bind_throttle_blocked_total` étiquetée `subject=ip|system_id`.
- **⚠️ Topologie IP non tranchée (R3, à décider avant prod)** : sous LB TCP L4 sans PROXY-protocol,
  `nc.RemoteAddr()` = IP du LB → le compteur par IP throttle *tous* les clients (auto-DoS). Déployer en
  terminaison L7 / PROXY-protocol, ou neutraliser la moitié IP pour cette topologie. Commentaire
  d'exploitation dans `remoteIP` (`internal/smppserver/listener.go`).

## Hors périmètre
Rotation d'identifiant (step-027) ; corrélation avec un WAF/ingress (exploitation, hors code).
