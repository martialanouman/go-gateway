# Index des steps — plan d'implémentation de la passerelle SMS

Dérivé de `docs/plan-execution-passerelle.md`. **Un fichier `step-NNN.md` = une PR** : petite,
reviewable, laisse le dépôt vert une fois mergée. Découpage par jalon (M0…M12) ; les jalons trop gros
sont éclatés en plusieurs PRs. Numérotation par blocs de 20 (marge d'insertion), ordre = exécution.

**Workflow :** on prend le prochain `step-NNN.md` dans `tasks-todo/`, on l'exécute (1 session = 1 PR), puis
on déplace le fichier dans `tasks-done/`. Un jalon est terminé quand toutes ses steps sont dans `tasks-done/`.

Légende : `[x]` = fait (dans `tasks-done/`) · `[ ]` = à faire (dans `tasks-todo/`).

---

## M0 — Fondations & outillage  ✅ (commit `ecf2012`)
- [x] step-000 — M0 Fondations & outillage

## M1 — Plan de contrôle + Admin API (noyau)  ✅ (commit `22ce77b`)
- [x] step-001 — M1 Plan de contrôle minimal + Admin API core

## M2 — Squelette vertical MT  ✅ (commit `8b2e6e1`)
- [x] step-002 — M2 Walking skeleton (REST → codec SMPP → faux SMSC → CDR)

---

## M3 — Ingress SMPP serveur + sessions + complétion API publique
- [x] step-020 — Proto SessionRegistry + génération du code gRPC
- [x] step-021 — Registre de sessions Redis (bind/unbind/lookup atomiques, max_sessions)
- [x] step-022 — session-manager-svc : serveur gRPC SessionRegistry (:7000)
- [x] step-023 — Machine à états de session SMPP serveur (internal/smpp/session)
- [x] step-024 — smpp-server-svc : listener :2775, auth bind + max_sessions (invariant d)
- [x] step-025 — submit_sm → mt.inbound (pipeline identique REST) + bascules query/cancel
- [x] step-026 — Anti-brute-force sur le bind SMPP
- [x] step-027 — Rotation d'identifiant de bind avec fenêtre de grâce
- [x] step-028 — API publique : get-account (projection lecture seule)
- [x] step-029 — API publique : list-messages (pagination par curseur sur le CDR)
- [x] step-030 — cancel_sm (SMPP) : annulation SMPP-only, sans surface REST (ADR-0009)
- [x] step-031 — En-tête Idempotency-Key (REST, fenêtre 24 h Redis)
- [x] step-032 — Déconnexion forcée des sessions SMPP (fin de grâce, révocation, suspension)

## M4 — Voie retour MO/DLR + webhooks + numéros entrants
- [x] step-040 — Numéros entrants : repo + Admin (CRUD + assign)
- [x] step-041 — Mots-clés entrants : repo + Admin (CRUD)
- [x] step-042 — Mapping de corrélation DLR à l'envoi (dlrmap Redis, §1.11)
- [x] step-043 — Réception deliver_sm : classification MO vs DLR → mo.inbound / dlr.events
- [x] step-044 — mo-dlr-router-svc : squelette + corrélation DLR → CDR
- [x] step-045 — Résolution MO (dédié / mot-clé / non routé) + list-unrouted-mo
- [x] step-046 — Remise deliver_sm côté smpp-server via SessionRegistry.Deliver
- [x] step-047 — Webhooks signés HMAC-SHA256 (retries, dead-letter)
- [x] step-048 — Décision de remise MO : bind actif (gRPC) ou webhook

## M5 — Conformité sur le chemin critique
- [x] step-060 — Étape pipeline : autorisation Sender ID (§6.19)
- [x] step-061 — Opt-out : repos suppressions/keywords + Bloom par portée
- [x] step-062 — Étape pipeline : opt-out MT bloquant (union des portées)
- [x] step-063 — Détection STOP côté MO : suppression scopée + auto-réponse (jamais facturée)
- [x] step-064 — Admin opt-out : suppressions + opt-out keywords
- [x] step-065 — Anti-spam : moteur + règles contenu & doublons (étape MT activée)
- [x] step-066 — Anti-spam : vélocité + réputation (état partagé Redis, fail-open)
- [x] step-067 — Admin anti-spam : *-antispam-rule

## M6 — Encodage/segmentation + gestion du débit
- [x] step-080 — Poser le socle Redis + moteur de scripts Lua (EVALSHA atomique)
- [x] step-081 — Détection d'encodage GSM-7/UCS-2/8-bit + calcul du nombre de segments
- [x] step-082 — Découper les messages longs en segments UDH (étape pipeline)
- [x] step-083 — Réassembler les MO concaténés (multipart)
- [x] step-084 — Token-bucket Lua atomique + repo `rate_limits`
- [x] step-085 — Brancher l'étape débit dans le pipeline + précédence des plafonds
- [x] step-086 — Throttling adaptatif AIMD piloté par `ESME_RTHROTTLED`
- [x] step-087 — Limite de débit dédiée pour `query_sm`
- [x] step-088 — Fenêtrage du `submit_sm` entrant (traitement concurrent borné par session)

## M7 — Routage avancé
- [x] step-100 — Repo `exact_routes` (numéros exacts, portabilité)
- [x] step-101 — Bloom en mémoire + `exactroute:{msisdn}` Redis + court-circuit L0
- [x] step-102 — Admin exact-routes : list / create / update / delete / lookup
- [x] step-103 — Admin import-exact-routes (asynchrone)
- [x] step-104 — Instantané de routage immuable + pointeur atomique
- [x] step-105 — `cmd/config-sync` + pub/sub d'invalidation d'instantané
- [x] step-106 — Hot reload des filtres de Bloom (exact + suppressions) sans downtime
- [x] step-107 — Repo `routing_scripts` + cycle de statut (draft/active/disabled)
- [x] step-108 — Runtime de script JS (goja) poolé, plafond d'instructions = garde primaire
- [x] step-109 — Runtime de script Lua (gopher-lua) poolé, mêmes gardes
- [x] step-110 — Contrat `resolveRoute`, résolution de scope, intégration pipeline (repli déclaratif)
- [x] step-111 — Admin routing-scripts : CRUD + list-versions
- [x] step-112 — Admin assign / validate / test / publish routing-script
- [x] step-113 — Stratégies de distribution déterministes : round_robin, weighted, hash_based
- [x] step-114 — Stratégies failover_priority, least_loaded + fallback_route
- [x] step-115 — Invariant (b) : un message routé L0 traverse toute la conformité

## M8 — Résilience connecteurs  *(bascule au vrai simulateur SMSC)*
- [x] step-120 — Basculer les tests d'intégration au vrai simulateur SMSC
- [x] step-121 — Disjoncteur par connecteur : machine à états locale
- [x] step-122 — Agrégation multi-pod du disjoncteur par majorité (Redis)
- [x] step-123 — Le routeur lit `breaker:state` à la (re)construction de l'instantané
- [x] step-124 — Pool de binds (`bind_pool_size > 1`) + partition par shard
- [x] step-125 — `fallback_chain` en en-tête + reroute unilatéral
- [x] step-126 — Draineur borné + `mt.reroute-park` (rafales de reroute)
- [x] step-127 — Auto-reconnexion opt-in (backoff + jitter), `link_status` distinct du breaker
- [x] step-128 — Admin connecteurs : rebind / status / reconnect-policy / bind-pool
- [x] step-129 — Dead-letter (`mt.dead-letter` / `mo.dead-letter`) + retraitement
- [x] step-130 — Dé-`Skip` des tests de résilience + scénarios d'injection de pannes

## M9 — Facturation opt-in
> ⚠️ **Jalon coché avec une dette :** step-146 a livré le règlement MT en fail-open en s'appuyant sur un
> « reaper » qui n'a jamais été implémenté. Le rattrapage est **step-190** (section Audit pré-production).
> Les steps ci-dessous restent faites ; c'est leur filet de sécurité qui manquait.
- [x] step-140 — Poser le contrat gRPC billing + l'outillage protoc
- [x] step-141 — Repos Postgres billing : balances, config, grand livre partitionné
- [x] step-142 — Réserve/capture/libère MT en Lua atomique, idempotent par message_id
- [x] step-142b — Config facturation (floor overdraft/postpaid) + TTL du cache de solde
- [x] step-142c — Interdire overdraft/hard limit quand balance_scope=smpp_account
- [x] step-142d — Consolider la config billing sur customers (ADR-0010)
- [x] step-143 — Compteur MO séparé : plancher, arrêt + alerte, jamais bloquant pour le MT
- [x] step-144 — Câbler billing-svc (gRPC :7001) + port ops
- [x] step-145 — Réserve MT dans le router (étape 8) ; désactivée = zéro appel réseau
- [x] step-146 — Capture/libère dans connector-pool ; idempotent sous double livraison
- [x] step-147 — Adaptateur de facturation externe (§6.10) derrière une interface
- [x] step-148 — Admin billing : config client, soldes, top-up/transfert, change-balance-scope
- [x] step-149 — Admin billing : grand livre, rate-plans, providers externes

## M10 — Contenu, chiffrement & RGPD
- [x] step-160 — Interface KMS + implémentation locale de dev (enveloppe)
- [x] step-161 — content_keys : cycle de vie de clé par client (hébergé par billing-svc)
- [x] step-162 — Chiffrement du contenu à l'écriture CDR + politique content_storage
- [x] step-163 — Lecture de contenu gardée et auditée (get-message-content)
- [x] step-164 — Crypto-shred : destruction de clé + erase-customer-content
- [x] step-165 — Rétention & tiering par drop de partition (§6.14)
- [x] step-166 — Effacement RGPD (client + MSISDN) + attestation asynchrone
- [x] step-167 — Extraire la garde des clés de contenu dans un service dédié (content-key-svc)

## M11 — Observabilité complète & temps réel
- [x] step-180 — Catalogue de métriques à labels BORNÉS + test de garde de cardinalité
- [x] step-181 — Spans complets par étape (pipeline.* / connector.*), 100 % sur erreur
- [x] step-182 — Émettre les mises à jour temps réel vers metrics.stream
- [x] step-183 — Gateway WS/SSE (coder/websocket) + stream-metrics
- [x] step-184 — Flux stream-sessions + stream-billing-alerts
- [x] step-185 — get-message-trace : trace complète d'un message, sans aucun corps
- [ ] step-186 — search-messages avec masquage MSISDN par rôle
- [ ] step-187 — Export de messages asynchrone (row-cap, MSISDN masqué)

## Audit pré-production — correctifs issus de la revue du 2026-08-01
Trois de ces dettes étaient décrites **en commentaire dans le code** mais n'existaient dans aucune step :
un commentaire n'est pas un backlog. Les steps 190-192 corrigent des risques de production, 193-194 sont
structurelles et à faire **avant** que M12 n'empile dessus.
- [ ] step-190 — Reaper de réservations orphelines (**BLOQUE step-208 : le fail-open de step-146 n'a
      aujourd'hui aucun filet**)
- [ ] step-191 — PROXY protocol sur le listener SMPP (sinon throttle de bind global derrière un LB L4)
- [ ] step-192 — Topic `webhook.retry` différé (sortir les retries du chemin chaud)
- [ ] step-193 — Câblage de router-svc / connector-pool-svc en constructeurs testables
- [ ] step-193b — Même patron pour mo-dlr-router-svc, admin-api-svc, smpp-server-svc
- [ ] step-194 — Découper `connectorpool.go` (extraction du mapping SMPP/CDR)

## M12 — Durcissement, charge & mise en production
- [ ] step-200 — Harnais de charge k6/vegeta + générateur de binds SMPP (NFR)
- [ ] step-201 — Tuning de débit : partitions Kafka, batch ClickHouse, pool pgx
- [ ] step-202 — Chaos : perte Redis (chaque politique de panne) + flapping connecteur
- [ ] step-203 — Chaos : drain gracieux + PDB + binds préservés ; failover Postgres
- [ ] step-204 — Sécurité : gosec, govulncheck, secrets, piste d'audit
- [ ] step-205 — TLS / SMPP-TLS / mTLS sur les transports
- [ ] step-206 — Auth opérateur réelle (OIDC/mTLS) remplaçant le stub M1
- [ ] step-207 — Manifests deploy/ Kubernetes (Deployments, Services, HPA, PDB, probes)
- [ ] step-208 — Dérouler la checklist de mise en production (go-live) — **bloquée par step-190**
