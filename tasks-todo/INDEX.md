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
- [ ] step-067 — Admin anti-spam : *-antispam-rule

## M6 — Encodage/segmentation + gestion du débit
- [ ] step-080 — Poser le socle Redis + moteur de scripts Lua (EVALSHA atomique)
- [ ] step-081 — Détection d'encodage GSM-7/UCS-2/8-bit + calcul du nombre de segments
- [ ] step-082 — Découper les messages longs en segments UDH (étape pipeline)
- [ ] step-083 — Réassembler les MO concaténés (multipart)
- [ ] step-084 — Token-bucket Lua atomique + repo `rate_limits`
- [ ] step-085 — Brancher l'étape débit dans le pipeline + précédence des plafonds
- [ ] step-086 — Throttling adaptatif AIMD piloté par `ESME_RTHROTTLED`
- [ ] step-087 — Limite de débit dédiée pour `query_sm`
- [ ] step-088 — Fenêtrage du `submit_sm` entrant (traitement concurrent borné par session)

## M7 — Routage avancé
- [ ] step-100 — Repo `exact_routes` (numéros exacts, portabilité)
- [ ] step-101 — Bloom en mémoire + `exactroute:{msisdn}` Redis + court-circuit L0
- [ ] step-102 — Admin exact-routes : list / create / update / delete / lookup
- [ ] step-103 — Admin import-exact-routes (asynchrone)
- [ ] step-104 — Instantané de routage immuable + pointeur atomique
- [ ] step-105 — `cmd/config-sync` + pub/sub d'invalidation d'instantané
- [ ] step-106 — Hot reload des filtres de Bloom (exact + suppressions) sans downtime
- [ ] step-107 — Repo `routing_scripts` + cycle de statut (draft/active/disabled)
- [ ] step-108 — Runtime de script JS (goja) poolé, plafond d'instructions = garde primaire
- [ ] step-109 — Runtime de script Lua (gopher-lua) poolé, mêmes gardes
- [ ] step-110 — Contrat `resolveRoute`, résolution de scope, intégration pipeline (repli déclaratif)
- [ ] step-111 — Admin routing-scripts : CRUD + list-versions
- [ ] step-112 — Admin assign / validate / test / publish routing-script
- [ ] step-113 — Stratégies de distribution déterministes : round_robin, weighted, hash_based
- [ ] step-114 — Stratégies failover_priority, least_loaded + fallback_route
- [ ] step-115 — Invariant (b) : un message routé L0 traverse toute la conformité

## M8 — Résilience connecteurs  *(bascule au vrai simulateur SMSC)*
- [ ] step-120 — Basculer les tests d'intégration au vrai simulateur SMSC
- [ ] step-121 — Disjoncteur par connecteur : machine à états locale
- [ ] step-122 — Agrégation multi-pod du disjoncteur par majorité (Redis)
- [ ] step-123 — Le routeur lit `breaker:state` à la (re)construction de l'instantané
- [ ] step-124 — Pool de binds (`bind_pool_size > 1`) + partition par shard
- [ ] step-125 — `fallback_chain` en en-tête + reroute unilatéral
- [ ] step-126 — Draineur borné + `mt.reroute-park` (rafales de reroute)
- [ ] step-127 — Auto-reconnexion opt-in (backoff + jitter), `link_status` distinct du breaker
- [ ] step-128 — Admin connecteurs : rebind / status / reconnect-policy / bind-pool
- [ ] step-129 — Dead-letter (`mt.dead-letter` / `mo.dead-letter`) + retraitement
- [ ] step-130 — Dé-`Skip` des tests de résilience + scénarios d'injection de pannes

## M9 — Facturation opt-in
- [ ] step-140 — Poser le contrat gRPC billing + l'outillage protoc
- [ ] step-141 — Repos Postgres billing : balances, config, grand livre partitionné
- [ ] step-142 — Réserve/capture/libère MT en Lua atomique, idempotent par message_id
- [ ] step-143 — Compteur MO séparé : plancher, arrêt + alerte, jamais bloquant pour le MT
- [ ] step-144 — Câbler billing-svc (gRPC :7001) + port ops
- [ ] step-145 — Réserve MT dans le router (étape 8) ; désactivée = zéro appel réseau
- [ ] step-146 — Capture/libère dans connector-pool ; idempotent sous double livraison
- [ ] step-147 — Adaptateur de facturation externe (§6.10) derrière une interface
- [ ] step-148 — Admin billing : config client, soldes, top-up/transfert, change-balance-scope
- [ ] step-149 — Admin billing : grand livre, rate-plans, providers externes

## M10 — Contenu, chiffrement & RGPD
- [ ] step-160 — Interface KMS + implémentation locale de dev (enveloppe)
- [ ] step-161 — content_keys : cycle de vie de clé par client (hébergé par billing-svc)
- [ ] step-162 — Chiffrement du contenu à l'écriture CDR + politique content_storage
- [ ] step-163 — Lecture de contenu gardée et auditée (get-message-content)
- [ ] step-164 — Crypto-shred : destruction de clé + erase-customer-content
- [ ] step-165 — Rétention & tiering par drop de partition (§6.14)
- [ ] step-166 — Effacement RGPD (client + MSISDN) + attestation asynchrone

## M11 — Observabilité complète & temps réel
- [ ] step-180 — Catalogue de métriques à labels BORNÉS + test de garde de cardinalité
- [ ] step-181 — Spans complets par étape (pipeline.* / connector.*), 100 % sur erreur
- [ ] step-182 — Émettre les mises à jour temps réel vers metrics.stream
- [ ] step-183 — Gateway WS/SSE (coder/websocket) + stream-metrics
- [ ] step-184 — Flux stream-sessions + stream-billing-alerts
- [ ] step-185 — get-message-trace : trace complète d'un message, sans aucun corps
- [ ] step-186 — search-messages avec masquage MSISDN par rôle
- [ ] step-187 — Export de messages asynchrone (row-cap, MSISDN masqué)

## M12 — Durcissement, charge & mise en production
- [ ] step-200 — Harnais de charge k6/vegeta + générateur de binds SMPP (NFR)
- [ ] step-201 — Tuning de débit : partitions Kafka, batch ClickHouse, pool pgx
- [ ] step-202 — Chaos : perte Redis (chaque politique de panne) + flapping connecteur
- [ ] step-203 — Chaos : drain gracieux + PDB + binds préservés ; failover Postgres
- [ ] step-204 — Sécurité : gosec, govulncheck, secrets, piste d'audit
- [ ] step-205 — TLS / SMPP-TLS / mTLS sur les transports
- [ ] step-206 — Auth opérateur réelle (OIDC/mTLS) remplaçant le stub M1
- [ ] step-207 — Manifests deploy/ Kubernetes (Deployments, Services, HPA, PDB, probes)
- [ ] step-208 — Dérouler la checklist de mise en production (go-live)
