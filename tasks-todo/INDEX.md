# Index des steps — plan d'implémentation de la passerelle SMS

Dérivé de `docs/plan-execution-passerelle.md`. **Un fichier `step-NNN.md` = une PR** : petite,
reviewable, laisse le dépôt vert une fois mergée. Découpage par jalon (M0…M12) ; les jalons trop gros
sont éclatés en plusieurs PRs. **La numérotation croissante EST l'ordre d'exécution** : une step ne
dépend jamais que de numéros plus petits. Les jalons livrés sont numérotés par blocs de 20, le reste à
faire par dizaines — la marge d'insertion existe parce que six steps de ce dépôt sont nées d'une revue,
pas du plan.

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
> ℹ️ **Dette résorbée :** step-146 avait livré le règlement MT en fail-open en s'appuyant sur un « reaper »
> jamais implémenté. **step-190** l'a livré — le filet existe désormais.
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
- [x] step-186 — search-messages avec masquage MSISDN par rôle
- [x] step-187 — Export de messages asynchrone (row-cap, MSISDN masqué)

## Audit pré-production — correctifs issus de la revue du 2026-08-01
Trois de ces dettes étaient décrites **en commentaire dans le code** mais n'existaient dans aucune step :
un commentaire n'est pas un backlog. Les steps 190-192 corrigent des risques de production, 193-194 sont
structurelles et à faire **avant** que M12 n'empile dessus.
- [x] step-190 — Reaper de réservations orphelines (le filet manquant du fail-open de step-146)
- [x] step-191 — PROXY protocol sur le listener SMPP (sinon throttle de bind global derrière un LB L4)
- [x] step-192 — Topic `webhook.retry` différé (sortir les retries du chemin chaud)
- [x] step-193 — Câblage de router-svc / connector-pool-svc en constructeurs testables
- [x] step-193b — Même patron pour mo-dlr-router-svc, admin-api-svc, smpp-server-svc
- [x] step-193c — Les cinq mains restées hors du patron — le premier test de câblage a trouvé que
  `billing-svc` **paniquait au boot** depuis step-190 (label `action` hors vocabulaire borné)
- [x] step-193d — `billing-svc` lit `cfg.Billing.*` sans déclarer `SectionBilling` : personne ne la valide.
  Les champs sont partis dans `SectionBillingReaper` (la section client ne validait aucun d'eux), et une
  garde remplace la relecture — elle a trouvé `smpp-server-svc` dans le même cas
- [x] step-193e — l'ordre de fermeture est une propriété du câblage que le test synthétique ne gardait pas
  (chez `admin-api-svc` il est load-bearing, et l'inverser laissait toute la suite verte). Les 28 closers
  des dix services sont nommés, et l'assertion s'observe en exécutant `close()` — la relire à l'envers
  aurait rejoué sa boucle des deux côtés
- [x] step-194 — Découper `connectorpool.go` (extraction du mapping SMPP/CDR)

## M12 — Durcissement, charge & mise en production
- [x] step-200 — Harnais de charge k6/vegeta + générateur de binds SMPP (NFR)
- [x] step-201 — Tuning de débit : partitions Kafka, batch ClickHouse, pool pgx (+ instruments de mesure)
- [x] step-201c — Le CDR sortant devient une projection (goulot du connector pool : 192 → 892 submit_sm/s)
- [x] step-201d — Le routeur est le goulot suivant du débit traversant (mesuré après step-201c)
- [x] step-201e — Attribuer le plafond : c'était la co-résidence, ni le routeur ni le broker
- [x] step-201f — Isoler le pool de connecteurs : c'était l'hôte ; l'écriture DLR coûte 37 % du débit
- [x] step-209 — `cancelled` ne doit plus enterrer un message réellement livré (course élargie par step-201c)
- [x] step-230 — Les gardes du banc `loadref` refusent enfin — et le banc du routeur publiait son préchauffage
- [x] step-240 — Le rejeu d'un dead-letter ne remet plus sur le fil un message annulé

Ce qui restait de M12 — chaos, sécurité, transports, manifests, campagne NFR, go-live — a été renuméroté
et vit dans **Reste à faire**, plus bas, à sa place dans l'ordre d'exécution.

## Audit post-M11 — dettes du flux temps réel (revue du 2026-08-07)
Deux dettes laissées par step-184, de nature différente : un compteur construit que personne ne lit, et une
promesse de contrat dont la source durable n'existe pas.
- [x] step-210 — Les rejets du flux temps réel redeviennent visibles (le plafond tronquait en silence)

La seconde — `billing.events` durable — est devenue **step-400**.

---

# Reste à faire — dans l'ordre où elles doivent être écrites

⛓ marque une position **forcée** par une dépendance. Les autres sont un choix d'ordonnancement : elles ne
bloquent rien et peuvent bouger sans rien casser.

## Débloquer la mesure, puis le déploiement
Une seule chaîne, et elle commande tout le reste de M12 : le banc devait refuser ce qu'il nomme avant que
la campagne ne publie un chiffre — c'est fait (step-230, livrée) — et les manifests doivent exister avant
qu'on mesure un environnement représentatif.
- [ ] step-245 — Un jeton d'annulation gagné sans sa ligne CDR laisse un message rejouable (résiduel de step-240)
- [ ] step-250 — Chaos : perte Redis (chaque politique de panne) + flapping connecteur
- [ ] step-260 — Chaos : drain gracieux + PDB + binds préservés ; failover Postgres ⛓ step-250
- [ ] step-270 — Manifests deploy/ Kubernetes (Deployments, Services, HPA, PDB, probes) ⛓ step-260
- [ ] step-280 — Campagne NFR pleine échelle sur environnement représentatif ⛓ step-230, step-270

step-245 ne bloque rien et pourrait aller n'importe où ; elle est placée ici parce qu'elle est ce qui
reste du seul **défaut de correction** du lot. step-240 a fermé le rejeu d'un message annulé ; il subsiste
le cas où l'annulation a gagné son jeton sans jamais écrire sa ligne CDR, que la garde du rejeu ne peut
donc pas voir. Son numéro n'est pas un multiple de dix parce qu'aucun n'était libre à sa place dans
l'ordre — la suite doit venir après son parent et avant step-250.

## Sécurité et authentification
Indépendantes de la chaîne de charge : parallélisables si deux mains travaillent.
- [ ] step-290 — Sécurité : gosec, govulncheck, secrets, piste d'audit
- [ ] step-300 — TLS / SMPP-TLS / mTLS sur les transports
- [ ] step-310 — Auth opérateur réelle (OIDC/mTLS) remplaçant le stub M1 ⛓ step-300

## Écart contrat ↔ implémentation (revue du 2026-08-10)
`api/openapi-admin.yaml` déclare **133 opérations** sous `paths:` ; `internal/adminapi` en enregistre
**103**. Les **30** restantes sont décrites par la spec (§6.16, §6.17, §6.22, §6.23) et attendues par le
tableau de bord — la plupart ont même leur table en base — mais **aucun jalon ne les portait**. Aucune
garde ne voyait l'écart : les tests de contrat vont tous dans le sens *implémenté → déclaré*, jamais
l'inverse. step-320 pose la garde et le triage ; les sept suivantes construisent les surfaces, dans
l'ordre qu'on voudra sauf là où une dépendance le fixe.
- [ ] step-320 — La garde contrat ↔ implémentation, et le triage des 30 ⛓ bloque step-410 et step-330→390
- [ ] step-330 — Groupes de clients (§6.17) : la table existe, rien ne la remplit ⛓ step-320
- [ ] step-340 — Webhooks : le repo est livré depuis M4, l'admin n'a jamais été écrite ⛓ step-320
- [ ] step-350 — Réécriture de sender ID (§6.16) : ni l'admin, ni l'évaluation dans le pool ⛓ step-320 ;
      **sa PR2 doit merger après step-280**, sinon elle ajoute un étage au chemin d'envoi entre la
      caractérisation du pool et la campagne, et périme le dimensionnement sans que personne ne le voie
- [ ] step-360 — Sessions SMPP : le flux temps réel existe, la lecture REST non ⛓ step-320
- [ ] step-370 — Politiques de contenu (§6.23) : la plateforme n'a pas de défaut configurable ⛓ step-320
- [ ] step-380 — Métriques agrégées en lecture : le flux pousse, rien ne se lit ⛓ step-320, step-330
- [ ] step-390 — Réglages de compte créables mais non modifiables, et trois opérations orphelines ⛓ step-320

## La dette du tableau de bord, puis la porte
- [ ] step-400 — `billing.events` durable : le BFF doit pouvoir détecter, pas seulement afficher
- [ ] step-410 — **GO-LIVE** : dérouler la checklist de mise en production
      ⛓ step-260, step-270, step-280, step-290, step-310, step-320

---

## Correspondance des numéros — renumérotation du 2026-08-27

Les fiches **livrées** (`tasks-done/`), les ADR, le glossaire et `test/load/README.md` ont été écrits
quand les anciens numéros étaient les bons. `tasks-done/step-201f.md` dit « reporté à step-201b », et il
l'a dit ce jour-là : réécrire un document daté lui fait affirmer ce qu'il n'a jamais affirmé. Ils gardent
donc leurs mots, et cette table est ce qui les rend lisibles.

Ce qui **a** été réécrit : les fiches encore ouvertes, les commentaires Go qui renvoient à une step à
venir, `.goreleaser.yaml` et le glossaire — tous des documents vivants, qui décrivent un futur, pas un
passé. La `git history` n'est pas concernée : aucune step renumérotée n'avait de commit.

| Cité dans l'historique | Fiche aujourd'hui | |
|---|---|---|
| step-201b | **step-280** | campagne NFR |
| step-201g | **step-230** | gardes du banc `loadref` |
| step-202 | **step-250** | chaos Redis |
| step-203 | **step-260** | chaos drain / failover |
| step-204 | **step-290** | sécurité |
| step-205 | **step-300** | TLS / mTLS |
| step-206 | **step-310** | auth OIDC |
| step-207 | **step-270** | manifests Kubernetes |
| step-208 | **step-410** | go-live |
| step-211 | **step-400** | `billing.events` durable |
| step-212 | **step-240** | rejeu d'un annulé |
| step-213 | **step-320** | garde contrat ↔ implémentation |
| step-214 | **step-330** | groupes de clients |
| step-215 | **step-340** | webhooks admin |
| step-216 | **step-350** | réécriture de sender ID |
| step-217 | **step-360** | sessions REST |
| step-218 | **step-370** | politiques de contenu |
| step-219 | **step-380** | métriques en lecture |
| step-220 | **step-390** | réglages de compte |
