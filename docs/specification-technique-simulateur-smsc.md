# Simulateur SMSC — Spécification Technique
**Modèle :** RESHADED (Requirements → Estimation → Storage Schema → High-Level Design → API Design → Detailed Design → Evaluation → Distinctive Component)
**Composant :** Simulateur SMSC configurable (Go) — outil de test/CI
**Documents compagnons :** `specification-technique-passerelle-sms.md` (système sous test), `specification-technique-tableau-de-bord.md`
**Statut :** v2.0

*Note de convention : les blocs de code (schémas, endpoints API, diagrammes, JSON, y compris leurs commentaires) restent en anglais. Seul le texte narratif est en français.*

---

## 1. Exigences (Requirements)

### 1.1 Exigences fonctionnelles

- **Émulation de serveur SMPP** — accepte les binds SMPP (`bind_transmitter`/`bind_receiver`/`bind_transceiver`) exactement comme un vrai SMSC opérateur, pour que `connector-pool-svc` de la passerelle (§4.1 compagnon) s'y connecte comme à un vrai connecteur, sans changement de code côté passerelle.
- **Plusieurs SMSC virtuels simultanés** — un seul processus héberge plusieurs SMSC virtuels configurés indépendamment (ports, identifiants, TON/NPI, TLS, profils de comportement), pour exercer plusieurs connecteurs (routes multi-connecteurs, distribution, bascule) contre un seul simulateur.
- **Résultats de `submit_sm` configurables** — par SMSC virtuel (ou par règle) : mélange pondéré de succès, code d'erreur SMPP spécifique (`ESME_RTHROTTLED`, `ESME_RSUBMITFAIL`, `ESME_RINVDSTADR`…), timeout, ou coupure de connexion en cours de transaction.
- **Injection de latence configurable** — par règle, une distribution (fixe, plage uniforme, normale, ou rafales périodiques « spike ») appliquée avant de répondre.
- **Génération de DLR configurable** — `deliver_sm` DLR asynchrones avec distribution de délai et mélange de résultats (livré/échec/expiré), corrélés au `submit_sm` d'origine.
- **Injection de messages MO** — `deliver_sm` non sollicités, déclenchés à la demande via l'API de contrôle (tests déterministes) ou auto-planifiés à un débit configuré (tests de charge/endurance du chemin MO).
- **Simulation d'instabilité de connexion** — coupures de bind aléatoires configurables (probabilité/intervalle) pour exercer le disjoncteur (§6.15 compagnon) et la reconnexion automatique (§6.13 compagnon).
- **Simulation de plafond de débit** — un SMSC virtuel applique son propre plafond, retournant `ESME_RTHROTTLED` au-delà, pour exercer le throttling adaptatif de la passerelle (§6.4 compagnon).
- **Profils de scénario** — préréglages nommés interchangeables à chaud (« healthy », « flaky-carrier », « throttling-carrier », « dead-carrier », « slow-carrier ») appliqués via l'API de contrôle sans redémarrage.
- **Mode déterministe/à graine** — une graine PRNG fixe par SMSC virtuel produit toujours la même séquence de résultats et de latences, pour des assertions CI reproductibles ; un mode « chaos » sans graine est disponible pour les tests exploratoires. Le déterminisme du mode à graine s'appuie sur un compteur logique de PDU par session de bind, jamais sur l'horloge murale (§6.3).
- **Enregistrement de PDU** — journal borné et interrogeable des `submit_sm` reçus par SMSC virtuel, pour vérifier ce que la passerelle a réellement envoyé (adresses, contenu, TON/NPI, codage).
- **Support TLS** — optionnel par SMSC virtuel, avec génération intégrée de certificat auto-signé, reflétant `tls_enabled` du connecteur (§3.1 compagnon).
- **Injection de cas limites protocolaires** — opt-in, PDU malformées activées par scénario (longueur invalide, `command_id` invalide, numéros de séquence hors ordre) pour tester la robustesse du parsing — désactivé par défaut.
- **Export de métriques** — métriques Prometheus par SMSC virtuel (binds, `submit_sm` reçus, résultats servis, scénario) pour vérifier le trafic observé côté simulateur.
- **Intégration CI/CD** — une image Docker unique, un `docker-compose.yml` d'exemple câblant le simulateur comme connecteur(s) SMSC, et un modèle de Job Kubernetes pour les pipelines.

### 1.2 Exigences non fonctionnelles

| Catégorie | Cible |
|---|---|
| Débit par instance | Chaque SMSC virtuel soutient au moins la part de trafic de pic par connecteur (jusqu'à 15 000+ msg/s pour les tests de charge dédiés) |
| Temps de démarrage | < 2 s à froid, pour un démarrage/arrêt peu coûteux par exécution de test |
| Empreinte | Binaire Go statique unique ; < 50 Mo de mémoire de base par SMSC virtuel au repos |
| Déterminisme (séquence/contenu) | Reproductible **par session de bind** pour une graine et une séquence d'entrée données (codes d'erreur, choix de DLR, contenu MO). L'ordre global entre binds concurrents n'est pas garanti (§6.3) |
| Déterminisme (timing) | Reproductible uniquement pour les mécanismes exprimés en ticks logiques ; les mécanismes en horloge murale (mode chaos) ne prétendent qu'à la reproductibilité de séquence/contenu |
| Progression des planifications au repos | Les planifications en ticks logiques (DLR, MO auto, `spike`) ne se figent pas quand le trafic cesse : un flush de quiescence les draine dans l'ordre de tick (§6.3) |
| Isolation | Aucune dépendance envers l'infrastructure de la passerelle (Kafka, Postgres, Redis, ClickHouse) |
| Concurrence | Un processus héberge confortablement 10–20+ SMSC virtuels pour les topologies multi-connecteurs |

### 1.3 Contraintes

- Langage : Go — correspond à la passerelle (§1.3 compagnon), permettant de partager le codec de PDU SMPP et les patterns de connexion.
- En mémoire par défaut : aucun magasin externe. Définitions de scénario et journaux de PDU en mémoire processus, bornés, non censés survivre à un redémarrage.
- Pas un composant de production : outil de test/CI uniquement, jamais déployé aux côtés de la passerelle de production.

---

## 2. Estimation

- **Échelle de test de charge** : dimensionné pour égaler ou dépasser la cible de pic de la passerelle (15 000 msg/s) par SMSC virtuel.
- **Échelle CI/fonctionnel typique** : dizaines à quelques centaines de msg/s par test (la plupart valident la correction, pas le débit brut).
- **Multi-instance** : une topologie de test réaliste exécute 3–10 SMSC virtuels simultanément.
- **Enregistrement PDU** : tampon circulaire par défaut ~10 000 PDU par SMSC virtuel (`pdu_buffer_size`). À 15 000+ msg/s il boucle en moins d'une seconde — suffisant pour des assertions ciblées à faible volume, à augmenter pour inspecter un test de charge complet.

---

## 3. Schéma de stockage (Storage Schema)

Volontairement minimal — outil sans état entre exécutions.

### 3.1 Configuration d'exécution (en mémoire, chargée depuis un fichier de config et/ou l'API de contrôle)

```
virtual_smsc
  id (uuidv7)
  name, port
  bind_credentials     (system_id, password)
  addr_ton, addr_npi, address_range
  tls_enabled (bool), tls_config       -- self-signed cert auto-generated if enabled and none supplied
  active_scenario_id (fk -> scenario)
  throughput_limit_per_sec (nullable)
  seed (nullable)                       -- null = unseeded/chaos mode; set = deterministic mode
  pdu_buffer_size (default 10000)       -- ring buffer capacity; raise per-instance for full load-test PDU inspection

scenario                               -- a named, reusable behavior bundle
  id (uuidv7)
  name                                  (e.g. "healthy", "flaky-carrier", "throttling-carrier", "dead-carrier")
  response_rules_json                   -- ordered {match:{sourcePattern?, destPattern?, contentPattern?},
                                          outcomes:[{type:success|error|timeout|disconnect, errorCode?, weight}],
                                          latency:{distribution:fixed|uniform|normal|spike, params}}
  dlr_config_json                       -- {delayDistribution, outcomeWeights:{delivered, failed, expired}, clock:logical|wallclock}
                                          -- in seeded mode the DLR delay is anchored to the ORIGIN submit_sm's
                                          -- per-bind tick; the quiescence flush drains pending DLRs when traffic pauses
  quiescence_flush_ms (default 250)      -- after this idle gap with no new submit_sm on a bind, pending logical-tick
                                          -- DLR/MO/spike events for that bind are flushed in deterministic tick order
  mo_injection_config_json              -- {mode:manual|auto, ratePerSec?, contentTemplate?, clock:logical|wallclock}
                                          -- in seeded mode 'logical' is enforced (ticks on the Nth processed submit_sm);
                                          -- 'wallclock' only when seed is null (chaos)
  flakiness_config_json                 -- {disconnectProbability, checkIntervalSec, clock:logical|wallclock}
  protocol_edge_cases_enabled (bool, default false)   -- opt-in malformed-PDU injection
```

### 3.2 État d'exécution (en mémoire, borné, par SMSC virtuel)

```
received_pdus   -- ring buffer (size = pdu_buffer_size) of recent submit_sm PDUs, queryable for assertions
active_binds    -- current bind sessions (account/system_id, bind type, connected_at)
metrics         -- Prometheus counters/histograms (binds, submit_sm received/outcome, current scenario, latency)
logical_clock   -- per virtual SMSC monotonic counter of processed submit_sm; a GLOBAL observation/assertion
                   reference (exposed via GET /logical-clock). NOT the deterministic timing reference (its increment
                   order across concurrent binds is non-reproducible) — see per_bind_clock
per_bind_clock  -- per (virtual SMSC, bind session) monotonic counter of submit_sm on THAT bind; the deterministic
                   timing reference for clock=logical mechanisms. Determinism of sequence/content is guaranteed per
                   bind; global cross-bind order is not (pin bind_pool_size=1 for globally-ordered assertions)
pending_logical_schedule  -- per-bind ordered set of DLR/MO/spike events due at a future tick; drained when the bind's
                   per_bind_clock reaches the due tick, or by the quiescence flush when the bind goes idle
```

Aucune base persistante ; pour conserver des définitions de scénario across redémarrages, on les versionne dans le fichier de config.

---

## 4. Conception de haut niveau (High-Level Design)

```
+---------------------------------------------------------------------------+
|                     smsc-simulator (single Go binary)                     |
|  +------------------------+   +------------------------+                  |
|  | Virtual SMSC #1         |   | Virtual SMSC #2 ...N    |  <- one SMPP    |
|  | (SMPP Server Engine)    |   | (SMPP Server Engine)    |     listener    |
|  |  - bind handling         |   |  - bind handling         |     per port   |
|  |  - Scenario Engine        |   |  - Scenario Engine        |             |
|  |  - Fault Injector          |   |  - Fault Injector          |           |
|  |  - DLR Scheduler             |   |  - DLR Scheduler             |       |
|  |  - MO Injector                 |   |  - MO Injector                 |   |
|  |  - per_bind_clock                |   |  - per_bind_clock                | |
|  |  - PDU Recorder (ring buffer)      |   |  - PDU Recorder (ring buffer)      | |
|  +------------+-------------+   +------------+-------------+              |
|               |                              |                            |
|  +------------v------------------------------v-------------+             |
|  |              Control API (HTTP, single port)              |             |
|  |  scenario CRUD/assign - PDU inspection/reset - MO inject  |             |
|  |  force-disconnect - health - Prometheus /metrics          |             |
|  +-------------------------------------------------------------+           |
+-----------------------------------------------------------------------------+
                     ^ SMPP binds (one per virtual SMSC port)
        +------------+--------------+
        |   Gateway under test        |  (connector-pool-svc treats each virtual SMSC as a real smsc_connector)
        +-------------------------------+
```

### 4.1 Composants

1. **SMPP Server Engine** (un par SMSC virtuel) — accepte les connexions TCP sur son port, gère bind, `enquire_link`, `submit_sm`/`submit_sm_resp`, `deliver_sm`, `unbind`, réutilisant le codec de PDU partagé avec la passerelle.
2. **Scenario Engine** — à chaque `submit_sm`, met en correspondance avec les `response_rules` du scénario actif, sélectionne un résultat pondéré, le transmet au Fault Injector. Incrémente à chaque PDU le `per_bind_clock` de la session (référence de timing déterministe) et le `logical_clock` global (observable d'assertion uniquement).
3. **Fault Injector** — applique la distribution de latence et, pour `disconnect`, coupe la connexion TCP en cours de transaction (avant/après réponse partielle, configurable).
4. **DLR Scheduler** — pour les messages « soumis » avec succès, planifie un `deliver_sm` DLR asynchrone. En mode déterministe, le délai est ancré au tick du `submit_sm` d'origine (`per_bind_clock`) ; les DLR en attente sont drainés à l'atteinte du tick dû ou par le flush de quiescence quand le bind cesse de recevoir du trafic.
5. **MO Injector** — envoie des `deliver_sm` non sollicités à la demande ou sur minuteur auto-planifié — piloté par `per_bind_clock` en mode déterministe, par horloge murale uniquement en mode chaos ; soumis au flush de quiescence.
6. **PDU Recorder** — ajoute chaque PDU reçue au tampon circulaire borné, exposé via l'API de contrôle.
7. **Control API** — surface HTTP unique pour configurer les SMSC virtuels, changer de scénario, inspecter les PDU, déclencher l'injection MO, forcer des déconnexions, scraper les métriques.

---

## 5. Conception de l'API (API Design)

### 5.1 Control API — `http://localhost:<control-port>/v1`

```
GET     /health

# Virtual SMSC management
GET     /virtual-smscs
POST    /virtual-smscs                          # create at runtime (port, credentials, scenario)
PATCH   /virtual-smscs/{id}                      # update config (throughput_limit_per_sec, pdu_buffer_size)
DELETE  /virtual-smscs/{id}                      # tear down (closes listener and binds)
PATCH   /virtual-smscs/{id}/scenario             # hot-swap active scenario
POST    /virtual-smscs/{id}/disconnect-all       # force-drop all active binds

# PDU inspection
GET     /virtual-smscs/{id}/received-pdus?sourceAddr=&destAddr=&since=   # paginated
DELETE  /virtual-smscs/{id}/received-pdus                                 # reset between test runs
GET     /virtual-smscs/{id}/binds
GET     /virtual-smscs/{id}/logical-clock                                 # current global tick count

# MO injection
POST    /virtual-smscs/{id}/inject-mo            # { sourceAddr, destAddr, content } — sends one deliver_sm now

# Scenarios
GET     /scenarios                               # built-in + custom
POST    /scenarios
GET     /scenarios/{id}

# Observability
GET     /metrics                                 # Prometheus exposition format
```

### 5.2 Interface SMPP (par port de SMSC virtuel)

Comportement serveur SMPP v3.4 standard (v5.0 optionnel) — `bind_*`, `submit_sm`, `deliver_sm` (MO + DLR), `enquire_link`, `unbind` — surface identique à ce que `connector-pool-svc` attend d'un vrai SMSC (§5.1 compagnon), plus les modes opt-in de PDU malformées activés par `protocol_edge_cases_enabled`.

---

## 6. Conception détaillée (Detailed Design)

### 6.1 Moteur de scénario & profils intégrés

Les règles de réponse sont évaluées comme les règles de routage de la passerelle (correspondance source/dest/contenu, première correspondance gagnante, repli par scénario). Profils intégrés :

| Profil | Comportement | Ce qu'il exerce dans la passerelle |
|---|---|---|
| `healthy` | 100 % succès, latence fixe basse | Chemin nominal/référence |
| `flaky-carrier` | ~80 % succès, ~20 % erreurs/timeouts, déconnexions périodiques | Disjoncteur (§6.15), retry/dead-letter (§6.7) |
| `throttling-carrier` | `ESME_RTHROTTLED` au-delà d'un débit | Throttling adaptatif (§6.4) |
| `dead-carrier` | Refuse les binds, ou fait timeout sur chaque `submit_sm` | Disjoncteur `open` (§6.15), repli de routage (§6.1), auto-reconnexion (§6.13) |
| `slow-carrier` | Latence haute bornée (2–4 s), aucune erreur | `response_timeout_ms`/`window_size`, durée de span (§6.11 compagnon) |
| `throughput-capped` | Applique son propre plafond, throttle au-delà | Boucle de throttling adaptatif bout-en-bout |

Scénarios interchangeables à chaud en cours de test.

### 6.2 Mécanique d'injection de panne

- **Latence** : `fixed`, `uniform`, `normal` (borné à non-négatif), `spike` (référence basse avec rafales périodiques). En mode déterministe, l'intervalle `spike` est exprimé en ticks (`per_bind_clock`) ; en mode chaos, en durée réelle.
- **Timeouts** : le simulateur retient `submit_sm_resp` au-delà du `response_timeout_ms` attendu — le timeout propre de la passerelle se déclenche naturellement.
- **Déconnexions** : configurable avant de répondre (connexion coupée, aucune réponse) ou après une fraction de messages (coupure en cours de session).

### 6.3 Déterminisme & modes chaos

Le déterminisme du mode à graine s'appuie sur un compteur logique de PDU, jamais sur l'horloge murale (une panne périodique pilotée par l'horloge murale ne peut pas être reproductible d'une exécution à l'autre : gigue CI, pauses GC, ordonnancement réseau).

- **Horloge par bind (`per_bind_clock`)** : la référence de timing déterministe est un compteur **par session de bind**. Un DLR est planifié « M ticks après le `submit_sm` d'origine, sur le bind d'origine » ; `spike` et MO auto sur le tick du bind concerné. Au sein d'un bind (flux TCP ordonné), séquence et contenu sont reproductibles.
- **Portée de la garantie** : la reproductibilité est **par bind**, pas globalement — la passerelle scale un connecteur avec `bind_pool_size` binds parallèles (§6.8 compagnon), et l'ordre d'entrelacement entre binds concurrents dépend de l'ordonnancement, non reproductible. Une assertion à ordre global doit épingler `bind_pool_size = 1`. La plupart des tests fonctionnels/CI (comportement précis à faible volume) sont dans ce cas ; les tests de charge multi-bind conservent le déterminisme par bind et l'agrégation statistique.
- **Compteur global (`logical_clock`)** : exposé comme observable d'assertion (`GET /logical-clock`), jamais comme référence de planification.
- **Flush de quiescence** : une planification en ticks n'avance qu'avec le trafic entrant. Dans le cas CI majoritaire (soumettre un lot puis attendre les DLR), le trafic cesse et le compteur se figerait. Chaque bind tient un `pending_logical_schedule` drainé (a) à l'atteinte du tick en fonctionnement normal, ou (b) par un flush après `quiescence_flush_ms` (défaut 250 ms) sans nouveau `submit_sm`, dans l'ordre de tick déterministe. Le déterminisme de séquence/contenu est préservé ; seule la latence murale absolue d'un DLR au repos n'est pas garantie — sans conséquence pour une assertion de résultat.
- **Mode chaos (sans graine)** : `clock=wallclock` autorisé et par défaut pour les mécanismes périodiques ; aucune prétention de reproductibilité.

### 6.4 Intégration CI/CD

- **Image Docker** : binaire unique, base minimale, config via fichier monté ou variables d'environnement.
- **docker-compose** : le simulateur câblé comme une ou plusieurs entrées `smsc_connectors` de la passerelle pointant vers ses ports — indistinguable d'une vraie connexion opérateur.
- **Test de charge** : associer un scénario `throughput-capped`/`healthy` à fort débit avec l'outillage de génération de charge de la passerelle pour valider débit/latence bout-en-bout. Augmenter `pdu_buffer_size` si l'inspection de PDU sur toute la durée importe.
- **Test de résilience** : associer `dead-carrier`/`flaky-carrier` avec des assertions contre l'API Admin de la passerelle (statut connecteur, disjoncteur) et les endpoints `/received-pdus`/`/binds` du simulateur pour vérifier que les comportements de résilience documentés (§6.13/§6.15/§6.1 compagnon) se produisent réellement.

---

## 7. Évaluation (Evaluation)

| Décision | Compromis |
|---|---|
| Simulateur sur mesure vs simulateur SMPP open-source | Les fonctionnalités de résilience spécifiques de cette passerelle (disjoncteur, auto-reconnexion, throttling adaptatif) valent un outil sur mesure avec profils nommés, API scriptable et Go natif (outillage partagé), face à l'adaptation d'un outil générique. |
| État uniquement en mémoire vs stockage persistant | Correspond à la nature éphémère des exécutions CI et garde l'outil sans dépendance ; les définitions de scénario sont refournies à chaque exécution (fixtures versionnées). |
| Un processus hébergeant plusieurs SMSC virtuels | Empreinte plus légère, orchestration plus simple ; un crash fait tomber tous les SMSC virtuels du processus — non-problème pour un outil de test. |
| Mode déterministe à graine comme principal, chaos en secondaire | La reproductibilité a plus de valeur pour le cas majoritaire (vérifier un comportement précis en CI) ; le chaos reste pour le test exploratoire sans rendre la plupart des tests instables. |
| Horloge de timing **par bind** (`per_bind_clock`) plutôt que le compteur global | Le pool de binds multiple de la passerelle rend l'ordre global entre binds non reproductible ; le compteur par bind restaure un déterminisme réel, au prix d'une garantie scopée « par bind » (`bind_pool_size = 1` requis pour un ordre global). |
| Flush de quiescence pour les planifications logiques | Sans lui, un test à faible volume se fige sur un compteur gelé ; le flush draine les planifications en attente dans l'ordre de tick, au prix d'abandonner la garantie de latence murale absolue d'un DLR au repos. |

**Ce qu'on revisiterait :** sharder la gestion de connexion d'un SMSC virtuel sur plusieurs pools de goroutines, ou des instances dédiées par shard de test de charge, si le débit par instance devient un goulot ; un mode de fuzzing programmatique si la couverture de cas limites protocolaires devient prioritaire ; un mode d'échantillonnage du PDU recorder si `pdu_buffer_size` étendu pousse la mémoire au-delà de la cible.

---

## 8. Composant distinctif (Distinctive Component)

**Moteur d'injection de panne orienté résilience, avec garanties de déterminisme honnêtes.**

Un simulateur SMPP générique répond aux binds et fait écho aux `submit_sm_resp`. Celui-ci est construit autour de l'affirmation que la passerelle fait sur elle-même : résilience face aux pannes opérateur via disjoncteur, auto-reconnexion, throttling adaptatif et repli de routage. Les profils nommés (`dead-carrier`, `flaky-carrier`, `throttling-carrier`, `slow-carrier`) déclenchent chacun de ces mécanismes à la demande et en combinaison, avec une API de contrôle qui permet à un test de vérifier non seulement « la passerelle a continué » mais « le disjoncteur s'est ouvert dans les N secondes et le trafic s'est réorienté ».

Le second trait distinctif est une **séparation honnête entre déterminisme de séquence/contenu et déterminisme de timing** : plutôt que de promettre une reproductibilité que les mécanismes pilotés par l'horloge murale ne peuvent tenir, le simulateur ancre le déterminisme **par session de bind** (`per_bind_clock`) — parce que le pool de binds multiple de la passerelle rend l'ordre global non reproductible — et draine les planifications logiques par un **flush de quiescence** quand le trafic cesse. Une assertion CI construite sur ce simulateur repose ainsi sur une garantie réellement vraie, y compris sous la topologie multi-bind et en période de silence.