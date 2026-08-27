# Glossaire du domaine — Passerelle SMS

Vocabulaire SMPP / SMS et termes propres au projet. À lire une fois ; sert de référence quand un terme du code ou d'une spec n'est pas clair. Les termes sont regroupés par thème. Les identifiants entre backticks renvoient à des colonnes du DDL ou des champs des specs OpenAPI.

## Protocole SMPP

**SMPP** (Short Message Peer-to-Peer) — protocole binaire sur TCP entre un client de messagerie (ESME) et un centre SMSC. Versions v3.4 (par défaut ici) et v5.0. La passerelle est **serveur** SMPP côté clients et **client** SMPP côté SMSC.

**ESME** (External Short Messaging Entity) — l'entité cliente qui se connecte à un SMSC pour envoyer/recevoir des SMS. Nos clients sont des ESME vis-à-vis de la passerelle ; la passerelle est un ESME vis-à-vis des SMSC opérateurs.

**SMSC** (Short Message Service Center) — l'élément réseau de l'opérateur qui relaie les SMS vers/depuis les téléphones. Côté sortant, chaque `smsc_connectors` représente un lien vers un SMSC.

**Bind** — l'ouverture de session SMPP authentifiée sur une connexion TCP. Trois types : **TX** (`bind_transmitter`, envoi seul), **RX** (`bind_receiver`, réception seule), **TRX** (`bind_transceiver`, les deux). Le champ `allowed_bind_types`/`bind_type` contrôle ce qui est permis.

**PDU** (Protocol Data Unit) — un message SMPP encodé (en-tête + corps). Le codec vit dans `internal/smpp`. Les principales PDU utilisées :

- **`submit_sm`** — soumission d'un SMS MT par un ESME.
- **`submit_sm_resp`** — réponse, portant le `command_status` (succès ou erreur) et l'ID de message SMSC.
- **`deliver_sm`** — sert **deux** usages : livrer un MO entrant, **ou** livrer un DLR (accusé de remise). Le flag `esm_class` distingue les deux.
- **`enquire_link`** — keep-alive : ping périodique pour vérifier qu'un bind est vivant.
- **`unbind`** — fermeture gracieuse d'une session.
- **`query_sm`** / **`cancel_sm`** — interroger/annuler un message (optionnels, désactivables par compte).
- Non supportés ici : `replace_sm`, `data_sm`.

**`command_status`** — le code de résultat d'une PDU réponse (ex. `ESME_ROK`, `ESME_RTHROTTLED`, `ESME_RINVSRCADR`). Mappé 1:1 avec le `code` d'erreur métier (guide d'ingénierie §11.3).

**Fenêtre (window)** — nombre maximum de `submit_sm` en vol non acquittés sur un bind (`window_size`). Contrôle de flux au niveau protocole, distinct du token-bucket métier.

**TON / NPI** (Type Of Number / Numbering Plan Indicator) — deux petits entiers qui qualifient une adresse SMPP : le TON dit *quelle sorte* de numéro (international, national, alphanumérique…), le NPI *quel plan de numérotation* (E.164, télex…). Présents sur le connecteur (`source_addr_ton`, `dest_addr_npi`, etc.).

**`data_coding`** — l'octet SMPP qui indique l'encodage du corps (GSM-7, UCS-2, 8-bit). Surchargalable par connecteur (`data_coding_default`).

**TLV** (Tag-Length-Value) — champs optionnels en fin de PDU (SMPP v3.4+), utilisés notamment pour l'UDH et les payloads > 254 octets.

## SMS et contenu

**MT** (Mobile Terminated) — un SMS **vers** un téléphone : le message qu'un client envoie. C'est le sens principal, facturé sur le solde MT.

**MO** (Mobile Originated) — un SMS **depuis** un téléphone : une réponse d'un abonné, arrivant d'un SMSC. Routé vers le bon compte via le numéro entrant/mot-clé. Facturé sur un compteur MO distinct.

**DLR** (Delivery Receipt) — accusé de remise renvoyé par le SMSC pour un MT, corrélé au message d'origine par `message_id`. Met à jour le CDR (`delivered_at`, `status`).

**Segment** — un SMS a une taille max (160 caractères en GSM-7, 70 en UCS-2). Un message plus long est découpé en **segments** concaténés réassemblés sur le téléphone. Le coût se calcule **par segment** : `credits = segment_count × credits_per_segment`.

**UDH** (User Data Header) — en-tête inséré dans le corps pour indiquer qu'un segment fait partie d'un message concaténé (numéro de référence, position). La segmentation/réassemblage vit dans `internal/…/encoding`.

**GSM-7 / UCS-2 / 8-bit** — les trois encodages. GSM-7 (alphabet SMS standard, 7 bits) ; UCS-2 (Unicode, pour accents/emoji, réduit la capacité à 70 car.) ; 8-bit (binaire, data SMS).

**`message_id` / `trace_id`** — identifiants UUIDv7 générés **à l'ingestion** (avant Kafka). `message_id` identifie le message (exposé au client) ; `trace_id` corrèle toute la trace OpenTelemetry. **ID de message logique** : tous les segments UDH d'un message concaténé le partagent, ce qui les garde sur le même bind et dans l'ordre.

**MSISDN** — le numéro de téléphone au format international (ex. `+2250700000000`). Normalisé **E.164** à l'ingestion. C'est une donnée personnelle (RGPD).

**E.164** — le standard international de numérotation (préfixe `+`, indicatif pays, numéro). La normalisation E.164 est la toute première étape du pipeline — sans elle, dédup, opt-out et numéro exact seraient contournables par un écart de format.

## Routage

**Sender ID** (`source_addr`) — l'adresse d'expéditeur affichée : alphanumérique (« ACME ») ou numérique. Doit être **autorisée** (enregistrée pour le client) selon `sender_id_policy`. Un alphanumérique n'a pas de chemin retour (on ne peut pas lui répondre STOP).

**Numéro entrant** (`inbound_numbers`) — shortcode ou long code détenu par le fournisseur, sur lequel arrivent les MO. **Dédié** (tout MO → un compte) ou **partagé** (résolu par mot-clé). Source de vérité du routage MO et de l'opt-out.

**MNP** (Mobile Number Portability) — la portabilité des numéros : un abonné garde son numéro en changeant d'opérateur. Casse le routage par préfixe (le préfixe n'identifie plus l'opérateur), d'où le niveau **numéro exact** (`exact_routes`), typiquement alimenté en masse depuis une base MNP.

**MCC-MNC** (Mobile Country Code – Mobile Network Code) — le couple qui identifie un opérateur mobile. Sert au matching déclaratif et à la tarification par destination.

**Route** vs **connecteur** — un **connecteur** (`smsc_connectors`) est un lien technique vers un SMSC. Une **route** (`routes`) est une règle qui choisit vers quel(s) connecteur(s) envoyer, avec une **stratégie de distribution** (`static`, `round_robin`, `weighted`, `failover_priority`, `least_loaded`, `hash_based`).

**Résolution à 3 niveaux** — l'ordre de décision du routage : **L0** numéro exact (court-circuit, MNP) → **L1** script de routage → **L2** matching déclaratif. Premier gagnant. Le court-circuit L0 saute la résolution, jamais la conformité.

**Filtre de Bloom** — structure probabiliste en mémoire à propriété clé : **jamais de faux négatif**. Utilisée pour numéros exacts et suppressions : « absent » = certainement pas de correspondance (pas d'appel réseau, ~99 % du trafic) ; « peut-être » = on lit Redis pour confirmer.

## Fiabilité et débit

**Disjoncteur (circuit breaker)** — machine à états par connecteur (`closed → open → half-open`) qui coupe l'envoi vers un SMSC qui se dégrade. `breaker_state` est **distinct** de `link_status` (état de la connexion) — les deux ne sont jamais confondus.

**`fallback_chain`** — la liste ordonnée de connecteurs de repli portée en en-tête de chaque message routé, permettant à `connector-pool-svc` de rerouter unilatéralement si le connecteur cible a son disjoncteur ouvert.

**Backpressure** — quand l'aval sature, on ralentit la consommation Kafka plutôt que de perdre des messages (ils restent durables en file).

**AIMD** (Additive Increase / Multiplicative Decrease) — l'algorithme du throttling adaptatif : baisse brutale du débit sur signal `ESME_RTHROTTLED`, remontée progressive ensuite.

**Token-bucket** — l'algorithme de limitation de débit métier (par compte/connecteur/route), implémenté en Lua atomique dans Redis.

**Pool de binds** (`bind_pool_size`) — plusieurs binds SMPP parallèles par connecteur pour lever le plafond de débit d'un bind unique. `mt.routed` est partitionné pour garder les segments d'un message sur un seul bind.

**Lane** — dans le routeur, une goroutine qui traite tous les records d'**une partition** Kafka présente dans un lot de poll ; séquentielle à l'intérieur, parallèle entre lanes. La lane **est** la partition, jamais la clé de compte : c'est ce qui laisse intact le rayon de duplication (ADR-0014). Le nombre de lanes est donc celui des partitions **assignées au pod**, pas celui du topic — d'où la règle de dimensionnement de step-270 (les partitions doivent dominer le plafond de l'HPA).

**Dead-letter** — file des messages ayant épuisé leurs retries (y compris `fallback_chain`), remontés pour retraitement.

## Facturation

**Crédit SMS** — l'unité de solde : un **compteur entier**, jamais une somme monétaire. Un message concaténé consomme plusieurs crédits (un par segment).

**Solde MT vs compteur MO** — le **solde MT** est un vrai solde bloquant (réserve → capture/libère ; zéro bloque l'envoi en prépayé). Le **compteur MO** est postpayé et ne bloque **rien** (le MO est déjà remis) ; il descend jusqu'à `mo_billing_floor` puis s'arrête avec alerte.

**Réserve / capture / libère** — le schéma bloquant du prépayé MT : `router-svc` **réserve** le crédit, `connector-pool-svc` **capture** au succès ou **libère** à l'échec. Idempotent par `message_id`.

**`balance_scope`** — qui possède les soldes : `customer` (pool partagé par direction) ou `smpp_account` (soldes isolés). Verrouillé : changeable seulement si tous les soldes sont à zéro.

**Grand livre (ledger)** — `billing_ledger`, append-only partitionné par jour. **Autorité durable** du solde ; le cache Redis en est une projection réhydratable.

**`charge_on`** — moment de facturation : `submission` (à l'acceptation SMSC) ou `delivery` (au DLR ; un échec déclenche un `refund`).

## Conformité

**Opt-out / STOP** — désabonnement **scopé au canal** : un STOP répondu à un numéro entrant crée une `suppressions` sur ce canal. L'étape MT bloquante bloque si le destinataire figure dans **l'une** des portées applicables (platform/customer/account/inbound_number).

**Suppression** — une entrée de la liste de désabonnement (`suppressions`), scopée. Sans expiration (l'expirer serait une violation).

**Crypto-shred** — effacement par **destruction de la clé** de chiffrement du contenu : détruire `content_keys` rend tout le contenu d'un client illisible d'un geste, sans réécrire le CDR. Un des mécanismes d'effacement RGPD.

**RGPD / DSAR** — Règlement général sur la protection des données ; **DSAR** = Data Subject Access Request (demande d'un individu). Effacement asymétrique : un **client** se crypto-shredde ; une **personne (MSISDN)** exige une suppression ligne à ligne across clients (clé partagée).

**`content_storage`** — politique de stockage du corps : `off` / `stored_plaintext` (déconseillé) / `stored_encrypted` (recommandé). Ne gouverne **que** le CDR ; logs et traces ne portent jamais le corps, sous aucune politique.

## Termes du projet / infra

**Plan de contrôle / plan de données** — le contrôle = la configuration (PostgreSQL, Admin API) ; les données = le traitement des messages (Kafka, services sans état). Le `config-sync` pousse l'un vers l'autre.

**CDR** (Call Detail Record) — l'enregistrement par message dans ClickHouse (métadonnées, statut, contenu chiffré optionnel). Interrogé par le tableau de bord pour recherche et traçage.

**Instantané immuable (snapshot)** — la configuration de routage compilée, échangée par pointeur atomique lors du hot reload, pour une lecture sans verrou sur le chemin chaud.

**Faux SMSC (fake SMSC)** — le double de test minimal in-repo (`internal/testutil/fakesmsc`) qui joue le SMSC pour les tests tant que le vrai simulateur n'est pas prêt. À ne pas confondre avec le **simulateur SMSC**, le projet compagnon plus complet (injection de pannes) requis à partir de M8.

**Config-sync / hot reload** — la propagation des changements de config du plan de contrôle vers le plan de données par pub/sub, sans redémarrage.
