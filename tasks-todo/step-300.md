# step-300 — TLS / SMPP-TLS / mTLS sur les transports

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — (step-193c livrée) · **Bloque :** step-310

## But
Chiffrer et authentifier les transports : TLS sur les APIs HTTP, SMPP-TLS sur les binds, mTLS entre
services internes (dont l'Admin API et le gRPC billing).

## Périmètre (ce que fait CETTE PR)
- HTTP : TLS sur `rest-api-svc` (public) et mTLS sur `admin-api-svc` (interne).
- SMPP : option SMPP-TLS pour `smpp-server-svc` (entrant) et `connector-pool-svc` (sortant).
- gRPC : mTLS sur les **quatre** serveurs du dépôt — `billing-svc`, `content-key-svc`,
  `session-manager-svc` et le `SessionRegistry` par pod servi par **`smpp-server-svc`** (remise
  `deliver_sm`, step-046). Nommés, pas « et futurs services » : les **huit** clients gRPC du code de
  production sont aujourd'hui construits avec `insecure.NewCredentials()`, celui de
  `internal/modlrrouter/poddeliverer.go` compris.
- Config TLS (certs/clés/CA) via `internal/config`, jamais de secret en dur.

## Points d'implémentation clés
- L'Admin API est **interne** et déjà pensée derrière un ingress mTLS (`internal/adminapi/api.go` :
  scheme « mTLS + operator bearer ») — matérialiser le mTLS ici.
- **Prérequis levé (step-193c).** Les dix services ont désormais leur `wiring.go`/`wiring_test.go` :
  `billing-svc` (cible du mTLS gRPC) et `rest-api-svc` (cible du TLS public) inclus. Un handshake TLS
  qui échoue au boot doit donc remonter **en valeur** depuis `newXxxApp`, jamais en `log.Fatal`, et son
  test de câblage l'exige déjà — c'est le point d'accroche à utiliser, pas à réinventer.
- **`ctx7`** avant toute API `crypto/tls` avancée / config TLS de `grpc` (credentials) / `coder/websocket` TLS.
- Certs/clés/CA fournis par config ou secrets, jamais commités ; rotation possible.
- Ne pas casser les tests d'intégration : TLS activable par config (off en test unitaire, on en prod).

## Ajouté par step-290d — la DEK circule en clair, sans authentification

ADR-0011 fait de `content-key-svc` le **seul détenteur de la KMS**, avec une surface volontairement
minimale pour que le dépositaire de la clé reste auditable. Cette surface est un gRPC que
`admin-api-svc` et `router-svc` appellent avec `insecure.NewCredentials()`
(`cmd/admin-api-svc/wiring.go`, `cmd/router-svc/wiring.go`).

Conséquence : la **clé de données** d'un client voyage en clair sur le réseau, et le service ne vérifie
pas qui la demande. Le chiffrement du contenu au repos (step-162) protège contre un vol de base ; il ne
protège de rien contre qui écoute ce lien ou sait l'appeler. C'est le lien le plus sensible du dépôt, et
c'est celui que le périmètre de cette step ne nommait pas.

**Ce que cette step doit donc faire :** mTLS sur `content-key-svc` avec une **liste d'appelants
autorisés** (le certificat client identifie le service), pas seulement un tunnel chiffré. Un tunnel sans
autorisation laisse n'importe quel pod du cluster demander n'importe quelle clé.

**Cinq commentaires écrits à la main affirment un mesh qui n'existe pas** — `deploy/` n'installe ni
istio, ni linkerd, ni sidecar. Les corriger fait partie de cette step, sans quoi elle laisserait derrière
elle la justification de ce qu'elle vient de réparer :

- `api/proto/contentkeys.proto` (deux occurrences, « ride the intra-mesh mTLS ») — et il faut
  **régénérer** `internal/contentkeys/pb/`, qui en recopie quatre : corriger le `.proto` seul laisse
  l'affirmation dans le Go compilé ;
- `internal/modlrrouter/poddeliverer.go` (« terminates at the mesh ») ;
- `cmd/smpp-server-svc/wiring.go` et `cmd/admin-api-svc/wiring.go` (« transport security is terminated at
  the mesh, not here »), qui sont précisément les deux extrémités du `SessionRegistry` ajouté ci-dessus
  aux cibles mTLS.

## Design arrêté

Arrêté le 2026-09-17, avant la première ligne de code. Deux forks tranchés par l'utilisateur
(provenance des certificats, conduite de la bascule) ; le découpage en quatre PR est validé.

### Ce que la lecture a changé à la fiche

- **Il n'y a aucun TLS nulle part.** `internal/config` n'a ni section, ni chemin de certificat ;
  `crypto/tls` n'apparaît dans aucun fichier de production. Ce n'est pas une option à activer, c'est
  une surface neuve.
- **Seize points d'insertion** : deux `http.Server` (`cmd/rest-api-svc/wiring.go`,
  `cmd/admin-api-svc/wiring.go`) plus celui des ops (`internal/observability/ops.go`), quatre
  `grpc.NewServer`, huit clients gRPC, le `net.ListenConfig` du SMPP entrant
  (`internal/smppserver/listener.go`) et le `net.Dialer` du SMPP sortant
  (`internal/connectorpool/bind.go`).
- **`deploy/k8s` ne monte aucun fichier** : uniquement des `secretKeyRef` en variables
  d'environnement, aucun `volumeMounts`. Cette step y touche, ainsi qu'à la garde `internal/deploy`,
  bien que la fiche renvoyât les manifests à step-270 (livrée).

### Périmètre : nos écoutes et nos appels, pas les magasins de données

Le TLS **client** vers les quatre magasins est un autre axe, et il n'est pas uniforme :

| | Atteignable aujourd'hui |
|---|---|
| PostgreSQL | **oui**, sans code — `sslmode=require` dans `POSTGRES_URL`, lu par pgx |
| Redis | **oui**, sans code — le schéma `rediss://` pose le `TLSConfig` dans `ParseURL` |
| Kafka | **non** — `dialOpts` ne pose que `DialTimeout`, il manque `kgo.DialTLS` |
| ClickHouse | **non** — les `Options` sont construites à la main, sans champ `TLS` |

Hors périmètre, donc, et **step-305 est ouverte pour le porter** (dernière PR de cette step) : les deux
lignes vertes sont une ligne de checklist au go-live, les deux rouges sont du code.

**Le serveur d'ops reste en clair**, définitivement. Il sert `/metrics`, `/healthz` et `/readyz` à la
kubelet et au scrapeur sur le réseau du pod. Le chiffrer obligerait chaque probe à porter une confiance
de CA et casserait le scraping pour un gain nul : rien de sensible n'y transite, la garde de cardinalité
interdisant déjà tout identifiant dans les métriques.

### Provenance des certificats : des fichiers montés (fork tranché)

Le code ne connaît que **des chemins**. Le contrat est un `Secret` de type `kubernetes.io/tls` par
service, aux clés standard `tls.crt` / `tls.key`, plus `ca.crt`. Qui le remplit reste à l'exploitant :
cert-manager, une PKI interne, un agent Vault.

Écarté : **le PEM en variable d'environnement**, qui interdit la rotation sans recréer le pod et met une
clé privée dans l'environnement d'un process que ce dépôt journalise au démarrage
(`logger.InfoContext(ctx, "starting", "config", cfg, …)`). Écarté aussi : **SPIFFE/SPIRE**, qui est la
bonne réponse pour un parc mûr et une infrastructure entière que `deploy/` n'a pas.

**cert-manager n'est pas déployé par ce dépôt** : c'est un opérateur cluster-wide, avec ses CRD et son
webhook d'admission. Même frontière que Postgres, Kafka, ClickHouse, Redis, les règles Alertmanager, le
collecteur OTel et l'Ingress — `deploy/README.md` la pose déjà. Ce que la step livre : le contrat du
`Secret`, un exemple de `Certificate` sous `deploy/k8s/tls/` (**hors** du répertoire que `kubeconform`
valide, qui ne connaît pas ces CRD), le générateur pour les clusters sans cert-manager, et une ligne de
la checklist de go-live (step-410) qui vérifie qu'un émetteur existe.

**Deux pièges de montage :**
- **`subPath` annule la propagation.** Un volume monté avec `subPath` ne reçoit jamais les mises à jour
  du `Secret` : la rotation deviendrait silencieusement inopérante jusqu'au prochain redémarrage.
  Interdit ici, et la garde `internal/deploy` le refuse.
- **L'identité est un SAN DNS, pas un `CN`.** Le `CN` est déprécié comme identité, et cert-manager
  remplit `dnsNames` naturellement. L'allowlist compare les SAN DNS du certificat client vérifié.

### Bascule : dure, sans mode transitoire (fork tranché)

Le serveur n'écoute qu'en TLS. Ni mode permissif par reniflage du premier octet (`0x16`), ni double port
pendant une transition.

La raison qui rend la question presque théorique : **cette passerelle n'a jamais été déployée**. Le
go-live est step-410. Il n'existe aucun cluster en service où faire une bascule ; quand la première
release partira, TLS sera là depuis le début. Construire un mécanisme de transition, c'est le porter
ensuite pour toujours.

**Contrepartie assumée :** si un jour un cluster tourne en clair, la bascule coûtera une fenêtre
d'indisponibilité, ou le double port qu'on n'aura pas construit. La procédure se documente ; elle ne se
code pas aujourd'hui.

### La forme partagée par les quatre PR

**Une identité par pod, pas une par surface.** Le même certificat sert au service quand il écoute et
quand il appelle — `smpp-server-svc` est serveur de son `SessionRegistry` **et** client de
`session-manager-svc`. Quatre variables :

```
TLS_ENABLED, TLS_CERT_FILE, TLS_KEY_FILE, TLS_CLIENT_CA_FILE
TLS_ALLOWED_CLIENTS   # les SAN admis, par serveur ; vide = tout porteur d'un certificat de la CA
```

`TLS_ENABLED` gouverne tout le pod, à deux exceptions près, écrites : **l'API REST publique** ne réclame
pas de certificat client (ses clients sont des intégrateurs, pas nos pods), et **les ops** restent en
clair.

**Le rechargement passe par `GetConfigForClient`, jamais par `GetCertificate` seul.** C'est le détail qui
décide de la rotation : `ClientCAs` est lu au début du handshake et aucun callback ne le rafraîchit, donc
un pool de CA posé une fois ne bouge plus. `GetConfigForClient` rend une `*tls.Config` neuve à chaque
handshake, si bien que la CA et le certificat tournent ensemble.

**Le cache est clé sur `(mtime, taille)` des deux fichiers**, sous `RWMutex` : un `stat` par handshake,
et aucune dépendance neuve — pas de `fsnotify`. La paire plutôt que la seule `mtime`, parce que deux
écritures dans le même tick d'horloge existent et qu'un test qui les enchaîne verrait une rotation
manquée passer pour un succès ; le test pilote les dates avec `os.Chtimes`.

**Versions minimales asymétriques :** TLS 1.3 sur tout ce qui est interne, 1.2 sur l'API REST publique.
Imposer 1.3 à des intégrateurs coupe des clients qu'on ne contrôle pas ; entre nos pods, il n'y a
personne à ménager.

**Garde de production**, sur le modèle des autres : en production, `TLS_ENABLED=false` est refusé au
démarrage. Off par défaut, donc `make check` et les tests d'intégration ne changent pas.

Le paquet vit dans **`internal/platform/tlsconf`**, à côté de `errors`, `humaspec`, `keyset` et
`supervisor`.

### Découpage : quatre PR, dans l'ordre a → d

**300a — la brique et sa configuration.** Le paquet, la section de configuration, la garde de
production, le helper de test, le générateur, la documentation de déploiement. **Aucun service câblé.**

**300b — gRPC.** Les quatre serveurs (`billing-svc`, `content-key-svc`, `session-manager-svc`, et le
`SessionRegistry` par pod de `smpp-server-svc`), les huit clients, et l'**autorisation** par SAN sur
`content-key-svc`. Corrige les cinq commentaires qui invoquent un maillage inexistant et régénère
`internal/contentkeys/pb`.

**300c — HTTP.** TLS public sur `rest-api-svc`, mTLS sur `admin-api-svc`.

**300d — SMPP-TLS**, entrant et sortant. L'ordre PROXY-protocol / TLS y est un piège : l'en-tête PROXY
précède le handshake sur le fil, donc l'enveloppe TLS va **à l'intérieur** de celle du PROXY. Ouvre
step-305 et déplace cette fiche en `tasks-done/`.

### 300a — détail validé le 2026-09-17, avant le code

L'API du paquet tient en trois déclarations :

```go
type Files struct{ Cert, Key, ClientCA string }

func (f Files) ServerConfig(allowedClients []string) (*tls.Config, error)
func (f Files) ClientConfig() (*tls.Config, error)
```

L'autorisation passe par `VerifyPeerCertificate`, appelé **après** la vérification de chaîne : on y
compare les SAN DNS du certificat vérifié à la liste. L'erreur nomme le SAN présenté — ce n'est pas un
secret, et sans lui un refus mTLS est indébogable.

`internal/config` gagne `SectionTLS` — trois endroits à toucher en même temps, la garde AST qui l'exige
existe déjà. `internal/testutil/tlstest` fabrique une CA ECDSA et les feuilles demandées en mémoire, les
écrit dans `t.TempDir()`, rend les chemins. `test/tlsgen` est le même code en ligne de commande, **sous
`test/` et non `cmd/`**, où la garde `internal/deploy` exigerait d'un binaire neuf qu'il ait son image
publiée et sa ligne GoReleaser. `deploy/k8s/tls/README.md` porte le contrat du `Secret` et l'exemple.

**Preuves**, sur un serveur jetable en TLS brut et un `http.Server` — pas de gRPC, le paquet ne rend
qu'une `*tls.Config` et gRPC est le sujet de 300b :

1. handshake mTLS réussi entre deux pairs de la CA ;
2. client **sans** certificat : refusé ;
3. client d'une **autre** CA : refusé ;
4. client dont le SAN n'est pas dans la liste : refusé, et le message nomme le SAN présenté ;
5. **rotation** : les fichiers changent, le handshake suivant sert le nouveau certificat, sans
   redémarrage ;
6. en production, `TLS_ENABLED=false` refuse le démarrage.

Mutations qui doivent faire tomber quelque chose : retirer `ClientAuth: RequireAndVerifyClientCert` fait
tomber 2 ; retirer la comparaison de SAN fait tomber 4 ; figer le cache au premier chargement fait tomber
5 ; retirer la garde fait tomber 6.

**Ce que 300a ne prouve pas :** qu'un seul service s'en serve. Aucun câblage, donc aucune régression
possible sur les dix binaires — et c'est aussi pourquoi cette PR ne peut pas être « presque tout le
travail ».

## Tests (écrits dans la même PR)
- Handshake TLS/mTLS réussi ; un client sans cert client est rejeté sur les endpoints mTLS.
- SMPP-TLS : bind chiffré établi (faux SMSC/simulateur).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] TLS/SMPP-TLS/mTLS activables par config ; aucun secret en dur

## Hors périmètre
Auth opérateur réelle (OIDC) → step-310. Manifests k8s → step-270.
