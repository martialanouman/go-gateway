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
`Secret`, un exemple de `Certificate` **dans le README** de `deploy/k8s/tls/` et non en fichier YAML —
`make manifests` passe kubeconform sur `deploy/k8s` en entier, et il ne sait pas valider une CRD
cert-manager —, le générateur pour les clusters sans cert-manager, et une ligne de la checklist de
go-live (step-410) qui vérifie qu'un émetteur existe.

**Deux pièges de montage :**
- **`subPath` annule la propagation.** Un volume monté avec `subPath` ne reçoit jamais les mises à jour
  du `Secret` : la rotation deviendrait silencieusement inopérante jusqu'au prochain redémarrage.
  Interdit ici, et écrit dans le README de `deploy/k8s/tls/`. **Aucune garde ne le refuse aujourd'hui** :
  quand 300b montera le premier volume, elle sera à ajouter à `internal/deploy`.
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

**Côté client, la CA ne tourne pas — arbitrage Fable du 2026-09-18.** `GetClientCertificate` couvre le
certificat feuille, mais `RootCAs` est lu dans la `*tls.Config` remise une fois pour toutes au transport,
et `crypto/tls` n'offre aucun équivalent client de `GetConfigForClient`. On accepte la limite : **un
changement de CA exige un redémarrage des clients**, documenté dans le runbook.

Les deux contournements sont écartés pour la même raison : ni l'un ni l'autre ne re-vérifie les
connexions **déjà établies**. Une CA compromise a des connexions ouvertes ; relire le pool à chaque
handshake ne les ferme pas, seul un redémarrage le fait — et il est plus rapide (le drain est déjà
contractualisé) et plus auditable. Ils n'achètent donc que de la disponibilité pendant une rotation de CA
*planifiée*, événement rare et de toute façon multi-phase. `InsecureSkipVerify: true` avec vérification
manuelle (ce que fait `grpc/security/advancedtls`) coûterait une justification à chaque revue et à chaque
scan gosec ; une `credentials.TransportCredentials` maison coûterait deux implémentations par transport.

**Ce que la limite impose en échange**, parce que son mode de panne est silencieux — CA tournée sans
redémarrage, les nouveaux handshakes échouent en `x509: unknown authority`, sans lien évident : le
chargeur compare le SHA-256 de `ca.crt` à celui lu au démarrage et journalise un `WARN` nommant le
redémarrage requis. Il lit déjà le disque à chaque handshake ; la garde ne coûte que la comparaison.

Si une rotation de CA sans redémarrage devient un jour exigée, l'escalade est la
`credentials.TransportCredentials` côté gRPC seulement — jamais `InsecureSkipVerify`.

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

L'API du paquet tient en quatre déclarations — `ServerOptions` et le champ `Logger` sont arrivés en
revue, pour les raisons dites plus bas :

```go
type Files struct{ Cert, Key, ClientCA string; Logger *slog.Logger }
type ServerOptions struct{ AllowedClients, NextProtos []string }

func (f Files) ServerConfig(opts ServerOptions) (*tls.Config, error)
func (f Files) ClientConfig() (*tls.Config, error)
```

L'autorisation compare les SAN DNS du certificat vérifié à la liste. L'erreur nomme le SAN présenté et
les SAN admis — ce n'est pas un secret, et sans eux un refus mTLS est indébogable.

**Corrigé à l'écriture : le hook est `VerifyConnection`, pas `VerifyPeerCertificate`.** Sur une
**reprise de session** TLS 1.3, `VerifyPeerCertificate` n'est jamais rappelé : un appelant retiré de
l'allowlist garderait son accès tant que vit son ticket. `VerifyConnection` est appelé sur les deux
chemins, et l'état qu'il reçoit porte les chaînes vérifiées dans les deux cas. gosec le signale
(G123), donc le lint garde ce choix.

`internal/config` gagne `SectionTLS` — **cinq** endroits à toucher en même temps, et non trois : la
constante, `SectionAll`, la branche de validation, le champ de `Config`, et le registre `knownVars` des
tests. La garde AST se dérive du code, donc elle couvre la section neuve sans modification. `internal/testutil/tlstest` fabrique une CA ECDSA et les feuilles demandées en mémoire, les
écrit dans `t.TempDir()`, rend les chemins. `test/tlsgen` est le même code en ligne de commande, **sous
`test/` et non `cmd/`**, parce que c'est un outil et non un service — la même place que
`test/load/bindgen`. `deploy/k8s/tls/README.md` porte le contrat du `Secret` et l'exemple.

**Preuves**, sur un serveur jetable en TLS brut et un `http.Server` — pas de gRPC, le paquet ne rend
qu'une `*tls.Config` et gRPC est le sujet de 300b :

1. handshake mTLS réussi entre deux pairs de la CA ;
2. client **sans** certificat : refusé ;
3. client d'une **autre** CA : refusé ;
4. client dont le SAN n'est pas dans la liste : refusé, et le message nomme le SAN présenté ;
5. **rotation de la feuille** : les fichiers changent, le handshake suivant sert le nouveau certificat,
   sans redémarrage ;
6. **rotation de la CA** : le `ca.crt` change sur disque, un `WARN` nomme le redémarrage requis — la
   panne silencieuse devient bruyante ;
7. en production, `TLS_ENABLED=false` refuse le démarrage.

Mutations qui doivent faire tomber quelque chose : retirer `ClientAuth: RequireAndVerifyClientCert` fait
tomber 2 ; retirer la comparaison de SAN fait tomber 4 ; figer le cache au premier chargement fait tomber
5 ; retirer la comparaison d'empreinte de CA fait tomber 6 ; retirer la garde fait tomber 7.

**Corrigé en revue (2026-09-18).** Trois revues en lecture seule ont trouvé un bloquant et cinq
décisions du design que rien ne tenait :

- **Une mutation restait invisible : ajouter `InsecureSkipVerify: true` à `ClientConfig` laissait toute
  la suite verte.** Le code ne l'a jamais contenu — c'est la preuve qui manquait, pas le correctif.
  Aucun test ne mettait un client face à un serveur d'une autre autorité — dans le test d'intrusion, l'intrus fait justement
  confiance à la nôtre comme racine. Le seul chemin client → serveur non vérifié n'était exercé nulle
  part, alors que le design s'interdit `InsecureSkipVerify`.
- **L'ALPN entre par la signature** (`ServerOptions`), parce qu'il ne peut pas entrer autrement : ni
  `net/http` ni gRPC ne laissent la config externe atteindre `GetConfigForClient`, tous deux passent un
  clone. Une liste vide n'est pas un défaut mais une rétrogradation muette — `negotiateALPN` rend un
  protocole vide **sans erreur**, et HTTP/2 s'éteint sans une ligne de log. C'est donc 300a, pas 300c.
  Le `MinVersion` public à 1.2 attendra son propre constructeur en 300c, plutôt qu'un levier qui
  abaisserait aussi le plancher interne.
- **`tls.LoadX509KeyPair` ne regarde jamais `NotAfter`** : un certificat expiré démarrait vert et passait
  sa sonde. Annoncé au chargement, sans refuser le démarrage — qui transformerait une panne partielle en
  CrashLoopBackOff pendant que cert-manager renouvelle.
- **Tour 2 :** le contrôle d'expiration ne tournait qu'au rechargement, donc jamais dans le cas qu'il
  documente — quand cert-manager cesse de renouveler, les fichiers ne changent pas. Il passe par le
  chemin chaud, avec un niveau plutôt qu'un drapeau pour que l'Error suive le Warn. Le contrôle de blocs
  PEM est supprimé : il comparait deux populations différentes et criait sur un bundle valide au milieu
  d'une rotation.
- Preuves ajoutées, chacune avec sa mutation : vérification du serveur côté client, rotation de la **CA**
  côté serveur, plancher TLS 1.3, moitié « taille » de la clé de cache, unicité de l'avertissement,
  identité conservée sur une session reprise, SAN in-cluster du générateur, garde de configuration hors
  production. Deux tests de refus qui ne regardaient pas le motif l'exigent désormais : un EOF les
  rendait verts.

**Ce que 300a ne prouve pas :** qu'un seul service s'en serve. Aucun câblage, donc aucune régression
possible sur les dix binaires — et c'est aussi pourquoi cette PR ne peut pas être « presque tout le
travail ».

### 300b — détail validé le 2026-09-19, avant le code

#### Ce que la lecture a trouvé, et qui change la PR

Trois défauts **préexistants**, que la cartographie des points d'insertion a sortis :

- **Aucun binaire ne déclare `config.SectionTLS`.** La garde de production livrée en 300a — en
  production, `TLS_ENABLED=false` est refusé — ne tourne donc dans aucun des dix services : `Load` ne
  valide que les sections déclarées, et 300a a ajouté la section sans l'inscrire nulle part. 300a a
  livré une garde morte. Les huit services de 300b la déclarent.
- **`deploy/k8s/configmap.yaml` porte `ENVIRONMENT: production` et aucun `TLS_*`.** Le point précédent
  corrigé, les huit pods refusent de démarrer. Les manifests bougent donc **dans cette PR** ; ce n'est
  pas un supplément qu'on pourrait reporter.
- **`<pod-id>.smpp-server-headless` ne résout pas.** Un enregistrement A par pod n'existe que si le pod
  porte `spec.hostname` *et* un `spec.subdomain` égal au nom du service headless ; le contrôleur
  d'endpoints ne recopie que `spec.hostname`, qu'un `Deployment` ne peut fixer qu'en une seule chaîne
  statique pour toutes ses répliques. Seul un `StatefulSet` donne un nom par pod. La remise MO/DLR par
  pod échoue donc sur la résolution DNS, **avant** tout handshake — un défaut antérieur à cette step et
  indépendant d'elle. → **step-302**, ouverte par cette PR ; l'arbitrage ci-dessous fait que 300b n'en
  dépend pas.

#### Arbitrage : l'identité vérifiée du dial pod-à-pod (Fable, 2026-09-19)

Sept des huit clients composent un nom de `Service` (`billing-svc:7000`) : grpc-go prend l'autorité de
la cible comme `ServerName` quand `tls.Config.ServerName` est vide, le SAN correspond, il n'y a rien à
faire. Le huitième, `internal/modlrrouter/poddeliverer.go`, joint une adresse **de pod** alors que le
certificat est **par `Deployment`**, de SAN `smpp-server-svc`.

> **Depuis, step-302 est livrée** (voir `tasks-done/step-302.md`) : ce huitième client ne compose plus
> `<pod-id>.smpp-server-headless` — ce nom ne résolvait pas —, il dial l'adresse IP que le pod publie
> dans le registre de sessions. **L'arbitrage ci-dessous ne change pas, et se trouve renforcé** : une
> cible IP rend l'épinglage de `ServerName` non pas préférable mais obligatoire, un SAN joker ne
> s'appliquant jamais à une adresse IP. Ne pas relire ce qui suit comme une carte du code actuel.

**Retenu : épingler `ServerName = "smpp-server-svc"` sur ce seul appelant.** Le certificat est partagé
par toutes les répliques : la seule identité qu'il puisse attester est « un pod du `Deployment`
`smpp-server-svc` », et c'est exactement ce que cette vérification demande. Que le pod joint soit le
bon est déjà contrôlé ailleurs — `Deliver` répond `delivered:false` quand le pod ne détient pas le bind
—, et ce n'est pas à TLS de le dire.

Écarté : le **SAN joker** (`*.smpp-server-headless`), qui ferait vérifier une identité *par pod* que la
PKI ne délivre pas, et souderait le certificat à un schéma d'adressage que `poddeliverer.go` annonce
lui-même comme temporaire. Il casserait au renommage du service headless — une deuxième vérité à tenir
en accord avec le gabarit d'adresse, dans deux systèmes — et il ne peut pas exister du tout maintenant
que step-302 est résolue par `status.podIP`, un joker ne s'appliquant jamais à une adresse IP. Écarté aussi :
les **certificats par pod** (csi-driver cert-manager, SPIFFE), qui demandent une infrastructure que ce
dépôt ne déploie pas.

Le nom reste une **constante dans le câblage**, pas une variable d'environnement : c'est le nom du
`Deployment`, fixé par `deploy/k8s`, et la configurer serait de la configuration pour une valeur qui ne
change pas. Le jour où quelqu'un le renomme, l'erreur le dit en toutes lettres
(`x509: certificate is valid for smpp-server-svc, not …`). L'épinglage est posé par
`tls.Config.ServerName`. **Corrigé en revue :** la fiche justifiait ce choix contre `grpc.WithAuthority`
en disant que celui-ci « réécrirait aussi l'en-tête `:authority` » — c'est faux, et dans les deux sens.
`ClientConn.initAuthority` lit le `ServerName` des credentials et le promeut en autorité de la
connexion, laquelle EST l'en-tête `:authority` : les deux voies ont exactement le même effet, et
grpc-go refuse même le dial si les deux sont posées et divergent. Le choix tient toujours — une seule
chose à poser plutôt que deux à garder d'accord — mais pas pour la raison écrite.

#### Où vit la colle

`tlsconf` reste sans dépendance hors bibliothèque standard : 300c et 300d s'en servent pour HTTP et pour
SMPP, et lui faire importer gRPC pour deux fonctions le rendrait faux pour eux. Un paquet neuf,
**`internal/grpctls`** — voisin d'`internal/storage`, qui importe `config` comme lui — porte la
construction elle-même, branche désactivée comprise :

```go
func NewServer(cfg config.TLS, logger *slog.Logger) (*grpc.Server, error)
func NewClient(cfg config.TLS, logger *slog.Logger, addr string) (*grpc.ClientConn, error)
func Dialer(cfg config.TLS, logger *slog.Logger, serverName string) (Dial, error)
```

**Corrigé en revue :** le design rendait des *options* (`ServerOption`, `DialOption`), que le câblage
passait ensuite à `grpc.NewServer`/`grpc.NewClient`. La garde devait alors remonter un argument jusqu'à
son origine — donc résoudre un nom de variable —, et deux mutations ont montré que ça ne tient pas : une
réaffectation après la bonne ligne, ou le même nom dans une autre fonction du fichier, laissaient partir
un serveur en clair au vert. Le paquet construit donc le serveur et la connexion, et la garde porte sur
**quelle fonction est appelée** : aucun nom à résoudre, aucune portée à suivre, et
`grpc.WithInsecure()` — l'autre évasion trouvée — tombe avec, alors qu'une règle sur
`insecure.NewCredentials` ne la voyait pas.

**`runGRPC` n'est pas touché**, et ses quatre copies restent dupliquées : des credentials sont une
`grpc.ServerOption`, elles entrent au `grpc.NewServer` dans `wiring.go`. Ce sont quatre fichiers, pas
huit, et le dédoublonnage de `runGRPC` n'est pas le sujet de cette PR.

**Vérifié chez grpc-go (v1.83) :** `credentials.NewTLS` *enveloppe* `GetConfigForClient` et ajoute `h2`
à `NextProtos` sur la config que le rappel a rendue. C'est pourquoi `ServerOptions{NextProtos: nil}` est
correct ici — et seulement ici : `net/http` ne fait pas ce service, d'où le contrat inverse en 300c.

#### Preuves, et la mutation qui fait tomber chacune

| # | Preuve | Mutation |
|---|---|---|
| 1 | handshake mTLS gRPC réussi entre deux pairs de la CA | — |
| 2 | client **sans** certificat : refusé | retirer `ClientAuth` |
| 3 | pair d'une **autre** CA refusé, **dans les deux sens** | `ClientCAs: nil` ; `RequireAnyClientCert` |
| 4 | `TLS_ENABLED=false` : les deux côtés parlent en clair | inverser la branche |
| 5 | `content-key-svc` **câblé** refuse un SAN hors liste, par son `app.grpc` | retirer `cfg.TLS.AllowedClients` du câblage |
| 6 | `PodClients` : épinglé → `Deliver` passe ; non épinglé → l'erreur nomme l'adresse composée | supprimer la ligne `ServerName` ; changer la constante |
| 7 | garde de source : aucun fichier de production n'appelle `insecure.NewCredentials()` | remettre un neuvième dial en clair |
| 8 | garde manifeste `no-subpath` (et `subPathExpr`) | retirer la règle |

La preuve 7 remplace douze tests de câblage à conteneur par la garde qui attrape la régression réelle :
le neuvième appel en clair qu'une step future ajouterait sans y penser. La 5 existe parce que 7 ne
prouve rien d'une **allowlist** — seule une liste effectivement transmise le prouve, et c'est le lien de
la DEK. La moitié « non épinglé » de la 6 est ce qui empêche la fixture d'être creuse : sans elle, c'est
le dialer de test, et non l'épinglage, qui pourrait faire passer la moitié verte.

**Corrigé en revue (2026-09-19).** Trois revues en lecture seule ont trouvé deux défauts qui laissaient
passer du clair, et une liste de défauts d'attribution :

- **La garde des serveurs comptait les arguments sans les lire.** `grpc.NewServer(grpc.EmptyServerOption{})`
  sur `billing-svc` sert en clair et laissait **tout le dépôt vert**, cette garde comprise. Elle remonte
  désormais l'argument jusqu'à `grpctls.ServerOption`, appelée en ligne ou par la variable qui la reçoit.
  Même classe pour la garde des clients : un import aliasé (`noTLS "…/credentials/insecure"`) passait,
  parce qu'elle comparait un identifiant au lieu de résoudre le chemin d'import du fichier.
- **Trois serveurs sur quatre ne nommaient aucun appelant**, alors que chacun en a un ou deux. La fiche
  disait « ce que veulent les serveurs sans appelants nommés » ; la lecture du graphe d'appel montre
  qu'il n'y en a aucun. Sans liste, tout pod obtenant un certificat dans le namespace peut appeler
  `Release` pour rembourser un crédit jamais réservé, `Disconnect` pour tomber les binds vivants, ou
  `Deliver` pour injecter un DLR forgé dans le bind d'un client. Les trois manifests nomment maintenant
  leurs appelants ; c'est le seul changement de fond par rapport au design validé.
- **`TestAClientFromAnotherAuthorityIsRefused` prouvait le refus du CLIENT.** Sous TLS 1.3 le certificat
  du serveur arrive en premier : un appelant qui ne fait confiance qu'à sa propre CA abandonne avant
  d'avoir rien présenté, et le `ClientCAs` du serveur n'entre jamais en jeu. Le test est renommé pour ce
  qu'il prouve, et la direction manquante — celle sur laquelle reposent entièrement les serveurs — a
  désormais la sienne.
- La règle `no-subpath` ne prouvait ni son point d'appel sur les `Job`, ni sa moitié `subPathExpr` : la
  fixture ne violait qu'un `subPath` sur un `Deployment`. Le `Job` cassé en porte un.
- **La commande `tlsgen` du README n'émettait que 4 certificats sur 8, en sortant 0** — une continuation
  `\` suivie d'une ligne indentée coupe la liste. Et la boucle qui la remplace ne pouvait pas s'écrire
  `for svc in ${SVCS//,/ }` : zsh ne découpe pas une expansion non quotée.
- `router-svc` construisait deux chargeurs de fichiers pour une seule identité, donc disait deux fois
  « la CA a changé, redémarrez » pendant une rotation. Un seul, comme `admin-api-svc`.
- Deux tests étaient creux sans être faux : la moitié « vraiment en clair » rejouait les mêmes
  credentials trois lignes plus haut (supprimée), et le nom du `Deployment` dérivé de la constante
  suivait celle-ci partout où elle dérivait (littéral).

`internal/deploy` ne **voit** pas `volumeMounts` aujourd'hui — décodage non strict sur des structures
typées, donc toute clé sans champ correspondant est jetée en silence. Il faut ajouter le champ avant la
règle, sans quoi elle passerait au vert sur une population vide.

#### Ce que 300b ne fait pas

`rest-api-svc` reste sans `SectionTLS` (c'est 300c) et `config-sync` n'en aura jamais : il n'écoute ni
n'appelle aucun transport de ce dépôt. Pas de règle « `TLS_ENABLED=true` exige un volume monté » :
l'interrupteur est posé **à côté** du volume dans chaque manifeste, ce qui les lie sans garde. Et pas de
joker dans `test/tlsgen` — l'arbitrage n'en demande aucun.

### 300c — détail validé le 2026-09-19, avant le code

#### Ce que la lecture a trouvé

- **Les deux surfaces sont au même endroit.** Chaque service construit son `http.Server` dans
  `newHTTPServer` (wiring) et le sert dans une copie quasi identique de `runHTTP` (main). `newHTTPServer`
  ne rend aujourd'hui **aucune erreur** : TLS lui en donne une — trois chemins illisibles au boot —, donc
  sa signature en gagne une et `newRestAPIApp` / `newAdminApp` la propagent. C'est l'accroche que la
  fiche réclamait : un échec TLS remonte **en valeur**, jamais en `log.Fatal`.
- **`admin-api-svc` est déjà à moitié câblé.** 300b lui a donné `TLS_ENABLED=true`, le volume et le
  montage, parce qu'il est client gRPC de `session-manager-svc` et de `content-key-svc`. Seule son
  **écoute** HTTP est en clair. `rest-api-svc` n'a rien : ni `SectionTLS`, ni volume, ni variable.
- **`ListenAndServeTLS("", "")` suffit** (vérifié dans Go 1.26, `net/http/server.go` : `configHasCert`
  teste `GetConfigForClient != nil`). Aucun chemin de fichier ne repasse par `net/http`, la rotation
  reste entièrement dans `tlsconf`.

#### ALPN : asymétrique, et c'est la spec qui tranche

Le contrat inverse de 300b s'applique : **`net/http` n'ajoute rien** à la liste que rend
`GetConfigForClient`. Une liste vide n'est pas un défaut, c'est HTTP/2 éteint sans une ligne de journal.

- **`rest-api-svc` → `["http/1.1"]`, et non `["h2", "http/1.1"]` — corrigé en revue.** La spec offre
  deux voies pour les 10 000+ connexions simultanées, « HTTP/2 **ou** keep-alive avec pool »
  (`specification-technique` §93, guide §82), et c'est la seconde qui est déjà implémentée. Annoncer
  `h2` ici **supprimait la garde anti-slowloris de la surface publique** : le serveur HTTP/2 de Go ne
  lit jamais `ReadHeaderTimeout`, il arme sa deadline de flux depuis `ReadTimeout`
  (`net/http/h2_bundle.go:6099`, dont le commentaire dit « technically more like the http1 Server's
  ReadHeaderTimeout »), et `IdleTimeout` s'en déduit à son tour (`:4239`). Or ce dépôt ne pose que
  `ReadHeaderTimeout` (`internal/config/config.go`, défaut 5 s, exigé par gosec G112) : les deux autres
  valent zéro, donc aucune deadline n'aurait été armée. HTTP/2 est un levier de **débit**, il se paie
  d'un `ReadTimeout` à mesurer ; il n'a rien à faire dans une PR de chiffrement. Reporté, avec sa
  raison, à une step qui mesure.
- **`admin-api-svc` → `["http/1.1"]` seul.** `internal/adminapi/stream.go` appelle `websocket.Accept`,
  qui **hijacke** la connexion ; `net/http` ne sert pas le CONNECT étendu (RFC 8441). Sous h2, le flux
  temps réel de l'Admin API est mort. `negotiateALPN` (vérifié) parcourt la liste **du serveur** en
  premier : ne pas nommer `h2` est ce qui garantit qu'il ne sera jamais choisi. Un client qui n'offrirait
  *que* h2 se verrait refusé — aucun n'existe pour une API JSON.

Planchers déjà arrêtés plus haut : **1.2** côté public (des intégrateurs qu'on ne contrôle pas), **1.3**
côté interne.

#### Le constructeur public

```go
func (f Files) PublicServerConfig(nextProtos []string) (*tls.Config, error)
```

Plancher 1.2, aucun certificat demandé au pair, et **le même `GetConfigForClient`** que le constructeur
mutuel : la rotation se comporte identiquement sur les neuf pods.

Écarté : un booléen de plus sur `ServerOptions`. `AllowedClients` n'a aucun sens sans certificat client,
et un champ silencieusement ignoré est exactement le piège que 300b a payé deux fois. Le paramètre
`nextProtos` plutôt qu'une liste en dur, parce que 300d sert du SMPP, qui n'a pas d'ALPN, et passera
`nil`.

**Le fichier de CA reste exigé des neuf services**, `rest-api-svc` compris, qui ne s'en sert pour rien :
`tlsProblems` valide les trois chemins ensemble, le `Secret` monté porte `ca.crt` de toute façon, et une
identité de pod qui change de forme selon la surface coûterait plus que le `stat` qu'elle épargne.

#### Pas de paquet `httptls`

La projection `config.TLS` → `tlsconf.Files` reste écrite au point d'appel, deux fois cinq lignes.
`internal/config` n'importe aucun paquet interne et doit le rester ; `internal/grpctls` ne peut pas
l'héberger pour HTTP sans imposer gRPC à qui sert du HTTP. Un paquet pour deux littéraux coûte plus
qu'il ne rend. À la quatrième copie — 300d, entrant et sortant — l'extraction se posera d'elle-même.

#### L'allowlist de l'Admin API est vide, et c'est un choix

Aucun pod de ce dépôt n'appelle l'Admin API : la liste ne peut nommer personne. Vide admet donc tout
porteur d'un certificat de notre CA, et l'autorisation réelle reste le **second facteur** déjà en place,
le bearer opérateur (`HTTP_ADMIN_TOKENS`, schéma « mTLS + operator bearer » d'`internal/adminapi/api.go`).
Nommer l'ingress serait de la configuration pour une valeur que `deploy/` ne déploie pas.

Le serveur d'ops ne bouge pas, définitivement — la raison est écrite plus haut.

#### Preuves, et la mutation qui fait tomber chacune

| # | Preuve | Mutation |
|---|---|---|
| 1 | `rest-api-svc` : un client **sans** certificat, plafonné à TLS 1.2, est servi | `ClientAuth: RequireAndVerifyClientCert` ; `MinVersion: VersionTLS13` |
| 1b | `rest-api-svc` : les **trois** fichiers sont exigés au boot, `ca.crt` compris | un `loader` qui tolère une CA absente |
| 2 | `rest-api-svc` : l'ALPN **négociée** vaut `http/1.1` face à un client qui offre `h2` en tête | `NextProtos: nil` |
| 3 | `admin-api-svc` : le pair de la CA passe, le client **sans** certificat est refusé | `NoClientCert` |
| 4 | `admin-api-svc` : l'ALPN **négociée** vaut `http/1.1` face à un client qui offre `h2` en tête | ajouter `"h2"` en tête ; `NextProtos: nil` |
| 5 | les deux wirings : `TLS_ENABLED=true` ⇒ `app.http.TLSConfig != nil` ; un chemin illisible ⇒ erreur de boot **rendue** | supprimer l'affectation ; journaliser au lieu de rendre |
| 6 | `runHTTP` sert bien en TLS quand `TLSConfig` est posé, dans les deux services | forcer `ListenAndServe` |

La preuve 6 est ce qui empêche la 5 d'être creuse : une `*tls.Config` posée sur le serveur et jamais
servie laisserait toute la colonne verte. La 1 et la 3 sont la même assertion dans les deux sens — c'est
la seule façon de prouver qu'une surface publique n'a pas hérité du mutuel, et l'inverse.

`rest-api-svc` déclarant `SectionTLS`, la garde AST de `internal/config/sections_guard_test.go` couvre
l'oubli symétrique sans qu'on ajoute quoi que ce soit.

#### Ce que 300c ne fait pas

Le TLS **client** vers les quatre magasins (step-305), le SMPP-TLS (300d), l'ingress. Pas de règle
« `TLS_ENABLED=true` exige un volume monté » — même raison qu'en 300b.

#### Corrigé en revue (2026-09-19)

Trois revues en lecture seule (mécanisme · tests · code en trop), aucun constat bloquant.

- **Le défaut de fond était HTTP/2**, et aucun des deux autres axes ne l'a vu : il ne se lit ni dans le
  diff ni dans les tests, seulement dans les sources de `net/http`. Voir l'ALPN ci-dessus.
- **Deux assertions d'ALPN étaient creuses** dès lors que `h2` disparaissait : `resp.Proto` vaut
  `HTTP/1.1` aussi bien quand la liste est annoncée que quand elle est **vide** — `negotiateALPN` rend
  alors une chaîne vide *sans erreur*. Les deux tests portent désormais sur
  `resp.TLS.NegotiatedProtocol`, qui distingue les deux, et la mutation `NextProtos: nil` tombe des deux
  côtés.
- **L'offre `h2` du client était une prémisse implicite**, posée par `ForceAttemptHTTP2` dans les
  entrailles de `net/http`. Elle est maintenant écrite dans le test.
- **Le refus sans certificat n'avait pas de contrôle positif** sur le même serveur : un serveur jamais
  lié l'aurait satisfait. Le test fait d'abord passer un client porteur d'un certificat.
- **Le plancher 1.2 n'était prouvé qu'en `tlsconf`** : le client du test de câblage n'avait pas de
  `MaxVersion` et négociait 1.3.
- **La CA exigée de `rest-api-svc` n'était prouvée par rien** — la simplification « la surface publique
  n'en a pas besoin » laissait toute la suite verte. Le test de boot couvre les trois fichiers.
- **Rejeté : remplacer les clients de test par `tlsconf.ClientConfig()`** (−27 lignes). Cette
  configuration pose `GetClientCertificate` : le client présenterait un certificat, et la mutation
  « exiger un certificat client » ne ferait plus tomber le test qui prouve précisément qu'aucun n'est
  demandé. Une coupe qui rend une preuve creuse n'est pas une coupe.
- Coupé, en revanche : un test public entièrement couvert par son voisin, et un paramètre qu'aucun
  appelant ne faisait varier.

#### Dettes ouvertes par 300c

- **`srv.ErrorLog` n'est posé nulle part** (`grep ErrorLog cmd/` : zéro). Les erreurs de handshake —
  « tls: client didn't provide a certificate », le diagnostic n° 1 du nouveau port mTLS — partent sur
  stderr par le logger de la bibliothèque standard, hors `slog`, non structurées. Une ligne par serveur,
  plus sa preuve.
- **`announceCAChange` parle de « dial »** (`tlsconf.go`) : sur `rest-api-svc`, le chargeur n'est que
  serveur, et le message annonce une conséquence qui n'existe pas.
- **HTTP/2 sur l'API publique**, avec le `ReadTimeout` que sa deadline de flux exige — à décider sur une
  mesure, pas sur une intention.

### 300d — détail validé le 2026-09-19, avant le code

#### Ce que la lecture a trouvé

- **L'enveloppe TLS va à l'intérieur de celle du PROXY, et le code l'écrit déjà dans cet ordre.**
  `(*Listener).Run` possède toute la chaîne — écouter, décorer, accepter, servir — et
  `wrapProxyProtocol` décore le **listener**, pas la connexion (`internal/smppserver/listener.go:33`).
  Poser `tls.NewListener` juste après, sur le `lis` déjà réassigné, donne le seul ordre que le fil
  permet : l'en-tête PROXY précède le ClientHello. L'inverse se compilerait et échouerait à chaque
  handshake.
- **`Options` est la seule couture.** `Options.Addr` est une chaîne ; il n'existe aucun point
  d'injection d'un `net.Listener`. D'où un champ `TLSConfig *tls.Config`, construit au câblage —
  l'échec d'un chemin illisible reste une **valeur** rendue au boot, jamais un handshake à 3 h.
- **Les échéances survivent, et ce n'était pas acquis.** `session.New` prend un `io.ReadWriteCloser`
  et découvre le support des échéances par **assertion de type**
  (`internal/smpp/session/dispatch.go:18-20`). Un habillage maison sans `SetReadDeadline` aurait
  désarmé le drop d'inactivité **et** l'écriture bornée de l'`unbind`, sans erreur de compilation ni
  ligne de journal. `*tls.Conn` expose les deux : c'est la raison de prendre `tls.NewListener` du
  standard plutôt que d'écrire le wrapper.
- **Le handshake hérite de la garde anti-slowloris.** `armReadDeadline` court avant **chaque**
  `ReadPDU`, donc avant le premier : un pair qui ouvre une socket et n'envoie jamais de ClientHello
  tombe à `SMPP_IDLE_TIMEOUT`. C'est le miroir exact du piège h2 de 300c — la deadline armée ailleurs
  que là où on la cherche — et ici elle est déjà armée. Rien à ajouter, mais il fallait le vérifier
  avant de ne rien ajouter.
- **Sortant : un seul dial**, `internal/connectorpool/bind.go:94`, sans aucun habillage de la
  connexion ; le codec prend `io.Reader`/`io.Writer`, donc un `*tls.Conn` s'y glisse inchangé.
- **`TLS_ENABLED` est déjà pris sur `connector-pool-svc`** : 300b en a fait l'interrupteur mTLS **du
  pod** (ses clients gRPC vers `billing-svc` et `content-key-svc`). Le lien SMSC est un autre pair et
  une autre décision ; réutiliser le nom aurait lié deux bascules que rien ne lie.
- **La configuration du bind sortant vient de l'environnement**, pas de la base : `connectorEnv`
  (`cmd/connector-pool-svc/main.go:36`) miroite onze colonnes de `smsc_connectors` avec la même note
  (« M3+ sources these from the control plane »). `tls_enabled` et `tls_config_json` existent en base
  depuis la migration initiale et sont servies par l'Admin API, mais **aucune ligne du pool ne les
  lit** — comme `bind_timeout_ms` ou `throughput_limit_per_sec`. Le douzième champ suit le même
  chemin que les onze autres ; inventer une lecture de base pour lui seul créerait deux sources de
  vérité là où il y en a une.

#### Trois prémisses vérifiées dans les sources de Go, pas de mémoire

`crypto/tls/tls.go`, fonction `dial` :
- `netDialer.Timeout` enveloppe le `ctx` **avant** le connect (`:134-138`) et
  `conn.HandshakeContext(ctx)` le consomme (`:170`) : `CONNECTOR_DIAL_TIMEOUT` borne donc le TCP
  **et** le handshake. Avec `net.Dialer` seul, il ne bornait que le TCP.
- `ServerName` vide est déduit de l'hôte de l'adresse composée (`:162-166`), **sur un `Clone()`** :
  la `*tls.Config` que `tlsconf.ClientConfig()` rend est partagée entre les binds d'un pool et n'est
  pas polluée. C'est ce qui permet de ne pas écrire de `ServerName` du tout.
- Conséquence à écrire dans le README : le certificat du pair sortant doit porter l'hôte de
  `CONNECTOR_ADDR` en SAN — `localhost` pour un sidecar.

#### Entrant : public, pas mutuel

`PublicServerConfig(nil)` — plancher **1.2**, aucune ALPN, aucun certificat client. Le constructeur a
été écrit en 300c pour exactement ce cas (« A transport with no ALPN at all — SMPP — passes nil »).

Le mutuel est écarté, et ce n'est pas une facilité : un ESME est un **client**, déjà authentifié par
son `bind_transmitter` (system_id + mot de passe argon2id, step-026). Exiger un certificat de notre CA
supposerait d'en émettre un par client et de nommer chacun dans `TLS_ALLOWED_CLIENTS` — une offre de
PKI que ce dépôt ne vend pas, et un second facteur qui remplacerait le premier au lieu de s'y ajouter.
Même raison que pour l'API REST publique, mêmes clients : des intégrateurs qu'on ne contrôle pas.

#### Sortant : notre CA — arbitrage Fable du 2026-09-19

Le lien réutilise `tlsconf.ClientConfig()` : plancher 1.3, `RootCAs` = notre CA, et notre certificat
de pod présenté — donc **mTLS**, ce que la spec appelle « mTLS optionnel pour les liens SMSC ». Zéro
variable d'identité neuve : le pod monte déjà ses trois fichiers.

Écarté : des racines **système** avec un plancher 1.2 et un `CONNECTOR_TLS_CA_FILE`. Cela joindrait un
opérateur réel — qui n'existe pas —, au prix d'une variable pour une valeur qui ne varie pas, et
sans qu'aucun test puisse exister (le simulateur du dépôt est signé par une CA jetable).

**Ce que ce choix rend impossible, et qui part en fiche de dette :** joindre **directement depuis le
pod** un SMSC signé par une CA publique ou par la PKI de l'opérateur, ou qui n'accepte que TLS 1.2.
Ce cas passe par un sidecar — la forme de production que le code nomme déjà
(`cmd/connector-pool-svc/main.go:81`). `tls_config_json` est la colonne prévue pour le traiter un
jour ; personne ne la lit, et cette PR ne commence pas à la lire.

**La question du barreau 1, posée par l'arbitre et qui mérite sa réponse :** si la forme de production
est le sidecar, le pod n'a besoin d'aucun TLS sortant. Elle est écartée parce que le sidecar est *une*
forme et non la seule, que le périmètre de la step la demande explicitement, et que la colonne
`tls_enabled` attend un lecteur depuis la migration initiale. Elle reste vraie pour le **plancher** :
rien de plus que le drapeau n'est livré.

#### Pas de paquet pour la projection `config.TLS` → `tlsconf.Files`

300c annonçait qu'« à la quatrième copie l'extraction se poserait d'elle-même ». Elle se pose, et la
réponse ne change pas : quatre littéraux de cinq lignes coûtent moins qu'un paquet et son test.
`tlsconf` ne peut pas l'héberger sans importer `internal/config`, ce que son en-tête refuse (« It
knows nothing about who writes them ») ; `internal/grpctls` le refuse aussi, pour ne pas imposer gRPC
à qui sert du SMPP — son propre en-tête le dit déjà de la couche SMPP.

#### Les deux bascules, et la garde qui les relie

`SMPP_TLS` n'existe pas : l'entrant suit `TLS_ENABLED`, qui gouverne le pod. Le sortant prend
`CONNECTOR_TLS_ENABLED`, faux par défaut, parce qu'il décrit un **pair** et non ce pod.

`CONNECTOR_TLS_ENABLED=true` avec `TLS_ENABLED=false` est refusé au démarrage, dans
`validateConnectorEnv` : sans lui, l'échec est un `stat` sur une chaîne vide, et le message accuse un
fichier absent au lieu de la variable qui manque.

#### Preuves, et la mutation qui fait tomber chacune

| # | Preuve | Mutation |
|---|---|---|
| 1 | entrant : un ESME **sans** certificat, plafonné à TLS 1.2, bind et soumet | `ClientAuth: RequireAndVerifyClientCert` ; plancher 1.3 |
| 2 | entrant : un ESME **en clair** sur un listener TLS échoue | servir `lis` sans l'envelopper |
| 3 | entrant : l'ordre PROXY→TLS — un en-tête PROXY suivi d'un ClientHello donne l'IP réelle **et** un bind | inverser les deux décorations |
| 4 | entrant : `TLS_ENABLED=false` ⇒ `Options.TLSConfig == nil`, l'ESME en clair passe | poser la config inconditionnellement |
| 5 | entrant : un chemin illisible ⇒ erreur de boot **rendue** par `newListener` | journaliser au lieu de rendre |
| 6 | sortant : le bind s'établit vers un faux SMSC **en TLS**, et échoue en clair contre lui | `cfg.TLS` ignoré dans `dialAndBind` |
| 7 | sortant : `CONNECTOR_TLS_ENABLED=false` ⇒ dial en clair | composer en TLS inconditionnellement |
| 8 | sortant : `CONNECTOR_TLS_ENABLED=true` + `TLS_ENABLED=false` ⇒ refus au démarrage nommant la variable | supprimer la garde |

La preuve 3 est la seule qui prouve l'**ordre** ; 1 et 2 laisseraient les deux décorations inversées
passer, l'en-tête PROXY étant simplement absent de leurs connexions.

#### Ce que 300d ne fait pas

Le TLS client vers Kafka et ClickHouse — **step-305, créée par cette PR**, ce qui paie la dette qui
n'avait de fiche que pour dire que la step manquait. Aucune lecture de `tls_config_json`. Aucun test
d'intégration du simulateur en TLS : son image sait le faire (`tls: enabled:` dans sa configuration)
mais il faudrait lui monter des certificats dans le conteneur, et le faux SMSC en processus prouve la
même chose sans Docker.

## Tests (écrits dans la même PR)
- Handshake TLS/mTLS réussi ; un client sans cert client est rejeté sur les endpoints mTLS.
- SMPP-TLS : bind chiffré établi (faux SMSC/simulateur).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] TLS/SMPP-TLS/mTLS activables par config ; aucun secret en dur

## Hors périmètre
Auth opérateur réelle (OIDC) → step-310. Manifests k8s → step-270.
