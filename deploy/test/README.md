# `deploy/test/` — environnement de test k3s

## 1. Ce que c'est

Un environnement de **test**, pas la production : un seul nœud k3s sur un VPS, dimensionné pour
prouver que le pipeline tourne, pas pour tenir 8 000 SMS/s. C'est un overlay kustomize qui **rejoue**
`deploy/k8s/` sans le modifier — patches (proxy CIDRs, admin token, réplicas, HPA), Jobs par phase,
dépendances (`deps/`), Ingress Traefik. `make test-env` rend l'overlay avec un tag fictif et vérifie
ce qui ne se verrait qu'au déploiement (kubeconform + les invariants propres à cet overlay :
`deploy/test/check.sh`). À chaque déploiement, deux Jobs bornent la preuve : `test-seed` peuple un
compte SMPP `smoke` et sa route, puis `smoke` envoie un SMS de bout en bout par ce compte — un `smoke`
en échec fait échouer le workflow **Deploy test**.

## 2. Prérequis

- VPS Rocky Linux 10, **8 vCPU / 16 Go minimum** (Contabo ou équivalent), SELinux laissé en `Enforcing`.
- Accès root par clé SSH déjà installé sur le VPS (sinon `install.sh` désactive l'authentification par
  mot de passe et vous enferme dehors).
- DNS Cloudflare, deux enregistrements A vers l'IP du VPS :
  - `api-test` — **proxifié** (nuage orange), SSL/TLS en **Full (strict)** par une Configuration Rule
    (Rules → Configuration Rules : Hostname equals `api-test.manouman.com` → SSL Full (strict)) : le
    mode de la zone s'applique à tous ses hôtes, et le passer en strict coupe ceux dont l'origine n'a
    pas de certificat valide.
    Un seul niveau de sous-domaine : le certificat Universal SSL de Cloudflare ne couvre que
    `*.manouman.com`, et un `api.test.manouman.com` échoue à la poignée de main TLS dès la bordure.
  - `smpp.test` — **DNS only** (nuage gris) : SMPP n'est pas du HTTP, Cloudflare ne peut pas le
    proxifier.
- Un certificat **Origin CA** Cloudflare pour `api-test.manouman.com`. Générer la clé et la CSR sur le
  poste, pour que la clé ne quitte jamais la machine, puis coller la CSR dans Cloudflare → SSL/TLS →
  Origin Server → « Use my private key and CSR » et enregistrer le certificat en `origin.crt` :
  `openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -keyout origin.key
  -subj /CN=api-test.manouman.com -addext subjectAltName=DNS:api-test.manouman.com -out origin.csr`
- L'image `ghcr.io/martialanouman/go-smsc-simulator:v0.9.1` publiée en `linux/amd64` par la CI de ce
  dépôt-là (`deps/smsc-simulator.yaml` la référence telle quelle).

## 3. Préparer l'hôte

```bash
ssh-keygen -t ed25519 -f cd-key -N ''
scp deploy/test/host/install.sh deploy/test/host/gateway-deploy root@IP:/root/
ssh root@IP bash /root/install.sh "$(cat cd-key.pub)"
```

`install.sh` active firewalld (SSH, 80/443, 2775 et les réseaux pods/Services de k3s ; cockpit fermé), installe k3s, crée le namespace
`gateway` (étiqueté `pod-security.kubernetes.io/enforce=baseline` : une clé de CD fuitée ne peut pas y
faire tourner un pod privilégié ou hostPath/hostNetwork), un utilisateur `deploy` avec un kubeconfig
**restreint à ce namespace** (`/home/deploy/.kube/config`), et une `authorized_keys` à **commande
forcée** : la clé de CD ne peut exécuter que `/usr/local/bin/gateway-deploy` (`gateway-deploy` copié à
l'étape précédente), rien d'autre. Notez la dernière ligne affichée — l'empreinte de l'hôte, nécessaire
à l'étape suivante.

**Mettre à jour `gateway-deploy`.** L'hôte n'exécute que la copie posée par `install.sh` ; le workflow
ne la remplace jamais. Toute modification de `deploy/test/host/gateway-deploy` doit donc être réinstallée
**avant** le merge qui la porte (le déploiement de ce merge tournerait sinon avec l'ancienne) :

```bash
scp deploy/test/host/gateway-deploy root@IP:/tmp/gateway-deploy \
  && ssh root@IP install -m 755 /tmp/gateway-deploy /usr/local/bin/gateway-deploy
```

## 4. Secrets GitHub (environnement `test`)

Dans l'environnement GitHub `test` du dépôt :

- `TEST_SSH_KEY` = le contenu de `cd-key` (la clé **privée**).
- `TEST_SSH_HOST` = l'IP du VPS.
- `TEST_SSH_KNOWN_HOSTS` = la ligne d'empreinte affichée par `install.sh`.

Puis `shred -u cd-key` : la clé privée ne doit plus exister sur le poste de l'exploitant une fois
copiée dans GitHub.

## 5. Secrets du cluster

```bash
deploy/test/bootstrap-secrets.sh --host root@IP --origin-cert origin.crt --origin-key origin.key
```

Une seule fois : le script refuse s'ils existent déjà (Postgres fige son mot de passe à l'initdb, un
second tirage le désynchronise). Pour tout régénérer — perte de données, c'est un environnement de
test — `kubectl delete namespace gateway`, recréer le namespace, relancer `bootstrap-secrets.sh` puis
le premier déploiement.

Pour remplacer le seul certificat Origin CA, sans toucher aux autres secrets :

```bash
kubectl -n gateway create secret tls api-origin-tls --cert=origin.crt --key=origin.key \
  --dry-run=client -o yaml | ssh root@IP kubectl apply -f -
```

Un environnement bootstrappé avant step-275 n'a pas encore `operator-tls` (`bootstrap-secrets.sh` le
crée désormais avec les autres). Le poser sans toucher aux autres secrets, depuis les fichiers déjà
déposés par le run précédent de ce script dans `~/.config/go-gateway-test/` — la clé ne transite
jamais dans les arguments de la commande sur l'hôte, seulement sur son entrée standard :

```bash
kubectl -n gateway create secret generic operator-tls --dry-run=client -o yaml \
  --from-file=tls.crt=$HOME/.config/go-gateway-test/operator.crt \
  --from-file=tls.key=$HOME/.config/go-gateway-test/operator.key \
  --from-file=ca.crt=$HOME/.config/go-gateway-test/ca.crt | ssh root@IP kubectl apply -f -
```

## 6. Premier déploiement

Le premier push de ce dépôt crée les paquets GHCR (`go-gateway/*` — treize paquets depuis step-275,
`test-env` compris bien qu'il ne passe pas par GoReleaser — et `go-smsc-simulator`) **privés** : k3s ne
peut pas encore les tirer. Avant le tout premier déploiement, dans l'ordre :

1. Lancer une fois le workflow (étape ci-dessous) : il pousse les images puis le déploiement échoue en
   `ImagePullBackOff`, Jobs `test-seed` et `smoke` compris — c'est attendu.
2. Sur GitHub, basculer chaque paquet (`go-gateway/*`, `test-env` compris, et `go-smsc-simulator`) en
   **public**. Alternative sans rendre les paquets publics : §9 (`registries.yaml` avec un PAT
   `read:packages`), à poser dès l'installation de l'hôte.
3. Relancer le workflow avec le même SHA — il retrouve les images déjà poussées.

Un environnement déjà déployé avant step-275 n'a jamais tiré `test-env` : ce paquet, créé privé par le
premier déploiement qui le pousse, doit passer en **public** (ou §9, `registries.yaml`) avant le
déploiement qui lance les Jobs `test-seed`/`smoke` — sinon `ImagePullBackOff`, et le workflow échoue.
Réinstaller aussi `gateway-deploy` (§3) avant ce merge.

GitHub → Actions → **Deploy test** → *Run workflow*, avec le SHA complet (40 caractères) de `main` en
entrée (`sha`).

## 7. Rollback

Le même geste, avec le SHA complet (40 caractères) d'un commit antérieur. Ça ne marche qu'entre commits
qui n'ont ajouté aucune migration : revenir à un commit antérieur à une migration fait échouer le Job
`migrate` (golang-migrate : « no migration found for version N »), qui bloque le déploiement avant
l'application. Dans ce cas, repartir d'un namespace neuf (étape 5) plutôt que de rollback.

## 8. Accès exploitant

**Admin API**, par un seul tunnel SSH — un terminal :

```bash
ssh -L 8081:127.0.0.1:8081 root@IP kubectl -n gateway port-forward svc/admin-api-svc 8081:8081
```

et dans un autre :

```bash
curl --cacert ~/.config/go-gateway-test/ca.crt \
  --cert ~/.config/go-gateway-test/operator.crt --key ~/.config/go-gateway-test/operator.key \
  --resolve admin-api-svc:8081:127.0.0.1 \
  -H "Authorization: Bearer $(cat ~/.config/go-gateway-test/admin-token)" \
  https://admin-api-svc:8081/v1/admin/customers
```

Les quatre fichiers viennent de `bootstrap-secrets.sh` (étape 5), déposés dans
`~/.config/go-gateway-test/` sur le poste où il a tourné.

**SMPP** : `smpp.test.manouman.com:2775`, en **TLS** sous la CA de la passerelle (pas Let's Encrypt —
c'est la même CA `tlsgen` que les Secrets `*-tls` internes). Le certificat porte le SAN
`smpp-server-svc`, donc le client SMPP doit poser `ServerName=smpp-server-svc` à la connexion, sinon
la vérification du nom échoue. `~/.config/go-gateway-test/bind-credentials` porte les identifiants
**passerelle → simulateur** (`CONNECTOR_SYSTEM_ID`/`CONNECTOR_PASSWORD`), pas un compte client. Pour un
bind client, utiliser le compte `smoke` et sa credential `smoke` (posés par le Job `test-seed`) ; le
secret s'obtient par `POST /v1/admin/smpp-accounts/{id}/credentials/{credId}/rotate` sur l'Admin API —
le Job `smoke` du déploiement suivant le re-rotate, donc le relever avant de redéployer.

**Exposition réseau** : firewalld n'ouvre que 22, 80, 443 et 2775 ; l'API k8s (6443), le kubelet
(10250) et VXLAN (8472/udp) restent fermés. `smpp-server-svc` (2775) et Traefik (80/443) sont publiés
par le ServiceLB de k3s.

## 9. Passer les paquets GHCR en privé

Une fois que le déploiement (étape 6) a prouvé que les images publiques se tirent, basculez-les en
privé :

1. Créer un PAT avec le scope `read:packages`.
2. Sur le VPS, `/etc/rancher/k3s/registries.yaml` :

   ```yaml
   configs:
     ghcr.io:
       auth:
         username: <votre login GitHub>
         password: <le PAT>
   ```

3. `systemctl restart k3s`.
4. **Alors seulement**, basculer la visibilité des paquets sur GitHub. Dans le mauvais ordre,
   `ImagePullBackOff` au prochain redéploiement : k3s ne connaîtrait pas encore les identifiants au
   moment où les paquets deviennent privés.

## 10. Ce qui n'y est pas

Observabilité (collecteur OTel, alerting), sauvegardes. Cet environnement prouve, via les Jobs
`test-seed`/`smoke`, qu'un message traverse le pipeline bout en bout, pas qu'il survit à une panne.
La charge s'y mesure (§11), mais **pas de façon représentative** : un nœud de 8 vCPU porte tout, et
le verdict NFR appartient à step-409.

## 11. Campagne de charge (step-280)

`deploy/test-load/run.sh`, depuis le poste de l'exploitant (accès root SSH). `VERSION` est le tag
déployé (`v0.0.1-sha-<12 hex>`). **Aucun merge sur `main` pendant une campagne** : le déploiement
suivant ramène l'overlay de test et ses réplicas uniques.

Le premier déploiement qui pousse l'image `smsc-ceiling` crée son paquet GHCR **privé** : le passer en
public (ou §9) avant `ceiling`, sinon le Job reste en `ImagePullBackOff`.

```bash
H=root@IP V=v0.0.1-sha-…
deploy/test-load/run.sh $H apply            # rest-api, router, pool : 2 réplicas figés
deploy/test-load/run.sh $H seed $V 0.2      # part portée 20 % ; refuse si une règle anti-spam
                                            # duplicate/velocity couvre le compte
deploy/test-load/run.sh $H ceiling $V       # plafond du simulateur, 10 → 80 binds
deploy/test-load/run.sh $H k6 sustained off 10m
deploy/test-load/run.sh $H k6 sustained on 10m
deploy/test-load/run.sh $H k6 peak off 10m
```

`seed-load` sème 24 clients `load-00`…`load-23` (facturation postpayée à plafond non bloquant, une
clé API chacun, tournée à chaque `seed`), active l'auto-reconnexion du connecteur et lève son
`bind_pool_size` (`seed-load.yaml`). Plusieurs clients parce que `mt.inbound` est partitionné par compte
et que chaque capture verrouille la ligne de solde de son client : un seul client mesure une partition
et une ligne, pas la passerelle.

**Un 202 ne prouve rien en aval** : `rest-api-svc` n'applique ni crédit ni anti-spam. Pour chaque run,
relever côte à côte les 202 de k6, les `submit_sm` servis par le simulateur, les CDR, le lag des
consumers (`rpk group describe`), `kubectl top pod` et `iostat` sur le nœud. Latence bout-en-bout :
`e2e-budget` par `port-forward` sur le port 9090 d'un pod `connector-pool-svc`, via le tunnel du §8.
