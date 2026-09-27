# `deploy/test/` — environnement de test k3s

## 1. Ce que c'est

Un environnement de **test**, pas la production : un seul nœud k3s sur un VPS, dimensionné pour
prouver que le pipeline tourne, pas pour tenir 8 000 SMS/s. C'est un overlay kustomize qui **rejoue**
`deploy/k8s/` sans le modifier — patches (proxy CIDRs, admin token, réplicas, HPA), Jobs par phase,
dépendances (`deps/`), Ingress Traefik. `make test-env` rend l'overlay avec un tag fictif et vérifie
ce qui ne se verrait qu'au déploiement (kubeconform + les invariants propres à cet overlay :
`deploy/test/check.sh`).

## 2. Prérequis

- VPS Ubuntu 24.04, **8 vCPU / 16 Go minimum** (Contabo ou équivalent).
- Accès root par clé SSH déjà installé sur le VPS (sinon `install.sh` désactive l'authentification par
  mot de passe et vous enferme dehors).
- DNS Cloudflare, deux enregistrements A vers l'IP du VPS :
  - `api.test` — **proxifié** (nuage orange), SSL/TLS en mode **Full (strict)**.
  - `smpp.test` — **DNS only** (nuage gris) : SMPP n'est pas du HTTP, Cloudflare ne peut pas le
    proxifier.
- Un certificat **Origin CA** Cloudflare pour `api.test.manouman.com`, téléchargé en `origin.crt` /
  `origin.key` (Cloudflare → SSL/TLS → Origin Server).
- L'image `ghcr.io/martialanouman/go-smsc-simulator:v0.7.0` publiée en `linux/amd64` par la CI de ce
  dépôt-là (`deps/smsc-simulator.yaml` la référence telle quelle).

## 3. Préparer l'hôte

```bash
ssh-keygen -t ed25519 -f cd-key -N ''
scp deploy/test/host/install.sh deploy/test/host/gateway-deploy root@IP:/root/
ssh root@IP bash /root/install.sh "$(cat cd-key.pub)"
```

`install.sh` pose ufw (SSH + les réseaux pods/Services de k3s), installe k3s, crée le namespace
`gateway` (étiqueté `pod-security.kubernetes.io/enforce=baseline` : une clé de CD fuitée ne peut pas y
faire tourner un pod privilégié ou hostPath/hostNetwork), un utilisateur `deploy` avec un kubeconfig
**restreint à ce namespace** (`/home/deploy/.kube/config`), et une `authorized_keys` à **commande
forcée** : la clé de CD ne peut exécuter que `/usr/local/bin/gateway-deploy` (`gateway-deploy` copié à
l'étape précédente), rien d'autre. Notez la dernière ligne affichée — l'empreinte de l'hôte, nécessaire
à l'étape suivante.

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

## 6. Premier déploiement

Le premier push de ce dépôt crée les paquets GHCR (`go-gateway/*` et `go-smsc-simulator`) **privés** :
k3s ne peut pas encore les tirer. Avant le tout premier déploiement, dans l'ordre :

1. Lancer une fois le workflow (étape ci-dessous) : il pousse les images puis le déploiement échoue en
   `ImagePullBackOff` — c'est attendu.
2. Sur GitHub, basculer chaque paquet (`go-gateway/*` et `go-smsc-simulator`) en **public**. Alternative
   sans rendre les paquets publics : §9 (`registries.yaml` avec un PAT `read:packages`), à poser dès
   l'installation de l'hôte.
3. Relancer le workflow avec le même SHA — il retrouve les images déjà poussées.

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
la vérification du nom échoue. Les identifiants de bind sont dans
`~/.config/go-gateway-test/bind-credentials`.

**Exposition réseau** : `smpp-server-svc` (2775) est un `Service` `LoadBalancer` — le ServiceLB de k3s
publie les ports qu'il expose par des règles iptables posées **avant** la chaîne ufw, donc aucune
règle ufw n'est nécessaire pour 2775, ni pour 80/443 (Traefik, bundlé avec k3s, exposé de la même
façon). `install.sh` n'ouvre que 22/tcp et les réseaux internes du cluster.

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

Observabilité (collecteur OTel, alerting), sauvegardes, mesure de charge (step-280), seed de données
et preuve bout-en-bout (step-275). Cet environnement prouve que le pipeline se déploie et répond, pas
qu'il tient la charge ni qu'il survit à une panne.
