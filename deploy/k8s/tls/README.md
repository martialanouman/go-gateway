# TLS — le contrat du `Secret`, et deux façons de le remplir

Ce répertoire ne contient **aucun manifeste**, volontairement : `make manifests` passe kubeconform sur
`deploy/k8s` en entier, et il ne sait pas valider une CRD cert-manager. Les exemples ci-dessous sont donc
à recopier, pas à appliquer depuis ici.

Le code ne connaît que **trois chemins de fichiers**. Qui les remplit — cert-manager, une PKI interne, un
agent Vault, un `kubectl` — ne le regarde pas.

## Le contrat

Un `Secret` par service, aux deux clés d'un `kubernetes.io/tls`, plus l'autorité — que ce type ne
connaît pas : `ca.crt` est un ajout de cert-manager, et la voie manuelle produit de toute façon un
`Secret` de type `Opaque`. Le code ne regarde que les chemins, pas le type.

| Clé | Contenu |
|---|---|
| `tls.crt` | le certificat du service |
| `tls.key` | sa clé privée |
| `ca.crt`  | l'autorité qui valide ses pairs |

**Ce que `TLS_ENABLED` couvre : le gRPC interne, les deux APIs HTTP et le port SMPP entrant.** Les
surfaces ne se ressemblent pas. Sont **publiques** — elles prouvent leur identité, plancher TLS 1.2,
et ne demandent aucun certificat : `rest-api-svc` et le port SMPP de `smpp-server-svc`, dont les
clients sont des intégrateurs et des ESME qu'on ne contrôle pas et qui s'authentifient autrement (clé
d'API, `bind_transmitter`). Sont **mutuelles**, plancher 1.3, et refusent un appelant sans certificat
de notre CA : l'Admin API et les quatre serveurs gRPC.

**Le bind SMPP sortant ne suit pas `TLS_ENABLED`**, parce qu'il décrit un **pair** et non ce pod :
il a son propre `CONNECTOR_TLS_ENABLED`, faux par défaut, qui miroite `smsc_connectors.tls_enabled`.
Posé à vrai, `connector-pool-svc` compose l'SMSC en **mTLS avec notre CA** — donc utilisable pour un
sidecar ou un SMSC dont nous tenons la PKI, pas pour un opérateur signé par une autorité publique
(voir `debts/ancre-de-confiance-par-connecteur.md`). Il exige `TLS_ENABLED=true`, sans quoi le pod
refuse de démarrer : sans identité montée, il n'aurait aucun certificat à présenter.

**Le certificat du pair sortant doit porter l'hôte de `CONNECTOR_ADDR` en SAN** — `localhost` pour un
sidecar. `crypto/tls` déduit le `ServerName` de l'adresse composée ; aucun code ici ne le force.

**Les `Deployment` montent déjà ce volume** (step-300b) : les trois chemins sont dans `configmap.yaml`,
identiques partout, et chaque service pose `TLS_ENABLED` à côté du volume qui le rend vrai. Il ne reste
donc à fournir que le `Secret`, nommé `<service>-tls`. La forme, pour mémoire :

```yaml
          env:
            - {name: TLS_ENABLED, value: "true"}
          volumeMounts:
            - {name: tls, mountPath: /etc/gateway/tls, readOnly: true}
      volumes:
        - name: tls
          secret:
            secretName: content-key-svc-tls
            defaultMode: 0444
```

**Un `Secret` manquant laisse le pod en `ContainerCreating`**, pas en `CrashLoopBackOff` : le kubelet ne
démarre pas un conteneur dont un volume ne se monte pas. C'est `kubectl describe pod` qui le dit, pas les
journaux du service.

`0444`, et pas `0400` : les fichiers d'un volume `Secret` appartiennent à l'uid 0 tant qu'aucun `fsGroup`
n'est posé, or les images tournent en `USER 65532` et `deploy/k8s` ne pose aucun `securityContext`. Avec
`0400`, le process prend un `EACCES` sur `tls.key` **au démarrage** — `tlsconf` charge les trois
fichiers avant de rendre sa configuration, donc c'est un `CrashLoopBackOff`, pas un handshake qui casse
plus tard. L'alternative est `0440` avec
`fsGroup: 65532` — un choix à faire le jour où ces manifests gagneront un `securityContext`.

**Jamais de `subPath`.** Le kubelet met à jour un volume de `Secret` par bascule atomique d'un lien
symbolique ; un montage en `subPath` ne suit pas. La rotation deviendrait silencieusement inopérante
jusqu'au prochain redémarrage — et comme les certificats se renouvellent tous les deux mois environ, la
panne arriverait longtemps après la faute.

**`TLS_ALLOWED_CLIENTS` restreint les appelants**, et les quatre serveurs gRPC le posent — chacun n'en a
qu'un ou deux. Vide, la variable admet tout porteur d'un certificat de la CA : cela prouve qu'un pair est
un de nos pods, jamais LEQUEL, et la frontière de confiance devient « tout ce qui obtient un certificat
dans le namespace ». La liste compare des **SAN DNS**, jamais un `CN`.

| service | appelants |
|---|---|
| `content-key-svc` | `router-svc`, `admin-api-svc` |
| `billing-svc` | `router-svc`, `connector-pool-svc` |
| `session-manager-svc` | `smpp-server-svc`, `mo-dlr-router-svc`, `admin-api-svc` |
| `smpp-server-svc` | `mo-dlr-router-svc` |

Les surfaces publiques n'y figurent pas, faute de pouvoir nommer qui que ce soit : `rest-api-svc` et le
port SMPP entrant ne demandent aucun certificat à leurs intégrateurs et à leurs ESME. `admin-api-svc`
exige le certificat mais ne nomme personne non plus — aucun pod de ce dépôt ne l'appelle, et son
autorisation réelle reste le bearer opérateur.

Ajouter un appelant à un de ces services, c'est ajouter son nom ici **avant** de déployer : sinon le
premier handshake est refusé, et le client ne lit qu'un « bad certificate » qui ne dit pas pourquoi.

**Aucun secret ne passe par l'environnement.** Une clé privée en variable d'environnement est lisible
dans `/proc`, et part avec tout ce qui journalise sa configuration au démarrage — ce que font les
services de ce dépôt.

## Avec cert-manager (recommandé)

Une fois pour le namespace : une racine auto-signée, puis l'autorité qui signe le reste.

```yaml
apiVersion: cert-manager.io/v1
kind: Issuer
metadata: {name: selfsigned, namespace: gateway}
spec: {selfSigned: {}}
---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata: {name: gateway-ca, namespace: gateway}
spec:
  isCA: true
  commonName: gateway-ca
  secretName: gateway-ca-tls
  issuerRef: {name: selfsigned, kind: Issuer}
---
apiVersion: cert-manager.io/v1
kind: Issuer
metadata: {name: gateway-ca, namespace: gateway}
spec: {ca: {secretName: gateway-ca-tls}}
```

Puis un `Certificate` par service :

```yaml
apiVersion: cert-manager.io/v1
kind: Certificate
metadata: {name: content-key-svc-tls, namespace: gateway}
spec:
  secretName: content-key-svc-tls          # le secretName que monte le Deployment
  dnsNames: [content-key-svc, content-key-svc.gateway.svc]
  usages: [digital signature, server auth, client auth]
  issuerRef: {name: gateway-ca, kind: Issuer}
```

`server auth` **et** `client auth`, parce que la plupart des services sont les deux :
`smpp-server-svc` sert son `SessionRegistry` et appelle `session-manager-svc`. Ce sont **ces deux-là** que
la vérification de Go contrôle (`checkChainForKeyUsage`) ; `digital signature` est ajouté parce que
`usages` **remplace** le défaut de cert-manager au lieu de s'y ajouter, et qu'une pile TLS autre que Go
peut, elle, regarder le Key Usage. `crypto/x509` ne le regarde pas : « KeyUsage status flags are
ignored ».

cert-manager renouvelle aux deux tiers de la durée de vie, le kubelet réécrit les fichiers, et le process
relit au handshake suivant. Rien à redémarrer.

**cert-manager n'est pas déployé par ce dépôt**, au même titre que Postgres, Kafka, le collecteur OTel ou
l'Ingress : c'est un opérateur cluster-wide, avec ses CRD et son webhook d'admission. La checklist de
go-live (step-410) vérifie qu'un émetteur existe.

## Sans cert-manager

```sh
SVCS=billing-svc,content-key-svc,session-manager-svc,smpp-server-svc,mo-dlr-router-svc,admin-api-svc,router-svc,connector-pool-svc,rest-api-svc

# Sur une seule ligne : une continuation « \ » suivie d'une ligne indentée coupe la liste en deux, et
# tlsgen sort 0 après n'avoir émis que la première moitié.
go run ./test/tlsgen -out .tls -ns gateway -services "$SVCS"
# .tls/ est ignoré par git : ce sont des clés privées.

# tr + read, et non « for svc in ${SVCS//,/ } » : zsh ne découpe pas une expansion non quotée, la
# boucle ne tournerait qu'une fois avec les huit noms collés.
echo "$SVCS" | tr ',' '\n' | while read -r svc; do
  kubectl -n gateway create secret generic "$svc-tls" \
    --from-file=tls.crt=".tls/$svc.crt" \
    --from-file=tls.key=".tls/$svc.key" \
    --from-file=ca.crt=.tls/ca.crt
done
```

**Les neuf, pas un.** Un `Secret` manquant ne se voit nulle part avant `kubectl describe pod`.

`create secret tls` ne prend que le couple certificat/clé, jamais un `ca.crt` : d'où la forme `generic`
avec les trois noms standard, pour que le `Deployment` se lise pareil dans les deux voies.

Ce que le générateur fait et qu'un `openssl` à la main oublie : poser les **SAN**. L'identité d'un
appelant est lue dans ses SAN DNS, jamais dans son `CN` — un certificat au bon `CN` et sans SAN est
refusé par un handshake qui ne dira pas pourquoi.

**Aucune rotation automatique par cette voie.** Les certificats vivent 90 jours ; un cluster qui dure
plus longtemps rejoue les deux commandes.

## Changer d'autorité

Un renouvellement de **certificat** ne demande rien. Un changement d'**autorité** demande un
`kubectl rollout restart` des clients : `crypto/tls` n'offre aucun moyen de rafraîchir les racines de
confiance d'un client déjà construit, et ce dépôt a choisi d'assumer la limite plutôt que de la
contourner — voir `tasks-done/step-300.md`. Le service journalise un avertissement nommant le
redémarrage dès qu'il voit son `ca.crt` changer sur disque, pour que la panne ne soit pas muette.
