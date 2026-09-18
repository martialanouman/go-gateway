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

Le `Deployment` le monte en volume et ne reçoit que des chemins :

```yaml
          env:
            - {name: TLS_ENABLED, value: "true"}
            - {name: TLS_CERT_FILE, value: /etc/gateway/tls/tls.crt}
            - {name: TLS_KEY_FILE, value: /etc/gateway/tls/tls.key}
            - {name: TLS_CLIENT_CA_FILE, value: /etc/gateway/tls/ca.crt}
          volumeMounts:
            - {name: tls, mountPath: /etc/gateway/tls, readOnly: true}
      volumes:
        - name: tls
          secret:
            secretName: content-key-svc-tls
            defaultMode: 0444
```

`0444`, et pas `0400` : les fichiers d'un volume `Secret` appartiennent à l'uid 0 tant qu'aucun `fsGroup`
n'est posé, or les images tournent en `USER 65532` et `deploy/k8s` ne pose aucun `securityContext`. Avec
`0400`, le process prend un `EACCES` sur `tls.key` au premier handshake. L'alternative est `0440` avec
`fsGroup: 65532` — un choix à faire le jour où ces manifests gagneront un `securityContext`.

**Jamais de `subPath`.** Le kubelet met à jour un volume de `Secret` par bascule atomique d'un lien
symbolique ; un montage en `subPath` ne suit pas. La rotation deviendrait silencieusement inopérante
jusqu'au prochain redémarrage — et comme les certificats se renouvellent tous les deux mois environ, la
panne arriverait longtemps après la faute.

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
  usages: [digital signature, key encipherment, server auth, client auth]
  issuerRef: {name: gateway-ca, kind: Issuer}
```

`server auth` **et** `client auth`, parce que la plupart des services sont les deux :
`smpp-server-svc` sert son `SessionRegistry` et appelle `session-manager-svc`. Les deux premiers usages
sont là parce que `usages` **remplace** le défaut de cert-manager au lieu de s'y ajouter : sans eux, le
certificat n'aurait pas de `digitalSignature`, que `crypto/tls` exige.

cert-manager renouvelle aux deux tiers de la durée de vie, le kubelet réécrit les fichiers, et le process
relit au handshake suivant. Rien à redémarrer.

**cert-manager n'est pas déployé par ce dépôt**, au même titre que Postgres, Kafka, le collecteur OTel ou
l'Ingress : c'est un opérateur cluster-wide, avec ses CRD et son webhook d'admission. La checklist de
go-live (step-410) vérifie qu'un émetteur existe.

## Sans cert-manager

```
go run ./test/tlsgen -out .tls -ns gateway -services content-key-svc,router-svc,admin-api-svc
# .tls/ est ignoré par git : ce sont des clés privées.

kubectl -n gateway create secret generic content-key-svc-tls \
  --from-file=tls.crt=.tls/content-key-svc.crt \
  --from-file=tls.key=.tls/content-key-svc.key \
  --from-file=ca.crt=.tls/ca.crt
```

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
contourner — voir `tasks-todo/step-300.md`. Le service journalise un avertissement nommant le
redémarrage dès qu'il voit son `ca.crt` changer sur disque, pour que la panne ne soit pas muette.
