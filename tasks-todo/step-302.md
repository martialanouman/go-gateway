# step-302 — La remise MO/DLR par pod n'a pas de nom DNS à joindre

> **Jalon :** Dette ouverte par step-300b · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-410 (go-live)

## Pourquoi cette fiche existe

step-300b devait faire vérifier, côté client, l'identité du pod joint par la voie retour. En
cartographiant ce dial, elle a trouvé plus bas que TLS : **l'adresse composée ne résout pas**. Le défaut
est antérieur à step-300 et indépendant d'elle — l'arbitrage de 300b épingle le nom vérifié sur le
`Deployment`, si bien que le correctif choisi ici ne touchera pas les certificats.

## Le constat

`mo-dlr-router-svc` remet un `deliver_sm` au pod `smpp-server-svc` qui détient le bind du client. Il
compose son adresse depuis un gabarit (`internal/config/config.go`) :

```
SMPP_POD_ADDR_TEMPLATE, défaut "%s.smpp-server-headless:7000"     # %s = pod_id
```

`pod_id` vaut `metadata.name` (`deploy/k8s/smpp-server-svc.yaml`, via `fieldRef`), et
`smpp-server-headless` est bien un `Service` de `clusterIP: None` dans le même fichier. Il manque
pourtant l'enregistrement A.

**Un nom DNS par pod derrière un service headless n'existe que si le pod porte `spec.hostname`**, et un
`spec.subdomain` égal au nom du service headless. Le contrôleur d'endpoints ne recopie dans
`EndpointSlice` que `spec.hostname` ; il ne le déduit pas de `metadata.name`. Or `spec.hostname` est un
champ du *template* de pod : un `Deployment` ne peut y mettre qu'**une seule chaîne**, identique pour
toutes ses répliques — ce qui ne donne pas un nom par pod, et casserait l'unicité si on essayait. Seul
un `StatefulSet` fabrique ces noms, un par pod, parce que c'est le contrôleur qui les pose.

`smpp-server-svc` est un `Deployment`. **Chaque `Deliver` échoue donc sur la résolution du nom**, avant
tout handshake, et l'appelant le lit comme un pod injoignable : `PodClients.Deliver` traduit l'échec de
dial en `codes.Unavailable`, que `deliverer.go` range avec « le bind n'est plus là » et qui arrête la
marche **sans erreur**, pour basculer sur le webhook puis sur la dead-letter (step-048).

**Rien n'est perdu, et c'est le problème :** un client qui tient un bind SMPP actif reçoit ses MO par
webhook s'il en a déclaré un, et les voit s'empiler en dead-letter sinon — dans les deux cas sans qu'une
seule erreur nomme la cause. Le canal SMPP retour est éteint, silencieusement.

Rien ne l'a signalé : les tests d'intégration de la remise par pod dialent un serveur local sur
`127.0.0.1`, jamais un nom de service.

## Ce qu'il faut trancher

Deux voies, et elles ne coûtent pas la même chose :

1. **`smpp-server-svc` devient un `StatefulSet`.** Les noms de pods deviennent stables
   (`smpp-server-svc-0`, `-1`, …) et résolvent naturellement sous le service headless. Le gabarit ne
   bouge pas. Bénéfice second : le registre de sessions garde des `pod_id` stables entre redémarrages,
   ce qui rend la trace d'un bind lisible. Coût : un `StatefulSet` déploie ses répliques en série par
   défaut (`podManagementPolicy: OrderedReady`) — à passer en `Parallel`, sans quoi le temps de
   déploiement grandit avec le nombre de pods —, et l'HPA, la PDB et le drain sont à relire sous ce
   contrôleur.
2. **`pod_id` devient l'adresse IP du pod** (`status.podIP`), gabarit `"%s:7000"`. Le DNS disparaît du
   chemin. Coût : le `pod_id` cesse d'être un identifiant stable — une IP est réattribuée —, or il est
   écrit dans le registre de sessions et sert à tracer un bind ; et l'autorité du dial devient une IP,
   forme sous laquelle **seul** l'épinglage de `ServerName` retenu par 300b vérifie quoi que ce soit.

La voie 1 corrige aussi la stabilité du `pod_id` ; la voie 2 la sacrifie. C'est ce que l'arbitrage doit
peser, pas la taille du diff.

## Definition of Done

- [ ] Une voie tranchée et écrite sous `## Design arrêté`, avec ce qu'elle coûte au drain et à l'HPA.
- [ ] Un test qui échouerait sur la configuration actuelle — la remise par pod exercée contre le nom
      **que le gabarit compose**, et non contre `127.0.0.1`.
- [ ] `internal/deploy` tient le lien : le gabarit de `SMPP_POD_ADDR_TEMPLATE` et ce que les manifests
      rendent résoluble ne peuvent plus diverger en silence.
- [ ] **Si la voie 1 est retenue, la garde doit suivre le changement de `kind`.** `inspect`
      (`internal/deploy/manifests_test.go`) ne connaît que `Deployment`, `Job`, `Service`, `ConfigMap` et
      les autres kinds déjà présents : un `StatefulSet` échapperait d'un coup à TOUTES les règles —
      sondes, période de grâce, image, variables, `no-subpath`. La bascule ne serait pas muette
      (`deployment-per-service` crierait qu'un binaire à superviseur n'a plus de `Deployment`), mais le
      message désignerait le mauvais problème. `spec.initContainers` est un angle mort du même ordre, et
      il l'est déjà : `deploy.PodSpec` ne le décode pas, donc un `subPath` y passerait.
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · `make manifests` verts

## Hors périmètre

L'identité TLS du pod joint : tranchée en step-300b, et volontairement indépendante de l'adressage —
c'est le `Deployment` (ou le `StatefulSet`) qui est vérifié, jamais le pod. Une identité **par pod**
demanderait des certificats par pod, donc une infrastructure que ce dépôt ne déploie pas.
