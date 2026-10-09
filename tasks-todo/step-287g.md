# step-287g — Les dépendances quittent l'hôte de la passerelle : second nœud k3s sur le VPC

> **Jalon :** M12 · **Statut :** EN COURS
> **Dépend de :** step-287f (PR1) · **Bloque :** step-287 (reprise de la campagne)
> Née du run 4 de step-287 (07/10/2026), demande humaine du 09/10/2026 (VPC Contabo, hôtes réinstallés).

## Pourquoi
Run 4 (`0397227`) : les 4 écrivains font passer la traversée de 269 à 348 `submit_sm`/s, mais l'admission
HTTP recule de 2 395 à 1 382 req/s. L'hôte unique n'a plus que 5 à 7 % de CPU libre : ClickHouse 1,6 cœur,
Postgres 1,3, Redpanda 0,45, et chaque gain d'un service se paie sur un autre. Tant que les dépendances
partagent l'hôte, aucune mesure ne départage un correctif.

Latence mesurée le 09/10/2026 entre les deux hôtes (200 pings) : réseau public, moyenne 8 à 9,5 ms ; VPC
(`eth1`, 10.0.0.0/22), p50 1,0 ms, p90 2 à 3 ms, p99 10 à 12 ms. Dans un même nœud : 0,08 ms.

## Design arrêté (09/10/2026)
- **Un second nœud k3s, agent, sur l'hôte de l'injecteur** (contabo75, 10.0.0.1). Le serveur reste la
  passerelle (contabo169, 10.0.0.2). Les deux prennent `--node-ip` et `--flannel-iface` sur l'interface du
  VPC : le trafic des pods (vxlan 8472/udp), du kubelet et de l'API ne passe jamais par l'IP publique.
- **Le nœud de dépendances est teinté** `gateway.test/role=deps:NoSchedule` et étiqueté
  `gateway.test/role=deps`. Postgres, Redpanda, ClickHouse et RustFS portent le `nodeSelector` et la
  tolérance, par un seul patch kustomize. Rien d'autre ne les porte : services, Jobs, Traefik, servicelb et
  le simulateur restent sur la passerelle sans être nommés.
- **Redis reste sur la passerelle.** Il est appelé plusieurs fois par message (débit, réserve, capture) en
  aller-retour unitaire : 1 ms de plus par appel se paie en série. Postgres (par lot), Redpanda (envois
  regroupés) et ClickHouse (écritures par lot) absorbent la milliseconde.
- **`nodeSelector` requis, pas une affinité préférée.** Une préférence laisserait le planificateur poser
  une dépendance sur la passerelle, et son volume `local-path` l'y figerait en silence : la mesure
  mentirait sans rien signaler. Un nœud absent rend le pod Pending, ce que le déploiement voit.
  L'environnement n'existe plus qu'à deux nœuds ; un seul nœud n'est plus pris en charge.
- **`install.sh` prend un rôle** : `server` (l'existant, plus `--node-ip`/`--flannel-iface`) et `agent`
  (paquets, pare-feu, durcissement sshd, k3s agent avec étiquette et teinte). Le jeton du serveur arrive par
  l'entrée standard, jamais en argument. L'interface du VPC passe en zone firewalld `trusted` sur les deux.
- **k6 partage l'hôte des dépendances** jusqu'à l'arrivée du troisième serveur. Il a coûté 13 % de CPU de
  l'injecteur au run 4 ; les relevés de chaque run donnent le CPU des deux hôtes.

## Definition of Done
- [x] `check.sh` : les 4 StatefulSets portent sélecteur et tolérance, Redis ni l'un ni l'autre (garde vue
      rouge avant le patch)
- [x] `install.sh server|agent`, README §3 à jour
- [ ] les deux hôtes réinstallés (fait le 09/10 : 2 nœuds Ready sur 10.0.0.0/22, teinte posée), premier déploiement et smoke verts, dépendances sur contabo75
- [ ] run 5 au protocole du run 4, CPU des deux hôtes relevé, journal de step-287 à jour
