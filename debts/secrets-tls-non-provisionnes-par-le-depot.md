# Rien ne provisionne les `Secret` TLS, et leur absence est muette

> **Statut :** OUVERTE · **Nature :** produit
> **Née de :** step-300 (`deploy/README.md:40`) · **Portée par :** —

Le dépôt livre le **contrat** du `Secret` et un générateur pour les clusters sans cert-manager, mais
pas le provisionnement : cert-manager est un opérateur cluster-wide, même frontière que Postgres,
Kafka ou l'Ingress. C'est défendable. Ce qui ne l'est pas, c'est le mode de panne.

**Ce qu'il en coûte.** Écrit : « un volume qui ne se monte pas laisse le pod en `ContainerCreating`
**sans limite de temps — sans une ligne dans les journaux du service, et sans jamais devenir un
`CrashLoopBackOff` qu'on remarquerait** ». S'y ajoutent deux autres murs nommés : les paquets GHCR
naissent privés, et le `nofile` du nœud est un prérequis que les manifests ne peuvent pas poser.

**À quoi on reconnaîtra qu'il faut la payer.** Au premier `kubectl apply` depuis un clone. La
distance entre « git clone » et « ça tourne » n'a aucune fiche.

Source : `deploy/README.md:40`
