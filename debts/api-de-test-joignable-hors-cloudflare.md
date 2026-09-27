# L'API REST de test est joignable sans passer par Cloudflare

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** l'environnement de test k3s (2026-09-27) · **Portée par :** —

**Ce qu'on a fait à la place.** `api.test.manouman.com` est proxifié par Cloudflare, mais le port 443
du VPS accepte toute source : Traefik le publie par le ServiceLB de k3s, en iptables, en amont de
`ufw` (`deploy/test/host/install.sh`). Qui connaît l'IP atteint l'API directement, sous un certificat
Origin CA qu'aucun navigateur ne reconnaît.

**Pourquoi.** Filtrer aux plages Cloudflare exige une règle iptables hors `ufw` ou une politique
réseau Traefik (`ipAllowList`), à tenir à jour avec les plages publiées ; disproportionné pour un
environnement de test sans données réelles.

**Ce qu'il en coûte si on ne la paie jamais.** L'API de test reste exposée aux scans directs, sans le
filtrage ni la limitation de débit de Cloudflare.

**À quoi on reconnaîtra qu'il faut la payer.** Au premier jeu de données réel ou client externe sur
l'environnement de test.
