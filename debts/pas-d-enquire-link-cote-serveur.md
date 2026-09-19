# Pas de keep-alive `enquire_link` côté serveur : un pair mort tient son quota 60 s

> **Statut :** OUVERTE · **Nature :** produit
> **Née de :** le code lui-même (« deferred », sans numéro) · **Portée par :** —

`smpp-server-svc` ne sonde jamais ses ESME : un `IdleTimeout` de 60 s « **stands in for the
enquire_link keep-alive (deferred)** », aligné sur le TTL du registre. L'asymétrie est réelle et
vérifiable — le pool **sortant**, lui, implémente bien `enquire_link`.

**Ce qu'il en coûte.** Une connexion TCP à demi-ouverte n'est détectée qu'au bout de 60 s, pendant
lesquelles le jeton de session reste au registre et consomme le quota `max_sessions` du client — qui
se voit refuser un rebind parfaitement légitime. C'est l'invariant (d) vécu à l'envers par le client.

**À quoi on reconnaîtra qu'il faut la payer.** Un client qui rapporte des refus de bind après une
coupure réseau. C'est la dette la plus visible de la liste pour un ESME, et elle n'a **aucun numéro
de step** aux deux endroits où elle est avouée.

Sources : `internal/smpp/session/session.go:73` · `internal/config/config.go:568`
