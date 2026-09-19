# HTTP/2 sur l'API publique est reporté, faute de `ReadTimeout`

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-300c (PR #200) · **Portée par :** — (à rapprocher de step-280)

La spec offre deux voies pour les 10 000+ connexions simultanées, « HTTP/2 **ou** keep-alive avec
pool », et c'est la seconde qui est implémentée. Annoncer `h2` supprimait la garde anti-slowloris de
la surface publique : le serveur HTTP/2 de Go ne lit jamais `ReadHeaderTimeout`, il arme la deadline
de ses flux depuis `ReadTimeout`, que ce dépôt ne pose pas. Report assumé : « HTTP/2 est un levier de
**débit**, il se paie d'un `ReadTimeout` à mesurer ; il n'a rien à faire dans une PR de chiffrement ».

**Ce qu'il en coûte.** Chaque client concurrent consomme une connexion TCP et un handshake TLS de
plus que nécessaire, à l'échelle visée par le NFR.

**À quoi on reconnaîtra qu'il faut la payer.** Une campagne qui mesure le coût par connexion —
step-280. **La step qui mesure n'est pas nommée dans la fiche d'origine.**

Source : `cmd/rest-api-svc/wiring.go` (`PublicServerConfig([]string{"http/1.1"})`)
