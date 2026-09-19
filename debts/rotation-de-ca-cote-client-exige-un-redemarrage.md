# Une rotation de CA exige un redémarrage de tous les clients

> **Statut :** OUVERTE (acceptée) · **Nature :** technique
> **Née de :** step-300a, arbitrage du 2026-09-18 · **Portée par :** — (limite assumée, aucune step ne la paiera)

`GetClientCertificate` couvre le certificat feuille, mais `RootCAs` est lu une fois pour toutes :
« `crypto/tls` n'offre **aucun équivalent client** de `GetConfigForClient` ». Les deux contournements
ont été écartés pour la même raison — ni l'un ni l'autre ne re-vérifie les connexions **déjà
établies**, et une CA compromise en a. Un redémarrage est plus rapide (le drain est contractualisé) et
plus auditable.

**Ce qu'il en coûte.** Écrit : « son mode de panne est **silencieux** — CA tournée sans redémarrage,
les nouveaux handshakes échouent en `x509: unknown authority`, sans lien évident ». Mitigation
livrée : comparaison SHA-256 du `ca.crt` et un `WARN` qui nomme le redémarrage requis.

**À quoi on reconnaîtra qu'il faut la payer.** Si une rotation de CA **sans redémarrage** devient
exigée. L'escalade est alors nommée : une `credentials.TransportCredentials` côté gRPC seulement —
**jamais** `InsecureSkipVerify`.

Source : `internal/platform/tlsconf/tlsconf.go` (`ClientConfig`, `announceCAChange`)
