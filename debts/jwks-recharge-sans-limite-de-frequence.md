# Chaque jeton invérifiable recharge le JWKS, sans limite de fréquence

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-310 · **Portée par :** —

**Ce qu'on a fait à la place.** `auth.OIDCVerifier` délègue les clés à `oidc.RemoteKeySet` de go-oidc v3,
tel quel. Un jeton qu'aucune clé en cache ne vérifie déclenche un rechargement, même si son `kid` est
connu. `inflight` en dédoublonne un à la fois, mais rien n'espace deux rechargements.

**Pourquoi.** L'appelant de l'Admin API est authentifié par le mTLS de step-300 (`ClientCA` +
`AllowedClients`) avant qu'un jeton ne soit lu. Seul un client admis peut donc provoquer ces rechargements,
et un espacement propre demanderait de remplacer le `KeySet` de la bibliothèque.

**Ce qu'il en coûte si on ne la paie jamais.** Un client admis, compromis ou bogué, qui boucle sur un jeton
forgé garde un rechargement en cours en permanence. Si l'IdP le limite (429), chaque jeton dont le `kid`
vient de tourner reçoit 503 pendant la limite. Les jetons dont le `kid` est en cache ne sont pas touchés.

**À quoi on reconnaîtra qu'il faut la payer.** Des rafales de `operator token not judged` dans les logs
d'admin-api-svc sans panne de l'IdP, un 429 de l'IdP, ou l'admission au mTLS d'un appelant moins sûr que le
BFF.

Source : `internal/auth/oidc.go:31`
